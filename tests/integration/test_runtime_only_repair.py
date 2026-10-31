"""Offline protocol fixtures exercise the real request gate; no real model."""
from dataclasses import replace
import http.client
import json
from pathlib import Path
import time
from types import SimpleNamespace
from urllib.parse import urlsplit
import pytest
from maestro.backends.codex_session.autonomous_backend import StrictCodexBackend
from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy
from maestro.backends.codex_session.runtime_repair import OneShotRuntimeRepairMixin, candidate_relative_path
from maestro.backends.base import BackendUnavailable
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.config import RunConfig
from maestro.execution.budgets import BudgetStore, BudgetExceeded
from maestro.execution.grants import GrantAuthority, GrantError
from maestro.execution.sandbox import RunReceipt
from maestro.tasks.runtime_failure import RuntimeFailure,RuntimePrecheckFailure,classify_runtime_failure
from maestro.types import AgentState,Plan,stable_hash,canonical_json


class Response:
    status=200
    headers={'Content-Type':'text/event-stream'}
    def iter_bytes(self,deadline):
        yield b'data: '+json.dumps({'type':'response.completed','response':{'id':'offline-runtime-test',
            'status':'completed','output':[],'usage':{'input_tokens':3,'output_tokens':2}}}).encode()+b'\n\n'
    def close(self):pass


class Client:
    model='test-model'
    supports_phase_identity=True
    def __init__(self):self.seen=[];self.identities=[];self.repair_text='print(1)\n';self.fail=False;self.initial_files=None
    def run_phase(self,prompt,*,base_url,deadline,tools,output_schema,session_id,agent_id=None):
        assert tools is None and session_id is None
        self.seen.append(prompt)
        self.identities.append(agent_id)
        url=urlsplit(base_url);c=http.client.HTTPConnection(url.hostname,url.port,timeout=5)
        c.request('POST',url.path+'/responses',json.dumps({'model':self.model,'input':[],'tools':[]}),
            {'Authorization':'Bearer offline-fixture','Content-Type':'application/json'})
        r=c.getresponse();r.read();c.close();assert r.status==200
        if prompt.startswith('ONE INITIAL'):
            value={'files':self.initial_files if self.initial_files is not None else
                   [{'path':'run','content':'print(missing)\n','executable':True}]}
        else:
            assert 'PRIVATE_MEMORY_SENTINEL' not in prompt and 'PLAN_SENTINEL' not in prompt
            value={'edits':[{'path':'run','start_line':1,'end_line':1,'replacement':self.repair_text}]}
        text=json.dumps(value);return SimpleNamespace(text=text,response_hash=stable_hash(text),thread_id='never-resume')
    def close(self):pass


@pytest.fixture
def setup(tmp_path):
    seed=tmp_path/'seed';seed.mkdir();(seed/'run').write_text('print(0)\n')
    client=Client();store=BudgetStore(tmp_path/'requests.sqlite')
    def proxy(*a,**kw):return CodexQuotaProxy(*a,upstream=lambda *args:Response(),**kw)
    backend=StrictCodexBackend(client,store,WorkspaceStore(tmp_path/'work',tmp_path/'cas'),
        precheck=None,seed_solver=seed,candidate_runner=object(),image='',config=RunConfig(rounds=1),
        proxy_factory=proxy,simulation=True)
    backend.begin_episode('runtime-test');parent=backend.prepare_seed();backend.register_parent(parent['candidate_hash'],parent['candidate_path'])
    state=AgentState('a',parent['candidate_hash'],.5)
    plan=Plan('plan','version','a',state.parent_hash,'local_search','parameter_change','PLAN_SENTINEL','implement','hypothesis')
    ws=backend._workspace(state);(ws.private/'secret.md').write_text('PRIVATE_MEMORY_SENTINEL')
    authority=GrantAuthority()
    def grant(epoch):return authority.issue(episode_id=backend.episode_id,round_index=0,agent_id='a',
        plan_version_id=plan.version_id,parent_hash=plan.parent_hash,epoch=epoch,expires_at=time.time()+30,
            purpose='runtime_repair' if epoch else 'prebid_implementation',
            source_candidate_hash=(getattr(backend,'_one_shot_states',{}).get(
                (backend.episode_id,0,'a',plan.version_id),{}).get('candidate') if epoch else plan.parent_hash))
    yield backend,state,plan,client,authority,grant
    backend.close()


def evidence(backend,candidate):
    problem=RuntimeFailure(candidate['candidate_hash'],'NameError','unbound variable',(('run',1),))
    backend._precheck=lambda _:RuntimePrecheckFailure('runtime',problem)
    outcome=backend.precheck(candidate)
    return outcome.evidence


@pytest.mark.parametrize('ending',['\n','\r\n'])
@pytest.mark.parametrize('replacement',['    pass','    pass\n',''])
def test_runtime_line_patch_preserves_following_statement(ending,replacement):
    source=ending.join(['def main():','    missing','    return 1',''])
    files=[{'path':'run','content':source,'executable':True}]
    problem=RuntimeFailure('candidate','NameError','missing',(('run',2),))
    patch={'edits':[{'path':'run','start_line':2,'end_line':2,'replacement':replacement}]}
    operation=OneShotRuntimeRepairMixin._implementation_operations(patch,files,problem)[0]
    content=operation['content']
    compile(content,'run','exec')
    assert 'pass    return' not in content
    assert content.splitlines()[-1]=='    return 1'
    assert content.endswith(ending)


def test_runtime_line_patch_preserves_eof_without_adding_newline():
    files=[{'path':'run','content':'print(missing)','executable':True}]
    problem=RuntimeFailure('candidate','NameError','missing',(('run',1),))
    patch={'edits':[{'path':'run','start_line':1,'end_line':1,'replacement':'print(1)'}]}
    assert OneShotRuntimeRepairMixin._implementation_operations(patch,files,problem)[0]['content']=='print(1)'


def test_one_initial_request_then_fresh_runtime_patch(setup):
    b,s,p,c,authority,grant=setup;deadline=time.monotonic()+20
    candidate=b.generate(s,p,0,deadline=deadline,grant=grant(0),grant_authority=authority)
    assert len(c.seen)==1 and b.last_generation_usage['calls']==1
    assert 'return path "run"' in c.seen[0]
    problem=evidence(b,candidate)
    repaired=b.repair_runtime(s,p,candidate,problem,0,0,deadline,grant=grant(1),grant_authority=authority)
    assert (Path(repaired['candidate_path'])/'run').read_text()=='print(1)\n'
    assert len(c.seen)==2 and b.last_generation_usage['phase']=='RUNTIME_REPAIR'
    assert 'PLAN_SENTINEL' in c.seen[0] and 'PLAN_SENTINEL' not in c.seen[1]
    assert b.budget_store.usage('runtime-test:0:a:GENERATION:version')['calls']==1
    assert b.budget_store.usage('runtime-test:0:a:RUNTIME_REPAIR:version:0')['calls']==1
    with pytest.raises(GrantError):b.generate(s,p,0,deadline=deadline,grant=grant(0),grant_authority=authority)
    with pytest.raises(GrantError):b.generate(s,p,0,revision=1,deadline=deadline,grant=grant(1),grant_authority=authority)


def test_one_shot_calls_bounded_terminal_drain_after_native_exit(setup):
    b,s,p,c,authority,grant=setup
    original=c.run_phase;drained=[]
    def exited(*args,**kwargs):
        original(*args,**kwargs)
        raise BackendUnavailable('native process exited after terminal request')
    c.run_phase=exited
    b._drain_native_exit=lambda account,deadline:drained.append((account,deadline))
    with pytest.raises(BackendUnavailable):
        b.generate(s,p,0,deadline=time.monotonic()+10,grant=grant(0),grant_authority=authority)
    assert len(drained)==1 and drained[0][0]=='runtime-test:0:a:GENERATION:version'
    assert b.budget_store.usage(drained[0][0])['usage_complete'] is True
    assert b.budget_store.connection.execute('SELECT status FROM codex_phase_scopes WHERE account=?',
        (drained[0][0],)).fetchone()[0]=='COMPLETED'


def test_exactly_three_runtime_repairs_no_fourth(setup):
    b,s,p,c,a,g=setup;deadline=time.monotonic()+20
    candidate=b.generate(s,p,0,deadline=deadline,grant=g(0),grant_authority=a)
    for index in range(3):
        problem=evidence(b,candidate)
        candidate=b.repair_runtime(s,p,candidate,problem,0,index,deadline,grant=g(index+1),grant_authority=a)
    problem=evidence(b,candidate)
    with pytest.raises(BudgetExceeded):b.repair_runtime(s,p,candidate,problem,0,3,deadline,grant=g(4),grant_authority=a)
    assert len(c.seen)==4
    assert len(set(c.identities))==4 and all(i.startswith("runtime-repair-") for i in c.identities[1:])
    assert sum(b.budget_store.usage(f'runtime-test:0:a:RUNTIME_REPAIR:version:{i}')['calls'] for i in range(3))==3


def test_forged_or_unchecked_failure_does_not_dispatch(setup):
    b,s,p,c,a,g=setup;deadline=time.monotonic()+20
    candidate=b.generate(s,p,0,deadline=deadline,grant=g(0),grant_authority=a)
    problem=RuntimeFailure(candidate['candidate_hash'],'NameError','fake',(('run',1),))
    with pytest.raises(GrantError):b.repair_runtime(s,p,candidate,problem,0,0,deadline,grant=g(1),grant_authority=a)
    assert len(c.seen)==1


@pytest.mark.parametrize('path', ['solver/run', '/candidate/run', '../run', 'src/../../run',
    './run', 'src//run', 'src/', '', '.', 'private/notes', 'public/board.json',
    'history/index.json', 'outbox/plan.json', '.git/config', r'C:\run', r'src\run'])
def test_initial_output_rejects_wrong_namespace_and_noncanonical_paths(path):
    with pytest.raises(ValueError):
        candidate_relative_path(path)


def test_initial_output_allows_candidate_relative_subdirectories():
    operations=OneShotRuntimeRepairMixin._implementation_operations({'files':[
        {'path':'run','content':'#!/bin/sh\n','executable':True},
        {'path':'src/main.py','content':'print(1)\n','executable':False}]},[],None)
    assert [x['path'] for x in operations]==['solver/run','solver/src/main.py']


def test_bad_initial_path_does_not_write_retry_or_enable_repair(setup):
    b,s,p,c,a,g=setup;deadline=time.monotonic()+20
    c.initial_files=[{'path':'run','content':'SHOULD_NOT_BE_WRITTEN\n','executable':True},
        {'path':'solver/run','content':'WRONG_DIRECTORY\n','executable':True}]
    with pytest.raises(ValueError, match='candidate-relative'):
        b.generate(s,p,0,deadline=deadline,grant=g(0),grant_authority=a)
    workspace=b._workspace(s)
    assert (workspace.solver/'run').read_text()=='print(0)\n'
    assert not (workspace.solver/'solver').exists()
    assert len(c.seen)==1 and b.last_generation_usage['calls']==1
    with pytest.raises(GrantError):
        b.generate(s,p,0,deadline=deadline,grant=g(0),grant_authority=a)
    forged=RuntimeFailure('unaccepted','NameError','not executed',(('run',1),))
    with pytest.raises(GrantError):
        b.repair_runtime(s,p,{'candidate_hash':'unaccepted'},forged,0,0,deadline,
            grant=g(1),grant_authority=a)
    assert len(c.seen)==1


@pytest.mark.parametrize('kind,rc,reason,timeout',[
    ('SyntaxError',1,'COMPLETED',False),('ModuleNotFoundError',1,'COMPLETED',False),
    ('MemoryError',1,'COMPLETED',False),('NameError',-9,'CPU_LIMIT',True),
    ('NameError',-9,'OUTPUT_LIMIT',False),('NameError',137,'COMPLETED',False)])
def test_non_runtime_failures_are_not_repairable(kind,rc,reason,timeout):
    receipt=RunReceipt(rc,'',f'Traceback (most recent call last):\n  File "/candidate/run", line 1\n{kind}: bad',.1,.2,
        timed_out=timeout,diagnostics={'reason':reason,'aggregate_cgroup_cpu':True})
    assert classify_runtime_failure('hash',receipt) is None


def test_runtime_classifier_and_patch_scope():
    receipt=RunReceipt(1,'','Traceback (most recent call last):\n  File "/candidate/run", line 1\nNameError: bad',.1,.2,
        diagnostics={'reason':'COMPLETED','aggregate_cgroup_cpu':True})
    failure=classify_runtime_failure('hash',receipt);assert failure.exception_type=='NameError'
    files=[{'path':'run','content':'print(bad)\n','executable':True}]
    with pytest.raises(ValueError):OneShotRuntimeRepairMixin._implementation_operations(
        {'edits':[{'path':'new.py','start_line':1,'end_line':1,'replacement':'bad'}]},files,failure)


def test_generation_quota_is_tunable_without_enabling_extra_candidates(setup):
    b,s,p,c,a,g=setup
    b.config=replace(b.config,generation_calls=5)
    b.generation_calls=5
    b.generate(s,p,0,deadline=time.monotonic()+20,grant=g(0),grant_authority=a)
    snapshot=b.budget_store.snapshot('runtime-test:0:a:GENERATION:version')
    assert snapshot['max_calls']==5 and snapshot['calls']==1
    with pytest.raises(GrantError):
        b.generate(s,p,0,deadline=time.monotonic()+20,grant=g(0),grant_authority=a)
    assert len(c.seen)==1


@pytest.mark.parametrize('failure_kind,expected_repairs',[
    ('runtime',3),('invalid_solution',0),('format',0),('timeout',0),('infrastructure',0)])
def test_controller_only_retries_runtime_failures(failure_kind,expected_repairs):
    from contextlib import contextmanager
    from threading import RLock
    from maestro.loop import ExperimentEngine
    class Ledger:
        _lock=RLock()
        def __init__(self):self.usages=[];self.events=[]
        @contextmanager
        def transaction(self):yield self
        def execute(self,*a):return self
        def event(self,*a):self.events.append(a)
        def usage(self,*a,**kw):self.usages.append((a,kw))
        def record_candidate(self,*a):pass
    class Backend:
        def __init__(self):self.initial=0;self.repairs=0;self.last_generation_usage={}
        def activate_frozen_candidate(self,*a,**kw):pass
        def repair_runtime(self,*a,**kw):self.repairs+=1;self.last_generation_usage={'calls':1};return {'candidate_hash':'hash'}
        def precheck(self,candidate):
            if failure_kind=='runtime':return RuntimePrecheckFailure('runtime',RuntimeFailure('hash','NameError','bad',(('run',1),)))
            if failure_kind=='infrastructure':raise OSError('no executor')
            return False,failure_kind
    b=Backend();ledger=Ledger();cfg=RunConfig(execution_mode='real')
    # This fixture isolates repair classification; cooperative controller pause
    # is covered separately with a real ExperimentEngine/ledger.
    e=SimpleNamespace(config=cfg,backend=b,ledger=ledger,grants=GrantAuthority(),_check_pause=lambda:None, _register_source_candidate=lambda c:c)
    state=AgentState('a','parent',.5);plan=Plan('p','v','a','parent','local_search','parameter_change','x','x','x')
    assert ExperimentEngine._generate_once_runtime_only(e,'e',0,'a',state,plan,'WARM',time.monotonic()+30,initial_candidate={'candidate_hash':'hash'}) is None
    assert b.initial==0 and b.repairs==expected_repairs
    assert sum(args[3]=='runtime_repair' for args,kw in ledger.usages)==expected_repairs


def test_successful_or_nonruntime_check_revokes_old_evidence(setup):
    b,s,p,c,a,g=setup;deadline=time.monotonic()+20
    candidate=b.generate(s,p,0,deadline=deadline,grant=g(0),grant_authority=a)
    problem=evidence(b,candidate)
    b._precheck=lambda _:(True,'pass')
    b.precheck(candidate)
    with pytest.raises(GrantError):
        b.repair_runtime(s,p,candidate,problem,0,0,deadline,grant=g(1),grant_authority=a)
    assert len(c.seen)==1


def test_four_identical_candidates_keep_separate_runtime_evidence(setup):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    b,state,plan,c,authority,_=setup;barrier=Barrier(4);deadline=time.monotonic()+30
    b._precheck=lambda candidate:RuntimePrecheckFailure('runtime',RuntimeFailure(candidate['candidate_hash'],'NameError','bad',(('run',1),)))
    def worker(i):
        s=replace(state,agent_id=f'peer-{i}');p=replace(plan,agent_id=s.agent_id,version_id=f'v-{i}')
        def grant(epoch):return authority.issue(episode_id=b.episode_id,round_index=0,agent_id=s.agent_id,
            plan_version_id=p.version_id,parent_hash=p.parent_hash,epoch=epoch,expires_at=time.time()+25,
            purpose='runtime_repair' if epoch else 'prebid_implementation',
            source_candidate_hash=(b._one_shot_states[(b.episode_id,0,s.agent_id,p.version_id)]['candidate'] if epoch else p.parent_hash))
        candidate=b.generate(s,p,0,deadline=deadline,grant=grant(0),grant_authority=authority)
        problem=b.precheck(candidate).evidence
        barrier.wait(timeout=15)
        repaired=b.repair_runtime(s,p,candidate,problem,0,0,deadline,grant=grant(1),grant_authority=authority)
        assert (Path(repaired['candidate_path'])/'run').read_text()=='print(1)\n'
    with ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(worker,range(4)))
    assert len(c.seen)==8


@pytest.mark.parametrize('check_kind',['pass','invalid_first','runtime'])
@pytest.mark.parametrize('policy',['first_price_naive','ucb','uniform','adjusted_valuation'])
def test_prebid_four_implementations_then_frozen_auction_check_and_backfill(setup,tmp_path,check_kind,policy):
    """Injected authority + local protocol fixture; no real solver/model claim."""
    from threading import Barrier
    from maestro.loop import ExperimentEngine
    from maestro.agents.planner import mock_plan,PlanDecision
    from maestro.replay import verify_ledger
    b,s,p,c,authority,_=setup
    engine=ExperimentEngine(tmp_path/'authority-fixture',RunConfig(rounds=1))
    cfg=replace(engine.config,execution_mode='real',coral={'candidate_pipeline':'prebid_v2',
        'deepseek_transport_retry':{'warm_seed_fallback':True}})
    engine.config=b.config=cfg;engine.backend=b;engine.grants=authority
    e=b.episode_id
    states={f'agent-{i}':replace(s,agent_id=f'agent-{i}',history=[],lessons=[]) for i in range(1,5)}
    from decimal import Decimal
    from maestro.types import HistoryRecord
    for agent,state in states.items():
        state.history=[HistoryRecord('prior-'+agent,agent,.49,.5,.5,True,
            'local_search','parameter_change',Decimal('.25'))]
        state.ucb_n=1;state.ucb_total=Decimal('.25')
    seed={'candidate_hash':s.parent_hash,'candidate_path':str(b.seed_solver)}
    # prepare_seed freezes its source to CAS; use the registered canonical path.
    seed['candidate_path']=str(b._parents[s.parent_hash])
    engine.ledger.record_candidate(s.parent_hash,seed)
    engine.ledger.create_episode(e,policy,cfg,states)
    b.plan_with_receipt=lambda state,r,context,deadline:(
        PlanDecision('revise',mock_plan(state,r,0)),
        {'actual_requests':1,'input_tokens':0,'output_tokens':0},context)
    rendezvous=Barrier(4);original=c.run_phase
    if policy=='adjusted_valuation':
        from types import MethodType
        from maestro.backends.coral.valuation import ValuationBackendMixin
        b.value_with_receipt=MethodType(ValuationBackendMixin.value_with_receipt,b)
        b._dispatch_guard=b._assert_clear
        b._valuation_proxy=lambda account,deadline:CodexQuotaProxy(b.budget_store,account,c.model,
            deadline=deadline,max_calls=4,max_request_bytes=16*1024*1024,max_output_tokens=96000,upstream=lambda *args:Response())
    def run(prompt,**kwargs):
        if prompt.startswith('VALUATION_RO:'):
            c.seen.append(prompt)
            url=urlsplit(kwargs['base_url']);conn=http.client.HTTPConnection(url.hostname,url.port,timeout=5)
            conn.request('POST',url.path+'/responses',json.dumps({'model':c.model,'input':[]}),{'Authorization':'Bearer offline-fixture','Content-Type':'application/json'})
            response=conn.getresponse();response.read();conn.close();assert response.status==200
            tool=kwargs['tools']
            info=json.loads(tool('maestro_tool',{'action':'read','path':'public/contract.json'})['content'])
            tool('maestro_tool',{'action':'read','path':'solver/run'})
            tool('maestro_tool',{'action':'read','path':'public/trajectory.json'})
            p,curr=info['parent_score'],info['champion_score']
            value={'baseline_assessment':'underestimate','scenarios':[
                {'kind':'no_positive_reward','probability':0,'predicted_score':p},
                {'kind':'catchup','probability':0,'predicted_score':p},
                {'kind':'frontier','probability':1,'predicted_score':min(1,curr+.01)}],
                'evidence':[{'path':'solver/run','observation':'Inspected implementation'}],
                'rationale':'Offline protocol fixture, not a measured estimate','uncertainties':[]}
            return SimpleNamespace(text=json.dumps(value),response_hash=stable_hash(value))
        if prompt.startswith('ONE INITIAL'):
            assert engine.ledger.connection.execute('SELECT count(*) FROM holds').fetchone()[0]==0
            assert engine.ledger.connection.execute('SELECT count(*) FROM prechecks').fetchone()[0]==0
            assert engine.ledger.connection.execute('SELECT count(*) FROM round_submissions').fetchone()[0]==0
            rendezvous.wait(timeout=10)
            data=json.loads(prompt.split('\n',1)[1]);agent=data['plan']['agent_id']
            response=original(prompt,**kwargs)
            response.text=json.dumps({'files':[{'path':'run','content':f'print(missing)\n# {agent}\n','executable':True}]})
            return response
        return original(prompt,**kwargs)
    c.run_phase=run
    checked=[]
    def check(candidate):
        assert len([x for x in c.seen if x.startswith('ONE INITIAL')])==4
        assert engine.ledger.connection.execute("SELECT count(*) FROM events WHERE kind='PROPOSAL_FROZEN'").fetchone()[0]==4
        text=(Path(candidate['candidate_path'])/'run').read_text()
        checked.append(candidate['candidate_hash'])
        if check_kind=='invalid_first':
            with engine.ledger._lock:
                order=json.loads(engine.ledger.connection.execute('SELECT payload FROM priority_orders').fetchone()[0])['order']
            if f'# {order[0]}\n' in text:return False,'invalid solution'
        if check_kind=='runtime' and 'missing' in text:
            return RuntimePrecheckFailure('runtime',RuntimeFailure(candidate['candidate_hash'],
                'NameError','missing',(('run',1),),{'stdin':'p ds 2 1\n1 2\n'}))
        return True,'pass'
    b._precheck=check
    try:
        engine._round(e,0,states,policy)
        tickets=[json.loads(row[0]) for row in engine.ledger.connection.execute('SELECT payload FROM eval_tickets')]
        assert len(tickets)==2
        for ticket in tickets:
            assert ticket['repair_chain'][0]==ticket['proposal_hash']
            assert ticket['repair_chain'][-1]==ticket['candidate_hash']
            assert len(ticket['repair_chain'])==(2 if check_kind=='runtime' else 1)
        assert len([x for x in c.seen if x.startswith('ONE INITIAL')])==4
        assert len(c.seen)==(6 if check_kind=='runtime' else 4)+(4 if policy=='adjusted_valuation' else 0)
        assert len(checked)==({'pass':2,'invalid_first':3,'runtime':4}[check_kind])
        assert sum(len(state.history) for state in states.values())==6
        payments=[Decimal(row[0]) for row in engine.ledger.connection.execute('SELECT payment FROM final_allocations WHERE x=1')]
        assert all(v>0 if policy in ('first_price_naive','adjusted_valuation') else v==0 for v in payments)
        usage=engine.ledger.connection.execute("SELECT sum(llm_calls) FROM resource_usage WHERE phase='generation'").fetchone()[0]
        assert usage==4
        replay=verify_ledger(engine.root)
        assert replay['verified'] and type(replay['events']) is int
        canonical_json(replay)
        # A ticket cannot silently substitute another agent's pre-bid program.
        from maestro.execution.proposals import validate_proposal_binding
        ticket=dict(tickets[0]);ticket['proposal_hash']=tickets[1]['proposal_hash']
        sub=json.loads(engine.ledger.connection.execute('SELECT payload FROM round_submissions WHERE agent_id=?',
            (ticket['agent_id'],)).fetchone()[0])
        from maestro.types import AuthorizationError
        with pytest.raises(AuthorizationError):validate_proposal_binding(engine.ledger.connection,ticket,sub)
    finally:engine.close()


def test_frozen_candidate_rebinding_preserves_budget_deadline_and_rejects_tamper(setup):
    b,s,p,c,a,g=setup;deadline=time.monotonic()+20
    b.config=replace(b.config,coral={'candidate_pipeline':'prebid_v2'})
    grant=a.issue(episode_id=b.episode_id,round_index=0,agent_id=s.agent_id,
        plan_version_id=p.version_id,parent_hash=p.parent_hash,epoch=0,expires_at=time.time()+20,
        purpose='prebid_implementation',source_candidate_hash=p.parent_hash)
    candidate=b.generate(s,p,0,deadline=deadline,grant=grant,grant_authority=a);a.revoke(grant)
    account=f'{b.episode_id}:0:a:GENERATION:{p.version_id}';broker=b._brokers[account]
    before=broker.usage()
    check=a.issue(episode_id=b.episode_id,round_index=0,agent_id=s.agent_id,
        plan_version_id=p.version_id,parent_hash=p.parent_hash,epoch=0,expires_at=time.time()+20,
        purpose='postauction_check',source_candidate_hash=candidate['candidate_hash'])
    b.activate_frozen_candidate(s,p,candidate,0,deadline,grant=check,grant_authority=a)
    assert broker.usage()==before and broker.deadline==deadline
    assert len(c.seen)==1 and b.last_generation_usage['calls']==0
    with pytest.raises((ValueError,TimeoutError)):
        b.activate_frozen_candidate(s,p,candidate,0,deadline+10,grant=check,grant_authority=a)
    with pytest.raises(GrantError):
        b.activate_frozen_candidate(s,p,{**candidate,'candidate_hash':'forged'},0,deadline,
            grant=check,grant_authority=a)


def test_valuation_pause_resumes_frozen_candidate_check_without_new_tool_budget(setup):
    b,s,p,c,a,g=setup;deadline=time.monotonic()+20
    b.config=replace(b.config,coral={'candidate_pipeline':'prebid_v2'})
    initial=a.issue(episode_id=b.episode_id,round_index=0,agent_id=s.agent_id,
        plan_version_id=p.version_id,parent_hash=p.parent_hash,epoch=0,expires_at=time.time()+20,
        purpose='prebid_implementation',source_candidate_hash=p.parent_hash)
    candidate=b.generate(s,p,0,deadline=deadline,grant=initial,grant_authority=a);a.revoke(initial)
    account=f'{b.episode_id}:0:a:GENERATION:{p.version_id}'
    before=b._brokers[account].usage()
    started=time.monotonic()
    paused=b.pause_generation_brokers(0,{'a':p},{'a':candidate},started,deadline)
    with pytest.raises(Exception,match='expired|closed'):
        b._brokers[account].bind_grant(lambda: None)
    finished=time.monotonic();resumed=deadline+(finished-started)
    b.resume_generation_brokers(paused,finished,resumed)
    check=a.issue(episode_id=b.episode_id,round_index=0,agent_id=s.agent_id,
        plan_version_id=p.version_id,parent_hash=p.parent_hash,epoch=0,expires_at=time.time()+20,
        purpose='postauction_check',source_candidate_hash=candidate['candidate_hash'])
    b.activate_frozen_candidate(s,p,candidate,0,resumed,grant=check,grant_authority=a)
    assert b._brokers[account].deadline==resumed
    assert b._brokers[account].usage()==before
    with pytest.raises((ValueError,TimeoutError)):
        b.activate_frozen_candidate(s,p,candidate,0,resumed+1,grant=check,grant_authority=a)
    with pytest.raises(Exception,match='pause'):
        b.pause_generation_brokers(0,{'a':p},{'a':candidate},time.monotonic(),resumed)


def test_prebid_unknown_drains_peers_and_stops_before_any_bid_or_hold(tmp_path):
    from threading import Barrier
    from maestro.loop import ExperimentEngine
    from maestro.agents.planner import mock_plan
    from maestro.llm import InferenceRequestUnknown
    engine=ExperimentEngine(tmp_path,RunConfig(rounds=1))
    states={f'agent-{i}':AgentState(f'agent-{i}','parent',.5) for i in range(1,5)}
    engine.ledger.create_episode('stopped','uniform',engine.config,states)
    engine.ledger.open_round('stopped',0,{'states':engine._state_payload(states)})
    engine.ledger.set_round_phase('stopped',0,'IMPLEMENTATION')
    barrier=Barrier(4);returned=[]
    def implement(e,r,a,*args):
        barrier.wait(timeout=5);returned.append(a)
        if a=='agent-3':raise InferenceRequestUnknown('offline unresolved fixture',request_id='r',scope_id='s',context={})
        return None
    engine._implement_proposal=implement
    try:
        with pytest.raises(InferenceRequestUnknown):
            engine._implement_all('stopped',0,states,{a:mock_plan(s,0,0) for a,s in states.items()},
                'MAIN',time.monotonic()+5)
        assert len(returned)==4
        assert engine.ledger.connection.execute('SELECT status FROM rounds').fetchone()[0]=='IMPLEMENTATION_UNKNOWN'
        assert engine.ledger.connection.execute('SELECT status FROM episodes').fetchone()[0]=='INCOMPLETE'
        for table in ('holds','round_submissions','eval_tickets'):
            assert engine.ledger.connection.execute('SELECT count(*) FROM '+table).fetchone()[0]==0
        event=json.loads(engine.ledger.connection.execute("SELECT payload FROM events WHERE kind='IMPLEMENTATION_STOPPED'").fetchone()[0])
        assert event['unknown'] and 'InferenceRequestUnknown' in event['traceback']
    finally:engine.close()


def test_prebid_cleanup_grace_cannot_accept_a_late_proposal(tmp_path):
    from maestro.loop import ExperimentEngine
    from maestro.agents.planner import mock_plan
    from threading import Event
    engine=ExperimentEngine(tmp_path,RunConfig(rounds=1))
    states={'a':AgentState('a','parent',.5)};finished=Event()
    engine.ledger.create_episode('late','uniform',engine.config,states)
    engine.ledger.open_round('late',0,{'states':engine._state_payload(states)})
    engine.ledger.set_round_phase('late',0,'IMPLEMENTATION')
    def implement(*args):
        # Explicit local fixture: the callback returns a candidate only after
        # the shared deadline. The controller must discard it, not re-clock it.
        time.sleep(.1);finished.set();return {'candidate_hash':'late-source'}
    engine._implement_proposal=implement
    try:
        assert engine._implement_all('late',0,states,{'a':mock_plan(states['a'],0,0)},
            'MAIN',time.monotonic()+.03)=={}
        assert finished.is_set()
        assert engine.ledger.connection.execute("SELECT count(*) FROM events WHERE kind='PROPOSAL_FROZEN'").fetchone()[0]==0
        assert engine.ledger.connection.execute('SELECT count(*) FROM holds').fetchone()[0]==0
        event=json.loads(engine.ledger.connection.execute("SELECT payload FROM events WHERE kind='IMPLEMENTATION_WINDOW_EXPIRED'").fetchone()[0])
        assert event['accepted_proposals']==0 and not event['deadline_extended']
    finally:engine.close()


def test_structural_runtime_repair_restores_large_parent_function_and_moves_definition():
    helper='def swap_two_for_one(a, b):\n'+''.join(f'    x{i} = {i}\n' for i in range(125))+'    return list(a)\n'
    failed='result = swap_two_for_one([1], 0)\nprint(result)\n'
    files=[{'path':'run','content':failed,'executable':True}]
    problem=RuntimeFailure('h','NameError','missing',(('run',1),))
    edits={'edits':[{'path':'run','start_line':1,'end_line':1,'replacement':helper+failed.splitlines(keepends=True)[0]}]}
    ops=OneShotRuntimeRepairMixin._implementation_operations(edits,files,problem,
        parent_files=[{'path':'run','content':helper+failed}])
    compile(ops[0]['content'],'run','exec')
    assert helper in ops[0]['content'] and 'swap_two_for_one([1], 0)' in ops[0]['content']
    # A complete-file range can move an already-present definition before use.
    late=failed+helper
    ops=OneShotRuntimeRepairMixin._implementation_operations({'edits':[{'path':'run','start_line':1,
        'end_line':len(late.splitlines()),'replacement':helper+failed}]},
        [{'path':'run','content':late,'executable':True}],problem)
    compile(ops[0]['content'],'run','exec')


@pytest.mark.parametrize('replacement',[
    'swap_two_for_one = lambda a, b: list(a)\nresult = swap_two_for_one([1], 0)\nprint(result)\n',
    'result = [1]\nprint(result)\n',
    'def swap_two_for_one(a,b):\n    return list(a)\nresult = swap_two_for_one([1], 0)\nprint(result)\n'])
def test_structural_runtime_repair_rejects_noop_call_deletion_and_parent_stub(replacement):
    failed='result = swap_two_for_one([1], 0)\nprint(result)\n'
    parent='def swap_two_for_one(a,b):\n    result = actual_exchange(a,b)\n    return result\n'
    with pytest.raises(ValueError):
        OneShotRuntimeRepairMixin._implementation_operations({'edits':[{'path':'run','start_line':1,
            'end_line':2,'replacement':replacement}]},[{'path':'run','content':failed,'executable':True}],
            RuntimeFailure('h','NameError','missing',(('run',1),)),parent_files=[{'path':'run','content':parent}])


def test_runtime_repair_cannot_disable_python_solver_via_shell_launcher():
    files=[{'path':'run','content':'#!/bin/sh\nexec python3 /candidate/main.py\n','executable':True}]
    with pytest.raises(ValueError,match='non-Python launcher'):
        OneShotRuntimeRepairMixin._implementation_operations({'edits':[{'path':'run','start_line':1,
            'end_line':2,'replacement':'#!/bin/sh\nexit 0\n'}]},files,
            RuntimeFailure('h','NameError','missing',(('main.py',1),)))


@pytest.mark.parametrize('explicit',[False,True])
def test_direct_one_shot_cannot_dispatch_retired_pipeline(setup,explicit):
    from maestro.types import MaestroError
    b,s,p,c,authority,grant=setup
    raw=b.config.to_dict();raw['coral']={'candidate_pipeline':'postbid_v1'} if explicit else {}
    b.config=RunConfig.from_dict(raw)
    with pytest.raises(MaestroError,match='postbid_v1 execution is retired'):
        b.generate(s,p,0,deadline=time.monotonic()+5,grant=grant(0),grant_authority=authority)
    assert c.seen==[]
    assert not b.budget_store.connection.execute('SELECT * FROM inference_reservations').fetchone()
