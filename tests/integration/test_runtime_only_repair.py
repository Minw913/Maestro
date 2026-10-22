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
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.config import RunConfig
from maestro.execution.budgets import BudgetStore, BudgetExceeded
from maestro.execution.grants import GrantAuthority, GrantError
from maestro.execution.sandbox import RunReceipt
from maestro.tasks.pace.runtime_failure import RuntimeFailure,RuntimePrecheckFailure,classify_runtime_failure
from maestro.types import AgentState,Plan,stable_hash


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
        plan_version_id=plan.version_id,parent_hash=plan.parent_hash,epoch=epoch,expires_at=time.time()+30)
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
        def generate(self,*a,**kw):self.initial+=1;self.last_generation_usage={'calls':1};return {'candidate_hash':'hash'}
        def repair_runtime(self,*a,**kw):self.repairs+=1;self.last_generation_usage={'calls':1};return {'candidate_hash':'hash'}
        def precheck(self,candidate):
            if failure_kind=='runtime':return RuntimePrecheckFailure('runtime',RuntimeFailure('hash','NameError','bad',(('run',1),)))
            if failure_kind=='infrastructure':raise OSError('no executor')
            return False,failure_kind
    b=Backend();ledger=Ledger();cfg=RunConfig(execution_mode='real')
    # This fixture isolates repair classification; cooperative controller pause
    # is covered separately with a real ExperimentEngine/ledger.
    e=SimpleNamespace(config=cfg,backend=b,ledger=ledger,grants=GrantAuthority(),_check_pause=lambda:None)
    state=AgentState('a','parent',.5);plan=Plan('p','v','a','parent','local_search','parameter_change','x','x','x')
    assert ExperimentEngine._generate_once_runtime_only(e,'e',0,'a',state,plan,'WARM',time.monotonic()+30) is None
    assert b.initial==1 and b.repairs==expected_repairs
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
            plan_version_id=p.version_id,parent_hash=p.parent_hash,epoch=epoch,expires_at=time.time()+25)
        candidate=b.generate(s,p,0,deadline=deadline,grant=grant(0),grant_authority=authority)
        problem=b.precheck(candidate).evidence
        barrier.wait(timeout=15)
        repaired=b.repair_runtime(s,p,candidate,problem,0,0,deadline,grant=grant(1),grant_authority=authority)
        assert (Path(repaired['candidate_path'])/'run').read_text()=='print(1)\n'
    with ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(worker,range(4)))
    assert len(c.seen)==8
