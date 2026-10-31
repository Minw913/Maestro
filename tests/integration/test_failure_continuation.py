"""Local failure isolation and reports; synthetic endpoints only, no model calls."""
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
from pathlib import Path
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

from maestro.backends.coral.deepseek_backend import NativeDeepSeekBackend,LocalPhaseAbstention
from maestro.backends.coral.deepseek_retry import DeepSeekRetryStore,RetryPolicy,RetryingDeepSeekProxy,audit_retry_continuation
from maestro.backends.coral.deepseek_lifecycle import ledger_fingerprint
from maestro.backends.coral.failure_report import write_failure_report
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.config import RunConfig
from maestro.types import AgentState


def owner(tmp_path,*,cleanup=True,extra=10):
    policy=RetryPolicy(backoff_seconds=.01,max_extra_requests=extra,continue_after_network_failure=True,warm_seed_fallback=True)
    store=DeepSeekRetryStore(tmp_path/'requests.sqlite');store.enable_retries(policy);seen=[]
    class Client:
        model='offline';supports_resume=False
        def __init__(self):self.clean={}
        def run_phase(self,prompt,*,base_url,deadline,tools=None,**kwargs):
            try:
                u=urlsplit(base_url);c=http.client.HTTPConnection(u.hostname,u.port,timeout=3)
                c.request('POST',u.path+'/messages',json.dumps({'model':'offline','messages':[],
                    'tools':[{'name':'mcp__maestro__maestro_tool'}]}))
                r=c.getresponse();r.read();c.close()
                if r.status!=200:raise RuntimeError('local gateway error')
                return SimpleNamespace(text='offline evidence',response_hash='ok',thread_id='')
            finally:self.clean[base_url]={'local_process_group_reclaimed':cleanup,'tool_service_reclaimed':cleanup}
        def cleanup_receipt(self,url):return self.clean.get(url,{})
        def close(self):pass
    def proxy(*args,**kwargs):
        account=args[1]
        def upstream(value,deadline):
            seen.append(account)
            if account.split(':')[1]=='0':raise ConnectionResetError(104,'synthetic peer reset')
            return json.dumps({'type':'message','id':'local-done','stop_reason':'end_turn','content':[{'type':'text','text':'done'}],
                'usage':{'input_tokens':7,'output_tokens':3}}).encode(),'application/json'
        return RetryingDeepSeekProxy(*args,upstream=upstream,retry_policy=policy,**kwargs)
    seed=tmp_path/'seed';seed.mkdir();(seed/'run').write_text('offline fixture')
    b=NativeDeepSeekBackend(Client(),store,WorkspaceStore(tmp_path/'work',tmp_path/'cas'),precheck=None,
        seed_solver=seed,candidate_runner=object(),image='',simulation=True,proxy_factory=proxy,
        config=RunConfig(preparation_calls=1,coral={'deepseek_transport_retry':policy.__dict__}))
    b.begin_episode('offline-failure');parent=b.prepare_seed();b.register_parent(parent['candidate_hash'],parent['candidate_path'])
    return b,[AgentState(f'a{i}',parent['candidate_hash'],.5) for i in range(4)],seen


def test_four_failed_agents_abstain_then_next_wave_succeeds_after_global_retry_cap(tmp_path):
    b,states,seen=owner(tmp_path,extra=1)
    try:
        def first(s):
            with pytest.raises(LocalPhaseAbstention):b.prepare_with_receipt(s,0,{},deadline=time.monotonic()+5)
        with ThreadPoolExecutor(4) as p:list(p.map(first,states))
        assert len(seen)==5  # Four initial requests, globally only ONE extra attempt.
        b._assert_clear()
        for s in states:
            evidence,usage=b.prepare_with_receipt(s,1,{},deadline=time.monotonic()+5)
            assert evidence['status']=='PREPARED' and usage['calls']==1
        assert len(seen)==9
        assert b.budget_store.connection.execute("SELECT count(*) FROM codex_phase_scopes WHERE status='FAILED_LOCAL_RECLAIMED'").fetchone()[0]==4
        assert audit_retry_continuation(tmp_path/'requests.sqlite')['status']=='CONTINUABLE_WITH_UNKNOWN_COST'
        before=ledger_fingerprint(tmp_path/'requests.sqlite')
        r=write_failure_report(tmp_path,ledger_path=tmp_path/'requests.sqlite')
        assert r['counts']['unknown_requests']==5 and r['counts']['affected_phase_scopes']==4
        assert r['counts']['unknown_with_phase_abstention']==5
        assert ledger_fingerprint(tmp_path/'requests.sqlite')==before
        data=json.loads(Path(r['json']).read_text())
        assert all(x['phase_cleanup']['local_process_group_reclaimed'] for x in data['requests'])
        # The injected non-progress provider supplies a type but no transport
        # traceback. Reporting must not invent a stack for this fixture.
        assert all(not x['traceback_available'] for x in data['requests'])
        assert 'ConnectionResetError' in Path(r['tracebacks']).read_text()
    finally:b.close()


def test_missing_process_reclamation_cannot_be_relabelled_as_abstention(tmp_path):
    from maestro.llm import InferenceRequestUnknown
    b,states,seen=owner(tmp_path,cleanup=False)
    try:
        with pytest.raises(InferenceRequestUnknown):b.prepare_with_receipt(states[0],0,{},deadline=time.monotonic()+5)
        assert b.budget_store.connection.execute('SELECT count(*) FROM deepseek_abandoned_scopes').fetchone()[0]==0
        with pytest.raises(Exception):b._assert_clear()
        assert audit_retry_continuation(tmp_path/'requests.sqlite')['status']=='BLOCKED'
    finally:b.close()


def test_seed_fallback_keeps_empty_history_and_zero_evidence_bid_then_replays(tmp_path):
    from maestro.loop import ExperimentEngine
    from maestro.replay import verify_ledger
    from maestro.reports import report
    policy=RetryPolicy(continue_after_network_failure=True,warm_seed_fallback=True)
    config=RunConfig(rounds=2,warm_opportunities_per_agent=2,coral={'deepseek_transport_retry':policy.__dict__})
    with ExperimentEngine(tmp_path,config) as engine:
        original=engine._implement_proposal
        def generate(e,r,a,state,plan,purpose,deadline):
            if purpose=='WARM' and a=='agent-3':return None
            return original(e,r,a,state,plan,purpose,deadline)
        engine._implement_proposal=generate
        warm=engine.bootstrap();payload=json.loads(Path(warm['checkpoint']).read_text())['payload']
        assert list(payload['seed_fallback_agents'])==['agent-3']
        assert payload['states']['agent-3']['history']==[] and payload['states']['agent-3']['ucb_n']==0
        result=engine.run(warm['checkpoint'],'first_price_naive');assert result['status']=='COMPLETED'
        events=engine.ledger.connection.execute("SELECT payload FROM events WHERE kind='COLD_START_ZERO_VALUATION'").fetchall()
        assert events and all(json.loads(row[0])['agent_id']=='agent-3' for row in events)
    assert verify_ledger(tmp_path)['verified']
    assert all(not row['comparison_eligible'] for row in report(tmp_path)['episodes'])


def test_legacy_request_unknown_has_no_fabricated_traceback(tmp_path):
    from maestro.execution.budgets import BudgetStore
    db=tmp_path/'deepseek-native-authority';db.mkdir();store=BudgetStore(db/'requests.sqlite')
    store.create('warm-example:1:agent-3:PREPARATION',max_calls=1,max_request_bytes=100,max_output_tokens=20,deadline=time.time()+10)
    rid=store.reserve('warm-example:1:agent-3:PREPARATION','lost',request_bytes=1,output_tokens=20);store.finish(rid,failed=True);store.close()
    result=write_failure_report(tmp_path);data=json.loads(Path(result['json']).read_text())
    assert result['counts']['unknown_requests']==1 and result['counts']['unknown_without_traceback']==1
    row=data['requests'][0];assert row['wave_number']==2 and row['stage']=='PREPARATION'
    assert row['exception_chain']==[] and not row['traceback_available']


def test_preparation_deadline_allows_cleanup_but_discards_late_evidence(tmp_path):
    from maestro.loop import ExperimentEngine
    engine=ExperimentEngine(tmp_path,RunConfig(preparation_window_seconds=.02))
    class Backend:
        failure_continuation_enabled=True
        def prepare_with_receipt(self,*args,**kwargs):
            time.sleep(.06)
            return {'accepted_monotonic':time.monotonic(),'summary':'late'},{'calls':1,'usage_complete':True}
        def close(self):pass
    engine.backend=Backend();states={'a':AgentState('a','parent',.5)}
    engine.ledger.create_episode('e','warm',engine.config,states);engine.ledger.open_round('e',0,{})
    try:
        prepared,failures=engine._prepare_agents('e',0,states,{})
        assert not prepared and failures['a'].status=='ABSTAIN' and not failures['a'].inflight_unknown
        assert engine.ledger.connection.execute("SELECT count(*) FROM events WHERE kind='PHASE_CLEANUP_DRAIN'").fetchone()[0]==1
    finally:engine.close()


def test_planning_deadline_discards_reclaimed_late_result_without_global_unknown():
    from maestro.agents.planning_batch import parallel_plans
    class Backend:
        planning_capabilities={'single_logical_request':True,'max_request_attempts':2,
            'hard_request_timeout':True,'mutates_input_state':False,'cleanup_grace_seconds':.2}
        def plan_with_receipt(self,*args,**kwargs):
            time.sleep(.06)
            return None,{'actual_requests':1,'logical_requests':1,'retry_attempts':0},{'agent_id':'a'}
    result=parallel_plans({'a':AgentState('a','p',.5)},{'a':{}},Backend(),0,time.monotonic()+.02)['a']
    assert result.status=='ABSTAIN' and not result.inflight_unknown
