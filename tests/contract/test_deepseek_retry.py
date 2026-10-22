"""Explicit recovery accounting with synthetic providers; no real inference."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import threading
import time
from types import SimpleNamespace

import pytest

from maestro.backends.coral.deepseek_retry import RetryPolicy, DeepSeekRetryStore, RetryingDeepSeekProxy
from maestro.backends.coral.deepseek_gate import DEEPSEEK_TOOL, DeepSeekHTTPError
from maestro.backends.coral.deepseek_lifecycle import audit_ledger
from maestro.execution.budgets import BudgetExceeded


def message(content=None):
    return json.dumps({'type':'message','id':'accepted-message','stop_reason':'end_turn',
        'content':content or [{'type':'text','text':'accepted'}],
        'usage':{'input_tokens':7,'output_tokens':3}}).encode(),'application/json'


def payload(tools=False):
    return {'model':'offline','messages':[{'role':'user','content':'same frozen input'}],
        **({'tools':[{'name':DEEPSEEK_TOOL}]} if tools else {})}


def make(tmp_path,upstream,*,policy=None,account='e:0:a:PLAN_RO',calls=1,tools=False,deadline=None):
    policy=policy or RetryPolicy(backoff_seconds=.01)
    store=DeepSeekRetryStore(tmp_path/'requests.sqlite');store.enable_retries(policy)
    gate=RetryingDeepSeekProxy(store,account,'offline',deadline=deadline or time.monotonic()+5,
        max_calls=calls,max_request_bytes=100000,max_output_tokens=96000,per_request_tokens=96000,
        allowed_tool_names=(DEEPSEEK_TOOL,) if tools else (),final_response_on_last_request=tools,
        upstream=upstream,retry_policy=policy)
    return store,gate


def test_lost_stream_retries_identical_input_and_retains_unknown_cost(tmp_path):
    seen=[]
    def upstream(value,deadline):
        seen.append(value)
        if len(seen)==1:raise ConnectionResetError(104,'synthetic peer reset')
        return message()
    store,gate=make(tmp_path,upstream)
    try:
        assert gate.complete(payload())==message()
        gate.assert_resolved();usage=gate.usage()
        assert seen[0]==seen[1] and len(seen)==2
        assert usage['calls']==2 and usage['logical_calls']==1 and usage['retry_attempts']==1
        assert not usage['usage_complete'] and usage['input_tokens'] is None
        assert usage['unresolved_requests']==1 and usage['unknown_requests']==1
        assert usage['continuation_blocking_requests']==0 and usage['known_output_tokens']==3
        rows=store.connection.execute('SELECT status FROM inference_reservations').fetchall()
        assert sorted(r[0] for r in rows)==['COMPLETED','UNKNOWN']
        assert audit_ledger(tmp_path/'requests.sqlite')['status']=='BLOCKED'  # Cost is NOT settled.
        with pytest.raises(BudgetExceeded):gate.complete(payload())
        assert len(seen)==2
    finally:gate.close();store.close()


def test_recovered_preparation_can_continue_without_resetting_unknown_reserve(tmp_path):
    seen=[]
    def upstream(value,deadline):
        seen.append(value)
        if len(seen)==1:raise ConnectionResetError()
        return message()
    store,gate=make(tmp_path,upstream,account='e:0:a:PREPARATION',calls=2,tools=True)
    try:
        gate.complete(payload(True));gate.complete(payload(True));gate.assert_resolved()
        assert len(seen)==3 and 'tools' in seen[0] and seen[0]==seen[1]
        assert 'tools' not in seen[2] and 'LAST allowed model request' in str(seen[2]['system'])
        usage=gate.usage();assert usage['calls']==3 and usage['logical_calls']==2
        assert usage['charged_output_tokens']==96006 and usage['known_output_tokens']==6
        assert seen[2]['max_tokens']==95997
    finally:gate.close();store.close()


@pytest.mark.parametrize('error',[ValueError('invalid JSON'),PermissionError('tool violation'),
    DeepSeekHTTPError(400),DeepSeekHTTPError(401),DeepSeekHTTPError(402),DeepSeekHTTPError(422)])
def test_non_network_errors_are_not_retried(tmp_path,error):
    calls=[]
    def upstream(*args):calls.append(1);raise error
    store,gate=make(tmp_path,upstream)
    try:
        with pytest.raises(type(error)):gate.complete(payload())
        assert len(calls)==1 and gate.usage()['retry_attempts']==0
    finally:gate.close();store.close()


@pytest.mark.parametrize('status',[429,500,502,503,504])
def test_transient_status_can_recover_with_own_charge(tmp_path,status):
    calls=[]
    def upstream(*args):
        calls.append(1)
        if len(calls)==1:raise DeepSeekHTTPError(status)
        return message()
    store,gate=make(tmp_path,upstream)
    try:
        gate.complete(payload());gate.assert_resolved()
        assert len(calls)==2 and gate.usage()['calls']==2
    finally:gate.close();store.close()


def test_deadline_prevents_backoff_and_extra_dispatch(tmp_path):
    calls=[]
    def upstream(*args):calls.append(1);raise ConnectionResetError()
    store,gate=make(tmp_path,upstream,policy=RetryPolicy(backoff_seconds=.2),deadline=time.monotonic()+.1)
    try:
        with pytest.raises(TimeoutError):gate.complete(payload())
        assert len(calls)==1 and gate.usage()['retry_attempts']==0
        assert gate.usage()['continuation_blocking_requests']==1
    finally:gate.close();store.close()


def test_retry_exhaustion_never_claims_operational_recovery(tmp_path):
    calls=[]
    def upstream(*args):calls.append(1);raise ConnectionResetError()
    store,gate=make(tmp_path,upstream)
    try:
        with pytest.raises(ConnectionResetError):gate.complete(payload())
        assert len(calls)==2 and gate.usage()['unresolved_requests']==2
        assert gate.usage()['continuation_blocking_requests']==2
        assert store.connection.execute('SELECT count(*) FROM deepseek_recovered_attempts').fetchone()[0]==0
    finally:gate.close();store.close()


def test_four_way_retries_share_durable_global_cap(tmp_path):
    store=DeepSeekRetryStore(tmp_path/'requests.sqlite');policy=RetryPolicy(max_extra_requests=1,backoff_seconds=.01);store.enable_retries(policy)
    barrier=threading.Barrier(4);calls=[];lock=threading.Lock();gates=[]
    def run(i):
        count=0
        def upstream(*args):
            nonlocal count
            count+=1
            with lock:calls.append(i)
            if count==1:barrier.wait(2);raise ConnectionResetError()
            return message()
        gate=RetryingDeepSeekProxy(store,f'e:0:a{i}:PLAN_RO','offline',deadline=time.monotonic()+5,
            max_calls=1,max_request_bytes=100000,max_output_tokens=96000,per_request_tokens=96000,
            upstream=upstream,retry_policy=policy);gates.append(gate)
        try:gate.complete(payload());return True
        except BudgetExceeded:return False
        finally:gate.close()
    try:
        with ThreadPoolExecutor(4) as pool:results=list(pool.map(run,range(4)))
        assert sum(results)==1 and len(calls)==5
        assert store.connection.execute('SELECT count(*) FROM deepseek_request_attempts WHERE attempt_index>0').fetchone()[0]==1
        with pytest.raises(BudgetExceeded):store.enable_retries(replace(policy,max_extra_requests=10))
    finally:
        for gate in gates:gate.close()
        store.close()


def test_successful_empty_answer_is_not_regenerated(tmp_path):
    calls=[]
    def upstream(*args):calls.append(1);return message([{'type':'text','text':''}])
    store,gate=make(tmp_path,upstream)
    try:gate.complete(payload());assert len(calls)==1 and gate.usage()['retry_attempts']==0
    finally:gate.close();store.close()


def test_native_preparation_plan_generation_recover_without_duplicate_apply(tmp_path):
    import http.client
    from urllib.parse import urlsplit
    from maestro.agents.planning_batch import parallel_plans
    from maestro.backends.coral.deepseek_backend import NativeDeepSeekBackend
    from maestro.backends.coral.workspace import WorkspaceStore
    from maestro.config import RunConfig
    from maestro.execution.grants import GrantAuthority
    from maestro.types import AgentState,stable_hash
    policy=RetryPolicy(backoff_seconds=.01);store=DeepSeekRetryStore(tmp_path/'requests.sqlite');store.enable_retries(policy)
    class Client:
        model='offline';supports_resume=False
        def run_phase(self,prompt,*,base_url,deadline,tools=None,output_schema=None,**kwargs):
            u=urlsplit(base_url);c=http.client.HTTPConnection(u.hostname,u.port,timeout=3)
            c.request('POST',u.path+'/messages',json.dumps(payload(tools is not None)))
            response=c.getresponse();wire=response.read();c.close();assert response.status==200
            text=json.loads(wire)['content'][0]['text']
            return SimpleNamespace(text=text,response_hash=stable_hash(text),thread_id='')
        def close(self):pass
    seen=[]
    def proxy(*args,**kwargs):
        count=0;root=args[1]
        def upstream(value,deadline):
            nonlocal count
            count+=1;seen.append((root,value))
            if count==1:raise ConnectionResetError()
            if root.endswith(':PREPARATION'):text='Evidence from supplied frozen seed; no further reads.'
            elif root.endswith(':PLAN_RO'):
                text=json.dumps({'action':'revise','plan':{'target':'tie_break','mod_type':'rule_replacement',
                    'diagnosis':'inspect seed','change':'change tie break','expected_effect':'reduce set',
                    'evidence_run_ids':[]},'lessons':[]})
            else:text=json.dumps({'files':[{'path':'run','content':'#!/bin/sh\necho changed\n','executable':True}]})
            return message([{'type':'text','text':text}])
        return RetryingDeepSeekProxy(*args,upstream=upstream,retry_policy=policy,**kwargs)
    seed=tmp_path/'seed';seed.mkdir();(seed/'run').write_text('seed')
    backend=NativeDeepSeekBackend(Client(),store,WorkspaceStore(tmp_path/'work',tmp_path/'cas'),
        precheck=lambda candidate:(True,'offline'),seed_solver=seed,candidate_runner=object(),image='',
        config=RunConfig(rounds=1,preparation_calls=1),proxy_factory=proxy,simulation=True)
    try:
        backend.begin_episode('offline');parent=backend.prepare_seed();backend.register_parent(parent['candidate_hash'],parent['candidate_path'])
        state=AgentState('a',parent['candidate_hash'],.5)
        evidence,usage=backend.prepare_with_receipt(state,0,{},deadline=time.monotonic()+5)
        assert usage['calls']==2 and usage['logical_calls']==1 and not usage['usage_complete']
        outcome=parallel_plans({'a':state},{'a':{}},backend,0,time.monotonic()+5)['a']
        assert outcome.status=='PLANNED' and outcome.receipt.actual_requests==2 and outcome.receipt.logical_requests==1
        assert outcome.receipt.provider_request_id=='accepted-message'
        authority=GrantAuthority();plan=outcome.decision.plan
        grant=authority.issue(episode_id='offline',round_index=0,agent_id='a',plan_version_id=plan.version_id,
            parent_hash=state.parent_hash,epoch=0,expires_at=time.time()+5)
        candidate=backend.generate(state,plan,0,deadline=time.monotonic()+5,grant=grant,grant_authority=authority)
        assert ( __import__('pathlib').Path(candidate['candidate_path'])/'run').read_text()=='#!/bin/sh\necho changed\n'
        assert backend.last_generation_usage['calls']==2 and backend.last_generation_usage['logical_calls']==1
        assert backend.last_generation_usage['continuation_blocking_requests']==0
        assert len(seen)==6 and store.connection.execute("SELECT count(*) FROM inference_reservations WHERE status='UNKNOWN'").fetchone()[0]==3
        backend._assert_clear()
        assert all(x[0]=='COMPLETED' for x in store.connection.execute('SELECT status FROM codex_phase_scopes'))
    finally:backend.close()


def test_continuation_audit_is_read_only_and_does_not_settle_cost(tmp_path):
    from maestro.backends.coral.deepseek_retry import audit_retry_continuation
    from maestro.backends.coral.deepseek_lifecycle import ledger_fingerprint
    calls=[]
    def upstream(*args):
        calls.append(1)
        if len(calls)==1:raise ConnectionResetError()
        return message()
    store,gate=make(tmp_path,upstream)
    try:
        gate.complete(payload());before=ledger_fingerprint(tmp_path/'requests.sqlite')
        audit=audit_retry_continuation(tmp_path/'requests.sqlite')
        assert audit['status']=='CONTINUABLE_WITH_UNKNOWN_COST' and not audit['cost_usage_complete']
        assert audit['cost_audit']['status']=='BLOCKED' and audit['quarantined_unknown_requests']==1
        assert ledger_fingerprint(tmp_path/'requests.sqlite')==before
        # Durable proof cannot authorize a different logical request's result.
        store.connection.execute("UPDATE provider_receipts SET request_hash='different-payload'")
        assert audit_retry_continuation(tmp_path/'requests.sqlite')['status']=='BLOCKED'
    finally:gate.close();store.close()


def test_pause_during_retry_backoff_prevents_second_dispatch(tmp_path):
    from maestro.execution.pause import PauseController,PauseRequested
    pause=PauseController(tmp_path);calls=[]
    def upstream(*args):calls.append(1);pause.request();raise ConnectionResetError()
    store,gate=make(tmp_path,upstream);gate.guard=pause.check
    try:
        with pytest.raises(PauseRequested):gate.complete(payload())
        assert len(calls)==1 and gate.usage()['retry_attempts']==0
        assert gate.usage()['continuation_blocking_requests']==1
    finally:gate.close();store.close()


def test_eof_before_terminal_sse_is_retried_but_not_schema_errors(tmp_path):
    calls=[]
    def upstream(*args):
        calls.append(1)
        if len(calls)==1:return b'data: {"type":"message_start","message":{"id":"lost","content":[]}}\n\n','text/event-stream'
        return message()
    store,gate=make(tmp_path,upstream)
    try:
        gate.complete(payload());gate.assert_resolved()
        assert len(calls)==2 and gate.usage()['unresolved_requests']==1
    finally:gate.close();store.close()
