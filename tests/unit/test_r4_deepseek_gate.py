"""R4 uses the physical request gate even when other phases permit retries."""
from types import SimpleNamespace
from contextlib import nullcontext
import threading
import time
import json
from pathlib import Path

import pytest

from maestro.agents.valuation_reasoning import ValuationReadBroker
from maestro.backends.base import BackendUnavailable
from maestro.backends.coral.deepseek_backend import NativeDeepSeekBackend
from maestro.backends.coral.deepseek_gate import DeepSeekQuotaProxy,DEEPSEEK_TOOL
from maestro.backends.coral.deepseek_retry import DeepSeekRetryStore, RetryPolicy, RetryingDeepSeekProxy
from maestro.backends.coral.valuation import ValuationBackendMixin,ValuationOutputLimit,ValuationRepairFailed
from maestro.execution.budgets import BudgetStore,BudgetExceeded


def test_valuation_uses_nonretrying_physical_gate(tmp_path):
    store=DeepSeekRetryStore(tmp_path/'requests.sqlite')
    store.enable_retries(RetryPolicy(continue_after_network_failure=True))
    backend=object.__new__(NativeDeepSeekBackend)
    backend.budget_store=store
    backend.config=SimpleNamespace(valuation_calls=4,coral={'deepseek_reasoning_effort':'low'})
    backend.client=SimpleNamespace(model='deepseek-reasoner')
    backend._valuation_upstream=lambda *args,**kwargs: None
    backend.pause=None
    backend._closed=False
    backend._phase_proxies={}
    account='episode:0:agent-1:VALUATION_RO'
    deadline=time.monotonic()+30
    proxy=NativeDeepSeekBackend._valuation_proxy(backend,account,deadline)
    try:
        assert type(proxy) is DeepSeekQuotaProxy
        assert not isinstance(proxy,RetryingDeepSeekProxy)
        assert store.is_logical(account) is False
        assert store.snapshot(account)['max_calls']==5
        assert proxy.per_request_tokens==8192
        assert proxy.final_request_tokens==16384
        assert store.snapshot(account)['max_output_tokens']==57344
        assert proxy.deadline==deadline
        assert NativeDeepSeekBackend._local_abstention(backend,account,TimeoutError()) is False
        request={'model':'deepseek-reasoner','messages':[{'role':'user','content':'final'}],
                 'system':'Estimate the candidate.','tools':[{'name':DEEPSEEK_TOOL}]}
        with pytest.raises(PermissionError,match='trusted evidence paths'):
            proxy._payload(request,final_response=True)
        broker=ValuationReadBroker({'public/contract.json':'P=C','solver/run':'print(1)',
                                    'private/note.txt':'technical'},deadline)
        broker('maestro_tool',{'action':'list','path':'private'})
        broker('maestro_tool',{'action':'read','path':'public/contract.json'})
        broker('maestro_tool',{'action':'read','path':'solver/run'})
        proxy.bind_final_evidence_paths(broker.evidence_paths)
        final=proxy._payload(request,final_response=True)
        assert 'tools' not in final and 'tool_choice' not in final
        assert 'LAST allowed model request for VALUATION_RO' in final['system']
        assert 'LAST allowed model request for PREPARATION' not in final['system']
        allowed=json.loads(final['system'].split('Allowed evidence.path values: ',1)[1])
        assert allowed==['public/contract.json','solver/run']
        assert 'private' not in allowed and 'private/note.txt' not in allowed
        with pytest.raises(PermissionError,match='binding is unavailable'):
            proxy.bind_final_evidence_paths(broker.evidence_paths)
        forwarded=[]
        def upstream(payload,deadline):
            forwarded.append(payload)
            receipt={'type':'message','id':f'terminal-{len(forwarded)}',
                     'stop_reason':'end_turn','content':[{'type':'text','text':'done'}],
                     'usage':{'input_tokens':10,'output_tokens':16384 if len(forwarded)==4 else 3}}
            return json.dumps(receipt).encode(),'application/json'
        proxy.upstream=upstream
        proxy.guard=None
        with pytest.raises(PermissionError,match='unavailable'):proxy.enable_final_retry()
        for _ in range(4):proxy.complete(request)
        assert [p['max_tokens'] for p in forwarded]==[8192,8192,8192,16384]
        assert all(p.get('tools') for p in forwarded[:3]) and 'tools' not in forwarded[3]
        assert [r[0] for r in store.connection.execute(
            'select output_tokens from inference_reservations where account=? order by rowid',(account,))]==[
                8192,8192,8192,16384]
        with pytest.raises(BudgetExceeded,match='not been authorized'):proxy.complete(request)
        assert len(forwarded)==4
        proxy.enable_final_retry()
        proxy.complete(request)
        assert len(forwarded)==5
        assert forwarded[4]['max_tokens']==16384 and 'tools' not in forwarded[4]
        with pytest.raises(BudgetExceeded):proxy.complete(request)
    finally:
        proxy.close()
        store.close()


@pytest.mark.parametrize('case,expected',[
    ('truncated',True),('valid_json',False),('below_cap',False),
    ('unreclaimed',False),('wrong_header',False),('wrong_error',False),
    ('unknown_request',False),
])
def test_valuation_output_limit_requires_complete_cap_and_cleanup(tmp_path,case,expected):
    account='episode:0:agent-4:VALUATION_RO'
    store=BudgetStore(tmp_path/'requests.sqlite')
    store.create(account,max_calls=4,max_request_bytes=1000,max_output_tokens=40960,
                 deadline=time.time()+60)
    for index in range(4):
        reservation=store.reserve(account,f'call-{index}',request_bytes=1,
                                  output_tokens=16384 if index==3 else 8192)
        if case=='unknown_request' and index==3:continue
        actual=16383 if case=='below_cap' and index==3 else 16384 if index==3 else 10
        store.save_receipt(reservation,{'input_tokens':20,'output_tokens':actual},'a'*64,
                           f'provider-{index}',protocol='/v1/messages')
        store.finish(reservation,actual_output_tokens=actual)
    phase=tmp_path/'phase';logs=phase/'public/logs';logs.mkdir(parents=True)
    agent='other' if case=='wrong_header' else 'agent-4'
    answer='{"rationale":"unfinished' if case!='valid_json' else '{"rationale":"complete"}'
    (logs/'agent-4.0.log').write_text(json.dumps({'type':'coral','agent_id':agent,
        'prompt':'VALUATION_RO: estimate'})+'\n'+answer+'\n')
    cleanup={'phase_path':str(phase),'local_process_group_reclaimed':case!='unreclaimed',
             'tool_service_reclaimed':True}
    backend=SimpleNamespace(budget_store=store,config=SimpleNamespace(valuation_calls=4))
    error=RuntimeError('other failure') if case=='wrong_error' else BackendUnavailable('native failed')
    try:
        assert ValuationBackendMixin._terminal_output_limit(backend,account,'agent-4',
            SimpleNamespace(per_request_tokens=8192,final_request_tokens=16384),error,cleanup) is expected
    finally:store.close()


def test_clean_capped_native_exit_becomes_valuation_fallback_reason():
    calls=[]
    class Proxy:
        url='http://127.0.0.1/phase/v1'
        def bind_final_evidence_paths(self,supplier):pass
        def start(self):return self.url
        def close(self):calls.append('closed')
    class Client:
        supports_phase_identity=True
        def run_phase(self,*args,**kwargs):
            calls.append('native');raise BackendUnavailable('native CLI failed')
        def cleanup_receipt(self,url):
            return {'local_process_group_reclaimed':True,'tool_service_reclaimed':True}
    backend=object.__new__(ValuationBackendMixin)
    backend.budget_store=SimpleNamespace(_lock=threading.RLock(),
        connection=SimpleNamespace(execute=lambda *args:SimpleNamespace(fetchone=lambda:None)),
        usage=lambda account:{'unresolved_requests':0,'calls':4})
    backend.config=SimpleNamespace(valuation_calls=4)
    backend.episode_id='episode';backend.client=Client();backend.simulation=False
    backend._dispatch_guard=lambda:None
    backend._phase=lambda account:nullcontext()
    backend._valuation_proxy=lambda account,deadline:Proxy()
    backend._terminal_output_limit=lambda *args:True
    with pytest.raises(ValuationOutputLimit):
        backend.value_with_receipt('agent-4',0,{'public/contract.json':'{}'},deadline=time.monotonic()+10)
    assert calls==['native','closed']


@pytest.mark.parametrize('first_failure',['output_cap','malformed_json'])
@pytest.mark.parametrize('repair_fails',[False,True])
def test_settled_bad_final_allows_one_tool_free_regeneration(tmp_path,repair_fails,first_failure):
    store=DeepSeekRetryStore(tmp_path/'requests.sqlite')
    account='episode:0:agent-4:VALUATION_RO'
    store.connection.execute('CREATE TABLE codex_phase_scopes(account TEXT PRIMARY KEY)')
    phase=tmp_path/'phase';logs=phase/'public/logs';logs.mkdir(parents=True)
    (logs/'agent-4.0.log').write_text(json.dumps({'type':'coral','agent_id':'agent-4',
        'prompt':'VALUATION_RO: estimate'})+'\n{"rationale":"unfinished\n')
    holder={};sent=[]
    request={'model':'deepseek-reasoner','messages':[{'role':'user','content':'assess'}],
             'tools':[{'name':DEEPSEEK_TOOL}]}
    def upstream(payload,deadline):
        sent.append(payload)
        output=16384 if len(sent)==4 and first_failure=='output_cap' else 10
        return json.dumps({'type':'message','id':f'terminal-{len(sent)}',
            'stop_reason':'end_turn','content':[{'type':'text','text':'done'}],
            'usage':{'input_tokens':20,'output_tokens':output}}).encode(),'application/json'
    def make_proxy(scope,deadline):
        proxy=DeepSeekQuotaProxy(store,scope,'deepseek-reasoner',deadline=deadline,
            max_calls=5,max_request_bytes=100000,max_output_tokens=57344,upstream=upstream,
            per_request_tokens=8192,final_request_tokens=16384,
            allowed_tool_names=(DEEPSEEK_TOOL,),final_response_on_last_request=True,
            conditional_final_retry=True)
        holder['proxy']=proxy
        return proxy
    class Client:
        supports_phase_identity=True
        def run_phase(self,prompt,*,base_url,deadline,tools,output_schema,session_id,agent_id):
            assert base_url==holder['proxy'].url and agent_id=='agent-4'
            if tools is not None:
                for path in ('public/contract.json','public/trajectory.json','solver/run'):
                    tools('maestro_tool',{'action':'read','path':path})
                for _ in range(4):holder['proxy'].complete(request)
                if first_failure=='output_cap':raise BackendUnavailable('native final output truncated')
                json.loads('<tool-call>')
            assert 'one tool-free regeneration request' in prompt
            holder['proxy'].complete(request)
            if repair_fails:raise BackendUnavailable('native regeneration failed')
            return SimpleNamespace(text='{}')
        def cleanup_receipt(self,url):
            return {'phase_path':str(phase),'local_process_group_reclaimed':True,
                    'tool_service_reclaimed':True}
    backend=ValuationBackendMixin()
    backend.budget_store=store;backend.config=SimpleNamespace(valuation_calls=4)
    backend.episode_id='episode';backend.client=Client();backend.simulation=False
    backend._dispatch_guard=lambda:None
    backend._phase=lambda scope:nullcontext()
    backend._valuation_proxy=make_proxy
    files={'public/contract.json':'{}','public/trajectory.json':'[]','solver/run':'print(1)'}
    try:
        if repair_fails:
            with pytest.raises(ValuationRepairFailed):
                backend.value_with_receipt('agent-4',0,files,deadline=time.monotonic()+10)
        else:
            result=backend.value_with_receipt('agent-4',0,files,deadline=time.monotonic()+10)
            assert result['repair_attempted'] is True and result['usage']['calls']==5
        assert len(sent)==5
        assert [payload['max_tokens'] for payload in sent]==[8192,8192,8192,16384,16384]
        assert all('tools' not in payload for payload in sent[3:])
        assert store.usage(account)['usage_complete'] is True
    finally:store.close()


def test_native_exit_drain_waits_only_for_existing_terminal_evidence():
    account='episode:2:agent-4:GENERATION:plan'
    state={'terminal':False,'calls':0}
    class Store:
        _lock=threading.RLock()
        retry_policy=SimpleNamespace()
        def usage(self,requested):
            assert requested==account
            state['calls']+=1
            return {'calls':1,'logical_calls':1,'usage_complete':state['terminal'],
                    'unresolved_requests':0 if state['terminal'] else 1,
                    'unknown_requests':0 if state['terminal'] else 1}
        connection=SimpleNamespace(execute=lambda *args:SimpleNamespace(
            fetchall=lambda:[('COMPLETED' if state['terminal'] else 'DISPATCHING',)]))
    backend=object.__new__(NativeDeepSeekBackend)
    backend.budget_store=Store()
    backend.client=SimpleNamespace(cleanup_receipt=lambda url:{
        'local_process_group_reclaimed':True,'tool_service_reclaimed':True})
    backend._phase_proxies={account:SimpleNamespace(url='http://127.0.0.1/phase/v1')}
    worker=threading.Thread(target=lambda:(time.sleep(.07),state.update(terminal=True)))
    worker.start();started=time.monotonic()
    backend._drain_native_exit(account,started+.5)
    worker.join()
    assert .06<=time.monotonic()-started<.5 and state['calls']>=2
    state.update(terminal=False,calls=0)
    started=time.monotonic();backend._drain_native_exit(account,started+.06)
    assert .05<=time.monotonic()-started<.2 and state['terminal'] is False
    backend.budget_store.retry_policy=None
    state.update(terminal=True,calls=0)
    backend._drain_native_exit(account,time.monotonic()+.1)
    assert state['calls']==1
