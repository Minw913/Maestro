"""DeepSeek provider-boundary tests use synthetic receipts, never live inference."""
import json
import time
import urllib.request
from urllib.error import HTTPError

import pytest

from maestro.backends.coral.deepseek_gate import DeepSeekQuotaProxy,DEEPSEEK_TOOL,terminal_receipt,provider_endpoint
from maestro.execution.budgets import BudgetStore,BudgetExceeded
from maestro.llm import InferenceRequestUnknown


def message(*,usage=True,tool=None):
    value={'type':'message','id':'msg_offline','model':'offline-deepseek','stop_reason':'end_turn',
           'content':[{'type':'text','text':'done'}]}
    if usage:value['usage']={'input_tokens':10,'output_tokens':3,'cache_read_input_tokens':2}
    if tool:value['content']=[{'type':'tool_use','id':'tool1','name':tool,'input':{}}];value['stop_reason']='tool_use'
    return value


def sse(value):
    start={**value,'content':[],'stop_reason':None,'usage':{'input_tokens':10,'output_tokens':0}}
    events=[{'type':'message_start','message':start}]
    events += [{'type':'content_block_start','index':i,'content_block':b} for i,b in enumerate(value['content'])]
    events += [{'type':'message_delta','delta':{'stop_reason':value['stop_reason']},'usage':{'output_tokens':3}},
               {'type':'message_stop'}]
    return b''.join(b'data: '+json.dumps(e).encode()+b'\n\n' for e in events)


@pytest.fixture
def gate(tmp_path):
    store=BudgetStore(tmp_path/'requests.sqlite');forwarded=[]
    def upstream(payload,deadline):
        forwarded.append(payload)
        return json.dumps(message()).encode(),'application/json'
    gate=DeepSeekQuotaProxy(store,'episode:round:agent:PREPARATION','offline-deepseek',
        deadline=time.monotonic()+20,max_calls=2,max_request_bytes=100000,max_output_tokens=100,
        per_request_tokens=20,allowed_tool_names=(DEEPSEEK_TOOL,),upstream=upstream)
    yield gate,store,forwarded
    gate.close();store.close()


def payload(**values):return {'model':'offline-deepseek','messages':[],'max_tokens':50,
    'tools':[{'name':DEEPSEEK_TOOL,'input_schema':{}}],**values}


def test_actual_requests_usage_ceiling_and_no_quota_reset(gate):
    owner,store,forwarded=gate
    owner.complete(payload());owner.complete(payload())
    with pytest.raises(BudgetExceeded):owner.complete(payload())
    assert len(forwarded)==2 and forwarded[0]['max_tokens']==20
    usage=owner.usage()
    assert usage['calls']==2 and usage['input_tokens']==24 and usage['output_tokens']==6
    assert usage['unresolved_requests']==0 and owner.quota_denied
    with pytest.raises(BudgetExceeded):
        DeepSeekQuotaProxy(store,owner.account,owner.model,deadline=time.monotonic()+50,max_calls=3,
            max_request_bytes=100000,max_output_tokens=100,per_request_tokens=20,upstream=owner.upstream)


def test_native_tools_removed_and_broker_schema_owned(gate):
    owner,_,forwarded=gate
    owner.complete(payload(tools=[{'name':'bash','input_schema':{}},
        {'name':DEEPSEEK_TOOL,'input_schema':{'malicious':'schema'}}]))
    tools=forwarded[0]['tools']
    assert len(tools)==1 and tools[0]['name']==DEEPSEEK_TOOL
    assert 'malicious' not in tools[0]['input_schema']
    assert forwarded[0]['tool_choice']['disable_parallel_tool_use']


@pytest.mark.parametrize('stream',[False,True])
def test_unbrokered_response_never_reaches_runtime(gate,stream):
    owner,store,_=gate
    owner.upstream=lambda *_:(sse(message(tool='bash')),'text/event-stream') if stream else (json.dumps(message(tool='bash')).encode(),'application/json')
    with pytest.raises(PermissionError,match='Unbrokered'):owner.complete(payload())
    assert owner.usage()['protocol_violations']==1
    assert store.usage(owner.account)['unresolved_requests']==0
    assert store.usage(owner.account)['calls']==1


@pytest.mark.parametrize('kind',['usage','truncated','network'])
def test_unknown_is_persistent_and_prevents_another_dispatch(gate,kind):
    owner,store,forwarded=gate
    def unknown(*args):
        forwarded.append('one')
        if kind=='network':raise TimeoutError()
        if kind=='usage':return json.dumps(message(usage=False)).encode(),'application/json'
        return sse(message()).rsplit(b'data:',1)[0],'text/event-stream'
    owner.upstream=unknown
    with pytest.raises((ValueError,TimeoutError)):owner.complete(payload())
    assert store.usage(owner.account)['unresolved_requests']==1
    with pytest.raises(InferenceRequestUnknown):owner.assert_resolved()
    with pytest.raises(BudgetExceeded):owner.complete(payload())
    assert len(forwarded)==1


def test_wrong_model_and_extra_route_never_dispatch(gate):
    owner,store,forwarded=gate
    with pytest.raises(PermissionError):owner.complete(payload(model='wrong'))
    url=owner.start()
    request=urllib.request.Request(url+'/responses',data=json.dumps(payload()).encode())
    with pytest.raises(HTTPError) as error:urllib.request.urlopen(request,timeout=2)
    assert error.value.code==403
    assert store.usage(owner.account)['calls']==0 and not forwarded


def test_real_local_http_wire_with_synthetic_provider(gate):
    owner,store,_=gate
    owner.upstream=lambda *_:(sse(message()),'text/event-stream')
    request=urllib.request.Request(owner.start()+'/messages',data=json.dumps(payload()).encode(),
        headers={'Content-Type':'application/json','x-api-key':'synthetic-phase-only'})
    with urllib.request.urlopen(request,timeout=2) as response:
        assert terminal_receipt(response.read(),response.headers['Content-Type'])['id']=='msg_offline'
    assert store.usage(owner.account)['calls']==1
    owner.assert_resolved()


@pytest.mark.parametrize('url',['http://api.deepseek.com','https://user:secret@api.deepseek.com',
    'https://api.deepseek.com/other','https://api.deepseek.com?route=evil'])
def test_upstream_endpoint_restrictions(url):
    with pytest.raises(ValueError):provider_endpoint(url)


def test_tools_disabled_in_final_plan(gate):
    owner,_,forwarded=gate
    owner.tools=False
    owner.complete(payload(tools=[{'name':DEEPSEEK_TOOL}]))
    assert 'tools' not in forwarded[0]
    owner.upstream=lambda *_:(json.dumps(message(tool=DEEPSEEK_TOOL)).encode(),'application/json')
    with pytest.raises(PermissionError):owner.complete(payload())


def test_messages_effort_and_hidden_tool_registry_updates(gate):
    owner,_,forwarded=gate
    owner.complete(payload(output_config={'effort':'max'},messages=[{'role':'system','content':[
        {'type':'tool_addition','tool':{'type':'tool_reference','name':'bash'}},
        {'type':'text','text':'frozen instructions'}]}]))
    assert forwarded[0]['output_config']=={'effort':'low'}
    assert 'reasoning_effort' not in forwarded[0]
    assert forwarded[0]['messages'][0]['content']==[{'type':'text','text':'frozen instructions'}]


def test_upstream_dns_completion_cannot_dispatch_after_deadline(monkeypatch):
    from maestro.backends.coral.deepseek_gate import DeepSeekUpstream
    sent=[]
    class Connection:
        def __init__(self,*args,**kwargs):self.sock=None
        def connect(self):time.sleep(.04)
        def request(self,*args,**kwargs):self.connect();self.send(b'headers')
        def send(self,data):sent.append(data)
        def close(self):pass
    monkeypatch.setattr('maestro.backends.coral.deepseek_gate.http.client.HTTPSConnection',Connection)
    with pytest.raises(TimeoutError):
        DeepSeekUpstream('https://api.deepseek.com/anthropic','synthetic-secret')(payload(),time.monotonic()+.01)
    assert sent==[]



def test_missing_mcp_registration_rejected_without_provider_spend(gate):
    owner,store,forwarded=gate
    with pytest.raises(PermissionError,match='registered maestro'):
        owner.complete(payload(tools=[]))
    assert not forwarded and store.usage(owner.account)['calls']==0
