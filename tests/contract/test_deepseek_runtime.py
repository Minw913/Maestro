"""Real Coral runtime construction with injected CLI; no DeepSeek inference."""
import json
import os
from pathlib import Path
import sys
import time

import pytest
import yaml

from maestro.backends.coral.deepseek_client import CoralDeepSeekClient
from maestro.backends.coral.deepseek_exec import command,environment


@pytest.mark.skipif(os.environ.get('MAESTRO_RUN_NATIVE_WIRE')!='1'
                   or not os.environ.get('MAESTRO_DSH_EXECUTABLE'),
                   reason='Opt-in installed DSH local-only wire test')
def test_installed_dsh_final_valuation_sees_only_read_evidence_paths(tmp_path):
    from maestro.agents.valuation_reasoning import ValuationReadBroker
    from maestro.backends.coral.deepseek_gate import DeepSeekQuotaProxy,DEEPSEEK_TOOL
    from maestro.execution.budgets import BudgetStore
    forwarded=[];deadline=time.monotonic()+30
    files={'public/contract.json':'P=C','solver/run':'print(1)',
           'private/note.txt':'technical note'}
    broker=ValuationReadBroker(files,deadline)
    def upstream(payload, _deadline):
        forwarded.append(payload);final=len(forwarded)==2
        if final:
            assert 'tools' not in payload
            instruction=str(payload['system'])
            assert 'LAST allowed model request for VALUATION_RO' in instruction
            assert 'LAST allowed model request for PREPARATION' not in instruction
            allowed=json.loads(instruction.split('Allowed evidence.path values: ',1)[1])
            assert allowed==['public/contract.json']
            assert 'private' not in allowed and 'solver/run' not in allowed
            block={'type':'text','text':'{"ok":true}'}
        else:
            assert payload['tools'][0]['name']==DEEPSEEK_TOOL
            block={'type':'tool_use','id':'call_1','name':DEEPSEEK_TOOL,
                   'input':{'action':'read','path':'public/contract.json'}}
        events=[{'type':'message_start','message':{'type':'message','id':f'msg_{len(forwarded)}',
            'role':'assistant','model':'deepseek-flash','stop_reason':None,'content':[],
            'usage':{'input_tokens':10,'output_tokens':0}}},
            {'type':'content_block_start','index':0,'content_block':
             {'type':'text','text':''} if final else {**block,'input':{}}},
            {'type':'content_block_delta','index':0,'delta':
             {'type':'text_delta','text':block['text']} if final else
              {'type':'input_json_delta','partial_json':json.dumps(block['input'])}},
            {'type':'content_block_stop','index':0},
            {'type':'message_delta','delta':{'stop_reason':'end_turn' if final else 'tool_use'},
             'usage':{'output_tokens':3}}, {'type':'message_stop'}]
        return b''.join(b'data: '+json.dumps(event).encode()+b'\n\n' for event in events),'text/event-stream'
    store=BudgetStore(tmp_path/'budget.sqlite')
    proxy=DeepSeekQuotaProxy(store,'wire:0:agent:VALUATION_RO','deepseek-flash',
        deadline=deadline,max_calls=2,max_request_bytes=2*1024*1024,
        max_output_tokens=1000,per_request_tokens=100,allowed_tool_names=(DEEPSEEK_TOOL,),
        final_response_on_last_request=True,upstream=upstream)
    proxy.bind_final_evidence_paths(broker.evidence_paths)
    client=CoralDeepSeekClient(model='deepseek-flash',work_root=tmp_path/'native',
        executable=os.environ['MAESTRO_DSH_EXECUTABLE'],max_output_tokens=100,timeout=30)
    try:
        result=client.run_phase('Read a file, then return JSON.',base_url=proxy.start(),
            deadline=deadline,tools=broker,output_schema={'type':'object'})
        assert result.text=='{"ok":true}'
        proxy.assert_resolved()
        assert len(forwarded)==2 and broker.evidence_paths()==('public/contract.json',)
        cleanup=json.loads(next((tmp_path/'native').glob('*/cleanup.json')).read_text())
        assert cleanup['local_process_group_reclaimed'] and cleanup['tool_service_reclaimed']
    finally:client.close();proxy.close();store.close()


@pytest.mark.skipif(os.environ.get('MAESTRO_RUN_NATIVE_WIRE')!='1'
                   or not os.environ.get('MAESTRO_DSH_EXECUTABLE'),
                   reason='Opt-in installed DSH local-only wire test')
def test_installed_dsh_rejects_malformed_terminal_json_without_repair(tmp_path):
    from maestro.backends.coral.deepseek_gate import DeepSeekQuotaProxy
    from maestro.execution.budgets import BudgetStore
    forwarded=[]
    malformed='{"rationale":"brief reason"],"uncertainties":[]}'
    def upstream(payload, deadline):
        forwarded.append(payload)
        assert 'LAST allowed model request for VALUATION_RO' in str(payload['system'])
        assert 'LAST allowed model request for PREPARATION' not in str(payload['system'])
        assert 'Allowed evidence.path values: ["public/contract.json"]' in str(payload['system'])
        events=[
            {'type':'message_start','message':{'type':'message','id':'msg_1',
                'role':'assistant','model':'deepseek-flash','stop_reason':None,
                'content':[],'usage':{'input_tokens':10,'output_tokens':0}}},
            {'type':'content_block_start','index':0,'content_block':{'type':'text','text':''}},
            {'type':'content_block_delta','index':0,'delta':{'type':'text_delta','text':malformed}},
            {'type':'content_block_stop','index':0},
            {'type':'message_delta','delta':{'stop_reason':'end_turn'},'usage':{'output_tokens':20}},
            {'type':'message_stop'},
        ]
        return b''.join(b'data: '+json.dumps(event).encode()+b'\n\n' for event in events),'text/event-stream'
    store=BudgetStore(tmp_path/'budget.sqlite');deadline=time.monotonic()+30
    proxy=DeepSeekQuotaProxy(store,'wire:0:agent:VALUATION_RO','deepseek-flash',
        deadline=deadline,max_calls=1,max_request_bytes=2*1024*1024,
        max_output_tokens=1000,per_request_tokens=100,allowed_tool_names=(),
        final_response_on_last_request=True,upstream=upstream)
    proxy.bind_final_evidence_paths(lambda:('public/contract.json',))
    client=CoralDeepSeekClient(model='deepseek-flash',work_root=tmp_path/'native',
        executable=os.environ['MAESTRO_DSH_EXECUTABLE'],max_output_tokens=100,timeout=30)
    try:
        with pytest.raises(json.JSONDecodeError):
            client.run_phase('Return one complete JSON object.',base_url=proxy.start(),
                deadline=deadline,output_schema={'type':'object'})
        proxy.assert_resolved()
        assert len(forwarded)==1 and store.usage('wire:0:agent:VALUATION_RO')['calls']==1
        cleanup=json.loads(next((tmp_path/'native').glob('*/cleanup.json')).read_text())
        assert cleanup['local_process_group_reclaimed'] and cleanup['tool_service_reclaimed']
    finally:client.close();proxy.close();store.close()


@pytest.mark.skipif(os.environ.get('MAESTRO_RUN_NATIVE_WIRE')!='1'
                   or not os.environ.get('MAESTRO_DSH_EXECUTABLE'),
                   reason='Opt-in installed DSH local-only wire test')
@pytest.mark.parametrize('cap', [0, 1, 4])
def test_installed_dsh_final_preparation_request_is_tool_free(tmp_path, cap):
    from maestro.backends.coral.deepseek_gate import DeepSeekQuotaProxy,DEEPSEEK_TOOL
    from maestro.execution.budgets import BudgetStore
    forwarded=[];tool_calls=[]
    request_target=6 if cap==0 else cap
    def upstream(payload, deadline):
        forwarded.append(payload);final=len(forwarded)==request_target
        if final:
            if cap:
                assert 'tools' not in payload and 'tool_choice' not in payload
                assert 'LAST allowed model request' in str(payload['system'])
            else:assert payload['tools'][0]['name']==DEEPSEEK_TOOL
            content=[{'type':'text','text':'Evidence insufficient; no further reads.'}]
        else:
            assert payload['tools'][0]['name']==DEEPSEEK_TOOL
            content=[{'type':'tool_use','id':f'call_{len(forwarded)}','name':DEEPSEEK_TOOL,
                      'input':{'action':'list','path':'solver/'}}]
        block=content[0]
        events=[{'type':'message_start','message':{'type':'message','id':f'msg_{len(forwarded)}',
            'role':'assistant','model':'deepseek-flash','stop_reason':None,'content':[],
            'usage':{'input_tokens':10,'output_tokens':0}}},
            {'type':'content_block_start','index':0,'content_block':
             ({'type':'text','text':''} if final else {**block,'input':{}})},
            {'type':'content_block_delta','index':0,'delta':
             ({'type':'text_delta','text':block['text']} if final else
              {'type':'input_json_delta','partial_json':json.dumps(block['input'])})},
            {'type':'content_block_stop','index':0},
            {'type':'message_delta','delta':{'stop_reason':'end_turn' if final else 'tool_use'},
             'usage':{'output_tokens':3}}, {'type':'message_stop'}]
        return b''.join(b'data: '+json.dumps(event).encode()+b'\n\n' for event in events),'text/event-stream'
    store=BudgetStore(tmp_path/'budget.sqlite');deadline=time.monotonic()+30
    proxy=DeepSeekQuotaProxy(store,'wire:0:agent:PREPARATION','deepseek-flash',deadline=deadline,
        max_calls=cap,max_request_bytes=2*1024*1024,max_output_tokens=1000,
        per_request_tokens=100,allowed_tool_names=(DEEPSEEK_TOOL,),
        final_response_on_last_request=True,upstream=upstream)
    client=CoralDeepSeekClient(model='deepseek-flash',work_root=tmp_path/'native',
        executable=os.environ['MAESTRO_DSH_EXECUTABLE'],max_output_tokens=100,timeout=30)
    try:
        response=client.run_phase('PREPARATION: read evidence and finish within the configured cap.',
            base_url=proxy.start(),deadline=deadline,
            tools=lambda name,args:tool_calls.append((name,args)) or {'files':['solver/run']})
        assert response.text=='Evidence insufficient; no further reads.'
        assert len(tool_calls)==request_target-1 and len(forwarded)==request_target
        assert proxy.usage()['calls']==request_target
        proxy.assert_resolved()
        cleanup=json.loads(next((tmp_path/'native').glob('*/cleanup.json')).read_text())
        assert cleanup['local_process_group_reclaimed'] and cleanup['tool_service_reclaimed']
    finally:client.close();proxy.close();store.close()


def test_actual_coral_deepseek_runtime_start_and_scrubbed_prefix(tmp_path,monkeypatch):
    from coral.agent.builtin.deepseek_harness import DeepSeekHarnessRuntime
    client=CoralDeepSeekClient(model='offline-deepseek',work_root=tmp_path/'native',
        executable='/bin/true',runtime=DeepSeekHarnessRuntime())
    monkeypatch.setenv('DEEPSEEK_API_KEY','DO-NOT-INHERIT')
    monkeypatch.setenv('NODE_OPTIONS','DO-NOT-INHERIT')
    calls=[]
    class Process:
        pid=987654321;returncode=0;stdout=stderr=None
        def poll(self):return 0
    def popen(argv,**kwargs):
        data=json.loads(Path(argv[argv.index('--scope')+1]).read_text())
        actual=command(data,argv[argv.index('--')+1:])
        clean=environment(data)
        assert clean['DEEPSEEK_API_KEY']=='maestro-phase-gateway-only'
        assert 'NODE_OPTIONS' not in clean and 'DO-NOT-INHERIT' not in json.dumps(data)
        assert actual[-1]==data['prompt']
        patches=yaml.safe_load(Path(data['policy_patch']).read_text())
        rows={p['id']:p for p in patches if 'id' in p}
        assert rows['llm-retry']['disabled']
        assert rows['tool-fs']['disabled'] and rows['tool-bash']['disabled']
        assert rows['session-title-llm']['disabled'] and rows['session-telemetry-otel']['disabled']
        assert rows['llm-deepseek']['config']['retryPolicy']['maxRetries']==0
        assert rows['llm-deepseek']['config']['baseURL'].startswith('http://127.0.0.1:')
        assert patches[-1]['insert'][0]['config']['serverName']=='maestro'
        assert kwargs['stderr']!=__import__('subprocess').STDOUT
        kwargs['stdout'].write('autonomous preparation result\n');kwargs['stdout'].flush()
        calls.append(actual)
        return Process()
    monkeypatch.setattr('coral.agent.builtin.deepseek_harness.subprocess.Popen',popen)
    def absent(*args):raise ProcessLookupError()
    monkeypatch.setattr('maestro.backends.coral.native_client.os.killpg',absent)
    result=client.run_phase('PREPARATION: choose files',base_url='http://127.0.0.1:12345/scope/v1',
        deadline=time.monotonic()+10,tools=lambda *args:{})
    assert result.text=='autonomous preparation result' and len(calls)==1
    cleanup=json.loads(next((tmp_path/'native').glob('*/cleanup.json')).read_text())
    assert cleanup['local_process_group_reclaimed'] and cleanup['tool_service_reclaimed']
    client.close()


def test_deepseek_cleanup_records_reclaimed_tool_listener_with_unknown_tool_call(tmp_path,monkeypatch):
    import urllib.error
    import urllib.request
    from coral.agent.builtin.deepseek_harness import DeepSeekHarnessRuntime
    from maestro.backends.coral.native_client import NativeCoralUnknown
    from maestro.llm import InferenceRequestUnknown

    client=CoralDeepSeekClient(model='offline-deepseek',work_root=tmp_path/'native',
        executable='/bin/true',runtime=DeepSeekHarnessRuntime())
    class Process:
        pid=987654322;returncode=0;stdout=stderr=None
        def poll(self):return 0
    def popen(argv,**kwargs):
        data=json.loads(Path(argv[argv.index('--scope')+1]).read_text())
        patches=yaml.safe_load(Path(data['policy_patch']).read_text())
        endpoint=patches[-1]['insert'][0]['config']['env']['MAESTRO_TOOL_ENDPOINT']
        request=urllib.request.Request(endpoint,data=b'{}',headers={'Content-Type':'application/json'})
        with pytest.raises(urllib.error.HTTPError) as response:
            urllib.request.urlopen(request,timeout=3)
        assert response.value.code==403
        kwargs['stdout'].write('offline result\n');kwargs['stdout'].flush()
        return Process()
    monkeypatch.setattr('coral.agent.builtin.deepseek_harness.subprocess.Popen',popen)
    monkeypatch.setattr('maestro.backends.coral.native_client.os.killpg',
                        lambda *args:(_ for _ in ()).throw(ProcessLookupError()))
    def unknown_tool(_name,_args):
        raise InferenceRequestUnknown('Unresolved request',request_id='offline',scope_id='offline',context={})
    with pytest.raises(NativeCoralUnknown):
        client.run_phase('offline valuation',base_url='http://127.0.0.1:12345/scope/v1',
            deadline=time.monotonic()+10,tools=unknown_tool)
    cleanup=json.loads(next((tmp_path/'native').glob('*/cleanup.json')).read_text())
    assert cleanup['local_process_group_reclaimed'] is True
    assert cleanup.get('tool_service_reclaimed') is not True
    assert cleanup['tool_service_close_receipt']['transport_reclaimed'] is True
    assert cleanup['tool_service_close_receipt']['tool_error_type']=='InferenceRequestUnknown'
    assert cleanup['tool_service_close_error']['exception_class']=='NativeCoralUnknown'
    client.close()


def test_unexpected_native_flags_rejected(tmp_path):
    scope={'executable':'dsh','prompt':'task','model_patch':'model.yml','policy_patch':'policy.yml'}
    expected=['dsh','--profile','headless','--patch','model.yml','task']
    assert command(scope,expected)==expected[:-1]+['--patch','policy.yml','task']
    with pytest.raises(PermissionError):command(scope,expected+['--session-id','old'])


def test_missing_unqualified_cli_stops_without_inference(tmp_path):
    from maestro.backends.base import BackendUnavailable
    client=CoralDeepSeekClient(model='offline',work_root=tmp_path,executable='/does/not/exist')
    with pytest.raises(BackendUnavailable,match='Install'):client.preflight()
    client=CoralDeepSeekClient(model='offline',work_root=tmp_path,executable=sys.executable)
    with pytest.raises(BackendUnavailable,match='Freeze'):client.preflight()


@pytest.mark.skipif(os.name!='posix',reason='POSIX process-group cancellation')
def test_real_local_process_cancelled_and_cleanup_recorded(tmp_path):
    # A local inert CLI, not the model runtime. Exercises Coral Popen, enforcing
    # prefix, wall deadline and actual process-group reclamation.
    script=tmp_path/'inert-dsh'
    script.write_text('#!'+sys.executable+'\nimport time\ntime.sleep(10)\n');script.chmod(0o700)
    client=CoralDeepSeekClient(model='offline',work_root=tmp_path/'native',executable=str(script))
    with pytest.raises(TimeoutError):
        client.run_phase('cancel local process',base_url='http://127.0.0.1:12345/scope/v1',
            deadline=time.monotonic()+.4)
    cleanup=json.loads(next((tmp_path/'native').glob('*/cleanup.json')).read_text())
    assert cleanup['local_process_group_reclaimed'] and not client._active
    client.close()
