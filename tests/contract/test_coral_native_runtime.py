"""Offline upstream-Coral lifecycle and native prefix policy. No model calls."""
import io
import json
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from maestro.backends.coral.native_client import CoralCodexClient, ToolService, NativeCoralUnknown
from maestro.backends.coral.native_exec import command
from maestro.backends.coral.mcp_bridge import serve
from maestro.backends.codex_session.tools import StageToolBroker
from maestro.backends.coral.workspace import WorkspaceStore


def scope(tmp_path):
    return {'executable':'/usr/bin/codex','model':'test-model','prompt':'bounded phase',
            'settings':{'features.shell_tool':False},'result_path':str(tmp_path/'result'), 'schema_path':None}


def original(data):
    return ['codex','exec',data['prompt'],'--dangerously-bypass-approvals-and-sandbox',
            '--model',data['model'],'--json']


def test_prefix_replaces_unrestricted_upstream_flags(tmp_path):
    data=scope(tmp_path)
    argv=command(data,original(data))
    assert '--dangerously-bypass-approvals-and-sandbox' not in argv
    assert argv[argv.index('--sandbox')+1]=='read-only'
    assert {'--ignore-user-config','--ignore-rules','--ephemeral'} <= set(argv)
    assert 'features.shell_tool=false' in argv
    with pytest.raises(PermissionError):
        command(data,original(data)+['--add-dir','/'])
    with pytest.raises(PermissionError):
        command(data,['bash','-c','unsafe'])


def test_mcp_read_only_and_closed_capability(tmp_path, monkeypatch):
    seed=tmp_path/'seed';seed.mkdir();(seed/'run').write_text('frozen')
    workspaces=WorkspaceStore(tmp_path/'workspaces',tmp_path/'cas')
    workspace=workspaces.create('agent', seed)
    broker=StageToolBroker(workspace,phase='PREPARATION',deadline=time.monotonic()+10)
    service=ToolService(lambda name,args:broker(args),time.monotonic()+10)
    monkeypatch.setenv('MAESTRO_TOOL_DEADLINE',str(time.monotonic()+10))
    lines=[{'jsonrpc':'2.0','id':1,'method':'initialize'},
           {'jsonrpc':'2.0','id':2,'method':'tools/list'},
           {'jsonrpc':'2.0','id':3,'method':'tools/call','params':{'name':'maestro_tool',
             'arguments':{'action':'read','path':'solver/run'}}},
           {'jsonrpc':'2.0','id':4,'method':'tools/call','params':{'name':'maestro_tool',
             'arguments':{'action':'write','path':'solver/run','content':'bad'}}}]
    output=io.StringIO()
    try:
        serve(io.StringIO(''.join(json.dumps(v)+'\n' for v in lines)),output,service.url)
        replies=[json.loads(v) for v in output.getvalue().splitlines()]
        assert 'frozen' in replies[2]['result']['content'][0]['text']
        assert 'error' in replies[3]
        assert (workspace.solver/'run').read_text()=='frozen'
    finally:
        broker.close();service.close()


def test_tool_service_distinguishes_reclaimed_listener_from_unknown_tool_error():
    import urllib.error
    import urllib.request
    from maestro.llm import InferenceRequestUnknown

    def unknown_tool(_name,_args):
        raise InferenceRequestUnknown('Unresolved request',request_id='offline',scope_id='offline',context={})

    service=ToolService(unknown_tool,time.monotonic()+10)
    try:
        request=urllib.request.Request(service.url,data=b'{}',headers={'Content-Type':'application/json'})
        with pytest.raises(urllib.error.HTTPError) as response:
            urllib.request.urlopen(request,timeout=3)
        assert response.value.code==403
        with pytest.raises(NativeCoralUnknown):service.close()
        assert service.close_receipt=={
            'server_thread_reclaimed':True,'pending_calls':0,'transport_reclaimed':True,
            'tool_error_type':'InferenceRequestUnknown'}
    finally:
        if service.thread.is_alive():service.close()


def test_actual_upstream_runtime_start_is_used_without_live_inference(tmp_path,monkeypatch):
    from coral.agent.builtin.codex import CodexRuntime
    # Only the subprocess is injected. Upstream start(), command construction,
    # AgentSandboxSpec and AgentHandle are exercised unchanged.
    client=CoralCodexClient(model='test-model',work_root=tmp_path/'native',
                           executable='/bin/true',runtime=CodexRuntime())
    calls=[]
    class Process:
        pid=987654321
        returncode=0
        stdout=stderr=None
        def poll(self):return 0
    def popen(argv,**kwargs):
        calls.append((argv,kwargs))
        data=json.loads(Path(argv[argv.index('--scope')+1]).read_text())
        sanitized=command(data,argv[argv.index('--')+1:])
        assert '--ignore-user-config' in sanitized
        assert data['settings']['model_providers.maestro_quota.requires_openai_auth'] is True
        Path(data['result_path']).write_text('native lifecycle result')
        return Process()
    monkeypatch.setattr('coral.agent.builtin.codex.subprocess.Popen',popen)
    def absent(*args):raise ProcessLookupError()
    monkeypatch.setattr('maestro.backends.coral.native_client.os.killpg',absent)
    response=client.run_phase('bounded phase',base_url='http://127.0.0.1:12345/private',
                             deadline=time.monotonic()+10)
    assert response.text=='native lifecycle result'
    assert len(calls)==1 and calls[0][1]['start_new_session'] is True
    cleanup=json.loads(next((tmp_path/'native').glob('*/cleanup.json')).read_text())
    assert cleanup['local_process_group_reclaimed'] is True
    assert cleanup['tool_service_reclaimed'] is True
    assert cleanup['provider_termination_confirmed'] is False
    assert cleanup['cleanup_finished_monotonic'] >= cleanup['cleanup_started_monotonic']
    client.close()


def test_namespaced_gate_rejects_spoofed_or_flattened_calls(tmp_path):
    from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy, QuotaProxyError
    from maestro.execution.budgets import BudgetStore
    store=BudgetStore(tmp_path/'budget.sqlite')
    try:
        proxy=CodexQuotaProxy(store,'native:0:a:PREPARATION','test-model',deadline=time.monotonic()+5,
            max_calls=4,max_request_bytes=10000,max_output_tokens=100,
            allowed_tool_names=('maestro_tool',),tool_namespace='mcp__maestro')
        proxy._validate_item({'type':'function_call','namespace':'mcp__maestro','name':'maestro_tool'})
        for value in [
            {'type':'function_call','namespace':'functions','name':'maestro_tool'},
            {'type':'function_call','namespace':'mcp__peer','name':'maestro_tool'},
            {'type':'function_call','name':'maestro_tool'},
            {'type':'function_call','name':'mcp__maestro__maestro_tool'},
        ]:
            with pytest.raises(QuotaProxyError):proxy._validate_item(value)
    finally:store.close()


def test_unconfirmed_native_descendant_reclamation_remains_blocking(tmp_path,monkeypatch):
    client=CoralCodexClient(model='test-model',work_root=tmp_path/'native',
                           executable='/bin/true',runtime=object())
    pid=987654321
    client._active.add(pid)
    handle=SimpleNamespace(process=SimpleNamespace(pid=pid),stop=lambda:None)
    monkeypatch.setattr('maestro.backends.coral.native_client.os.killpg',lambda *args:None)
    ticks=iter([0,6])
    monkeypatch.setattr('maestro.backends.coral.native_client.time.monotonic',lambda:next(ticks))
    with pytest.raises(NativeCoralUnknown,match='reclamation unconfirmed'):
        client._stop_handle(handle)
    with pytest.raises(NativeCoralUnknown,match='still active'):
        client.close()


def test_phase_deadline_reclaims_real_local_process_and_records_cleanup(tmp_path):
    import os,signal,subprocess
    class Runtime:
        handle=None
        def start(self,**kwargs):
            logs=kwargs['log_dir'];logs.mkdir(parents=True,exist_ok=True)
            log=logs/'offline.log';log.write_text('')
            process=subprocess.Popen(['/bin/sleep','30'],start_new_session=True)
            class Handle:
                log_path=log
                @property
                def alive(self):return self.process.poll() is None
                def stop(self):
                    if self.process.poll() is None:os.killpg(self.process.pid,signal.SIGTERM)
                    self.process.wait(timeout=5)
            self.handle=Handle();self.handle.process=process
            return self.handle
    runtime=Runtime()
    client=CoralCodexClient(model='test-model',work_root=tmp_path/'native',executable='/bin/true',runtime=runtime)
    with pytest.raises(TimeoutError):
        client.run_phase('offline deadline acceptance',base_url='http://127.0.0.1:12345/private',deadline=time.monotonic()+.3)
    assert runtime.handle.process.poll() is not None
    cleanup=json.loads(next((tmp_path/'native').glob('*/cleanup.json')).read_text())
    assert cleanup['local_process_group_reclaimed'] and cleanup['tool_service_reclaimed']
    assert cleanup['provider_termination_confirmed'] is False
    assert cleanup['cleanup_started_monotonic'] >= cleanup['phase_deadline_monotonic']
    client.close()
