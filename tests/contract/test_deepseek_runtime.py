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
