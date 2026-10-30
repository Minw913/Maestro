"""Upstream Coral dsh lifecycle, isolated configuration and broker-only tools."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import uuid

import yaml

from maestro.backends.base import BackendUnavailable
from maestro.backends.codex_session.autonomous_client import _loopback_url
from maestro.types import stable_hash
from .deepseek_exec import DISABLED
from .deepseek_lifecycle import exception_details
from .native_client import ToolService, CoralCodexClient, NativeCoralUnknown


class CoralDeepSeekClient:
    supports_phase_identity=True
    supports_resume=False
    _stop_handle=CoralCodexClient._stop_handle  # Shared proven process-group cleanup.

    def __init__(self,*,model,work_root,executable='dsh',cli_version='',cli_sha256='',
                 reasoning_effort='low',max_output_tokens=32000,timeout=1200,runtime=None):
        self.model,self.root=model,Path(work_root).resolve();self.root.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.executable=str(Path(shutil.which(executable) or executable).resolve())
        self.cli_version,self.cli_sha256=cli_version,cli_sha256
        self.effort,self.max_tokens,self.timeout=reasoning_effort,max_output_tokens,timeout
        if runtime is None:
            from .runtime import resolve_runtime
            runtime=resolve_runtime('dsh')
        self.runtime=runtime;self._active=set();self._lock=threading.RLock()
        self._cleanup_by_url={}
        self.max_response_bytes=32*1024*1024

    def preflight(self):
        if not Path(self.executable).is_file():raise BackendUnavailable('Install the DeepSeek Harness dsh executable')
        if len(self.cli_sha256)!=64 or self.cli_version in ('','UNQUALIFIED'):
            raise BackendUnavailable('Freeze coral.deepseek_cli_version and deepseek_cli_sha256 before execution')
        digest=hashlib.sha256(Path(self.executable).read_bytes()).hexdigest()
        if digest!=self.cli_sha256:raise BackendUnavailable('DeepSeek executable SHA256 mismatch')
        # Version/help only, in fresh HOME/DSH_HOME with no provider credential.
        with tempfile.TemporaryDirectory(prefix='maestro-dsh-check-') as home:
            env={'PATH':os.environ.get('PATH',''),'HOME':home,'DSH_HOME':home,'DSH_TELEMETRY_DISABLED':'1'}
            try:
                version=subprocess.run([self.executable,'--version'],cwd=home,env=env,
                    capture_output=True,text=True,timeout=15,check=True).stdout.strip()
                help_text=subprocess.run([self.executable,'--help'],cwd=home,env=env,
                    capture_output=True,text=True,timeout=15,check=True).stdout
            except (OSError,subprocess.SubprocessError) as exc:
                raise BackendUnavailable('DeepSeek CLI version/help check failed') from exc
        if version!=self.cli_version:raise BackendUnavailable('DeepSeek CLI version mismatch')
        if '--profile' not in help_text or '--patch' not in help_text:
            raise BackendUnavailable('DeepSeek CLI lacks required profile/patch protocol')
        return {'cli_version':version,'cli_sha256':digest,'agent_lifecycle':
            'coral.agent.builtin.deepseek_harness.DeepSeekHarnessRuntime','live_qualified':False}

    def run_phase(self,prompt,*,base_url,deadline,tools=None,output_schema=None,session_id=None,agent_id=None):
        from coral.sandbox.protocol import AgentSandboxSpec
        if session_id:raise ValueError('DeepSeek phases are fresh; resume is disabled')
        if not isinstance(prompt,str) or not prompt or len(prompt.encode())>256*1024:raise ValueError('Bounded prompt required')
        base_url=_loopback_url(base_url)
        deadline=min(deadline,time.monotonic()+self.timeout)
        if deadline<=time.monotonic():raise TimeoutError('DeepSeek phase expired before launch')
        if output_schema is not None:
            prompt+='\nReturn only JSON matching this schema, without Markdown:\n'+json.dumps(output_schema)
        identity=agent_id or 'deepseek-native'
        if not identity or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in identity):
            raise ValueError('Unsafe agent identity')
        phase=self.root/uuid.uuid4().hex;phase.mkdir(mode=0o700)
        workdir=Path(tempfile.mkdtemp(prefix='maestro-coral-dsh-'))
        home=workdir/'home';home.mkdir(mode=0o700)
        (workdir/'.coral_agent_id').write_text(identity)
        service=None;handle=None
        try:
            service=ToolService(tools,deadline) if tools is not None else None
            patches=[{'id':key,'disabled':True} for key in DISABLED]
            patches.append({'id':'llm-deepseek','config':{'baseURL':base_url,
                'apiKeyEnv':'DEEPSEEK_API_KEY','maxTokens':self.max_tokens,
                'thinking':'disabled' if self.effort=='off' else 'enabled','reasoningEffort':self.effort,
                'retryPolicy':{'mode':'normal','maxRetries':0}}})
            patches.append({'id':'tools','config':{'mode':'native'}})
            if service:
                patches.append({'insert':[{'id':'maestro-mcp','name':'@deepseek-ai/dsh-mcp-client',
                    'config':{'transport':'stdio','serverName':'maestro','command':sys.executable,
                    'args':['-m','maestro.backends.coral.mcp_bridge'],
                    'env':{'MAESTRO_TOOL_ENDPOINT':service.url,'MAESTRO_TOOL_DEADLINE':str(deadline),
                           'PYTHONPATH':str(Path(__file__).resolve().parents[3])},
                    'failOnStartupError':True,'reconnect':{'enabled':False}}}]})
            policy=phase/'policy.yml';policy.write_text(yaml.safe_dump(patches,sort_keys=False))
            instructions=phase/'instructions.md';instructions.write_text(prompt)
            environment={key:os.environ[key] for key in ('PATH','LANG','LC_ALL','SSL_CERT_FILE','SSL_CERT_DIR') if key in os.environ}
            environment['PYTHONPATH']=str(Path(__file__).resolve().parents[3])
            scope={'executable':self.executable,'prompt':prompt,'base_url':base_url,
                'policy_patch':str(policy),'model_patch':str(workdir/'.dsh/coral-model.patch.yml'),
                'dsh_home':str(workdir/'.dsh'),'home':str(home),'environment':environment}
            scope_path=phase/'scope.json';scope_path.write_text(json.dumps(scope));scope_path.chmod(0o600)
            spec=AgentSandboxSpec(command_prefix=[sys.executable,'-m','maestro.backends.coral.deepseek_exec',
                '--scope',str(scope_path),'--'],env=environment)
            handle=self.runtime.start(worktree_path=workdir,coral_md_path=instructions,model=self.model,
                runtime_options={'command':self.executable,'profile':'headless','provider':'deepseek-official'},
                max_turns=0,log_dir=phase/'public/logs',prompt=prompt,prompt_source='maestro:deepseek',sandbox=spec)
            with self._lock:self._active.add(handle.process.pid)
            while handle.alive:
                pause=getattr(self,'pause',None)
                # Give the transport grace to persist a terminal receipt first.
                local_deadline=min(deadline,pause.drain_deadline(deadline)+2) if pause else deadline
                if time.monotonic()>=local_deadline:raise TimeoutError('DeepSeek common/drain deadline elapsed')
                if (handle.log_path.stat().st_size>self.max_response_bytes
                        or (handle.err_path and handle.err_path.stat().st_size>self.max_response_bytes)):
                    raise ValueError('DeepSeek log exceeds cap')
                time.sleep(.02)
            if handle.process.returncode!=0:raise BackendUnavailable('DeepSeek native CLI failed; inspect private phase logs')
            if handle.log_path.stat().st_size>self.max_response_bytes:raise ValueError('DeepSeek output exceeds cap')
            lines=handle.log_path.read_text().splitlines()
            # Coral prepends exactly one prompt record; dsh then prints its
            # final assistant text. Its diagnostics go to separate stderr.
            if not lines or json.loads(lines[0]).get('type')!='coral':raise ValueError('Missing Coral log header')
            text='\n'.join(lines[1:]).strip()
            if not text:raise ValueError('DeepSeek final output missing')
            if output_schema is not None:json.loads(text)  # No repair or extra request.
            if time.monotonic()>=deadline:raise TimeoutError('DeepSeek result missed common deadline')
            return SimpleNamespace(text=text,response_hash=stable_hash(text),thread_id='',turn_id='',
                tool_calls=service.calls if service else 0)
        finally:
            cleanup={'cleanup_started_monotonic':time.monotonic(),'phase_deadline_monotonic':deadline,
                     'provider_termination_confirmed':False,'local_process_group_reclaimed':False}
            try:
                if handle:self._stop_handle(handle)
                cleanup['local_process_group_reclaimed']=True
            finally:
                try:
                    if service:
                        try:service.close()
                        except BaseException as error:
                            # Keep the scope fail-closed, but distinguish an
                            # unreclaimed listener from a reclaimed listener
                            # carrying an unresolved tool execution error.
                            cleanup['tool_service_close_error']=exception_details(error)
                            raise
                    cleanup['tool_service_reclaimed']=True
                finally:
                    if service and service.close_receipt is not None:
                        cleanup['tool_service_close_receipt']=service.close_receipt
                    cleanup['cleanup_finished_monotonic']=time.monotonic()
                    (phase/'cleanup.json').write_text(json.dumps(cleanup,indent=2))
                    with self._lock:self._cleanup_by_url[base_url]={**cleanup,'phase_path':str(phase)}
                with self._lock:reclaimed=handle is None or handle.process.pid not in self._active
                if reclaimed:shutil.rmtree(workdir)

    def close(self):
        with self._lock:
            if self._active:raise NativeCoralUnknown('DeepSeek native process reclamation unconfirmed')

    def cleanup_receipt(self,base_url):
        with self._lock:return dict(self._cleanup_by_url.get(base_url,{}))
