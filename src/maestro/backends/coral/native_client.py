"""Coral's actual CodexRuntime lifecycle with MAESTRO-gated MCP workspace tools.

Uses upstream runtime.start(), AgentSandboxSpec, AgentHandle and JSONL logs.
No replacement model/tool loop is implemented here. Codex runs its native loop;
MCP delegates file/sandbox operations to the existing capability broker.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
import secrets
import signal
import sys
import threading
import tempfile
import shutil
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

from maestro.backends.base import BackendUnavailable
from maestro.backends.codex_session.autonomous_client import AutonomousCodexClient, _loopback_url
from maestro.backends.codex_session.client import _run
from maestro.types import stable_hash

MCP_TOOL = 'maestro_tool'
MCP_NAMESPACE = 'mcp__maestro'


class NativeCoralUnknown(BackendUnavailable):
    inflight_unknown = True
    local_quiescent = False


class ToolService:
    def __init__(self, callback, deadline):
        self.callback, self.deadline = callback, deadline
        self.error = None
        self.calls = 0
        self.pending = 0
        self.condition = threading.Condition()
        self.lock = threading.RLock()
        capability = '/'+secrets.token_urlsafe(32)
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                with owner.condition:
                    owner.pending += 1
                try:
                    if self.path != capability or time.monotonic() >= owner.deadline:
                        raise PermissionError('Expired tool capability')
                    length = self.headers.get('Content-Length', '')
                    if not length.isdecimal() or not 0 < int(length) <= 1024*1024:
                        raise ValueError('Invalid tool body size')
                    self.connection.settimeout(max(.001, owner.deadline-time.monotonic()))
                    arguments = json.loads(self.rfile.read(int(length)))
                    with owner.lock:
                        owner.calls += 1
                        value = owner.callback('maestro_tool', arguments)
                    payload = json.dumps(value, ensure_ascii=False).encode()
                    code = 200
                except Exception as error:
                    if getattr(error, 'inflight_unknown', False) or getattr(error, 'execution_unknown', False):
                        owner.error = error
                    payload = b'{"error":"Tool operation rejected"}'
                    code = 403
                try:
                    self.send_response(code)
                    self.send_header('Content-Length', str(len(payload)))
                    self.end_headers(); self.wfile.write(payload)
                except OSError:
                    pass
                finally:
                    with owner.condition:
                        owner.pending -= 1
                        owner.condition.notify_all()
        class Server(ThreadingHTTPServer):
            daemon_threads = True
        self.server = Server(('127.0.0.1', 0), Handler)
        self.server.timeout = .1
        self.url = f'http://127.0.0.1:{self.server.server_port}{capability}'
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        with self.condition:
            self.condition.wait_for(lambda: self.pending == 0, timeout=5)
            pending = self.pending
        if pending or self.thread.is_alive():
            raise NativeCoralUnknown('Coral MCP service termination not established')
        if self.error:
            raise NativeCoralUnknown('Coral tool execution requires reconciliation') from self.error


class CoralCodexClient(AutonomousCodexClient):
    supports_phase_identity = True
    supports_resume = False  # Each native phase is ephemeral; files carry memory.

    def __init__(self, *args, runtime=None, **kwargs):
        super().__init__(*args, **kwargs)
        if runtime is None:
            from .runtime import resolve_runtime
            runtime = resolve_runtime('codex')
        self.runtime = runtime
        self._active = set()
        self._lock = threading.RLock()

    def preflight(self, **kwargs):
        report = super().preflight(**kwargs)
        help_text = _run([str(self.executable), 'exec', '--help'], input_text='',
                         cwd=self.root, env=self.env, timeout=15, max_bytes=65536)
        for flag in ('--ignore-user-config', '--ignore-rules', '--ephemeral',
                     '--output-last-message', '--output-schema'):
            if flag not in help_text.stdout:
                raise BackendUnavailable('Coral native Codex requires '+flag)
        return {**report, 'agent_lifecycle': 'coral.agent.builtin.codex.CodexRuntime',
                'workspace_transport': 'phase_bound_mcp', 'live_qualified': False}

    def run_phase(self, prompt, *, base_url, deadline, tools=None, output_schema=None, session_id=None, agent_id=None):
        from coral.sandbox.protocol import AgentSandboxSpec
        from .native_exec import command
        base_url = _loopback_url(base_url)
        if not isinstance(prompt, str) or not prompt or len(prompt.encode()) > 256*1024:
            raise ValueError('Bounded nonempty prompt required')
        deadline = min(deadline, time.monotonic()+self.timeout)
        if deadline <= time.monotonic():
            raise TimeoutError('Coral phase missed its deadline before dispatch')
        if session_id:
            raise ValueError('Coral native phases are ephemeral; resume is not enabled')
        phase = self.root/uuid.uuid4().hex
        phase.mkdir(mode=0o700)
        workdir = Path(tempfile.mkdtemp(prefix='maestro-coral-'))
        identity = agent_id or 'coral-native'
        if not identity or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in identity):
            raise ValueError('Unsafe Coral agent identity')
        (workdir/'.coral_agent_id').write_text(identity)
        (phase/'workdir.json').write_text(json.dumps({'path':str(workdir)}))
        result_path = phase/'result.txt'
        schema_path = phase/'schema.json'
        if output_schema is not None:
            schema_path.write_text(json.dumps(output_schema))
        argv = self._argv(base_url)
        settings = {}
        # Reuse the audited provider/feature policy, without the app-server
        # transport. Ignore-user-config prevents inherited MCP/plugin merging.
        for index in range(5, len(argv), 2):
            key, value = argv[index].split('=', 1)
            import tomllib
            settings[key] = tomllib.loads('value='+value)['value']
        settings.pop('features.code_mode', None)
        settings.pop('tools.view_image', None)
        service = ToolService(tools, deadline) if tools is not None else None
        handle = None
        try:
            settings['features.code_mode.direct_only_tool_namespaces'] = ['mcp__maestro']
            settings['features.code_mode_only'] = False
            if service:
                settings['mcp_servers'] = {'maestro': {
                    'enabled': True, 'command': sys.executable,
                    'default_tools_approval_mode': 'approve',
                    'args': ['-m', 'maestro.backends.coral.mcp_bridge'],
                    'env': {'MAESTRO_TOOL_ENDPOINT': service.url,
                            'MAESTRO_TOOL_DEADLINE': str(deadline)},
                    'startup_timeout_sec': min(10.0, max(.001, deadline-time.monotonic())),
                    'tool_timeout_sec': max(.001, deadline-time.monotonic())}}
            settings['developer_instructions'] = (
                'Use only the phase-authorized maestro MCP tool for virtual solver/, private/, public/ paths. '
                'Native host tools and direct filesystem access are unavailable. Respect all tool denials.')
            scope = {'executable': str(self.executable), 'model': self.model, 'prompt': prompt,
                     'settings': settings, 'result_path': str(result_path),
                     'schema_path': str(schema_path) if output_schema is not None else None}
            scope_path = phase/'scope.json'; scope_path.write_text(json.dumps(scope)); scope_path.chmod(0o600)
            # Validate against upstream's pinned argv shape before runtime.start.
            command(scope, ['codex', 'exec', prompt, '--dangerously-bypass-approvals-and-sandbox',
                            '--model', self.model, '--json'])
            spec = AgentSandboxSpec(command_prefix=[sys.executable, '-m',
                'maestro.backends.coral.native_exec', '--scope', str(scope_path), '--'],
                env={key:value for key,value in self.env.items() if key in {
                    'HOME','USER','LOGNAME','PATH','CODEX_HOME','PYTHONPATH','LANG','LC_ALL',
                    'TMPDIR','SSL_CERT_FILE','SSL_CERT_DIR','HTTPS_PROXY','HTTP_PROXY','ALL_PROXY','NO_PROXY'}})
            instructions = phase/'instructions.md'; instructions.write_text(prompt)
            handle = self.runtime.start(worktree_path=workdir, coral_md_path=instructions,
                model=self.model, runtime_options={}, max_turns=0, log_dir=phase/'logs',
                prompt=prompt, prompt_source='maestro:coral-native',
                task_name='MAESTRO', task_description='Market-controlled Coral agent', sandbox=spec)
            with self._lock:
                self._active.add(handle.process.pid)
            while handle.alive:
                if time.monotonic() >= deadline:
                    raise TimeoutError('Coral native phase deadline elapsed')
                if handle.log_path.stat().st_size > self.max_response_bytes:
                    raise BackendUnavailable('Coral native output allowance exceeded')
                time.sleep(min(.02, max(.001, deadline-time.monotonic())))
            if handle.process.returncode != 0:
                raise BackendUnavailable('Coral native runtime failed; inspect private phase logs')
            if not result_path.exists() or result_path.stat().st_size > self.max_response_bytes:
                raise BackendUnavailable('Coral native final output missing or oversized')
            text = result_path.read_text()
            if time.monotonic() >= deadline:
                raise TimeoutError('Coral native result arrived after common deadline')
            return SimpleNamespace(text=text, thread_id='', turn_id='', response_hash=stable_hash(text),
                                   tool_calls=service.calls if service else 0)
        finally:
            cleanup_started = time.monotonic()
            cleanup = {'phase_deadline_monotonic':deadline,
                       'cleanup_started_monotonic':cleanup_started,
                       'provider_termination_confirmed':False,
                       'local_process_group_reclaimed':False}
            try:
                if handle is not None:
                    self._stop_handle(handle)
                cleanup['local_process_group_reclaimed'] = True
            finally:
                try:
                    if service:
                        service.close()
                    cleanup['tool_service_reclaimed'] = True
                finally:
                    cleanup['cleanup_finished_monotonic'] = time.monotonic()
                    (phase/'cleanup.json').write_text(json.dumps(cleanup,indent=2))
                # Preserve the private scope and its empty native working dir
                # when process reclamation is unknown. Never erase evidence.
                with self._lock:
                    reclaimed = handle is None or handle.process.pid not in self._active
                if reclaimed:
                    shutil.rmtree(workdir)

    def _stop_handle(self, handle):
        pid = handle.process.pid
        try:
            handle.stop()
            # Upstream stop() skips the group once its CLI leader has exited.
            # Kill/reap any MCP descendants before accepting phase completion.
            try:
                os.killpg(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            until = time.monotonic()+5
            while True:
                try:
                    os.killpg(pid, 0)
                except ProcessLookupError:
                    break
                if time.monotonic() >= until:
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    raise NativeCoralUnknown('Coral descendant process reclamation unconfirmed')
                time.sleep(.02)
        except (OSError, __import__('subprocess').TimeoutExpired) as error:
            raise NativeCoralUnknown('Coral runtime termination unconfirmed') from error
        else:
            with self._lock:
                self._active.discard(pid)

    def close(self):
        with self._lock:
            if self._active:
                raise NativeCoralUnknown('Coral phases still active; preserve runtime evidence')
