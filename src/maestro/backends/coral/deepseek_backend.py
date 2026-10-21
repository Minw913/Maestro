"""Additive Coral DeepSeek backend; Codex factory and authentication are untouched."""
from __future__ import annotations
import os
from pathlib import Path
import re
import time

from maestro.backends.base import BackendUnavailable
from maestro.execution.budgets import BudgetStore
from maestro.execution.grants import GrantError
from maestro.execution.sandbox import DockerRunner
from maestro.tasks.pace.manifest import assert_smoke_disjoint,load_panel
from maestro.tasks.pace.runner import smoke_test
from .deepseek_client import CoralDeepSeekClient
from .deepseek_gate import DeepSeekQuotaProxy,DeepSeekUpstream,DEEPSEEK_TOOL,provider_endpoint
from .native_backend import NativeCoralBackend
from .runtime import verify_checkout
from .workspace import WorkspaceStore
from .deepseek_lifecycle import check_restart_ledgers
from maestro.execution.pause import PauseController


class NativeDeepSeekBackend(NativeCoralBackend):
    """Share stage/grant/ticket semantics, override only the provider boundary."""
    def _dispatch_guard(self):
        pause=getattr(self,'pause',None)
        if pause:pause.check()
        return super()._dispatch_guard()

    def _proxy(self,account,deadline,calls,*,tools=False):
        return self.proxy_factory(self.budget_store,account,self.client.model,deadline=deadline,
            max_calls=calls,max_request_bytes=16*1024*1024,max_output_tokens=96000,
            per_request_tokens=self.config.coral.get('deepseek_max_output_tokens',32000),
            reasoning_effort=self.config.coral.get('deepseek_reasoning_effort','low'),
            allowed_tool_names=(DEEPSEEK_TOOL,) if tools else (),request_guard=self._dispatch_guard)

    def preflight(self,config=None):
        config=config or self.config
        if self._closed or config.profile!='v1_cooperative':raise BackendUnavailable('DeepSeek requires open v1_cooperative backend')
        with self._phase_lock:self._assert_clear()
        if not self.seed_solver.is_dir() or self._precheck is None:raise BackendUnavailable('Seed and bounded fixed precheck required')
        if self.simulation:
            if config.execution_mode!='mock':raise BackendUnavailable('Offline DeepSeek transport cannot run a real experiment')
            protocol={'simulation':True}
        else:
            settings=config.coral
            key=settings.get('deepseek_api_key_env','DEEPSEEK_API_KEY')
            if not os.environ.get(key):raise BackendUnavailable('Missing configured DeepSeek API credential environment variable')
            self.candidate_runner.preflight(image=self.image)
            protocol=self.client.preflight()
        return {**self.provenance,**protocol,'runtime':'dsh','external_api_key_required':True,
            'agent_lifecycle':'coral.agent.builtin.deepseek_harness.DeepSeekHarnessRuntime',
            'preparation_mode':'agent_selected_read_only','actual_inference_request_quota':True,
            'budget_unit':'provider_request','formal_ticket_required':True,
            'generation_mode':'coral_native_structured_implementation' if self.supports_runtime_only_repair else 'coral_native_deepseek_mcp',
            'physical_runtime_qualified':False,'live_qualified':False}


def native_factory(config,root=None):
    root=Path(root or 'artifacts/deepseek-native').resolve()
    pause=PauseController(root)
    pause.check()
    if config.profile!='v1_cooperative' or config.execution_mode!='real' or config.benchmark!='pace':
        raise BackendUnavailable('Native DeepSeek factory requires real PACE/v1_cooperative')
    settings=config.coral
    if settings.get('runtime')!='dsh' or settings.get('director_mode','predeclared')!='predeclared':
        raise BackendUnavailable('DeepSeek requires runtime=dsh and predeclared directions')
    model=settings.get('model')
    if not isinstance(model,str) or not model.strip():raise BackendUnavailable('Freeze coral.model for DeepSeek')
    if any(key.startswith('codex_') and key!='codex_executable' for key in settings):raise BackendUnavailable('DeepSeek runtime must not inherit Codex settings')
    origin=settings.get('deepseek_api_base','https://api.deepseek.com/anthropic');provider_endpoint(origin)
    key_name=settings.get('deepseek_api_key_env','DEEPSEEK_API_KEY')
    if not isinstance(key_name,str) or not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',key_name):
        raise BackendUnavailable('DeepSeek credential must name an environment variable')
    secret=os.environ.get(key_name)
    if not secret:raise BackendUnavailable('Set the configured DeepSeek API key in the controller environment; do not store it in YAML')
    try:check_restart_ledgers(root,artifacts_root=Path(__file__).resolve().parents[4]/'artifacts')
    except RuntimeError as error:raise BackendUnavailable(str(error)) from error
    provenance=verify_checkout(settings.get('checkout_path'))
    authority=root/'deepseek-native-authority';authority.mkdir(parents=True,exist_ok=True,mode=0o700)
    client=CoralDeepSeekClient(model=model,work_root=authority/'native',
        executable=settings.get('deepseek_executable','dsh'),
        cli_version=settings.get('deepseek_cli_version',''),cli_sha256=settings.get('deepseek_cli_sha256',''),
        reasoning_effort=settings.get('deepseek_reasoning_effort','low'),
        max_output_tokens=settings.get('deepseek_max_output_tokens',32000),
        timeout=max(config.preparation_window_seconds,config.planning_window_seconds,config.generation_window_seconds))
    client.preflight()  # Version/help only, never inference.
    image=config.pace.get('contract',{}).get('image','');runner=DockerRunner(image);runner.preflight(image=image)
    toy=Path(settings.get('smoke_graph',Path(__file__).resolve().parents[4]/'fixtures/toy_graphs/star5.gr'))
    assert_smoke_disjoint(load_panel(config.pace['panel_manifest'],expected_split='training'),toy)
    store=BudgetStore(authority/'requests.sqlite')
    upstream=DeepSeekUpstream(origin,secret,pause_controller=pause)
    def proxy(*args,**kwargs):return DeepSeekQuotaProxy(*args,upstream=upstream,**kwargs)
    def precheck(candidate):
        receipts=[];began=time.monotonic()
        class MeteredRunner:
            def run(self,request,limits):
                broker=backend._brokers.get(backend.last_generation_usage.get('account'))
                if broker is None:raise GrantError('Fixed precheck requires active generation invitation')
                receipt=backend._run_fixed_precheck(request,limits,deadline=broker.deadline)
                receipts.append(receipt);return receipt
        try:
            return smoke_test(candidate['candidate_path'],toy,MeteredRunner(),image=image,
                solver_argv=tuple(config.pace.get('contract',{}).get('solver_argv',('/candidate/run',))))
        finally:
            usage=dict(backend.last_generation_usage);cpu=sum(r.cpu_seconds for r in receipts)
            usage.update(precheck_cpu_seconds=cpu,precheck_wall_seconds=time.monotonic()-began,
                precheck_cpu_metering_complete=bool(receipts) and not any(r.diagnostics.get('cpu_measurement_incomplete') for r in receipts))
            usage['cpu_seconds']=usage.get('cpu_seconds',0.)+cpu
            usage['wall_seconds']=usage.get('wall_seconds',0.)+usage['precheck_wall_seconds']
            backend.last_generation_usage=usage
    backend=NativeDeepSeekBackend(client,store,WorkspaceStore(root/'coral-workspaces',root/'coral-cas'),
        precheck=precheck,seed_solver=config.pace.get('seed_solver',''),candidate_runner=runner,image=image,
        config=config,provenance=provenance,proxy_factory=proxy)
    backend.pause=pause
    client.pause=pause
    try:backend.preflight(config)
    except BaseException:
        backend.close();raise
    return backend
