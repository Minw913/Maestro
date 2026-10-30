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
from maestro.tasks.pace.sanity import default_sanity_graph_path
from .deepseek_client import CoralDeepSeekClient
from .deepseek_gate import DeepSeekQuotaProxy,DeepSeekUpstream,DEEPSEEK_TOOL,provider_endpoint
from .native_backend import NativeCoralBackend
from .runtime import verify_checkout
from .workspace import WorkspaceStore
from .deepseek_lifecycle import check_restart_ledgers,historical_artifacts_root
from .deepseek_retry import RetryPolicy, DeepSeekRetryStore, RetryingDeepSeekProxy
from maestro.execution.pause import PauseController


class LocalPhaseAbstention(RuntimeError):
    """Failed logical phase with proven local cleanup; remote cost may be unknown."""
    inflight_unknown=False
    local_quiescent=True


class NativeDeepSeekBackend(NativeCoralBackend):
    """Share stage/grant/ticket semantics, override only the provider boundary."""
    valuation_output_retry=True
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self._phase_proxies={}
        policy=getattr(self.budget_store,'retry_policy',None)
        if policy:
            self.planning_capabilities={'single_request':False,'single_logical_request':True,
                'max_request_attempts':policy.max_retries_per_request+1,'hard_request_timeout':True,
                'mutates_input_state':False}
            if policy.continue_after_network_failure:self.planning_capabilities['cleanup_grace_seconds']=5.

    @property
    def failure_continuation_enabled(self):
        return bool(getattr(self.budget_store,'retry_policy',None)
                    and self.budget_store.retry_policy.continue_after_network_failure)

    def _local_abstention(self,account,error):
        if account.endswith(':VALUATION_RO'):return False
        if not self.failure_continuation_enabled:return False
        if getattr(error,'execution_unknown',False) or getattr(error,'local_quiescent',None) is False:return False
        proxy=self._phase_proxies.get(account)
        if proxy is None or not hasattr(self.client,'cleanup_receipt'):return False
        proxy.close()
        if proxy._pending or proxy._attempt_proxy is not None:return False
        proof=self.client.cleanup_receipt(getattr(proxy,'url',None))
        proof={**proof,'proxy_reclaimed':bool(proxy._closed and (proxy._thread is None or not proxy._thread.is_alive())),
               'error_type':type(error).__name__}
        allowed=self.budget_store.abandon_phase(account,proof)
        if allowed and self.last_generation_usage.get('request_account')==account:
            self.last_generation_usage.update(continuation_blocking_requests=0,
                local_failure_abstained=True,phase_cleanup=proof)
        return allowed

    def _record_phase_cleanup(self,account):
        if not getattr(self.budget_store,'retry_policy',None):return
        proxy=self._phase_proxies.get(account)
        if proxy is None or not hasattr(self.client,'cleanup_receipt'):return
        proof=self.client.cleanup_receipt(getattr(proxy,'url',None))
        if not proof:return
        proof={**proof,'proxy_reclaimed':bool(proxy._closed and not proxy._pending
            and (proxy._thread is None or not proxy._thread.is_alive()))}
        with self.budget_store._transaction():
            self.budget_store.connection.execute('INSERT OR REPLACE INTO deepseek_phase_cleanup VALUES(?,?)',
                (account,__import__('json').dumps(proof,sort_keys=True)))

    def prepare_with_receipt(self,state,round_index,context,*,deadline):
        account=f'{getattr(self,"request_episode_id",self.episode_id)}:{round_index}:{state.agent_id}:PREPARATION'
        try:return super().prepare_with_receipt(state,round_index,context,deadline=deadline)
        except Exception as error:
            if self._local_abstention(account,error):raise LocalPhaseAbstention('Preparation failed; local resources reclaimed') from error
            raise

    def plan_with_receipt(self,state,round_index,context,*,deadline):
        account=f'{getattr(self,"request_episode_id",self.episode_id)}:{round_index}:{state.agent_id}:PLAN_RO'
        try:return super().plan_with_receipt(state,round_index,context,deadline=deadline)
        except Exception as error:
            if self._local_abstention(account,error):raise LocalPhaseAbstention('Planning failed; local resources reclaimed') from error
            raise

    def generate_once(self,state,plan,round_index,deadline,*,grant,grant_authority):
        account=f'{getattr(self,"request_episode_id",self.episode_id)}:{round_index}:{state.agent_id}:GENERATION:{plan.version_id}'
        try:return super().generate_once(state,plan,round_index,deadline,grant=grant,grant_authority=grant_authority)
        except Exception as error:
            if self._local_abstention(account,error):raise LocalPhaseAbstention('Generation failed; local resources reclaimed') from error
            raise

    def repair_runtime(self,state,plan,failed,evidence,round_index,repair_index,deadline,*,grant,grant_authority):
        account=f'{getattr(self,"request_episode_id",self.episode_id)}:{round_index}:{state.agent_id}:RUNTIME_REPAIR:{plan.version_id}:{repair_index}'
        try:return super().repair_runtime(state,plan,failed,evidence,round_index,repair_index,deadline,grant=grant,grant_authority=grant_authority)
        except Exception as error:
            if self._local_abstention(account,error):raise LocalPhaseAbstention('Runtime repair request failed; local resources reclaimed') from error
            raise

    def _assert_clear(self):
        from .native_backend import CodexPhaseUnknown
        store=self.budget_store
        if not getattr(store,'retry_policy',None):return super()._assert_clear()
        with store._lock:
            abandoned=store.connection.execute("SELECT account FROM codex_phase_scopes WHERE status IN ('DISPATCHING','UNKNOWN')").fetchall()
            pending=store.connection.execute('SELECT account FROM inference_budgets WHERE active>0').fetchall()
            if any(row[0] not in self._active_accounts for row in abandoned):
                raise CodexPhaseUnknown('Unreclaimed native phase blocks retry continuation')
            for row in pending:
                account=row[0];root=store.root_for_attempt(account)
                # Valuation uses a directly metered account, while preparation,
                # planning and generation use retry attempt subaccounts. Both
                # are safe only while their owning phase is actively supervised.
                if account in self._active_accounts or root in self._active_accounts or store.continuation_authorized(account):continue
                raise CodexPhaseUnknown('Unrecovered DeepSeek attempt blocks continuation')

    def _blocking_requests(self,usage):
        return usage.get('continuation_blocking_requests',usage.get('unresolved_requests',0))

    def _drain_native_exit(self,account,deadline):
        """Wait for a sent request's terminal receipt after a reclaimed CLI exits.

        No dispatch or deadline extension occurs. If the receipt never arrives,
        the original UNKNOWN path remains mandatory.
        """
        proxy=self._phase_proxies.get(account)
        if proxy is None or not hasattr(self.client,'cleanup_receipt'):
            return
        cleanup=self.client.cleanup_receipt(getattr(proxy,'url',None))
        if not (cleanup.get('local_process_group_reclaimed')
                and cleanup.get('tool_service_reclaimed')):
            return
        while True:
            usage=self.budget_store.usage(account)
            if not usage.get('logical_calls',usage.get('calls',0)):
                return
            if getattr(self.budget_store,'retry_policy',None):
                with self.budget_store._lock:
                    rows=self.budget_store.connection.execute(
                        'SELECT status FROM deepseek_logical_requests WHERE account=?',(account,)).fetchall()
                terminal=bool(rows) and all(row[0] in ('COMPLETED','FAILED') for row in rows)
            else:
                terminal=bool(usage.get('calls'))
            if (terminal and usage.get('usage_complete') is True
                    and not usage.get('unresolved_requests')
                    and not usage.get('unknown_requests')):
                return
            remaining=deadline-time.monotonic()
            if remaining<=0:return
            time.sleep(min(.05,remaining))

    def _planning_provider_id(self,account):
        if not getattr(self.budget_store,'retry_policy',None):return super()._planning_provider_id(account)
        with self.budget_store._lock:
            row=self.budget_store.connection.execute('''SELECT p.provider_request_id FROM deepseek_logical_requests l
                JOIN provider_receipts p ON p.reservation_id=l.final_reservation WHERE l.account=? AND l.status='COMPLETED' ''',(account,)).fetchall()
        return row[0][0] if len(row)==1 else ''

    def _dispatch_guard(self):
        pause=getattr(self,'pause',None)
        if pause:pause.check()
        return super()._dispatch_guard()

    def _valuation_proxy(self,account,deadline):
        # Physical request cap, no logical retries or UNKNOWN continuation.
        # Preserve concise read turns; the final structured answer gets more
        # room while retaining the same physical request count and deadline.
        from .deepseek_gate import DeepSeekQuotaProxy,DEEPSEEK_TOOL
        provider_limit=self.config.coral.get('deepseek_max_output_tokens',32000)
        read_limit=min(8192,provider_limit)
        final_limit=min(16384,provider_limit)
        return DeepSeekQuotaProxy(self.budget_store,account,self.client.model,upstream=self._valuation_upstream,deadline=deadline,
            max_calls=self.config.valuation_calls+1,max_request_bytes=16*1024*1024,
            max_output_tokens=(self.config.valuation_calls-1)*read_limit+2*final_limit,
            per_request_tokens=read_limit,final_request_tokens=final_limit,
            reasoning_effort=self.config.coral.get('deepseek_reasoning_effort','low'),
            allowed_tool_names=(DEEPSEEK_TOOL,),request_guard=self._dispatch_guard,
            final_response_on_last_request=True,conditional_final_retry=True)

    def _proxy(self,account,deadline,calls,*,tools=False):
        proxy=self.proxy_factory(self.budget_store,account,self.client.model,deadline=deadline,
            max_calls=calls,max_request_bytes=16*1024*1024,max_output_tokens=96000,
            per_request_tokens=self.config.coral.get('deepseek_max_output_tokens',32000),
            reasoning_effort=self.config.coral.get('deepseek_reasoning_effort','low'),
            allowed_tool_names=(DEEPSEEK_TOOL,) if tools else (),request_guard=self._dispatch_guard,
            final_response_on_last_request=account.endswith(':PREPARATION'))
        with self._phase_lock:self._phase_proxies[account]=proxy
        return proxy

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
            'transport_retry_policy':(getattr(self.budget_store,'retry_policy',None).__dict__
                if getattr(self.budget_store,'retry_policy',None) else None),
            'stage_call_cap_unit':'logical_request' if getattr(self.budget_store,'retry_policy',None) else 'provider_request',
            'generation_mode':'coral_native_structured_implementation' if self.supports_runtime_only_repair else 'coral_native_deepseek_mcp',
            'physical_runtime_qualified':False,'live_qualified':False}


def native_factory(config,root=None):
    from maestro.execution.pipeline import require_prebid,require_prebid_root
    require_prebid(config)
    root=Path(root or 'artifacts/runs/deepseek-native').resolve()
    require_prebid_root(root)
    from maestro.storage.paths import run_paths, require_separated_runtime
    require_separated_runtime(root)
    local=run_paths(root).local
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
    try:check_restart_ledgers(root,artifacts_root=historical_artifacts_root(root),
        allow_recovered_transport=RetryPolicy.parse(settings.get('deepseek_transport_retry')) is not None)
    except RuntimeError as error:raise BackendUnavailable(str(error)) from error
    provenance=verify_checkout(settings.get('checkout_path'))
    authority=root/'deepseek-native-authority';authority.mkdir(parents=True,exist_ok=True,mode=0o700)
    client=CoralDeepSeekClient(model=model,work_root=local/'deepseek-native-authority'/'native',
        executable=settings.get('deepseek_executable','dsh'),
        cli_version=settings.get('deepseek_cli_version',''),cli_sha256=settings.get('deepseek_cli_sha256',''),
        reasoning_effort=settings.get('deepseek_reasoning_effort','low'),
        max_output_tokens=settings.get('deepseek_max_output_tokens',32000),
        timeout=max(config.preparation_window_seconds,config.planning_window_seconds,config.generation_window_seconds,config.valuation_window_seconds))
    client.preflight()  # Version/help only, never inference.
    image=config.pace.get('contract',{}).get('image','');runner=DockerRunner(image);runner.preflight(image=image)
    toy=Path(settings.get('smoke_graph',default_sanity_graph_path()))
    assert_smoke_disjoint(load_panel(config.pace['panel_manifest'],expected_split='training'),toy)
    policy=RetryPolicy.parse(settings.get('deepseek_transport_retry'))
    store=DeepSeekRetryStore(authority/'requests.sqlite') if policy else BudgetStore(authority/'requests.sqlite')
    if policy:store.enable_retries(policy)
    upstream=DeepSeekUpstream(origin,secret,pause_controller=pause)
    def proxy(*args,**kwargs):
        if policy:return RetryingDeepSeekProxy(*args,upstream=upstream,retry_policy=policy,**kwargs)
        return DeepSeekQuotaProxy(*args,upstream=upstream,**kwargs)
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
    backend=NativeDeepSeekBackend(client,store,WorkspaceStore(local/'coral-workspaces',root/'coral-cas'),
        precheck=precheck,seed_solver=config.pace.get('seed_solver',''),candidate_runner=runner,image=image,
        config=config,provenance=provenance,proxy_factory=proxy)
    backend._valuation_upstream=upstream
    backend.pause=pause
    client.pause=pause
    try:backend.preflight(config)
    except BaseException:
        backend.close();raise
    return backend
