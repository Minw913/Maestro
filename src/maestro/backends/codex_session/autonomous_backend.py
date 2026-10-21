"""Strict request-metered native Codex, composed with Coral workspaces/memory.

Codex owns ChatGPT authentication. The trusted native process sees no model
tools except the broker; its authenticated HTTP requests pass through the quota
proxy. Candidate code runs only in the separate, networkless tool sandbox.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, replace
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

from maestro.agents.planner import parse_plan_decision
from maestro.agents.retrieval import build_context
from maestro.backends.base import BackendUnavailable
from maestro.backends.coral.backend import CoralBackend
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.execution.budgets import BudgetExceeded, BudgetStore
from maestro.execution.cpu_pool import GlobalCpuPool
from maestro.execution.grants import GrantAuthority, GrantError
from maestro.llm import InferenceRequestUnknown, PlanningReceipt, PlanningResponseError
from maestro.types import stable_hash

from .client import CodexSessionClient, _PLAN_SCHEMA
from .quota_proxy import CodexQuotaProxy
from .tools import StageToolBroker, ToolScopeExpired
from .runtime_repair import OneShotRuntimeRepairMixin


class CodexPhaseUnknown(BackendUnavailable):
    inflight_unknown = True


class StrictCodexBackend(OneShotRuntimeRepairMixin, CoralBackend):
    planning_capabilities = {"single_request": True, "hard_request_timeout": True,
                             "mutates_input_state": False}

    def __init__(self, client, budget_store, workspaces, *, precheck, seed_solver,
                 candidate_runner, image, config, authenticator=None, provenance=None,
                 proxy_factory=CodexQuotaProxy, broker_factory=StageToolBroker,
                 simulation=False):
        # Coral's own private repositories, CAS and public projections remain
        # authoritative. Native Codex has its own bounded per-phase lifecycle.
        supervisor = SimpleNamespace(close=lambda: None, _sessions={}, simulation=simulation)
        planner = SimpleNamespace(episode_id="unstarted")
        super().__init__(supervisor, workspaces, precheck=precheck, seed_solver=seed_solver,
                         planning_seconds=config.planning_window_seconds,
                         planner_client=planner, generation_calls=config.generation_calls)
        self.client, self.budget_store = client, budget_store
        self.candidate_runner, self.image, self.config = candidate_runner, image, config
        self.authenticator, self.provenance = authenticator, provenance or {}
        self.proxy_factory, self.broker_factory = proxy_factory, broker_factory
        self.simulation, self._closed = simulation, False
        self.tool_cpu_pool = GlobalCpuPool(config.cpu_workers)
        self._phase_lock = threading.RLock()
        self._phase_condition = threading.Condition(self._phase_lock)
        self._active_accounts = set()
        self._session_ids = {}
        self._tool_spent = {}
        self._brokers = {}
        with self.budget_store._transaction():
            self.budget_store.connection.execute("""CREATE TABLE IF NOT EXISTS codex_phase_scopes(
                account TEXT PRIMARY KEY,status TEXT NOT NULL)""")

    def _assert_clear(self):
        with self.budget_store._lock:
            abandoned = self.budget_store.connection.execute(
                "SELECT account FROM codex_phase_scopes WHERE status IN ('DISPATCHING','UNKNOWN')").fetchall()
            pending = self.budget_store.connection.execute(
                "SELECT account FROM inference_budgets WHERE active>0").fetchall()
        if any(row[0] not in self._active_accounts for row in (*abandoned, *pending)):
            raise CodexPhaseUnknown("Unresolved Codex phase/request requires executor reconciliation")

    @contextmanager
    def _phase(self, account):
        with self._phase_lock:
            if self._closed:
                raise BackendUnavailable("Closed Codex backend cannot dispatch")
            self._assert_clear()
            if account in self._active_accounts:
                raise BackendUnavailable("Codex phase already active")
            self._active_accounts.add(account)
            with self.budget_store._transaction():
                self.budget_store.connection.execute(
                    "INSERT INTO codex_phase_scopes VALUES(?,'DISPATCHING') "
                    "ON CONFLICT(account) DO UPDATE SET status='DISPATCHING'", (account,))
        uncertain = False
        try:
            yield
        except BaseException as exc:
            uncertain = bool(getattr(exc, "inflight_unknown", False) or getattr(exc, "execution_unknown", False))
            try:
                uncertain |= self.budget_store.snapshot(account)["active"] > 0
            except BudgetExceeded:
                pass
            if uncertain and isinstance(exc, Exception) and not getattr(exc, "inflight_unknown", False):
                raise InferenceRequestUnknown("Codex phase requires executor reconciliation",
                    request_id=account, scope_id=account, context={"phase_account":account}) from exc
            raise
        finally:
            with self._phase_lock:
                try:
                    uncertain |= self.budget_store.snapshot(account)["active"] > 0
                except BudgetExceeded:
                    pass
                with self.budget_store._transaction():
                    self.budget_store.connection.execute("UPDATE codex_phase_scopes SET status=? WHERE account=?",
                        ("UNKNOWN" if uncertain else "COMPLETED", account))
                self._active_accounts.discard(account)
                self._phase_condition.notify_all()

    def preflight(self, config=None):
        config = config or self.config
        if self._closed or config.profile != "v1_cooperative":
            raise BackendUnavailable("Strict Codex requires the open v1_cooperative backend")
        with self._phase_lock:
            self._assert_clear()
        if not self.seed_solver.is_dir() or self._precheck is None:
            raise BackendUnavailable("Verified seed and bounded toy precheck required")
        if self.simulation:
            if config.execution_mode != "mock":
                raise BackendUnavailable("Simulated Codex transport cannot run a physical PACE experiment")
            auth = {"simulation": True}
        else:
            if self.authenticator is None:
                raise BackendUnavailable("Codex ChatGPT authenticator required")
            auth = self.authenticator.require_chatgpt_login()
            self.candidate_runner.preflight(image=self.image)
            checker = getattr(self.client, "preflight", None)
            if checker is not None:
                protocol=checker()
                expected=config.coral.get("codex_cli_version")
                if not expected or protocol.get("cli_version")!=expected:
                    raise BackendUnavailable("Freeze and qualify coral.codex_cli_version before native execution")
        return {**auth, **self.provenance, "external_api_key_required": False,
                "strict_v1_request_quota": True, "actual_inference_request_quota": True,
                "budget_unit": "provider_request",
                "generation_mode": "one_implementation" if self.supports_runtime_only_repair else "native_codex_dynamic_tools",
                "runtime_repair_mode": "fresh_context_local_runtime_patch" if self.supports_runtime_only_repair else "legacy_shared_generation",
                "formal_ticket_required": True, "simulation": self.simulation}

    def begin_episode(self, episode_id, states=None):
        with self._phase_lock:
            self._assert_clear()
            if self._active_accounts:
                raise BackendUnavailable("Cannot change episode while a Codex phase is active")
            for broker in self._brokers.values():
                broker.close()
            self._brokers.clear()
            self._session_ids.clear()
            self._tool_spent.clear()
        return super().begin_episode(episode_id, states)

    def _proxy(self, account, deadline, calls, *, tools=False):
        # Request quotas are hard. Token reserves are accounting estimates:
        # ChatGPT-native Responses does not support max_output_tokens injection.
        return self.proxy_factory(self.budget_store, account, self.client.model,
            deadline=deadline, max_calls=calls, max_request_bytes=16*1024*1024,
            max_output_tokens=96_000, allowed_tool_names=("maestro_tool",) if tools else ())

    def _usage(self, account):
        try:
            return self.budget_store.usage(account)
        except BudgetExceeded:
            return {"calls": 0, "known_input_tokens": 0, "known_output_tokens": 0,
                    "input_tokens": None, "output_tokens": None, "unresolved_requests": 0}

    def _planning_provider_id(self, account):
        """Use the metered provider receipt identity, never the local thread ID."""
        with self.budget_store._lock:
            rows = self.budget_store.connection.execute("""SELECT p.provider_request_id
                FROM provider_receipts p JOIN inference_reservations r
                ON r.reservation_id=p.reservation_id
                WHERE r.account=? AND r.status='COMPLETED'""", (account,)).fetchall()
        return rows[0][0] if len(rows) == 1 else ""

    def plan_with_receipt(self, state, round_index, context, *, deadline):
        assembled = build_context(state, context.get("board", context))
        assembled["harness_context"] = self._planning_view(state)
        account = f"{self.episode_id}:{round_index}:{state.agent_id}:PLAN_RO"
        prompt = ("Return one JSON plan decision matching the output schema. Plan before editing. "
                  "Use only your supplied evidence. No tool calls or code changes in planning.\n" +
                  json.dumps(assembled, ensure_ascii=False, sort_keys=True, allow_nan=False))
        response = None
        with self._phase(account):
            proxy = self._proxy(account, deadline, self.config.planning_calls)
            try:
                base_url = proxy.start()
                response = self.client.run_phase(prompt, base_url=base_url, deadline=deadline,
                                                tools=None, output_schema=_PLAN_SCHEMA)
            except Exception as error:
                if self._usage(account)["unresolved_requests"] or getattr(error, "inflight_unknown", False):
                    raise InferenceRequestUnknown("Codex planning request requires reconciliation",
                        request_id=account, scope_id=account, context=assembled) from error
                usage = self._usage(account)
                receipt = PlanningReceipt(account, account, stable_hash(assembled), stable_hash("rejected"), "",
                    usage.get("input_tokens"), usage.get("output_tokens"), actual_requests=usage["calls"])
                raise PlanningResponseError("Codex planning ended without an admitted plan",
                                          receipt=receipt, context=assembled) from error
            finally:
                proxy.close()
            proxy.assert_resolved()
            usage = self._usage(account)
            receipt = PlanningReceipt(account, account, stable_hash(assembled), response.response_hash,
                self._planning_provider_id(account), usage.get("input_tokens"), usage.get("output_tokens"),
                actual_requests=usage["calls"])
            try:
                if usage["calls"] != 1 or time.monotonic() >= deadline:
                    raise ValueError("Planning requires exactly one timely metered request")
                decision = parse_plan_decision(response.text, state, round_index, namespace=self.episode_id)
            except ValueError as error:
                raise PlanningResponseError(str(error), receipt=receipt, context=assembled) from error
        return decision, receipt, assembled

    def cancel_planning(self, agent_id, round_index):
        # A locally terminated client cannot prove remote inference completion.
        return False

    def precheck(self, candidate):
        self.remember_runtime_failure(candidate, None)
        try:
            outcome = self._precheck(candidate)
            self.remember_runtime_failure(candidate, outcome)
            return outcome
        except Exception as error:
            if getattr(error,"execution_unknown",False) or getattr(error,"inflight_unknown",False):
                account=self.last_generation_usage.get("request_account", self.last_generation_usage.get("account"))
                if account:
                    with self.budget_store._transaction():
                        self.budget_store.connection.execute(
                            "UPDATE codex_phase_scopes SET status='UNKNOWN' WHERE account=?",(account,))
                raise CodexPhaseUnknown("Codex toy precheck requires executor reconciliation") from error
            raise

    def _run_fixed_precheck(self, request, limits, *, deadline):
        """Toy checks and agent debug shells consume the same CPU capacity."""
        with self.tool_cpu_pool.slot(deadline=deadline):
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Toy precheck missed the common generation deadline")
            bounded = replace(limits, wall_seconds=min(limits.wall_seconds, remaining))
            return self.candidate_runner.run(request, bounded)

    def generate(self, state, plan, round_index, revision=0, deadline=None, *, grant=None, grant_authority=None):
        if self.supports_runtime_only_repair:
            if revision != 0:
                raise GrantError("Initial generation is one-shot; only runtime repair may edit again")
            return self.generate_once(state, plan, round_index, deadline,
                                      grant=grant, grant_authority=grant_authority)
        if not isinstance(grant_authority, GrantAuthority) or grant is None:
            raise GrantError("Strict Codex generation requires an EditGrant")
        if deadline is None or time.monotonic() >= deadline:
            raise TimeoutError("Generation deadline elapsed")
        if (grant.episode_id, grant.round_index, plan.agent_id, plan.parent_hash) != (
                self.episode_id, round_index, state.agent_id, state.parent_hash):
            raise GrantError("Codex edit grant/plan belongs to another frozen scope")

        def verify_grant():
            grant_authority.verify(grant, agent_id=state.agent_id, plan_version_id=plan.version_id,
                                   parent_hash=plan.parent_hash, epoch=revision)

        verify_grant()
        account = f"{self.episode_id}:{round_index}:{state.agent_id}:GENERATION:{plan.version_id}"
        with self.budget_store._lock:
            previous_scope = self.budget_store.connection.execute(
                "SELECT status FROM codex_phase_scopes WHERE account=?", (account,)).fetchone()
        if previous_scope is not None and account not in self._brokers:
            # Mid-invitation recovery is deliberately explicit. A restarted
            # process has no proven tool CPU/session/epoch continuation; a
            # partially consumed model budget must not buy a fresh CPU budget.
            raise CodexPhaseUnknown("Interrupted Codex invitation requires reconciliation, not a fresh tool budget")
        workspace = self._workspace(state)
        key = (state.agent_id, round_index, plan.version_id)
        with self._workspace_lock:
            spent = self._generation_spent.get(key, 0)
            if revision != spent or spent >= self.config.max_candidate_revisions:
                raise GrantError("Generation revision already consumed")
            self._generation_spent[key] = spent + 1
            if revision == 0:
                self.workspaces.restore(workspace, self._parents.get(state.parent_hash, self.seed_solver))
        before = self._usage(account)
        started = time.monotonic()
        broker = None
        self.last_generation_usage = {}
        with self._phase(account):
            proxy = self._proxy(account, deadline, self.generation_calls, tools=True)
            terminal_failures_before = proxy.usage().get("terminal_failures", 0)
            remaining_cpu = self.config.generation_cpu_seconds - self._tool_spent.get(account, 0.0)
            broker = self._brokers.get(account)
            if broker is None:
                broker = self.broker_factory(workspace, phase="GENERATION", deadline=deadline,
                    runner=self.candidate_runner, image=self.image, cpu_budget=self.config.generation_cpu_seconds,
                    grant_checker=verify_grant, cpu_pool=self.tool_cpu_pool,
                    memory_mb=max(1,self.config.memory_bytes//(1024*1024)),pids=self.config.pids_max,
                    output_bytes=self.config.output_bytes)
                self._brokers[account] = broker
            else:
                broker.bind_grant(verify_grant)
            tool_cpu_before = broker.usage()["cpu_seconds"]

            def tool_call(operation, arguments):
                if operation != "maestro_tool":
                    raise GrantError("Unknown native dynamic tool")
                try:
                    return broker(arguments)
                except (GrantError, ToolScopeExpired):
                    raise
                except (ValueError, OSError) as error:
                    if getattr(error, "inflight_unknown", False) or getattr(error, "execution_unknown", False):
                        raise
                    # Recoverable tool input/virtual-path errors remain tool
                    # feedback. Never expose arbitrary host exception text.
                    return {"error":type(error).__name__, "message":"Tool operation rejected; check the virtual path and phase permissions."}
            prompt = ("GENERATION: the controller has admitted your plan and issued its signed EditGrant. "
                "Implement it using the external maestro_tool workspace service. Its virtual paths are "
                "separate from the read-only Codex host: authorized service write/batch operations are "
                "permitted here, while native host writes remain forbidden. Its virtual roots are solver/, "
                "private/ (your durable notes/skills), public/ (verified shared board/champion, read-only). "
                "Use read/list/search/write and isolated shell for bounded debugging. Use action=batch with "
                "operations=[{action:read,path:...},...] for related file reads, and one batch for related file writes. "
                "Each batch permits at most eight file operations, checks the grant for every operation, and stops "
                "at the first error; earlier writes remain. Shell is a separate action. Shell changes are scratch-only; "
                "persist code with write. Do not run formal eval or access credentials, panel or authority. "
                "You have at most three actual inference requests total across revisions, including continuations. "
                "Native parallel tool calls may be disabled; use the explicit batch action to save inference requests.\n" +
                json.dumps({"plan": asdict(plan), "direction": self._directions.get(state.agent_id, {}),
                    "revision": revision, "tool_cpu_remaining": max(0, remaining_cpu)}, ensure_ascii=False))
            try:
                if before["calls"] >= self.generation_calls or remaining_cpu <= 0:
                    raise BudgetExceeded("Generation request/CPU quota already exhausted")
                base_url = proxy.start()
                try:
                    response = self.client.run_phase(prompt, base_url=base_url, deadline=deadline,
                        tools=tool_call,
                        session_id=self._session_ids.get(key) if getattr(self.client, "supports_resume", True) else None,
                        **({"agent_id":state.agent_id} if getattr(self.client,"supports_phase_identity",False) else {}))
                    self._session_ids[key] = response.thread_id
                except Exception as error:
                    usage = self._usage(account)
                    if usage["unresolved_requests"] or getattr(error, "inflight_unknown", False) or getattr(error, "execution_unknown", False):
                        raise InferenceRequestUnknown("Codex generation/executor requires reconciliation",
                            request_id=account, scope_id=account, context={"agent_id":state.agent_id}) from error
                    # A declined fourth request is a clean budget stop after
                    # completed calls/tools. Preserve resulting code for the
                    # ordinary toy precheck; it grants no formal evaluation.
                    if not (proxy.quota_denied and usage["calls"] == self.generation_calls
                            and not proxy.usage().get("protocol_violations", 0)
                            and proxy.usage().get("terminal_failures", 0) == terminal_failures_before):
                        raise
                proxy.assert_resolved()
                if (proxy.usage().get("protocol_violations", 0)
                        or proxy.usage().get("terminal_failures", 0) != terminal_failures_before):
                    raise BackendUnavailable("Codex returned an unbrokered tool request or failed response")
                verify_grant()
                if time.monotonic() >= deadline:
                    raise TimeoutError("Codex generation missed the common deadline")
                return self.workspaces.freeze(workspace.solver)
            finally:
                proxy.close()
                measured = broker.usage()
                self._tool_spent[account] = measured["cpu_seconds"]
                after = self._usage(account)
                self.last_generation_usage = {
                    "calls": after["calls"] - before["calls"], "actual_requests": after["calls"] - before["calls"],
                    "known_input_tokens": after.get("known_input_tokens", 0)-before.get("known_input_tokens", 0),
                    "known_output_tokens": after.get("known_output_tokens", 0)-before.get("known_output_tokens", 0),
                    "usage_complete": after.get("usage_complete", False),
                    "unresolved_requests": after["unresolved_requests"], "request_count_unknown": False,
                    "cpu_seconds": measured["cpu_seconds"]-tool_cpu_before, "wall_seconds": time.monotonic()-started,
                    "tool_usage": measured, "account": account, "budget_unit": "provider_request",
                    "gateway_diagnostics": proxy.usage(),
                    "measurement": "native_codex_quota_proxy", "token_ceiling_enforced": False}

    def close(self):
        if not self._closed:
            with self._phase_condition:
                stop_deadline = time.monotonic()+30
                while self._active_accounts and time.monotonic() < stop_deadline:
                    self._phase_condition.wait(timeout=min(.1,max(.001,stop_deadline-time.monotonic())))
                if self._active_accounts:
                    raise CodexPhaseUnknown("Codex phase cleanup still active; preserve the backend until reconciliation")
                self._closed = True
            closer = getattr(self.client, "close", None)
            if closer:
                closer()
            for broker in self._brokers.values():
                broker.close()
            self.budget_store.close()


def strict_factory(config, root=None, *, client_factory=None, backend_class=None):
    from .autonomous_client import AutonomousCodexClient
    from maestro.backends.coral.runtime import verify_checkout
    from maestro.execution.sandbox import DockerRunner
    from maestro.tasks.pace.manifest import assert_smoke_disjoint, load_panel
    from maestro.tasks.pace.runner import smoke_test

    if getattr(config,"profile",None) != "v1_cooperative" or getattr(config,"execution_mode",None) != "real":
        raise BackendUnavailable("Strict Codex factory requires pace/v1_cooperative")
    settings = config.coral
    if not isinstance(settings.get("model"), str) or not settings["model"].strip():
        raise BackendUnavailable("Freeze coral.model for native Codex")
    if settings.get("director_mode", "predeclared") != "predeclared":
        raise BackendUnavailable("Strict Codex currently uses declared distinct startup directions")
    provenance = verify_checkout(settings.get("checkout_path", settings.get("checkout")))
    root = Path(root or "artifacts/strict-codex").resolve()
    authority = root / "codex-autonomous-authority"
    authority.mkdir(parents=True, exist_ok=True, mode=0o700)
    authenticator = CodexSessionClient(model=settings["model"], work_root=authority/"auth-check",
                                     executable=settings.get("codex_executable"))
    authenticator.require_chatgpt_login()
    image = config.pace.get("contract", {}).get("image", "")
    runner = DockerRunner(image)
    runner.preflight(image=image)
    client = (client_factory or AutonomousCodexClient)(model=settings["model"], work_root=authority/"native",
                                  executable=settings.get("codex_executable"),
                                  reasoning_effort=settings.get("codex_reasoning_effort", "low"),
                                  timeout=max(config.preparation_window_seconds, config.planning_window_seconds, config.generation_window_seconds))
    toy = Path(settings.get("smoke_graph", Path(__file__).resolve().parents[4]/"fixtures/toy_graphs/star5.gr"))
    assert_smoke_disjoint(load_panel(config.pace["panel_manifest"], expected_split="training"), toy)
    store = BudgetStore(authority/"requests.sqlite")
    def precheck(candidate):
        receipts=[]
        began=time.monotonic()
        class MeteredRunner:
            def run(self,request,limits):
                account=backend.last_generation_usage.get("account")
                broker=backend._brokers.get(account)
                if broker is None:
                    raise GrantError("Toy precheck requires the active generation invitation")
                receipt=backend._run_fixed_precheck(request,limits,deadline=broker.deadline)
                receipts.append(receipt)
                return receipt
        try:
            return smoke_test(candidate["candidate_path"],toy,MeteredRunner(),image=image,
                solver_argv=tuple(config.pace.get("contract",{}).get("solver_argv",("/candidate/run",))))
        finally:
            usage=dict(backend.last_generation_usage)
            cpu=sum(receipt.cpu_seconds for receipt in receipts)
            usage.update(precheck_cpu_seconds=cpu,precheck_wall_seconds=time.monotonic()-began,
                precheck_cpu_metering_complete=bool(receipts) and not any(
                    receipt.diagnostics.get("cpu_measurement_incomplete") for receipt in receipts))
            usage["cpu_seconds"]=usage.get("cpu_seconds",0.0)+cpu
            usage["wall_seconds"]=usage.get("wall_seconds",0.0)+usage["precheck_wall_seconds"]
            backend.last_generation_usage=usage
    backend = (backend_class or StrictCodexBackend)(client, store, WorkspaceStore(root/"coral-workspaces", root/"coral-cas"),
        precheck=precheck,
        seed_solver=config.pace.get("seed_solver", ""), candidate_runner=runner, image=image,
        config=config, authenticator=authenticator, provenance=provenance)
    try:
        backend.preflight(config)
    except BaseException:
        backend.close()
        raise
    return backend
