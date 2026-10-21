"""Explicit, single-use real connection qualification; never a warm experiment.

The five-request envelope is declared before any model call. Native Codex owns
login; this harness neither reads credentials nor runs generated code. Injected
client/proxy factories are solely for offline contract tests.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import http.client
import json
from pathlib import Path
import secrets
import shutil
import time
from urllib.parse import urlsplit

from maestro.backends.base import BackendUnavailable
from maestro.backends.coral.workspace import WorkspaceStore, candidate_tree_hash
from maestro.config import RunConfig
from maestro.execution.budgets import BudgetStore
from maestro.execution.grants import GrantAuthority, GrantError
from maestro.execution.sandbox import FailClosedRunner
from maestro.types import AgentState

from .autonomous_backend import StrictCodexBackend
from .autonomous_client import AutonomousCodexClient
from .quota_proxy import CodexQuotaProxy
from .tools import StageToolBroker


_CONTRACT = (
    "FILE-ONLY CONNECTION QUALIFICATION. Propose action=revise, target=other, mod_type=other, "
    "evidence_run_ids=[]; do not withdraw. The admitted change is exactly this contract. "
    'During generation request 1 call maestro_tool ONCE with {"action":"batch","operations":'
    '[{"action":"read","path":"private/notes.md"},{"action":"read","path":"public/board.json"}]}. '
    "The private file contains PRIVATE_NONCE=<value>; the board JSON version is the public nonce. "
    'During generation request 2 call maestro_tool ONCE with action="batch" and two operations: '
    "write solver/probe.py with exactly PRIVATE_MEMORY = \"<private nonce>\" followed by a newline, "
    "PUBLIC_MEMORY = \"<public nonce>\" followed by a newline; write private/notes.md with exactly "
    "QUALIFIED:<private nonce>:<public nonce> followed by a newline. Replace angle-bracket "
    "placeholders using actual read results. On the third response return a short final message. "
    "Do not execute code, use shell, inspect extra files, or request formal evaluation. "
    "There are three inference requests total. Use one batch function call per tool response; "
    "native parallel tool calls are unavailable. Each batch returns results in operation order."
)


class QualificationError(BackendUnavailable):
    pass


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _json(path, value, *, exclusive=False):
    with Path(path).open("x" if exclusive else "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def _require(value, reason):
    if not value:
        raise QualificationError(reason)


def _local_denial(proxy, model):
    """This synthetic credential must be rejected locally, never forwarded."""
    _require(proxy.store.snapshot(proxy.account)["calls"] == proxy.max_calls,
             "Only an already exhausted account may receive the local denial probe")
    url = urlsplit(proxy.start())
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=10)
    try:
        connection.request("POST", url.path+"/responses",
            json.dumps({"model": model, "input": [], "tools": []}),
            {"Authorization": "Bearer local-quota-denial-only", "Content-Type": "application/json"})
        response = connection.getresponse()
        status = response.status
        response.read()
        return status
    finally:
        connection.close()
        proxy.close()


def run_qualification(output_root, *, model, executable=None, historical_path=None,
                      historical_sha256=None, client_factory=AutonomousCodexClient,
                      proxy_factory=CodexQuotaProxy, phase_seconds=120, reasoning_effort="low"):
    """Run one user-authorized diagnostic, with a fixed maximum of five calls.

    ``output_root`` must not exist. Any outcome consumes it permanently; no
    retry/output-root rotation is performed. Historical uncertainty is retained
    independently, never refunded or declared resolved by a later success.
    The return value is sanitized and is also saved as ``result.json``.
    """
    if not isinstance(model, str) or not model.strip():
        raise ValueError("An explicit model is required")
    if type(phase_seconds) not in (int, float) or not 10 <= phase_seconds <= 300:
        raise ValueError("Phase deadline must be between 10 and 300 seconds")
    if reasoning_effort not in {"low", "medium", "high", "xhigh", "max", "ultra"}:
        raise ValueError("An explicit supported native reasoning effort is required")
    if bool(historical_path) != bool(historical_sha256):
        raise ValueError("Historical evidence requires both path and expected SHA-256")
    historical = None
    if historical_path:
        historical_path = Path(historical_path).resolve(strict=True)
        actual = _sha(historical_path)
        _require(actual == historical_sha256, "Historical UNKNOWN evidence hash mismatch")
        historical = {"path": str(historical_path), "sha256": actual,
                      "status": "HISTORICAL_UNRESOLVED_PRESERVED", "refunded": False}
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=False)
    episode = "qualification-"+secrets.token_hex(8)
    manifest = {"schema_version": 1, "episode_id": episode, "model": model,
        "reasoning_effort": reasoning_effort,
        "max_total_requests": 5, "phase_caps": {"PLAN_RO": 1, "GENERATION": 3, "CONTINUITY_READ": 1},
        "phase_deadline_seconds": {"PLAN_RO": min(120, phase_seconds),
            "GENERATION": phase_seconds, "CONTINUITY_READ": min(120, phase_seconds)},
        "phase_seconds": phase_seconds, "single_use": True, "automatic_retry": False,
        "candidate_execution_allowed": False, "warm_start_allowed": False,
        "historical_evidence": historical}
    _json(root/"manifest.json", manifest, exclusive=True)
    started = time.monotonic()
    result = {"status": "FAILED", "model": model, "reasoning_effort": reasoning_effort, "max_total_requests": 5,
        "warm_start_started": False, "formal_evaluation_started": False, "candidate_executed": False,
        "external_api_key_required": False, "historical_evidence": historical, "checks": {}}
    trace, proxies, clients = [], {}, []
    store = BudgetStore(root/"requests.sqlite")
    backend = None
    authority = grant = None
    accounts = {}

    def make_proxy(budget, account, requested_model, **options):
        phase = ("PLAN_RO" if account.endswith(":PLAN_RO") else
                 "GENERATION" if ":GENERATION:" in account else "CONTINUITY_READ")
        _require(account.startswith(episode+":"), "Undeclared diagnostic episode")
        _require(requested_model == model, "Diagnostic model differs from manifest")
        _require(options["max_calls"] == manifest["phase_caps"][phase], "Diagnostic quota differs from manifest")
        _require(phase not in accounts or accounts[phase] == account, "A phase cannot allocate a new account")
        accounts[phase] = account
        proxy = proxy_factory(budget, account, requested_model, **options)
        proxies[phase] = proxy
        return proxy

    def broker_factory(workspace, **options):
        class ObservedBroker(StageToolBroker):
            def __call__(self, arguments):
                # Only native dynamic-tool callbacks mark their actual nested
                # primitives as model-origin; controller permission probes do not.
                with self._lock:
                    depth = getattr(self, "_model_callback_depth", 0)
                    self._model_callback_depth = depth + 1
                    try:
                        return super().__call__(arguments)
                    finally:
                        self._model_callback_depth = depth

            def call(self, action, args=None):
                if not getattr(self, "_model_callback_depth", 0):
                    return super().call(action, args)
                args = args or {}
                path = args.get("path")
                entry = {"origin": "model", "phase": options["phase"],
                    "action": action if action in {"read", "write", "batch"} else "rejected",
                    "path": "<batch>" if action == "batch" else path if isinstance(path, str) and path in
                    {"solver/probe.py", "private/notes.md", "public/board.json"} else "<rejected>"}
                trace.append(entry)
                try:
                    if action not in {"read", "write", "batch"} or entry["path"] == "<rejected>":
                        raise PermissionError("This diagnostic permits only its declared file operations")
                    value = super().call(action, args)
                    if action == "batch":
                        entry.update(success=True, operation_count=value["completed"])
                    else:
                        content = value.get("content", args.get("content", ""))
                        entry.update(success=True, content_sha256=hashlib.sha256(content.encode()).hexdigest())
                    return value
                except Exception as error:
                    entry.update(success=False, error_type=type(error).__name__)
                    raise
        return ObservedBroker(workspace, **options)

    def new_client():
        client = client_factory(model=model, work_root=root/"native", executable=executable,
                                timeout=phase_seconds, reasoning_effort=reasoning_effort)
        clients.append(client)
        return client

    try:
        client = new_client()
        result["local_protocol"] = client.preflight()
        _require(result["local_protocol"].get("authenticated") is True, "Cached ChatGPT login required")
        seed = root/"seed"
        seed.mkdir()
        (seed/"probe.py").write_text("# File-only connection seed; never execute.\n", encoding="utf-8")
        def no_execution(*_args, **_kwargs):
            raise QualificationError("Qualification cannot run a precheck or candidate")
        config = RunConfig(execution_mode="real", rounds=1, generation_calls=3, planning_calls=1)
        backend = StrictCodexBackend(client, store, WorkspaceStore(root/"workspaces", root/"cas"),
            precheck=no_execution, seed_solver=seed, candidate_runner=FailClosedRunner(), image="",
            config=config, authenticator=client, proxy_factory=make_proxy, broker_factory=broker_factory)
        backend.begin_episode(episode)
        backend.set_directions([{"agent_id": "probe", "qualification_contract": _CONTRACT}])
        parent = backend.prepare_seed()
        backend.register_parent(parent["candidate_hash"], parent["candidate_path"])
        state = AgentState("probe", parent["candidate_hash"], 0.0)
        # Provision Git/filesystem prerequisites before spending the first call.
        workspace = backend._workspace(state)
        decision, receipt, _ = backend.plan_with_receipt(state, 0, {"board": {}},
                                                       deadline=time.monotonic()+min(120, phase_seconds))
        _require(decision.action == "revise" and decision.plan is not None, "Qualification needs an admitted plan")
        result["planning_receipt"] = asdict(receipt)
        result["checks"]["planning_one_request"] = receipt.actual_requests == 1
        plan = decision.plan
        before = sum(store.snapshot(account)["calls"] for account in accounts.values())
        try:
            backend.generate(state, plan, 0, deadline=time.monotonic()+phase_seconds)
        except GrantError:
            result["checks"]["missing_grant_denied_without_dispatch"] = before == sum(store.snapshot(a)["calls"] for a in accounts.values())
        else:
            raise QualificationError("Generation accepted a missing EditGrant")
        private_nonce, public_nonce = secrets.token_hex(16), secrets.token_hex(12)
        (workspace.private/"notes.md").write_text("PRIVATE_NONCE="+private_nonce+"\n", encoding="utf-8")
        backend.set_board({"version": public_nonce})
        board_hash = _sha(workspace.public/"board.json")
        authority = GrantAuthority()
        grant = authority.issue(episode_id=episode, round_index=0, agent_id=state.agent_id,
            plan_version_id=plan.version_id, parent_hash=state.parent_hash, epoch=0,
            expires_at=time.time()+phase_seconds)
        candidate = backend.generate(state, plan, 0, deadline=time.monotonic()+phase_seconds,
                                     grant=grant, grant_authority=authority)
        expected_code = f'PRIVATE_MEMORY = "{private_nonce}"\nPUBLIC_MEMORY = "{public_nonce}"\n'
        expected_notes = f"QUALIFIED:{private_nonce}:{public_nonce}\n"
        result["checks"]["memory_to_code_verified"] = (workspace.solver/"probe.py").read_text(encoding="utf-8") == expected_code
        result["checks"]["private_memory_updated"] = (workspace.private/"notes.md").read_text(encoding="utf-8") == expected_notes
        expected_actions = [("read", "private/notes.md"), ("read", "public/board.json"),
                            ("write", "solver/probe.py"), ("write", "private/notes.md")]
        actions = [(entry["action"], entry["path"]) for entry in trace if entry.get("success")]
        result["checks"]["model_origin_file_tools"] = all(item in actions for item in expected_actions)
        batches = [entry for entry in trace if entry["phase"] == "GENERATION" and entry["action"] == "batch"]
        result["checks"]["model_origin_batched_file_tools"] = len(batches) == 2 and all(
            entry.get("success") and entry.get("operation_count") == 2 for entry in batches)
        result["checks"]["file_tools_only"] = all(entry.get("success") for entry in trace)
        generation_account = accounts["GENERATION"]
        result["checks"]["generation_three_requests"] = store.snapshot(generation_account)["calls"] == 3
        _require(all(result["checks"].values()), "Generated artifacts or model tool evidence failed qualification")
        observed = backend._brokers[generation_account]
        before = store.snapshot(generation_account)["calls"]
        try:
            observed.call("write", {"path": "public/board.json", "content": "forbidden"})
        except PermissionError:
            result["checks"]["public_memory_write_denied"] = _sha(workspace.public/"board.json") == board_hash and store.snapshot(generation_account)["calls"] == before
        else:
            raise QualificationError("Shared memory accepted a private write")
        proxy = make_proxy(store, generation_account, model, deadline=time.monotonic()+phase_seconds,
            max_calls=3, max_request_bytes=16*1024*1024, max_output_tokens=96_000,
            allowed_tool_names=("maestro_tool",))
        result["checks"]["fourth_generation_dispatch_denied"] = _local_denial(proxy, model) == 429 and store.snapshot(generation_account)["calls"] == 3
        _require(all(result["checks"].values()), "A local authorization boundary failed qualification")
        memory = backend.workspaces.freeze(workspace.private)
        restored = WorkspaceStore(root/"restored", root/"cas").create("probe", candidate["candidate_path"])
        for source in Path(memory["candidate_path"]).iterdir():
            shutil.copy2(source, restored.private/source.name)
        result["checks"]["private_snapshot_restored"] = candidate_tree_hash(restored.private) == memory["candidate_hash"]
        key = (state.agent_id, 0, plan.version_id)
        session_id = backend._session_ids.get(key)
        _require(bool(session_id), "Generation must finish with a resumable native thread")
        continuity = new_client()
        deadline = time.monotonic()+min(120, phase_seconds)
        readonly = broker_factory(restored, phase="PLAN_RO", deadline=deadline, runner=FailClosedRunner())
        proxy = make_proxy(store, episode+":continuity:CONTINUITY_READ", model, deadline=deadline,
            max_calls=1, max_request_bytes=16*1024*1024, max_output_tokens=96_000,
            allowed_tool_names=("maestro_tool",))
        continuity_error = None
        try:
            continuity.run_phase("CONTINUITY_READ: Resume your own previous tool history. Use maestro_tool to read "
                "private/notes.md exactly once, and no other operation. This restored private memory is read-only. "
                "Only one inference request is authorized; a continuation after the read will be denied locally.",
                base_url=proxy.start(), deadline=deadline, tools=lambda name, args: readonly(args)
                if name == "maestro_tool" else no_execution(), session_id=session_id)
        except Exception as error:
            continuity_error = type(error).__name__
            usage = proxy.usage()
            _require(proxy.quota_denied and usage["actual_requests"] == 1 and not usage["unresolved_requests"]
                and not usage["protocol_violations"] and not usage["terminal_failures"],
                "Continuity failed without a clean one-request quota stop")
        finally:
            proxy.close()
            readonly.close()
        proxy.assert_resolved()
        reads = [entry for entry in trace if entry["phase"] == "PLAN_RO" and entry.get("success")]
        result["checks"]["new_process_restored_memory_read"] = len(reads) == 1 and reads[0]["action"] == "read" and reads[0]["path"] == "private/notes.md" and reads[0]["content_sha256"] == hashlib.sha256(expected_notes.encode()).hexdigest()
        result["continuity"] = {"native_thread_resumed": True, "new_client_instance": True,
            "ended_by_quota_stop": bool(continuity_error), "memory_snapshot_hash": memory["candidate_hash"]}
        result["artifacts"] = {"candidate_hash": candidate["candidate_hash"],
            "solver_sha256": _sha(workspace.solver/"probe.py"), "private_memory_sha256": _sha(workspace.private/"notes.md"),
            "public_board_sha256": board_hash}
        result["checks"]["public_memory_unchanged"] = _sha(workspace.public/"board.json") == board_hash
        _require(all(result["checks"].values()), "Qualification evidence was incomplete")
        result["status"] = "PASSED"
    except Exception as error:
        result["error_type"] = type(error).__name__  # Never save arbitrary native diagnostics.
    finally:
        result["tool_trace"] = trace
        result["phases"] = {phase: proxy.usage() for phase, proxy in proxies.items()}
        result["actual_requests"] = sum(store.snapshot(account)["calls"] for account in accounts.values())
        result["unresolved_requests"] = sum(store.snapshot(account)["active"] for account in accounts.values())
        result["provider_receipts"] = [dict(row) for row in store.connection.execute(
            "SELECT r.account,p.provider_request_id,p.request_hash,p.protocol,p.input_tokens,p.output_tokens,p.usage_complete "
            "FROM provider_receipts p JOIN inference_reservations r USING(reservation_id)")]
        result["checks"]["authoritative_usage_receipts"] = bool(result["actual_requests"]) and (
            len(result["provider_receipts"]) == result["actual_requests"] and
            all(receipt["usage_complete"] == 1 for receipt in result["provider_receipts"]))
        result["wall_seconds"] = round(time.monotonic()-started, 3)
        if historical:
            result["checks"]["historical_unknown_unchanged"] = _sha(historical_path) == historical_sha256
        if result["unresolved_requests"]:
            result["status"] = "FAILED_UNRESOLVED"
        if not all(result["checks"].values()) or result["actual_requests"] > 5:
            result["status"] = "FAILED" if not result["unresolved_requests"] else "FAILED_UNRESOLVED"
        for proxy in proxies.values():
            proxy.close()
        if authority is not None and grant is not None:
            authority.revoke(grant)
        if backend:
            backend.close()
        else:
            store.close()
        for client in clients:
            client.close()
        _json(root/"result.json", result)
    return result
