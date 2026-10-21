"""Explicit session-turn pilot using Coral artifacts and ticketed execution.

The strict V1 profile has an actual-inference-request contract and is rejected.
This opt-in profile limits proposal turns, does not expose coding tools to the
model, and continues to require physical containment for candidate execution.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
import threading
import time
from types import SimpleNamespace

from maestro.agents.retrieval import build_context
from maestro.backends.base import BackendUnavailable
from maestro.backends.coral.backend import CoralBackend
from maestro.backends.coral.runtime import verify_checkout
from maestro.backends.coral.workspace import WorkspaceStore, solver_files
from maestro.execution.grants import GrantAuthority, GrantError
from maestro.llm import InferenceBudgetExceeded, InferenceRequestUnknown, PlanningResponseError
from maestro.types import stable_hash

from .client import CodexSessionClient, CompletedProposalError, strict_v1_factory, _unsafe_component


@dataclass(frozen=True)
class SessionPlanningReceipt:
    request_id: str
    scope_id: str
    context_hash: str
    response_hash: str
    provider_request_id: str
    input_tokens: int | None
    output_tokens: int | None
    actual_requests: None = None
    session_turns: int = 1
    request_count_unknown: bool = True
    budget_unit: str = "session_turn"
    diagnostic_count: int = 0


class SessionTurnStore:
    """Durable turn reservations; unknown dispatches remain consumed.

    These are CLI turns, NOT provider inference requests. No model-generated
    code or runtime receives the authority SQLite path.
    """
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.lock = threading.RLock()
        self.db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS session_scopes(scope TEXT PRIMARY KEY, cap INTEGER NOT NULL,
          deadline REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS session_turns(request_id TEXT PRIMARY KEY, scope TEXT NOT NULL,
          state TEXT NOT NULL, receipt TEXT);
        """)

    def reserve(self, scope, request_id, *, cap, deadline):
        if type(cap) is not int or cap < 1 or deadline <= time.time():
            raise InferenceBudgetExceeded("Session-turn scope is invalid or expired")
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self.db.execute("INSERT OR IGNORE INTO session_scopes VALUES(?,?,?)", (scope, cap, deadline))
                if self.db.execute("SELECT 1 FROM session_turns WHERE state='UNKNOWN' LIMIT 1").fetchone():
                    raise InferenceBudgetExceeded("Unresolved Codex dispatch blocks later rounds/episodes; operator reconciliation required")
                old = self.db.execute("SELECT cap,deadline FROM session_scopes WHERE scope=?", (scope,)).fetchone()
                count = self.db.execute("SELECT count(*) FROM session_turns WHERE scope=?", (scope,)).fetchone()[0]
                if old[0] != cap or old[1] <= time.time() or count >= cap:
                    raise InferenceBudgetExceeded("Durable session-turn budget is consumed or expired")
                if self.db.execute("SELECT 1 FROM session_turns WHERE scope=? AND state IN('INFLIGHT','UNKNOWN')", (scope,)).fetchone():
                    raise InferenceBudgetExceeded("Earlier session dispatch is unresolved; reconcile instead of retrying")
                if self.db.execute("SELECT 1 FROM session_turns WHERE request_id=?", (request_id,)).fetchone():
                    raise InferenceBudgetExceeded("Session request was already dispatched; reconcile instead of retrying")
                self.db.execute("INSERT INTO session_turns VALUES(?,?,'INFLIGHT',NULL)", (request_id, scope))
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    def finish(self, request_id, state, receipt=None):
        with self.lock:
            self.db.execute("UPDATE session_turns SET state=?,receipt=? WHERE request_id=? AND state='INFLIGHT'",
                (state, json.dumps(receipt, sort_keys=True) if receipt is not None else None, request_id))

    def usage(self, scope):
        """Reserved turns and confirmed token receipts; request count is unknown.

        ``calls=0`` is an explicitly marked confirmed lower bound for legacy
        accounting columns, never a claim that no inference was dispatched.
        """
        with self.lock:
            rows = self.db.execute("SELECT state,receipt FROM session_turns WHERE scope=?", (scope,)).fetchall()
        receipts = [json.loads(row[1]) if row[1] else {} for row in rows]
        def known(key):
            return sum(item[key] for item in receipts
                if type(item.get(key)) is int and item[key] >= 0)
        return {"calls": 0, "actual_requests": None, "session_turns": len(rows),
                "request_count_unknown": bool(rows), "budget_unit": "session_turn",
                "usage_complete": False, "known_input_tokens": known("input_tokens"),
                "known_output_tokens": known("output_tokens"),
                "inflight_unknown": any(row[0] in {"INFLIGHT", "UNKNOWN"} for row in rows),
                "token_usage_complete": bool(rows) and all(row[0] == "COMPLETED" for row in rows)
                    and all(type(item.get("input_tokens")) is int and type(item.get("output_tokens")) is int
                            for item in receipts)}

    def close(self):
        self.db.close()

    def assert_no_unknown(self):
        with self.lock:
            if self.db.execute("SELECT 1 FROM session_turns WHERE state IN('INFLIGHT','UNKNOWN') LIMIT 1").fetchone():
                raise BackendUnavailable("An unresolved Codex dispatch blocks phase/episode changes; reconcile the durable turn store")


class CodexSessionPlanner:
    def __init__(self, client, store):
        self.client, self.store = client, store
        self.episode_id = "unstarted"

    def plan(self, state, round_index, board, *, request_id, harness_context=None,
             token_budget=32768, deadline=None):
        if deadline is None or deadline <= time.monotonic():
            raise TimeoutError("Common planning deadline elapsed before session dispatch")
        context = build_context(state, board, token_budget // 2)
        if harness_context:
            context["harness_context"] = harness_context
        canonical = json.dumps(context, ensure_ascii=False, sort_keys=True, allow_nan=False)
        if len(canonical.encode()) > token_budget:
            raise ValueError("Combined session planning context exceeds its byte allowance")
        scope = f"{self.episode_id}:{round_index}:{state.agent_id}:PLAN_RO"
        self.store.reserve(scope, request_id, cap=1,
                           deadline=time.time() + deadline - time.monotonic())
        try:
            decision, response = self.client.plan(state, round_index, context,
                deadline=deadline, namespace=self.episode_id)
        except CompletedProposalError as error:
            response = error.session_response
            receipt = SessionPlanningReceipt(request_id, scope, stable_hash(context),
                response.response_hash, response.thread_id, response.input_tokens, response.output_tokens,
                diagnostic_count=response.diagnostic_count)
            from dataclasses import asdict
            self.store.finish(request_id, "COMPLETED", asdict(receipt))
            raise PlanningResponseError(str(error), receipt=receipt, context=context) from error
        except Exception as error:
            self.store.finish(request_id, "UNKNOWN")
            raise InferenceRequestUnknown(
                f"Codex session returned no admitted proposal receipt: {type(error).__name__}",
                request_id=request_id, scope_id=scope, context=context) from error
        receipt = SessionPlanningReceipt(request_id, scope, stable_hash(context),
            response.response_hash, response.thread_id, response.input_tokens, response.output_tokens,
            diagnostic_count=response.diagnostic_count)
        from dataclasses import asdict
        self.store.finish(request_id, "COMPLETED", asdict(receipt))
        if time.monotonic() >= deadline:
            raise PlanningResponseError("Session planning missed the common deadline", receipt=receipt, context=context)
        return decision, receipt, context


class CodexSessionBackend(CoralBackend):
    require_edit_grant = True
    planning_capabilities = {"single_request": False, "single_session_turn": True,
        "hard_request_timeout": True, "mutates_input_state": False, "budget_unit": "session_turn"}

    def __init__(self, client, store, workspaces, *, precheck, seed_solver,
                 candidate_runner=None, provenance=None, simulation=False,
                 planning_seconds=120, generation_turns=3):
        # No second process lifecycle owner or autonomous CLI exists in this
        # broker profile. Coral workspace/CAS/memory methods remain reusable.
        supervisor = SimpleNamespace(close=lambda: None, _sessions={}, simulation=simulation,
                                     provider=SimpleNamespace(close=lambda: None))
        planner = CodexSessionPlanner(client, store)
        super().__init__(supervisor, workspaces, precheck=precheck, seed_solver=seed_solver,
                         planning_seconds=planning_seconds, planner_client=planner)
        self.session_client, self.turn_store = client, store
        self.candidate_runner, self.provenance = candidate_runner, provenance or {}
        self.generation_turns, self.simulation, self._closed = generation_turns, simulation, False

    def begin_episode(self, episode_id, states=None):
        self.turn_store.assert_no_unknown()
        return super().begin_episode(episode_id, states)

    def preflight(self, config=None):
        if config is not None and config.profile != "codex_session_pilot":
            raise BackendUnavailable("The structured proposal pilot cannot satisfy strict V1; use strict_v1_factory")
        if self._closed:
            raise BackendUnavailable("Closed Codex session backend cannot be reused")
        self.turn_store.assert_no_unknown()
        if not self.seed_solver.is_dir() or self._precheck is None:
            raise BackendUnavailable("A verified seed directory and fixed bounded toy precheck are required")
        if self.simulation:
            if config is not None and config.execution_mode != "mock":
                raise BackendUnavailable("Simulated session interface cannot enter real PACE mode")
            auth = {"authenticated": False, "simulation": True}
        else:
            if self.candidate_runner is None:
                raise BackendUnavailable("Codex login does not replace the enforced candidate sandbox")
            self.candidate_runner.preflight()
            auth = self.session_client.require_chatgpt_login()
        return {**auth, **self.provenance, "budget_unit": "session_turn",
                "actual_inference_request_quota": False, "formal_ticket_required": True,
                "generation_mode": "host_applied_structured_files", "simulation": self.simulation}

    def generate(self, state, plan, round_index, revision=0, deadline=None, *,
                 grant=None, grant_authority=None):
        if not isinstance(grant_authority, GrantAuthority) or grant is None:
            raise GrantError("Session generation requires the round owner's EditGrant")
        if plan.agent_id != state.agent_id or plan.parent_hash != state.parent_hash:
            raise GrantError("Session generation plan/agent/parent mismatch")
        if (grant.episode_id, grant.round_index) != (self.episode_id, round_index):
            raise GrantError("EditGrant belongs to another episode or round")
        grant_authority.verify(grant, agent_id=state.agent_id, plan_version_id=plan.version_id,
                               parent_hash=plan.parent_hash, epoch=revision)
        if deadline is None or deadline <= time.monotonic():
            raise TimeoutError("Common generation deadline elapsed before session dispatch")
        key = state.agent_id, round_index, plan.version_id
        spent = self._generation_spent.get(key, 0)
        if revision != spent or spent >= self.generation_turns:
            raise PermissionError("Session invitation turn budget already consumed")
        scope = f"{self.episode_id}:{round_index}:{state.agent_id}:GENERATION:{plan.version_id}"
        request_id = f"{scope}:{revision}"
        self.last_generation_usage = {"calls": None, "actual_requests": None, "session_turns": 1,
            "request_count_unknown": True, "budget_unit": "session_turn", "account": scope,
            "measurement": "codex_cli_turn", "usage_complete": False,
            "input_tokens": None, "output_tokens": None, "cpu_seconds": None, "wall_seconds": None}
        workspace = self._workspace(state)
        if revision == 0:
            parent = self._parents.get(state.parent_hash)
            if parent:
                self.workspaces.restore(workspace, parent)
        files, total = [], 0
        for source, relative in solver_files(workspace.solver):
            data = source.read_bytes()
            total += len(data)
            if total > 64 * 1024 or len(files) >= 32:
                raise ValueError("Seed/candidate is too large for this structured-proposal pilot")
            files.append({"path": relative.as_posix(), "content": data.decode("utf-8", "strict")})
        context = {"plan": {"version_id": plan.version_id, "parent_hash": plan.parent_hash,
            "target": plan.target, "mod_type": plan.mod_type, "change": plan.change,
            "diagnosis": plan.diagnosis, "expected_effect": plan.expected_effect},
            "files": files, "private_context": self._planning_view(state),
            "repair_revision": revision, "instruction": "Propose only changed files. Preserve the solver interface. No execution."}
        self.turn_store.reserve(scope, request_id, cap=self.generation_turns,
                                deadline=time.time() + deadline - time.monotonic())
        self._generation_spent[key] = spent + 1
        started = time.monotonic()
        response = None
        try:
            response = self.session_client.propose_files(context, grant=grant, authority=grant_authority,
                episode_id=self.episode_id, round_index=round_index, agent_id=state.agent_id,
                plan_version_id=plan.version_id, parent_hash=plan.parent_hash, epoch=revision,
                deadline=deadline)
            self.last_generation_usage.update(input_tokens=response.input_tokens,
                output_tokens=response.output_tokens, provider_request_id=response.thread_id,
                wall_seconds=time.monotonic()-started, token_usage_complete=True,
                diagnostic_count=response.diagnostic_count)
            self.turn_store.finish(request_id, "COMPLETED", self.last_generation_usage)
            grant_authority.verify(grant, agent_id=state.agent_id, plan_version_id=plan.version_id,
                                   parent_hash=plan.parent_hash, epoch=revision)
            if time.monotonic() >= deadline:
                raise TimeoutError("Generation proposal arrived after its common deadline")
            self._apply_proposed_files(workspace.solver, response.value["files"])
            return self.workspaces.freeze(workspace.solver)
        except Exception as error:
            if response is None:
                response = getattr(error, "session_response", None)
            if response is not None:
                # Model turn completed; rejected host edits cannot erase its
                # usage receipt or imply another remote inference is in flight.
                self.last_generation_usage.update(input_tokens=response.input_tokens,
                    output_tokens=response.output_tokens, provider_request_id=response.thread_id,
                    wall_seconds=time.monotonic()-started, token_usage_complete=True,
                    diagnostic_count=response.diagnostic_count)
                self.turn_store.finish(request_id, "COMPLETED", self.last_generation_usage)
                raise RuntimeError(f"Completed Codex generation proposal was rejected: {type(error).__name__}") from error
            self.turn_store.finish(request_id, "UNKNOWN", self.last_generation_usage)
            raise InferenceRequestUnknown(
                f"Codex generation dispatch requires reconciliation: {type(error).__name__}",
                request_id=request_id, scope_id=scope, context={"agent_id": state.agent_id}) from error

    @staticmethod
    def _apply_proposed_files(solver, files):
        """Validate the whole manifest before editing; no commands are run."""
        from pathlib import PurePosixPath
        root = Path(solver).resolve(strict=True)
        targets, seen = [], set()
        if not isinstance(files, list) or len(files) > 32:
            raise ValueError("Invalid generation file manifest")
        total = 0
        for item in files:
            if not isinstance(item, dict) or set(item) != {"path", "content"}:
                raise ValueError("Proposed file requires path/content")
            name, content = item["path"], item["content"]
            if not isinstance(name, str) or not isinstance(content, str):
                raise ValueError("Proposed path/content must be text")
            relative = PurePosixPath(name)
            if (not name or relative.is_absolute() or "\\" in name or ":" in name
                or any(_unsafe_component(part) for part in name.split("/"))
                or name != relative.as_posix() or name.casefold() in seen):
                raise ValueError("Proposed file path is unsafe")
            seen.add(name.casefold())
            target = root.joinpath(*relative.parts)
            if not target.resolve().is_relative_to(root):
                raise ValueError("Proposed path escaped the private solver root")
            for ancestor in (target, *target.parents):
                if ancestor == root:
                    break
                if ancestor.is_symlink():
                    raise ValueError("A proposed file traverses a symlink")
            total += len(content.encode())
            targets.append((target, content))
        if total > 128 * 1024:
            raise ValueError("Proposed files exceed the accepted size")
        for target, content in targets:
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                target.chmod(target.stat().st_mode | 0o200)
            target.write_text(content, encoding="utf-8", newline="\n")

    def close(self):
        if not self._closed:
            self.turn_store.close()
            self._closed = True


def session_factory(config, root=None):
    """Packaged tokenless pilot; candidate execution retains Docker limits."""
    if config.profile != "codex_session_pilot":
        return strict_v1_factory(config, root)
    settings = config.coral
    if not isinstance(settings.get("model"), str) or not settings["model"].strip():
        raise BackendUnavailable("Freeze coral.model for a reproducible Codex session pilot; no API token is needed")
    if settings.get("director_mode", "predeclared") != "predeclared":
        raise BackendUnavailable("Session pilot currently uses the declared balanced SearchDirector briefs")
    provenance = verify_checkout(settings.get("checkout_path", settings.get("checkout")))
    from maestro.execution.sandbox import DockerRunner
    from maestro.tasks.pace.runner import smoke_test
    from maestro.tasks.pace.manifest import load_panel, assert_smoke_disjoint
    root = Path(root or settings.get("run_root", "artifacts/codex-session-pilot")).resolve()
    authority_root = root / "codex-session-authority"
    client = CodexSessionClient(model=settings.get("model", ""), work_root=authority_root / "broker",
        executable=settings.get("codex_executable"),
        timeout=float(settings.get("session_timeout_seconds", 120)))
    client.require_chatgpt_login()
    contract = config.pace.get("contract", {})
    candidate_runner = DockerRunner(contract.get("image"))
    candidate_runner.preflight()
    toy = Path(settings.get("smoke_graph", Path(__file__).resolve().parents[4] / "fixtures/toy_graphs/star5.gr"))
    if not toy.is_file():
        raise BackendUnavailable("Configured fixed toy graph is missing")
    assert_smoke_disjoint(load_panel(config.pace["panel_manifest"], expected_split="training"), toy)
    store = SessionTurnStore(authority_root / "session-turns.sqlite")
    backend = CodexSessionBackend(client, store,
        WorkspaceStore(root / "coral-workspaces", root / "coral-cas"),
        precheck=lambda candidate: smoke_test(candidate["candidate_path"], toy, candidate_runner,
            image=contract.get("image", ""), solver_argv=tuple(contract.get("solver_argv", ("/candidate/run",)))),
        seed_solver=config.pace.get("seed_solver", ""), candidate_runner=candidate_runner,
        provenance=provenance, planning_seconds=config.planning_window_seconds,
        generation_turns=config.generation_calls)
    try:
        backend.preflight(config)
    except BaseException:
        backend.close()
        raise
    return backend
