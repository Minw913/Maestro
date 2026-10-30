"""Live adapter used by the V1 loop; deterministic mock is a separate backend."""
from __future__ import annotations

import json
from pathlib import Path
import time
import threading
from maestro.types import Plan
from maestro.backends.base import BackendUnavailable
from .runtime import load_factory
from .workspace import WorkspaceStore


class CoralBackend:
    require_edit_grant = True

    def __init__(self, supervisor, workspaces: WorkspaceStore, *, precheck,
                 seed_solver: str | Path, planning_seconds: float = 120,
                 planning_parser=None, planner_client=None, generation_calls: int = 3):
        self.supervisor, self.workspaces = supervisor, workspaces
        self._precheck, self.seed_solver = precheck, Path(seed_solver)
        self.planning_seconds, self.planning_parser = planning_seconds, planning_parser
        self.planner_client = planner_client
        if type(generation_calls) is not int or generation_calls < 1:
            raise ValueError("Generation request cap must be a positive integer")
        self.generation_calls = generation_calls
        self.episode_id = "unstarted"
        self._workspace_base = workspaces.root
        self.last_planning_receipt = None
        self.last_planning_context = None
        self._agents = {}
        self._parents: dict[str, str] = {}
        self._generation_spent: dict[tuple[str, int], int] = {}
        self._warm_memory = {}
        self._checkpoint_memory = {}
        self._directions = {}
        self.board = {}
        self._workspace_lock = threading.RLock()
        self._generation_usage = threading.local()

    @property
    def last_generation_usage(self):
        """Receipts belong to the calling generation worker, never another agent."""
        return getattr(self._generation_usage, "value", {})

    @last_generation_usage.setter
    def last_generation_usage(self, value):
        self._generation_usage.value = value

    def preflight(self, config=None) -> dict:
        if self.supervisor.simulation:
            # FakeRuntime contract tests explicitly opt in; a real CLI must reject.
            mode = getattr(config, "execution_mode", "mock") if config is not None else "mock"
            if mode != "mock":
                raise BackendUnavailable("A simulated supervisor cannot run a real backend profile")
        report = self.supervisor.preflight()
        if not self.seed_solver.is_dir():
            raise BackendUnavailable("Configured verified-seed solver directory is missing")
        if self._precheck is None:
            raise BackendUnavailable("A bounded toy-only precheck is required")
        if self.planner_client is None:
            raise BackendUnavailable("V1 requires a single structured PlannerClient; CLI planning is disabled")
        return report

    def begin_episode(self, episode_id: str, states=None):
        import re
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", episode_id):
            raise ValueError("Unsafe episode ID")
        self.supervisor.close()
        if self.episode_id.startswith("warm-") and not self._warm_memory:
            for agent_id, workspace in self._agents.items():
                self._warm_memory[agent_id] = self.workspaces.freeze(workspace.private)
        self.supervisor._sessions.clear()
        self._agents.clear()
        self.board = {}
        self._generation_spent.clear()
        self._checkpoint_memory.clear()
        self.episode_id = episode_id
        self.request_episode_id = episode_id
        self.planner_client.episode_id = episode_id
        self.workspaces = WorkspaceStore(self._workspace_base / episode_id, self.workspaces.artifact_root)

    def set_directions(self, briefs):
        self._directions = {brief["agent_id"]: dict(brief) for brief in briefs}

    def export_warm_memory(self):
        self.supervisor.close()
        if not self._warm_memory:
            for agent_id, workspace in self._agents.items():
                self._warm_memory[agent_id] = self.workspaces.freeze(workspace.private)
        return {agent: dict(reference) for agent, reference in self._warm_memory.items()}

    def restore_warm_memory(self, manifest):
        from .workspace import candidate_tree_hash
        validated = {}
        for agent, reference in manifest.items():
            if candidate_tree_hash(reference["candidate_path"]) != reference["candidate_hash"]:
                raise PermissionError("Warm private-memory snapshot identity mismatch")
            validated[agent] = dict(reference)
        self._warm_memory = validated

    def export_checkpoint_memory(self):
        """Current own semantic docs; does not overwrite the common warm prefix."""
        self.supervisor.close()
        manifest = {agent: dict(reference) for agent, reference in self._warm_memory.items()}
        manifest.update(self._checkpoint_memory)
        for agent, workspace in self._agents.items():
            manifest[agent] = self.workspaces.freeze(workspace.private)
        return manifest

    def restore_checkpoint_memory(self, manifest):
        from .workspace import candidate_tree_hash
        for reference in manifest.values():
            if candidate_tree_hash(reference["candidate_path"]) != reference["candidate_hash"]:
                raise PermissionError("Checkpoint memory identity mismatch")
        self._checkpoint_memory = {agent: dict(reference) for agent, reference in manifest.items()}

    def isolate_recovered_workspaces(self, namespace):
        """Keep an interrupted attempt's local files as evidence on resume."""
        import re
        if not re.fullmatch(r'[A-Za-z0-9_.-]+',namespace) or self._agents:
            raise PermissionError('Recovery workspace namespace is invalid or already active')
        target=self._workspace_base/namespace
        if target.exists() and any(target.iterdir()):
            raise FileExistsError('Recovery workspace namespace already consumed')
        self.workspaces=WorkspaceStore(target,self.workspaces.artifact_root)

    def set_board(self, board: dict):
        with self._workspace_lock:
            self._set_board_locked({} if self.episode_id.startswith('warm-') else board)

    def _set_board_locked(self, board: dict, *, workspaces=None):
        self.board = dict(board)
        # Materialize only evaluator-approved champion code and public facts.
        champion_hash = board.get("champion_hash")
        champion = self._parents.get(champion_hash)
        if not champion and champion_hash:
            candidate = self.workspaces.artifact_root / champion_hash
            champion = str(candidate) if candidate.is_dir() else None
        if champion:
            from .workspace import candidate_tree_hash
            if candidate_tree_hash(champion) != champion_hash:
                raise PermissionError("Public champion artifact hash mismatch")
        for workspace in (self._agents.values() if workspaces is None else workspaces):
            path = workspace.public / "board.json"
            path.write_text(json.dumps(board, ensure_ascii=False), encoding="utf-8")
            target = workspace.public / "champion"
            if target.exists() or target.is_symlink():
                from .workspace import remove_private_path
                remove_private_path(target, workspace.public)
            if champion and Path(champion).is_dir():
                import shutil
                from .workspace import solver_files
                target.mkdir()
                for source, relative in solver_files(Path(champion)):
                    destination = target / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, destination)

    def prepare_seed(self, path=None, config=None) -> dict:
        return self.workspaces.freeze(Path(path) if path else self.seed_solver)

    def register_parent(self, candidate_hash: str, candidate_path: str | Path) -> None:
        from .workspace import candidate_tree_hash
        if candidate_tree_hash(candidate_path) != candidate_hash:
            raise PermissionError("Parent artifact hash mismatch")
        self._parents[candidate_hash] = str(candidate_path)

    def _workspace(self, state):
        with self._workspace_lock:
            return self._workspace_locked(state)

    def _workspace_locked(self, state):
        if state.agent_id not in self._agents:
            parent = self._parents.get(state.parent_hash, str(self.seed_solver))
            self._agents[state.agent_id] = self.workspaces.create(state.agent_id, parent)
            workspace = self._agents[state.agent_id]
            memory = self._checkpoint_memory.get(state.agent_id, self._warm_memory.get(state.agent_id))
            if memory:
                import shutil
                from .workspace import solver_files
                for source, relative in solver_files(Path(memory["candidate_path"])):
                    destination = workspace.private / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, destination)
            # Another agent can already be generating. Provisioning this
            # workspace must not remove/rebuild that peer's mounted champion.
            self._set_board_locked(self.board, workspaces=(workspace,))
        return self._agents[state.agent_id]

    def _planning_view(self, state):
        from .workspace import solver_files
        parent = Path(self._parents.get(state.parent_hash, self.seed_solver))
        documents, manifest, remaining = [], [], 4096
        for source, relative in solver_files(parent):
            content = source.read_bytes()
            if len(manifest) < 32:
                manifest.append({"path": relative.as_posix()[:256], "bytes": len(content)})
            if remaining > 0:
                snippet = content[:remaining].decode("utf-8", "replace")
                documents.append({"path": relative.as_posix(), "text": snippet,
                                  "truncated": len(content) > remaining})
                remaining -= min(len(content), remaining)
        own_docs, memory_remaining = [], 2048
        workspace = self._agents.get(state.agent_id)
        reference = self._checkpoint_memory.get(state.agent_id, self._warm_memory.get(state.agent_id))
        private = workspace.private if workspace else (Path(reference["candidate_path"]) if reference else None)
        if private:
            for source, relative in solver_files(private):
                if memory_remaining <= 0 or len(own_docs) >= 16:
                    break
                content = source.read_bytes()
                own_docs.append({"path": relative.as_posix(),
                    "text": content[:memory_remaining].decode("utf-8", "replace"),
                    "truncated": len(content) > memory_remaining})
                memory_remaining -= min(len(content), memory_remaining)
        return {"parent_source": documents, "source_manifest": manifest,
                "direction_brief": self._directions.get(state.agent_id, {}),
                "own_private_docs": own_docs}

    def plan(self, state, round_index: int, context: dict) -> Plan:
        result = self.plan_with_receipt(state, round_index, context,
                                       deadline=time.monotonic()+self.planning_seconds)
        self.last_planning_receipt, self.last_planning_context = result[1], result[2]
        return result[0]

    planning_capabilities = {"single_request": True, "hard_request_timeout": True,
                             "mutates_input_state": False}

    def plan_with_receipt(self, state, round_index: int, context: dict, *, deadline: float):
        if self.planner_client is None:
            raise BackendUnavailable("V1 planning cannot start an autonomous CLI; configure PlannerClient")
        if deadline <= time.monotonic():
            raise TimeoutError("Common planning deadline elapsed before request")
        request_id = f"{self.episode_id}:{round_index}:{state.agent_id}:plan"
        decision, receipt, assembled = self.planner_client.plan(
            state, round_index, context.get("board", context), request_id=request_id,
            harness_context=self._planning_view(state), token_budget=32768, deadline=deadline)
        return decision, receipt, assembled

    def cancel_planning(self, agent_id, round_index):
        # Closing a client socket does not prove a remote model stopped.
        return False

    def generate(self, state, plan: Plan, round_index: int, revision: int = 0,
                 deadline: float | None = None, *, grant=None, grant_authority=None) -> dict:
        from maestro.execution.grants import EditGrant, GrantAuthority, GrantError
        if not isinstance(grant, EditGrant) or not isinstance(grant_authority, GrantAuthority):
            raise GrantError("Generation requires the round owner's signed EditGrant")
        grant_authority.verify(grant, agent_id=state.agent_id, plan_version_id=plan.version_id,
                               parent_hash=plan.parent_hash, epoch=revision)
        if (grant.episode_id, grant.round_index) != (self.episode_id, round_index):
            raise GrantError("EditGrant belongs to another episode or round")
        if plan.agent_id != state.agent_id or plan.parent_hash != state.parent_hash:
            raise PermissionError("Generation plan/agent/parent mismatch")
        key = state.agent_id, round_index, plan.version_id
        with self._workspace_lock:
            spent = self._generation_spent.get(key, 0)
            if revision != spent or spent >= self.generation_calls:
                raise PermissionError("Generation invitation budget already consumed")
            self._generation_spent[key] = spent + 1
        workspace = self._workspace(state)
        if revision == 0:
            parent_path = self._parents.get(state.parent_hash)
            if parent_path:
                self.workspaces.restore(workspace, parent_path)
        prompt = ("Implement only this admitted refinement plan; do not access the benchmark panel. "
                  "Toy debugging is bounded by the phase resource contract. Formal evaluation is denied "
                  "until a separately issued EvalTicket is claimed.\n" + json.dumps({
                      "plan_version_id": plan.version_id, "parent_hash": plan.parent_hash,
                      "target": plan.target, "mod_type": plan.mod_type, "change": plan.change,
                      "revision": revision}, ensure_ascii=False))
        shared = ("No global champion or peer board is available during Warm Start. "
                  if self.episode_id.startswith('warm-') else
                  f"Read-only shared verified board/champion: {workspace.public}. ")
        prompt += (f"\nPrivate memory: {workspace.private}. " + shared +
                   "Direction brief: " + json.dumps(self._directions.get(state.agent_id, {}), ensure_ascii=False))
        account = f"{self.episode_id}:{round_index}:{state.agent_id}:GENERATION:{plan.version_id}"
        usage_reader = getattr(self.supervisor.provider, "usage", lambda account: {})
        before = usage_reader(account)
        try:
            self.supervisor.run_phase(workspace, phase="GENERATE", prompt=prompt,
                                      deadline=deadline or time.monotonic() + 300,
                                      max_calls=self.generation_calls, plan_version_id=plan.version_id,
                                      parent_hash=plan.parent_hash, budget_scope=account, round_index=round_index)
        finally:
            after = usage_reader(account)
            self.last_generation_usage = {key: max(0, (after.get(key) or 0) - (before.get(key) or 0))
                for key in ("calls", "known_input_tokens", "known_output_tokens", "reserved_output_tokens", "cpu_seconds", "wall_seconds")}
            complete = after.get("usage_complete", False)
            self.last_generation_usage.update(usage_complete=complete,
                input_tokens=self.last_generation_usage["known_input_tokens"] if complete else None,
                output_tokens=self.last_generation_usage["known_output_tokens"] if complete else None)
            self.last_generation_usage["account"] = account
            self.last_generation_usage["measurement"] = "trusted_provider" if after else "unavailable"
            self.last_generation_usage["unresolved_requests"] = after.get("unresolved_requests", 0)
            if after.get("unresolved_requests", 0):
                # Stopping the local CLI is not evidence that a forwarded
                # provider inference completed. Keep its reservation and stop
                # this round before freezing/admitting any candidate.
                from maestro.llm import InferenceRequestUnknown
                raise InferenceRequestUnknown("Generation provider request remains unresolved",
                    request_id=account, scope_id=account,
                    context={"agent_id":state.agent_id,"plan_version_id":plan.version_id})
        return self.workspaces.freeze(workspace.solver)

    def precheck(self, candidate: dict) -> tuple[bool, str]:
        return self._precheck(candidate)

    def close(self):
        self.supervisor.close()
        if callable(getattr(self.supervisor.provider, "close", None)):
            self.supervisor.provider.close()
        if getattr(self, "_owns_budget_store", False):
            self.budget_store.close()
            self._owns_budget_store = False


def from_config(config, root=None, entrypoint: str | None = None) -> CoralBackend:
    """Construct operator-supplied provisioning without inventing credentials.

    The factory must return a CoralBackend (or compatible live driver), using a
    configured enforcing provider and persistent inference gateway transport.
    """
    settings = getattr(config, "coral", {}) or {}
    entrypoint = entrypoint or settings.get("backend_factory")
    backend = load_factory(entrypoint)(config, root) if entrypoint else default_factory(config, root)
    for name in ("preflight", "prepare_seed", "plan", "generate", "precheck"):
        if not callable(getattr(backend, name, None)):
            raise BackendUnavailable(f"Coral backend lacks required method: {name}")
    backend.preflight(config)
    return backend


def default_factory(config, root=None) -> CoralBackend:
    """Packaged live path; no user-written backend implementation is required.

    Required operator settings: digest-pinned ``generation_image``, planning and
    generation HTTPS API origins, model names and credential environment names.
    The optional custom factory remains available for another audited runtime.
    """
    import os
    from maestro.execution.budgets import BudgetStore, BudgetExceeded
    from maestro.execution.gateway import HTTPUpstream
    from maestro.execution.sandbox import DockerRunner, ResourceLimits
    from maestro.llm import PlannerClient, ProviderResponse
    from .provider import DockerAgentSandboxProvider
    from .runtime import resolve_runtime
    from .runtime import verify_checkout
    from .supervisor import ControlledCoralSupervisor
    from maestro.tasks.pace.runner import smoke_test
    from maestro.tasks.pace.sanity import default_sanity_graph_path

    settings = config.coral
    required = ("generation_image", "model", "planning_api_base", "planning_model", "generation_api_base")
    missing = [key for key in required if not settings.get(key)]
    if missing:
        raise BackendUnavailable("Default Coral backend requires coral." + ", coral.".join(missing))
    provider_model = settings.get("provider_model", settings["model"])
    if provider_model.lower() in {"sonnet", "opus", "haiku", "default", "best"}:
        raise BackendUnavailable("Configure a full canonical provider_model; runtime aliases cannot serve as a frozen wire-model contract")
    provenance = verify_checkout(settings.get("checkout_path"))
    if settings.get("runtime", "claude_code") != "claude_code":
        raise BackendUnavailable("Packaged default factory currently supports claude_code; custom runtimes require a qualified provider")
    planning_key_env = settings.get("planning_api_key_env", "MAESTRO_PLANNING_API_KEY")
    generation_key_env = settings.get("generation_api_key_env", "MAESTRO_GENERATION_API_KEY")
    if not os.environ.get(planning_key_env) or not os.environ.get(generation_key_env):
        raise BackendUnavailable(f"Set both credential environment variables: {planning_key_env}, {generation_key_env}")
    root = Path(root or settings.get("run_root", "artifacts/runs/live")).resolve()
    from maestro.storage.paths import run_paths, require_separated_runtime
    require_separated_runtime(root)
    local = run_paths(root).local
    authority = root / "coral-authority"
    authority.mkdir(parents=True, exist_ok=True, mode=0o700)
    store = BudgetStore(authority / "budgets.sqlite")
    planning_upstream = HTTPUpstream(settings["planning_api_base"], os.environ[planning_key_env])
    generation_upstream = HTTPUpstream(settings["generation_api_base"], os.environ[generation_key_env], auth_header="x-api-key",
                                       anthropic_beta=tuple(settings.get("anthropic_beta", ())))
    provider = DockerAgentSandboxProvider(local / "coral-authority", store, image=settings["generation_image"],
        upstream=generation_upstream, limits=ResourceLimits(
            cpu_seconds=config.generation_cpu_seconds, wall_seconds=config.generation_window_seconds,
            memory_mb=max(1, config.memory_bytes//(1024*1024)), pids=config.pids_max,
            output_bytes=config.output_bytes),
        max_output_tokens=int(settings.get("generation_output_tokens", 4096)), allowed_model=provider_model)
    reservations = {}
    raw_usages, request_hashes = {}, {}

    def reserve(scope, request_id, input_upper_bound, output_bound):
        try:
            store.snapshot(scope)
        except BudgetExceeded:
            store.create(scope, max_calls=1, max_request_bytes=max(input_upper_bound, 4096),
                         max_output_tokens=output_bound, deadline=time.time()+float(settings.get("planning_seconds", 120)))
        reservations[request_id] = store.reserve(scope, request_id,
            request_bytes=input_upper_bound, output_tokens=output_bound)
        return True

    def record_usage(receipt):
        reservation = reservations[receipt.request_id]
        usage = raw_usages.pop(receipt.request_id, {key: value for key,value in {
            "prompt_tokens": receipt.input_tokens, "completion_tokens": receipt.output_tokens}.items() if value is not None})
        store.save_receipt(reservation, usage,
            request_hashes.pop(receipt.request_id, receipt.context_hash), receipt.provider_request_id, protocol="/v1/chat/completions")
        store.finish(reservation, actual_output_tokens=receipt.output_tokens)

    class PlanningProvider:
        is_live = True

        def __call__(self, payload):
            remaining = payload.get("deadline", time.monotonic()+60) - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Shared planning deadline elapsed")
            transport = HTTPUpstream(settings["planning_api_base"], os.environ[planning_key_env],
                                     timeout=min(60,remaining), absolute_deadline=payload.get("deadline"))
            request_body = {"_maestro_http_path": "/v1/chat/completions",
                "model": settings["planning_model"], "messages": payload["messages"],
                "max_tokens": payload["max_output_tokens"], "temperature": 0,
                "response_format": {"type": "json_object"}}
            from maestro.types import stable_hash
            request_hashes[payload["request_id"]] = stable_hash(request_body)
            result = transport(request_body)
            decoded = json.loads(result["_raw_body"])
            usage = decoded.get("usage", {})
            raw_usages[payload["request_id"]] = usage
            return ProviderResponse(decoded["choices"][0]["message"]["content"], str(decoded.get("id", "")),
                                    usage.get("prompt_tokens"), usage.get("completion_tokens"))

    planner = PlannerClient(PlanningProvider(), reserve=reserve, record_usage=record_usage,
                            max_output_tokens=int(settings.get("planning_output_tokens", 2048)))
    supervisor = ControlledCoralSupervisor(resolve_runtime("claude_code"), provider,
        model=settings.get("runtime_model", settings["model"]), log_root=authority / "runtime-logs")
    workspace_store = WorkspaceStore(local / "coral-workspaces", root / "coral-cas")
    pace_contract = config.pace.get("contract", {})
    candidate_runner = DockerRunner(pace_contract.get("image"))
    toy = Path(settings.get("smoke_graph", default_sanity_graph_path()))
    if not toy.is_file():
        raise BackendUnavailable("Configured fixed toy smoke graph is missing")
    from maestro.tasks.pace.manifest import load_panel, assert_smoke_disjoint
    assert_smoke_disjoint(load_panel(config.pace["panel_manifest"], expected_split="training"), toy)

    def precheck(candidate):
        return smoke_test(candidate["candidate_path"], toy, candidate_runner,
            image=pace_contract.get("image", ""),
            solver_argv=tuple(pace_contract.get("solver_argv", ("/candidate/run",))))

    backend = CoralBackend(supervisor, workspace_store, precheck=precheck,
        seed_solver=config.pace.get("seed_solver", ""), planner_client=planner,
        planning_seconds=float(settings.get("planning_seconds", 120)), generation_calls=config.generation_calls)
    backend.candidate_runner = candidate_runner
    backend.provenance = provenance
    backend.budget_store = store
    backend._owns_budget_store = True
    if settings.get("director_mode", "live") == "live":
        def director_provider(context):
            from maestro.llm import PlanningReceipt
            request_id = f"{backend.episode_id}:DIRECTOR"
            scope = request_id
            prompt = {"messages": [{"role": "system", "content":
                "Return JSON {briefs:[...]}; assign every supplied agent a distinct strategy direction. "
                "Each brief requires direction_id,agent_id,objective,allowed_focus,initial_hypotheses,diversity_rationale. "
                "Use only allowed_targets. Do not allocate market tickets or set bids."},
                {"role": "user", "content": json.dumps(context)}],
                "request_id": request_id, "max_output_tokens": 2048}
            reserve(scope, request_id, len(json.dumps(prompt).encode())+2048, 2048)
            response = PlanningProvider()(prompt)
            reservation = reservations[request_id]
            from maestro.types import stable_hash
            store.save_receipt(reservation, raw_usages.pop(request_id, {}),
                request_hashes.pop(request_id, stable_hash(prompt)), response.provider_request_id, protocol="/v1/chat/completions")
            store.finish(reservation, actual_output_tokens=response.output_tokens)
            backend.last_director_receipt = {"request_id": request_id,
                "actual_requests": 1, "input_tokens": response.input_tokens,
                "output_tokens": response.output_tokens,
                "provider_request_id": response.provider_request_id}
            return response.text
        backend.director_provider = director_provider
    elif settings.get("director_mode") != "predeclared":
        raise BackendUnavailable("director_mode must explicitly be live or predeclared")
    try:
        backend.preflight(config)
    except BaseException:
        backend.close()
        raise
    return backend
