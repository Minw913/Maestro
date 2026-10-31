from pathlib import Path
from types import SimpleNamespace
import json
import time

import pytest

from maestro.backends.base import BackendUnavailable
from maestro.backends.coral.backend import CoralBackend
from maestro.backends.coral.supervisor import ControlledCoralSupervisor
from maestro.backends.coral.workspace import WorkspaceStore, candidate_tree_hash
from maestro.config import RunConfig
from maestro.llm import PlannerClient
from maestro.types import AgentState


class FakeBoundary:
    """Explicit simulated containment, never accepted by a live RunConfig."""
    def __init__(self):
        self.contexts, self.stopped = [], set()

    def preflight(self):
        return {key: True for key in ("filesystem_isolation", "process_tree_limits",
            "monotonic_phase_deadline", "gateway_only_inference", "persistent_inference_quota",
            "private_git_objects")} | {"simulation": True}

    def prepare_phase(self, context):
        self.contexts.append(context)
        return SimpleNamespace(command_prefix=[], env={})

    def stop_scope(self, agent_id, epoch):
        self.stopped.add((agent_id, epoch))

    def quiescent(self, agent_id, epoch):
        return (agent_id, epoch) in self.stopped


class FakeHandle:
    alive = False
    def __init__(self, log_path):
        self.log_path = log_path
    def stop(self):
        self.alive = False


class FakeRuntime:
    """Explicit signature mirrors the inspected public Coral AgentRuntime API."""
    def __init__(self):
        self.starts = []

    def start(self, worktree_path, coral_md_path, model="sonnet", runtime_options=None,
              max_turns=0, log_dir=None, verbose=False, resume_session_id=None,
              prompt=None, prompt_source=None, task_name=None, task_description=None,
              gateway_url=None, gateway_api_key=None, run_as_user=None, sandbox=None):
        self.starts.append({"worktree": worktree_path, "prompt_source": prompt_source,
                            "max_turns": max_turns, "session": resume_session_id})
        target = worktree_path / "solver.py"
        target.chmod(0o666)
        target.write_text("print('1\\n1')\n# revised\n", encoding="utf-8")
        log = Path(log_dir) / "fake.jsonl"; log.parent.mkdir(parents=True, exist_ok=True); log.write_text("{}")
        return FakeHandle(log)

    def extract_session_id(self, path):
        return "session-from-simulated-log"


def backend(tmp_path):
    seed = tmp_path / "seed"; seed.mkdir(); (seed / "solver.py").write_text("print('1\\n1')")
    runtime, boundary = FakeRuntime(), FakeBoundary()
    supervisor = ControlledCoralSupervisor(runtime, boundary, model="fake", log_root=tmp_path / "logs", simulation=True)
    client = PlannerClient(lambda payload: json.dumps({"action": "revise", "plan": {
        "target": "local_search", "mod_type": "parameter_change", "diagnosis": "bounded diagnosis",
        "change": "change one local-search constant", "expected_effect": "improve", "evidence_run_ids": []}}))
    store = WorkspaceStore(tmp_path / "agents", tmp_path / "cas")
    driver = CoralBackend(supervisor, store, precheck=lambda candidate: (True, "toy pass"),
                          seed_solver=seed, planner_client=client)
    frozen = driver.prepare_seed()
    driver.register_parent(frozen["candidate_hash"], frozen["candidate_path"])
    state = AgentState("a", frozen["candidate_hash"], 0.2)
    driver.begin_episode("warm-test", {"a": state})
    return driver, runtime, boundary, state


def test_single_structured_plan_never_starts_cli(tmp_path):
    driver, runtime, boundary, state = backend(tmp_path)
    decision = driver.plan(state, 0, {"champion_hash": state.parent_hash, "champion_score": 0.2})
    assert decision.action == "revise" and decision.plan.parent_hash == state.parent_hash
    assert runtime.starts == []
    assert driver.last_planning_receipt.actual_requests == 1
    driver.close()


def test_generation_uses_bound_coral_signature_stops_then_freezes(tmp_path):
    driver, runtime, boundary, state = backend(tmp_path)
    decision = driver.plan(state, 0, {})
    from maestro.execution.grants import GrantAuthority
    authority = GrantAuthority()
    grant = authority.issue(episode_id=driver.episode_id, round_index=0, agent_id=state.agent_id,
        plan_version_id=decision.plan.version_id, parent_hash=state.parent_hash,
        epoch=0, expires_at=time.time()+30)
    candidate = driver.generate(state, decision.plan, 0, 0, time.monotonic()+30,
                                grant=grant, grant_authority=authority)
    context = boundary.contexts[0]
    assert not context.read_only_solver and context.plan_version_id == decision.plan.version_id
    assert context.budget_scope.startswith("warm-test:0:a:GENERATION:")
    assert boundary.stopped == {("a", 0)}
    assert candidate_tree_hash(candidate["candidate_path"]) == candidate["candidate_hash"]
    assert runtime.starts[0]["max_turns"] == 0  # Actual calls are gateway-accounted.
    assert driver.precheck(candidate) == (True, "toy pass")
    driver.close()


def test_generation_without_signed_grant_never_starts_runtime(tmp_path):
    driver, runtime, boundary, state = backend(tmp_path)
    decision = driver.plan(state, 0, {})
    with pytest.raises(PermissionError, match='signed EditGrant'):
        driver.generate(state, decision.plan, 0, 0, time.monotonic()+30)
    assert not runtime.starts
    driver.close()


def test_generation_rejects_wrong_round_grant_before_runtime(tmp_path):
    from maestro.execution.grants import GrantAuthority
    driver, runtime, boundary, state = backend(tmp_path)
    decision = driver.plan(state, 0, {})
    authority = GrantAuthority()
    grant = authority.issue(episode_id=driver.episode_id, round_index=1, agent_id=state.agent_id,
        plan_version_id=decision.plan.version_id, parent_hash=state.parent_hash,
        epoch=0, expires_at=time.time()+30)
    with pytest.raises(PermissionError, match='another episode or round'):
        driver.generate(state, decision.plan, 0, 0, time.monotonic()+30,
                        grant=grant, grant_authority=authority)
    assert not runtime.starts
    driver.close()


def test_lower_configured_request_cap_reaches_the_provider(tmp_path):
    from maestro.execution.grants import GrantAuthority
    driver, runtime, boundary, state = backend(tmp_path)
    driver.generation_calls = 1
    decision = driver.plan(state, 0, {})
    authority = GrantAuthority()
    grant = authority.issue(episode_id=driver.episode_id, round_index=0, agent_id=state.agent_id,
        plan_version_id=decision.plan.version_id, parent_hash=state.parent_hash,
        epoch=0, expires_at=time.time()+30)
    driver.generate(state, decision.plan, 0, 0, time.monotonic()+30,
                    grant=grant, grant_authority=authority)
    assert boundary.contexts[0].max_calls == 1
    driver.close()


@pytest.mark.parametrize('unresolved',[True,False])
def test_generation_distinguishes_pending_inference_from_missing_usage(tmp_path,unresolved,monkeypatch):
    from maestro.execution.grants import GrantAuthority
    from maestro.llm import InferenceRequestUnknown
    driver, runtime, boundary, state = backend(tmp_path)
    decision = driver.plan(state, 0, {})
    readings = iter([{}, {'calls':1,'usage_complete':False,'unknown_requests':1,
                         'unresolved_requests':int(unresolved)}])
    boundary.usage = lambda account: next(readings)
    authority = GrantAuthority()
    grant = authority.issue(episode_id=driver.episode_id, round_index=0, agent_id=state.agent_id,
        plan_version_id=decision.plan.version_id, parent_hash=state.parent_hash,
        epoch=0, expires_at=time.time()+30)
    if unresolved:
        def forbidden_freeze(*args):
            raise AssertionError('Unresolved inference published a candidate')
        monkeypatch.setattr(driver.workspaces,'freeze',forbidden_freeze)
        with pytest.raises(InferenceRequestUnknown):
            driver.generate(state,decision.plan,0,deadline=time.monotonic()+30,
                grant=grant,grant_authority=authority)
    else:
        candidate=driver.generate(state,decision.plan,0,deadline=time.monotonic()+30,
            grant=grant,grant_authority=authority)
        assert candidate['candidate_hash']==candidate_tree_hash(candidate['candidate_path'])
    assert driver.last_generation_usage['usage_complete'] is False
    driver.close()


@pytest.mark.parametrize('termination_error',[True,False])
def test_unconfirmed_scope_stop_quarantines_generation(tmp_path,termination_error,monkeypatch):
    from maestro.execution.grants import GrantAuthority
    from maestro.backends.coral.supervisor import PhaseQuiescenceUnknown
    driver, runtime, boundary, state = backend(tmp_path)
    decision = driver.plan(state,0,{})
    authority = GrantAuthority()
    grant = authority.issue(episode_id=driver.episode_id,round_index=0,agent_id=state.agent_id,
        plan_version_id=decision.plan.version_id,parent_hash=state.parent_hash,
        epoch=0,expires_at=time.time()+30)
    original_stop, original_quiescent = boundary.stop_scope, boundary.quiescent
    if termination_error:
        def failed_stop(*args):
            raise OSError('executor disconnected during termination')
        monkeypatch.setattr(boundary,'stop_scope',failed_stop)
    else:
        monkeypatch.setattr(boundary,'quiescent',lambda *args: False)
    try:
        with pytest.raises(PhaseQuiescenceUnknown) as caught:
            driver.generate(state,decision.plan,0,deadline=time.monotonic()+30,
                grant=grant,grant_authority=authority)
        assert caught.value.inflight_unknown is True
        assert state.agent_id in driver.supervisor._handles
    finally:
        monkeypatch.setattr(boundary,'stop_scope',original_stop)
        monkeypatch.setattr(boundary,'quiescent',original_quiescent)
        driver.close()


def test_fake_runtime_cannot_enter_live_mode(tmp_path):
    driver, runtime, boundary, state = backend(tmp_path)
    with pytest.raises(BackendUnavailable):
        driver.preflight(RunConfig(execution_mode="real"))
    driver.close()


def test_warm_private_docs_clone_across_process_not_sessions(tmp_path):
    driver, runtime, boundary, state = backend(tmp_path)
    workspace = driver._workspace(state)
    (workspace.private / "lesson.md").write_text("A measured warm lesson")
    manifest = driver.export_warm_memory()
    driver.begin_episode("arm-one", {"a": state})
    assert driver._workspace(state).private.joinpath("lesson.md").read_text() == "A measured warm lesson"
    assert driver.supervisor._sessions == {}
    (driver._workspace(state).private / "lesson.md").chmod(0o666)
    (driver._workspace(state).private / "lesson.md").write_text("one-arm-private-change")
    driver.begin_episode("arm-two", {"a": state})
    assert driver._workspace(state).private.joinpath("lesson.md").read_text() == "A measured warm lesson"
    driver.restore_warm_memory(manifest)
    driver.close()


def test_public_champion_resolves_hash_to_readonly_artifact(tmp_path):
    driver, runtime, boundary, state = backend(tmp_path)
    driver.set_board({"champion_hash": state.parent_hash, "champion_score": 0.2})
    warm = driver._workspace(state)
    assert json.loads((warm.public / "board.json").read_text()) == {}
    assert not (warm.public / "champion").exists()
    driver.begin_episode("market-test", {"a": state})
    driver.set_board({"champion_hash": state.parent_hash, "champion_artifact": state.parent_hash, "champion_score": 0.2})
    workspace = driver._workspace(state)
    assert (workspace.public / "champion" / "solver.py").is_file()
    assert json.loads((workspace.public / "board.json").read_text())["champion_score"] == 0.2
    assert (workspace.solver / ".git").is_dir()
    assert not (workspace.solver / ".git" / "objects" / "info" / "alternates").exists()
    driver.close()


def test_new_workspace_does_not_replace_a_running_peers_public_tree(tmp_path,monkeypatch):
    import maestro.backends.coral.workspace as workspace_module
    driver, runtime, boundary, state = backend(tmp_path)
    driver.begin_episode("market-test", {"a": state})
    driver.set_board({'champion_hash':state.parent_hash,'champion_score':.2})
    active = driver._workspace(state)
    remove = workspace_module.remove_private_path
    def protected(path, allowed_root):
        if Path(path).resolve().is_relative_to(active.public):
            raise AssertionError('Provisioning B replaced the public tree mounted by active A')
        return remove(path,allowed_root)
    monkeypatch.setattr(workspace_module,'remove_private_path',protected)
    peer = driver._workspace(AgentState('b',state.parent_hash,.2))
    assert (active.public/'champion'/'solver.py').read_bytes()==(peer.public/'champion'/'solver.py').read_bytes()
    driver.close()


def test_current_checkpoint_memory_does_not_replace_common_warm(tmp_path):
    driver, runtime, boundary, state = backend(tmp_path)
    private = driver._workspace(state).private
    (private / "lesson.md").write_text("warm")
    warm = driver.export_warm_memory()
    driver.begin_episode("arm", {"a": state})
    current = driver._workspace(state).private / "lesson.md"
    current.chmod(0o666); current.write_text("current")
    checkpoint = driver.export_checkpoint_memory()
    assert checkpoint["a"]["candidate_hash"] != warm["a"]["candidate_hash"]
    driver.begin_episode("resumed-arm", {"a": state})
    driver.restore_checkpoint_memory(checkpoint)
    assert (driver._workspace(state).private / "lesson.md").read_text() == "current"
    assert driver._warm_memory == warm
    assert driver._planning_view(state)["own_private_docs"][0]["text"] == "current"
    driver.close()
