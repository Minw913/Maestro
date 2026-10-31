"""Offline interface tests; none of these dispatch a real model request."""
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

import pytest

from maestro.backends.base import BackendUnavailable
from maestro.backends.codex_session.client import CodexSessionClient, SessionResponse, strict_v1_factory
from maestro.backends.codex_session.backend import CodexSessionBackend, CodexSessionPlanner, SessionTurnStore
from maestro.backends.coral.workspace import WorkspaceStore, candidate_tree_hash
from maestro.execution.grants import GrantAuthority, GrantError
from maestro.llm import InferenceBudgetExceeded
from maestro.llm import InferenceRequestUnknown, PlanningResponseError
from maestro.types import AgentState, Plan


def events(value, *, extra=()):
    return "\n".join(json.dumps(value) for value in [
        {"type": "thread.started", "thread_id": "local-session-id"},
        {"type": "turn.started"}, *extra,
        {"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(value)}},
        {"type": "turn.completed", "usage": {"input_tokens": 80, "output_tokens": 20}}])


class FakeCLI:
    def __init__(self, value, *, auth="Logged in using ChatGPT", extra=()):
        self.value, self.auth, self.extra, self.calls = value, auth, extra, []

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if argv[1:3] == ["login", "status"]:
            return subprocess.CompletedProcess(argv, 0, "", self.auth)
        return subprocess.CompletedProcess(argv, 0, events(self.value, extra=self.extra), "")


def client(tmp_path, fake):
    executable = tmp_path / "codex.exe"
    executable.touch()
    return CodexSessionClient(model="test-frozen-model", work_root=tmp_path / "broker",
        executable=executable, runner=fake, environment={"CODEX_HOME": "existing-login-home",
            "OPENAI_API_KEY": "not-to-be-used", "CODEX_API_KEY": "not-to-be-used",
            "CODEX_ACCESS_TOKEN": "not-to-be-used"})


def test_login_probe_is_tokenless_sanitized_and_does_not_infer(tmp_path):
    fake = FakeCLI({})
    transport = client(tmp_path, fake)
    assert transport.login_status() == {"authenticated": True, "auth_method": "chatgpt",
        "external_api_key_required": False, "strict_v1_request_quota": False}
    assert len(fake.calls) == 1
    env = fake.calls[0][1]["env"]
    assert env == {"CODEX_HOME": "existing-login-home"}


def test_structured_probe_uses_readonly_no_shell_existing_login(tmp_path):
    fake = FakeCLI({"ok": True})
    transport = client(tmp_path, fake)
    response = transport.structured_proposal({"task": "return ok"}, {"type": "object"}, purpose="planning")
    assert response.value == {"ok": True}
    assert response.actual_inference_requests is None and response.completed_turns == 1
    argv, kwargs = fake.calls[1]
    assert "--ignore-user-config" in argv and "--ephemeral" in argv
    assert argv[argv.index("--sandbox") + 1] == "read-only"
    assert "features.shell_tool=false" in argv and "agents.enabled=false" in argv
    assert not any("bypass" in option for option in argv)
    assert kwargs["env"] == {"CODEX_HOME": "existing-login-home"}
    assert "return ok" in kwargs["input_text"]


def test_nonfatal_cli_error_items_before_turn_are_counted_without_body_export(tmp_path):
    fake = FakeCLI({"ok": True})
    transport = client(tmp_path, fake)
    original = fake.__call__
    def runner(argv, **kwargs):
        response = original(argv, **kwargs)
        if argv[1] == "exec":
            lines = response.stdout.splitlines()
            diagnostics = [json.dumps({"type": "item.completed", "item": {
                "type": "error", "message": "private diagnostic content"}})] * 2
            response.stdout = "\n".join([lines[0], *diagnostics, *lines[1:]])
        return response
    transport.runner = runner
    response = transport.structured_proposal({}, {}, purpose="planning")
    assert response.value == {"ok": True} and response.diagnostic_count == 2
    assert "private diagnostic content" not in str(response)


def test_no_model_override_is_used_for_an_explicit_default_model_probe(tmp_path):
    fake = FakeCLI({"ok": True})
    transport = client(tmp_path, fake)
    transport.model = None
    transport.structured_proposal({}, {}, purpose="planning")
    assert "--model" not in fake.calls[1][0]


def test_login_delay_cannot_dispatch_past_common_deadline(tmp_path):
    fake = FakeCLI({"ok": True})
    transport = client(tmp_path, fake)
    def delayed_login(argv, **kwargs):
        response = fake(argv, **kwargs)
        if argv[1:3] == ["login", "status"]:
            time.sleep(.02)
        return response
    transport.runner = delayed_login
    with pytest.raises(TimeoutError):
        transport.structured_proposal({}, {}, purpose="planning", deadline=time.monotonic()+.01)
    assert len(fake.calls) == 1 and fake.calls[0][0][1] == "login"


def test_tools_fail_closed_and_api_key_auth_is_not_substituted(tmp_path):
    fake = FakeCLI({"ok": True}, extra=[{"type": "item.completed",
        "item": {"type": "command_execution", "command": "not run by this fake"}}])
    with pytest.raises(BackendUnavailable, match="Tool activity"):
        client(tmp_path, fake).structured_proposal({}, {}, purpose="planning")
    fake = FakeCLI({}, auth="Logged in using an API key")
    with pytest.raises(BackendUnavailable, match="no API token required"):
        client(tmp_path, fake).structured_proposal({}, {}, purpose="planning")
    assert len(fake.calls) == 1


def test_strict_v1_dispatches_to_a_separate_actual_request_backend(monkeypatch):
    from maestro.backends.codex_session import autonomous_backend
    config = SimpleNamespace(profile="v1_cooperative")
    expected = object()
    monkeypatch.setattr(autonomous_backend,"strict_factory",lambda c,root: expected if c is config else None)
    assert strict_v1_factory(config,"run-root") is expected


def test_durable_turn_budget_survives_restart_and_unknown(tmp_path):
    path = tmp_path / "turns.sqlite"
    store = SessionTurnStore(path)
    store.reserve("agent-plan", "request-1", cap=1, deadline=time.time()+60)
    store.finish("request-1", "UNKNOWN")
    store.close()
    reopened = SessionTurnStore(path)
    measured = reopened.usage("agent-plan")
    assert measured["calls"] == 0 and measured["session_turns"] == 1
    assert measured["request_count_unknown"] and measured["inflight_unknown"]
    assert measured["actual_requests"] is None and not measured["usage_complete"]
    with pytest.raises(InferenceBudgetExceeded):
        reopened.reserve("agent-plan", "request-2", cap=1, deadline=time.time()+60)
    with pytest.raises(InferenceBudgetExceeded, match="later rounds/episodes"):
        reopened.reserve("another-round-or-episode", "request-3", cap=1, deadline=time.time()+60)
    reopened.close()


def test_session_planning_receipt_is_not_a_one_inference_receipt(tmp_path):
    fake = FakeCLI({"action": "withdraw", "plan": None, "lessons": [], "reason": "no change can help this parent"})
    store = SessionTurnStore(tmp_path / "turns.sqlite")
    planner = CodexSessionPlanner(client(tmp_path, fake), store)
    state = AgentState("a", "frozen-parent", 0.2)
    decision, receipt, context = planner.plan(state, 0, {}, request_id="request-plan",
        deadline=time.monotonic()+30)
    assert decision.action == "withdraw" and context["agent_id"] == "a"
    assert receipt.actual_requests is None and receipt.session_turns == 1
    assert receipt.budget_unit == "session_turn" and receipt.request_count_unknown
    measured = store.usage(receipt.scope_id)
    assert measured["known_input_tokens"] == 80 and measured["known_output_tokens"] == 20
    assert measured["token_usage_complete"] and not measured["inflight_unknown"]
    with pytest.raises(InferenceBudgetExceeded):
        planner.plan(state, 0, {}, request_id="new-id-same-round", deadline=time.monotonic()+30)
    assert len(fake.calls) == 2  # One auth status, one proposal turn.
    store.close()


def backend(tmp_path):
    seed = tmp_path / "seed"
    seed.mkdir()
    (seed / "solver.py").write_text("print('seed')\n")
    fake = FakeCLI({"files": [{"path": "solver.py", "content": "print('candidate')\n"}]})
    transport = client(tmp_path, fake)
    driver = CodexSessionBackend(transport, SessionTurnStore(tmp_path / "turns.sqlite"),
        WorkspaceStore(tmp_path / "agents", tmp_path / "cas"), precheck=lambda c: (True, "toy"),
        seed_solver=seed, simulation=True)
    frozen = driver.prepare_seed()
    driver.register_parent(frozen["candidate_hash"], frozen["candidate_path"])
    driver.begin_episode("episode")
    state = AgentState("a", frozen["candidate_hash"], .2)
    plan = Plan(plan_id="plan", version_id="pv", agent_id="a", parent_hash=state.parent_hash,
        target="local_search", mod_type="parameter_change", diagnosis="bounded",
        change="replace one value", expected_effect="improve")
    authority = GrantAuthority()
    grant = authority.issue(episode_id="episode", round_index=0, agent_id="a", plan_version_id="pv",
        parent_hash=state.parent_hash, epoch=0, expires_at=time.time()+60)
    return driver, fake, state, plan, authority, grant


def test_generation_needs_valid_edit_grant_before_any_session_or_source_edit(tmp_path):
    driver, fake, state, plan, authority, grant = backend(tmp_path)
    with pytest.raises(GrantError):
        driver.generate(state, plan, 0, deadline=time.monotonic()+30)
    with pytest.raises(GrantError):
        driver.generate(state, plan, 0, deadline=time.monotonic()+30,
            grant=replace(grant, episode_id="another"), grant_authority=authority)
    assert not fake.calls
    assert driver._agents == {}
    candidate = driver.generate(state, plan, 0, deadline=time.monotonic()+30,
        grant=grant, grant_authority=authority)
    assert candidate_tree_hash(candidate["candidate_path"]) == candidate["candidate_hash"]
    assert (Path(candidate["candidate_path"]) / "solver.py").read_text() == "print('candidate')\n"
    assert driver.last_generation_usage["calls"] is None
    assert driver.last_generation_usage["session_turns"] == 1
    assert driver.last_generation_usage["request_count_unknown"] is True
    assert driver.precheck(candidate) == (True, "toy")
    # Nothing in generation runs a candidate or creates an evaluation ticket.
    assert len(fake.calls) == 2
    driver.close()


@pytest.mark.parametrize("unsafe", ["../outside", "/outside", "C:/outside", ".git/config",
    "a/../../outside", "a\\outside", "./solver.py", "a//solver.py", ".codex/config.toml",
    ".CLAUDE/settings.json", ".git./config", "CON.txt", "a/.. /outside"])
def test_host_apply_rejects_whole_manifest_before_any_edit(tmp_path, unsafe):
    solver = tmp_path / "solver"
    solver.mkdir()
    source = solver / "solver.py"
    source.write_text("seed")
    with pytest.raises(ValueError):
        CodexSessionBackend._apply_proposed_files(solver, [
            {"path": "solver.py", "content": "must-not-apply"}, {"path": unsafe, "content": "bad"}])
    assert source.read_text() == "seed"


def test_revoked_generation_grant_never_dispatches_cli(tmp_path):
    driver, fake, state, plan, authority, grant = backend(tmp_path)
    authority.revoke(grant)
    with pytest.raises(GrantError):
        driver.generate(state, plan, 0, deadline=time.monotonic()+30,
            grant=grant, grant_authority=authority)
    assert fake.calls == []
    driver.close()


def test_restart_cannot_bypass_unresolved_inflight_scope(tmp_path):
    driver, fake, state, plan, authority, grant = backend(tmp_path)
    driver.turn_store.reserve("old-agent-scope", "crashed-inflight-request", cap=1, deadline=time.time()+60)
    with pytest.raises(BackendUnavailable, match="unresolved Codex dispatch"):
        driver.begin_episode("next-episode")
    driver.close()


def test_known_completed_bad_plan_has_receipt_not_unknown(tmp_path):
    fake = FakeCLI({"action": "invented", "plan": None, "lessons": []})
    store = SessionTurnStore(tmp_path / "turns.sqlite")
    planner = CodexSessionPlanner(client(tmp_path, fake), store)
    with pytest.raises(PlanningResponseError) as caught:
        planner.plan(AgentState("a", "parent", .2), 0, {}, request_id="known-bad-plan",
            deadline=time.monotonic()+30)
    assert caught.value.receipt.actual_requests is None
    assert caught.value.receipt.session_turns == 1
    assert store.db.execute("SELECT state FROM session_turns").fetchone()[0] == "COMPLETED"
    store.close()


def test_generation_unknown_is_quarantined_across_retries_and_episodes(tmp_path):
    driver, fake, state, plan, authority, grant = backend(tmp_path)
    def unknown(*args, **kwargs):
        raise TimeoutError("no confirmed remote receipt")
    driver.session_client.propose_files = unknown
    with pytest.raises(InferenceRequestUnknown) as caught:
        driver.generate(state, plan, 0, deadline=time.monotonic()+30,
            grant=grant, grant_authority=authority)
    assert caught.value.inflight_unknown is True
    assert driver.turn_store.db.execute("SELECT state FROM session_turns").fetchone()[0] == "UNKNOWN"
    with pytest.raises(BackendUnavailable, match="unresolved Codex dispatch"):
        driver.begin_episode("next-episode")
    driver.close()


def test_completed_unsafe_files_keep_usage_and_do_not_change_solver(tmp_path):
    driver, fake, state, plan, authority, grant = backend(tmp_path)
    fake.value = {"files": [{"path": ".GIT/config", "content": "unsafe"}]}
    with pytest.raises(RuntimeError, match="Completed Codex generation"):
        driver.generate(state, plan, 0, deadline=time.monotonic()+30,
            grant=grant, grant_authority=authority)
    assert driver.turn_store.db.execute("SELECT state FROM session_turns").fetchone()[0] == "COMPLETED"
    assert driver.last_generation_usage["input_tokens"] == 80
    assert driver.last_generation_usage["token_usage_complete"] is True
    assert driver.last_generation_usage["usage_complete"] is False
    assert driver._agents["a"].solver.joinpath("solver.py").read_text() == "print('seed')\n"
    driver.close()


def test_stale_generation_epoch_is_rejected_before_dispatch(tmp_path):
    driver, fake, state, plan, authority, grant = backend(tmp_path)
    later = authority.issue(episode_id="episode", round_index=0, agent_id="a", plan_version_id="pv",
        parent_hash=state.parent_hash, epoch=1, expires_at=time.time()+60)
    with pytest.raises(GrantError):
        driver.generate(state, plan, 0, deadline=time.monotonic()+30,
            grant=later, grant_authority=authority)
    assert fake.calls == []
    driver.close()


def test_legacy_session_protocol_cannot_enter_prebid_market(tmp_path):
    """Injected CLI + simulated scorer: never a claim of physical PACE execution."""
    from maestro.config import RunConfig
    from maestro.loop import ExperimentEngine
    from maestro.types import HistoryRecord
    driver, fake, state, _, _, _ = backend(tmp_path)
    raw_runner = fake.__call__
    def routed(argv, **kwargs):
        if argv[1] == 'exec':
            schema = json.loads(Path(argv[argv.index('--output-schema')+1]).read_text())
            if 'files' in schema.get('properties',{}):
                fake.value = {'files':[{'path':'solver.py','content':"print('revised')\n"}]}
            else:
                fake.value = {'action':'revise','plan':{'target':'local_search',
                    'mod_type':'parameter_change','diagnosis':'own measured gain',
                    'change':'refine the same bounded parameter','expected_effect':'improve',
                    'evidence_run_ids':['warm-owned']},'lessons':[]}
        return raw_runner(argv, **kwargs)
    driver.session_client.runner = routed
    state.history.append(HistoryRecord('warm-owned','a',.15,.15,.2,True,
        'local_search','parameter_change',5,'COMPLETED','past-simulated-hash'))
    config = RunConfig(n_agents=1,slots=1,rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path/'controller',config) as engine:
        # Enter only the adapter branch under test; the scorer stays explicitly
        # simulated. Production pace preflight never accepts this FakeCLI.
        engine.config = replace(config,execution_mode='real',profile='codex_session_pilot')
        engine.backend = driver
        states = {'a':state}
        engine.ledger.create_episode('episode','first_price_naive',engine.config,states)
        parent = driver.prepare_seed()
        engine.ledger.record_candidate(parent['candidate_hash'],parent)
        from maestro.types import MaestroError
        with pytest.raises(MaestroError,match='separated implementation/runtime repair'):
            engine._round('episode',0,states,'first_price_naive')
        assert not engine.ledger.connection.execute('SELECT * FROM eval_tickets').fetchone()
        assert not engine.ledger.connection.execute('SELECT * FROM invitations').fetchone()
        assert not engine.ledger.connection.execute('SELECT * FROM settlements').fetchone()
