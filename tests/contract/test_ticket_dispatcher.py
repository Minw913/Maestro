from decimal import Decimal
from pathlib import Path

import pytest

from maestro.backends.mock import MockEvaluator
from maestro.backends.coral.grading import TicketedGradeBridge
from maestro.backends.coral.workspace import WorkspaceStore
from maestro.config import RunConfig
from maestro.execution.dispatcher import Dispatcher
from maestro.storage.ledger import Ledger
from maestro.types import AgentState, AuthorizationError, IntegrityError, EvaluationResult, Plan, Submission


def prepare_ticket(tmp_path):
    seed = tmp_path / "seed"; seed.mkdir(); (seed / "solver.py").write_text("print('1\\n1')")
    store = WorkspaceStore(tmp_path / "agents", tmp_path / "cas")
    candidate = store.freeze(seed)
    ledger = Ledger(tmp_path / "ledger.sqlite")
    state = AgentState("a", "parent", 0.2)
    ledger.create_episode("e", "first_price", RunConfig(), {"a": state})
    ledger.open_round("e", 0, {"purpose": "MAIN", "contract_hash": "contract", "panel_hash": "panel",
                                 "states": {"a": state}, "board": {"champion_score": 0.2}})
    plan = Plan("p", "pv", "a", "parent", "local_search", "parameter_change", "diagnosis", "change", "effect")
    ledger.submit("e", 0, plan, Submission("a", "pv", Decimal(3), Decimal(3)), {})
    ledger.invite("e", 0, "a", Decimal("3"))
    ledger.record_candidate(candidate["candidate_hash"], candidate)
    with ledger.transaction() as db:
        db.execute("INSERT INTO prechecks VALUES(?,?,?,?,?,?,?)", ("check", "e", 0, "a", candidate["candidate_hash"], 1, "{}"))
    ledger.set_round_phase("e", 0, "ADMISSION")
    ticket = dict(candidate, ticket_id="t", job_id="j", episode_id="e", round_index=0,
        agent_id="a", parent_hash="parent", parent_score=0.2, champion_score=0.2,
        contract_hash="contract", panel_hash="panel", plan_version_id="pv", phase_epoch=0,
        purpose="MAIN", target="local_search", mod_type="parameter_change")
    return ledger, ticket


@pytest.fixture
def admitted(tmp_path):
    ledger, ticket = prepare_ticket(tmp_path)
    ledger.admit(ticket, Decimal("3"), 2)
    ledger.set_round_phase("e", 0, "EVALUATING")
    yield ledger, ticket
    ledger.close()


def test_invalid_ticket_never_invokes_evaluator(admitted):
    ledger, ticket = admitted
    evaluator = MockEvaluator(1)
    dispatcher = Dispatcher(ledger, evaluator)
    for ticket_id, job, epoch in (("missing", "j", 0), ("t", "wrong", 0), ("t", "j", 1)):
        with pytest.raises(AuthorizationError):
            dispatcher.dispatch(ticket_id, job, epoch)
    assert evaluator.calls == []


def test_admission_cannot_substitute_registered_candidate_path(tmp_path):
    ledger, ticket = prepare_ticket(tmp_path)
    try:
        substitute = tmp_path / "substitute"
        substitute.mkdir()
        with pytest.raises(AuthorizationError, match="candidate path"):
            ledger.admit({**ticket, "candidate_path": str(substitute)}, Decimal(3), 2)
        assert ledger.balance("e", "a") == Decimal(2000)
        assert ledger.connection.execute('SELECT count(*) FROM eval_tickets').fetchone()[0] == 0
    finally:
        ledger.close()


def test_parallel_dispatchers_claim_a_ticket_at_most_once(admitted):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    ledger, _ = admitted
    entered, release = threading.Event(), threading.Event()
    evaluator = MockEvaluator(1)
    class BlockedEvaluator:
        def evaluate(self, ticket):
            entered.set()
            assert release.wait(5)
            return evaluator.evaluate(ticket)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(Dispatcher(ledger, BlockedEvaluator()).dispatch, 't', 'j')
        assert entered.wait(5)
        # A second SQLite connection models a second dispatcher process.
        with Ledger(ledger.path) as other:
            with pytest.raises(AuthorizationError):
                Dispatcher(other, evaluator).dispatch('t', 'j')
        release.set()
        first.result(timeout=5)
    assert evaluator.calls == ['j']


def test_dispatch_claim_result_retry_is_single_execution(admitted):
    ledger, ticket = admitted
    evaluator = MockEvaluator(1)
    result = Dispatcher(ledger, evaluator).dispatch("t", "j")
    assert Dispatcher(ledger, evaluator).dispatch("t", "j") == result
    assert evaluator.calls == ["j"]
    assert ledger.balance("e", "a") == Decimal("1997")


def test_ambiguous_failure_is_unknown_and_not_relaunched(admitted):
    ledger, ticket = admitted
    class Unavailable:
        calls = 0
        def evaluate(self, ticket):
            self.calls += 1
            assert ledger.ticket("t")["status"] == "CLAIMED"
            raise RuntimeError("Preflight failed before physical launch")
    evaluator = Unavailable()
    with pytest.raises(RuntimeError):
        Dispatcher(ledger, evaluator).dispatch("t", "j")
    with pytest.raises(AuthorizationError):
        Dispatcher(ledger, evaluator).dispatch("t", "j")
    assert evaluator.calls == 1
    job = ledger.connection.execute("SELECT status,receipt FROM jobs WHERE id='j'").fetchone()
    assert job[0] == "UNKNOWN"
    assert job[1] is None  # No invented physical-start receipt.


def test_unbound_result_is_rejected(admitted):
    ledger, ticket = admitted
    class Forged:
        def evaluate(self, ticket):
            return EvaluationResult("j", "another-candidate", 1.0, True)
    with pytest.raises(PermissionError):
        Dispatcher(ledger, Forged()).dispatch("t", "j")
    assert ledger.existing_result("j") is None


def test_modified_frozen_artifact_cannot_execute(admitted):
    ledger, ticket = admitted
    source = Path(ticket["candidate_path"]) / "solver.py"
    source.chmod(0o666)
    source.write_text("modified after admission")
    evaluator = MockEvaluator(1)
    with pytest.raises(PermissionError):
        Dispatcher(ledger, evaluator).dispatch("t", "j")
    assert not evaluator.calls
    assert ledger.ticket("t")["status"] == "ADMITTED"


def test_grader_metadata_binding_is_not_arbitrary_kwargs(admitted):
    ledger, ticket = admitted
    class Task:
        metadata = {"maestro": {key: value for key, value in ticket.items()}}
    task = Task()
    evaluator = MockEvaluator(1)
    task.metadata["maestro"]["panel_hash"] = "attacker-panel"
    with pytest.raises(PermissionError):
        TicketedGradeBridge(ledger, evaluator).grade_request(ticket["candidate_path"], [task])
    assert not evaluator.calls


def test_completed_replay_rejects_stale_epoch(admitted):
    ledger, _ = admitted
    evaluator = MockEvaluator(1)
    Dispatcher(ledger, evaluator).dispatch("t", "j")
    with pytest.raises(PermissionError):
        Dispatcher(ledger, evaluator).dispatch("t", "j", expected_epoch=1)
    assert evaluator.calls == ["j"]


def test_terminal_result_cannot_be_downgraded_to_unknown(admitted):
    ledger, _ = admitted
    Dispatcher(ledger, MockEvaluator(1)).dispatch("t", "j")
    with pytest.raises(AuthorizationError):
        ledger.mark_unknown("j", "late worker exception")
    assert ledger.ticket("t")["status"] == "RESULT_RECORDED"
    assert ledger.existing_result("j") is not None


def test_stale_fence_cannot_replay_an_identical_result(admitted):
    ledger, _ = admitted
    result = Dispatcher(ledger, MockEvaluator(1)).dispatch("t", "j")
    with pytest.raises(IntegrityError, match="Conflicting"):
        ledger.record_result(result, 0)


def test_launch_attempt_cannot_be_refunded_as_never_started(admitted):
    ledger, _ = admitted
    class LostExecutor:
        def evaluate(self, ticket):
            ledger.mark_launch_intent(ticket["job_id"], ticket["fence"])
            raise RuntimeError("executor connection lost after dispatch")
    with pytest.raises(RuntimeError):
        Dispatcher(ledger, LostExecutor()).dispatch("t", "j")
    with pytest.raises(AuthorizationError):
        ledger.void_never_started("t", {"confirmed_not_started": True,
                                         "executor_receipt": "unsupported assertion"})
    assert ledger.ticket("t")["status"] == "UNKNOWN"
    assert ledger.balance("e", "a") == Decimal("1997")


@pytest.mark.parametrize("proof", [{"confirmed_not_started": True},
    {"confirmed_not_started": 1, "executor_receipt": "x"},
    {"confirmed_not_started": True, "executor_receipt": " "}])
def test_unproven_not_started_compensation_is_denied(admitted, proof):
    ledger, _ = admitted
    with pytest.raises(AuthorizationError):
        ledger.void_never_started("t", proof)
    assert ledger.balance("e", "a") == Decimal("1997")


def test_grade_bridge_rejects_mismatched_coral_task_id(admitted):
    ledger, ticket = admitted
    class Task:
        id = "another-job"
        metadata = {"maestro": dict(ticket)}
    evaluator = MockEvaluator(1)
    with pytest.raises(PermissionError, match="Task ID"):
        TicketedGradeBridge(ledger, evaluator).grade_request(ticket["candidate_path"], [Task()])
    assert not evaluator.calls


def test_mock_outcome_ignores_policy_episode_and_dispatch_order():
    base = {"agent_id": "a", "round_index": 1, "parent_score": 0.2,
            "semantic_plan_key": "same-refinement", "target": "local_search", "mod_type": "rule_replacement"}
    first = MockEvaluator(7).evaluate(dict(base, candidate_hash="h1", job_id="uniform:j", policy="uniform"))
    second = MockEvaluator(7).evaluate(dict(base, candidate_hash="h2", job_id="market:j", policy="first_price", bid=100))
    assert (first.score, first.cpu_seconds) == (second.score, second.cpu_seconds)
