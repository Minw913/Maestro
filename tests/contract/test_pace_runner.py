import hashlib
import json
from pathlib import Path

import pytest

from maestro.config import RunConfig
from maestro.execution.authorization import execution_scope
from maestro.execution.sandbox import RunReceipt, SandboxUnavailable
from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.storage.ledger import Ledger
from maestro.types import AgentState, AuthorizationError
from maestro.tasks.pace.manifest import EvaluationContract, ManifestError, load_panel
from maestro.tasks.pace.runner import PaceEvaluator, smoke_test


class QueueRunner:
    def __init__(self, receipts):
        self.receipts = list(receipts)
        self.calls = []

    def run(self, request, limits):
        self.calls.append((request, limits))
        return self.receipts.pop(0)


def receipt(output="1\n2\n", *, timeout=False, returncode=0):
    return RunReceipt(returncode, output, "", 0.1, 0.2, timeout)


def setup(tmp_path, receipts, *, split="training"):
    # Distinct checksums and paths are intentional; manifest duplicates are illegal.
    graphs = ["p ds 3 2\n1 2\n2 3\n", "c second graph\np ds 4 3\n2 1\n2 3\n2 4\n"]
    entries = []
    for index, graph in enumerate(graphs):
        path = tmp_path / f"graph{index}.gr"
        path.write_bytes(graph.encode())
        entries.append({"instance_id": f"g{index}", "path": path.name,
                        "sha256": hashlib.sha256(graph.encode()).hexdigest(), "reference_size": 1})
    (tmp_path / "panel.json").write_text(json.dumps({"schema_version": 1, "panel_id": "test",
        "split": split, "instances": entries}))
    panel = load_panel(tmp_path / "panel.json")
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    contract = EvaluationContract(instances=2)
    runner = QueueRunner(receipts)
    evaluator = PaceEvaluator(panel, contract, runner, simulate_containment=True)
    ticket = {"ticket_id": "ticket1", "job_id": "job1", "episode_id": "pace-test",
              "round_index": 0, "agent_id": "a", "plan_version_id": "setup", "phase_epoch": 0,
              "candidate_hash": candidate_tree_hash(candidate), "candidate_path": str(candidate),
              "panel_hash": panel.panel_hash, "contract_hash": contract.contract_hash,
              "purpose": "HELDOUT" if split == "heldout" else "SETUP"}
    # Unit tests still exercise the scoring/containment adapter, but their
    # launch permission now comes from a real SQLite claim, not a fake dict.
    ledger = Ledger(tmp_path / "authority.sqlite")
    evaluator._test_ledger = ledger
    ledger.create_episode("pace-test", "warm", RunConfig(slots=1), {"a": AgentState("a", ticket["candidate_hash"], 0)})
    ledger.open_round("pace-test", 0, {key: ticket[key] for key in
        ("purpose", "candidate_hash", "panel_hash", "contract_hash")})
    ledger.record_candidate(ticket["candidate_hash"], {key: ticket[key] for key in ("candidate_hash", "candidate_path")})
    ledger.invite("pace-test", 0, "a", 0)
    ledger.set_round_phase("pace-test", 0, "ADMISSION")
    ledger.admit(ticket, 0, 1)
    ledger.set_round_phase("pace-test", 0, "EVALUATING")
    return evaluator, runner, ticket, panel


def evaluate_under_claim(evaluator, ticket, *, ledger=None):
    ledger = ledger or evaluator._test_ledger
    claim = ledger.claim_ticket("ticket1", "job1")
    with execution_scope(ledger, claim):
        return evaluator.evaluate({**ticket, "fence": claim["fence"]})


def test_full_panel_execution_and_restricted_mounts(tmp_path):
    evaluator, runner, ticket, panel = setup(tmp_path, [receipt(), receipt()])
    result = evaluate_under_claim(evaluator, ticket)
    assert result.valid and result.score == 1
    assert len(result.instance_results) == 2
    assert result.diagnostics["execution_kind"] == "simulated_containment"
    assert all(limits.cpus == 1 for _, limits in runner.calls)
    assert all(len(request.mounts) == 1 and request.mounts[0].target == "/candidate"
               for request, _ in runner.calls)
    assert all(request.mounts[0].read_only for request, _ in runner.calls)
    assert "reference_size" not in runner.calls[0][0].stdin
    assert result.cpu_seconds >= 0.2


def test_one_timeout_does_not_invalidate_other_instance(tmp_path):
    evaluator, _, ticket, _ = setup(tmp_path, [receipt(), receipt("", timeout=True, returncode=-9)])
    result = evaluate_under_claim(evaluator, ticket)
    assert result.valid and result.score == 0.5
    assert result.status == "COMPLETED_WITH_TIMEOUTS"
    assert result.instance_results[1]["valid"] is None


def test_valid_partial_output_on_timeout_is_independently_verified(tmp_path):
    evaluator, _, ticket, _ = setup(tmp_path, [receipt(), receipt(timeout=True, returncode=-15)])
    result = evaluate_under_claim(evaluator, ticket)
    assert result.valid and result.score == 1
    assert result.instance_results[1]["timeout"]


@pytest.mark.parametrize("bad", [receipt("1\n0\n"), receipt(""), receipt(returncode=1)])
def test_verified_illegal_or_failed_result_invalidates_whole_candidate(tmp_path, bad):
    evaluator, runner, ticket, _ = setup(tmp_path, [receipt(), bad])
    result = evaluate_under_claim(evaluator, ticket)
    assert not result.valid and result.score == 0
    assert len(runner.calls) == 2


def test_ticket_binding_rejected_before_runner(tmp_path):
    evaluator, runner, ticket, _ = setup(tmp_path, [receipt(), receipt()])
    ticket["contract_hash"] = "wrong"
    with pytest.raises(AuthorizationError, match="differs"):
        evaluate_under_claim(evaluator, ticket)
    assert not runner.calls


def test_heldout_requires_separate_purpose(tmp_path):
    evaluator, runner, ticket, _ = setup(tmp_path, [receipt(), receipt()], split="heldout")
    ticket["purpose"] = "MAIN"
    with pytest.raises(AuthorizationError, match="differs"):
        evaluate_under_claim(evaluator, ticket)
    assert not runner.calls


def test_changed_input_fails_before_execution(tmp_path):
    evaluator, runner, ticket, panel = setup(tmp_path, [receipt(), receipt()])
    panel.instances[0].path.write_text("p ds 3 0\n")
    with pytest.raises(ManifestError, match="changed"):
        evaluate_under_claim(evaluator, ticket)
    assert not runner.calls


def test_real_mode_fails_closed_without_prerequisites(tmp_path):
    evaluator, _, ticket, panel = setup(tmp_path, [])
    real = PaceEvaluator(panel, evaluator.contract)
    with pytest.raises(SandboxUnavailable):
        evaluate_under_claim(real, ticket, ledger=evaluator._test_ledger)


def test_fixed_smoke_returns_only_pass_fail(tmp_path):
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    graph = tmp_path / "smoke.gr"
    graph.write_text("p ds 1 0\n")
    assert smoke_test(candidate, graph, QueueRunner([receipt("1\n1\n")]), image="test") == (True, "pass")
    failed, reason = smoke_test(candidate, graph, QueueRunner([receipt("0\n")]), image="test")
    assert not failed and "dominating" in reason


def test_runtime_sanity_evidence_carries_actual_input_contract_limits_and_receipt(tmp_path):
    from hashlib import sha256
    from maestro.tasks.pace.runtime_failure import RuntimePrecheckFailure
    candidate=tmp_path/'candidate';candidate.mkdir();(candidate/'run').write_text('print(missing)\n')
    graph=tmp_path/'sanity.gr';data='p ds 2 1\n1 2\n';graph.write_text(data)
    failed=RunReceipt(1,'partial\n','Traceback (most recent call last):\n'
        '  File "/candidate/run", line 1\nNameError: missing',.1,.2,
        diagnostics={'reason':'COMPLETED','aggregate_cgroup_cpu':True})
    outcome=smoke_test(candidate,graph,QueueRunner([failed]),image='offline-fixture',
        solver_argv=('python3','/candidate/run'))
    assert isinstance(outcome,RuntimePrecheckFailure)
    ctx=outcome.evidence.test_context
    assert ctx['stdin']==data and ctx['input_sha256']==sha256(data.encode()).hexdigest()
    assert ctx['solver_argv']==['python3','/candidate/run']
    assert ctx['limits']['cpu_seconds']==1 and ctx['limits']['wall_seconds']==3
    assert ctx['receipt']['returncode']==1 and ctx['receipt']['stdout']=='partial\n'
    assert 'first non-comment line' in ctx['problem_contract']


def test_plain_dictionary_cannot_launch_formal_experiment(tmp_path):
    evaluator, runner, ticket, _ = setup(tmp_path, [receipt(), receipt()])
    # Even an otherwise valid ticket, or guessed claimed status/fence, conveys
    # no process-local authority outside the trusted dispatcher.
    with pytest.raises(AuthorizationError, match="dispatcher"):
        evaluator.evaluate({**ticket, "status": "CLAIMED", "fence": 1})
    assert not runner.calls


def test_resolver_cannot_substitute_another_candidate_path(tmp_path):
    evaluator, runner, ticket, _ = setup(tmp_path, [receipt(), receipt()])
    substitute = tmp_path / "substitute"
    substitute.mkdir()
    evaluator.candidate_resolver = lambda _: substitute
    with pytest.raises(ValueError, match="resolver"):
        evaluate_under_claim(evaluator, ticket)
    assert not runner.calls


def test_actual_runner_entry_records_intent_before_call_and_cannot_refund(tmp_path):
    from maestro.execution.dispatcher import Dispatcher
    evaluator, runner, ticket, _ = setup(tmp_path, [])
    ledger = evaluator._test_ledger
    def lose_receipt(request, limits):
        assert ledger.ticket("ticket1")["status"] == "LAUNCHING"
        raise RuntimeError("lost executor receipt")
    runner.run = lose_receipt
    with pytest.raises(RuntimeError):
        Dispatcher(ledger, evaluator).dispatch("ticket1", "job1")
    assert ledger.ticket("ticket1")["status"] == "UNKNOWN"
    with pytest.raises(AuthorizationError):
        ledger.void_never_started("ticket1", {"confirmed_not_started": True,
                                             "executor_receipt": "unverified assertion"})


def test_each_instance_rechecks_the_shared_evaluation_phase(tmp_path):
    from maestro.execution.dispatcher import Dispatcher
    evaluator, runner, _, _ = setup(tmp_path, [receipt(), receipt()])
    ledger = evaluator._test_ledger
    original = runner.run
    def close_phase(request, limits):
        result = original(request, limits)
        ledger.set_round_phase("pace-test", 0, "RECOVERY_REQUIRED")
        return result
    runner.run = close_phase
    with pytest.raises(AuthorizationError, match="phase"):
        Dispatcher(ledger, evaluator).dispatch("ticket1", "job1")
    assert len(runner.calls) == 1
    assert ledger.ticket("ticket1")["status"] == "UNKNOWN"


def test_authority_does_not_survive_dispatcher_scope(tmp_path):
    evaluator, runner, ticket, _ = setup(tmp_path, [receipt(), receipt()])
    evaluate_under_claim(evaluator, ticket)
    with pytest.raises(AuthorizationError, match="dispatcher"):
        evaluator.evaluate({**ticket, "fence": 1})
    assert len(runner.calls) == 2


def test_one_claim_cannot_enter_formal_evaluator_twice(tmp_path):
    evaluator, runner, ticket, _ = setup(tmp_path, [receipt(), receipt(), receipt(), receipt()])
    ledger = evaluator._test_ledger
    claimed = ledger.claim_ticket('ticket1', 'job1')
    bound = {**ticket, 'fence': claimed['fence']}
    with execution_scope(ledger, claimed):
        evaluator.evaluate(bound)
        with pytest.raises(AuthorizationError, match="already entered"):
            evaluator.evaluate(bound)
    assert len(runner.calls) == 2


def test_verifier_launch_rechecks_ticket_after_last_solver_instance(tmp_path):
    from maestro.execution.dispatcher import Dispatcher
    evaluator, runner, _, _ = setup(tmp_path, [receipt(), receipt()])
    ledger = evaluator._test_ledger
    original = runner.run
    verifier_calls = []
    class Verifier:
        def verify(self, *args):
            verifier_calls.append(True)
            raise AssertionError('revoked verifier must never run')
    evaluator.verification_worker = Verifier()
    def close_after_final_instance(request, limits):
        result = original(request, limits)
        if len(runner.calls) == 2:
            ledger.set_round_phase('pace-test', 0, 'RECOVERY_REQUIRED')
        return result
    runner.run = close_after_final_instance
    with pytest.raises(AuthorizationError, match="phase"):
        Dispatcher(ledger, evaluator).dispatch('ticket1', 'job1')
    assert len(runner.calls) == 2
    assert not verifier_calls
