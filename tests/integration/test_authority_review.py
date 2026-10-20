"""Regression cases discovered by review of trusted admission and replay."""
from dataclasses import replace
from decimal import Decimal
import json
import math
import pytest

from maestro.config import RunConfig
from maestro.types import (AgentState, Submission, EvaluationResult,
                           AuthorizationError, IntegrityError, canonical_json, to_jsonable, stable_hash)
from maestro.agents.planner import mock_plan
from maestro.storage import Ledger
from maestro.loop import ExperimentEngine
from maestro.replay import verify_ledger


def prepare_admission(tmp_path, *, agents=2, rounds=1):
    config = RunConfig(n_agents=agents, slots=1, rounds=rounds, warm_opportunities_per_agent=1)
    states = {f"a{i}": AgentState(f"a{i}", "a" * 64, .6) for i in range(agents)}
    ledger = Ledger(tmp_path / "ledger.sqlite")
    ledger.create_episode("review", "first_price", config, states)
    snapshot = {"purpose": "MAIN", "states": to_jsonable(states),
                "board": {"champion_score": .6}, "contract_hash": "b" * 64,
                "panel_hash": "d" * 64}
    ledger.open_round("review", 0, snapshot)
    plans = {}
    for index, (agent, state) in enumerate(states.items()):
        plan = mock_plan(state, 0, 4)
        plans[agent] = plan
        ledger.submit("review", 0, plan, Submission(agent, plan.version_id, Decimal(3), Decimal(3)), {})
    ledger.set_round_phase("review", 0, "GENERATION")
    tickets = []
    for index, (agent, state) in enumerate(states.items()):
        plan = plans[agent]
        digest = str(index + 1) * 64
        ledger.invite("review", 0, agent, Decimal(3), {"plan_version_id": plan.version_id})
        ledger.record_candidate(digest, {"candidate_hash": digest, "kind": "mock"})
        with ledger.transaction() as db:
            db.execute("INSERT INTO prechecks VALUES(?,?,?,?,?,?,?)", (
                "precheck-" + agent, "review", 0, agent, digest, 1, "{}"))
        tickets.append({"ticket_id": f"ticket-{agent}", "job_id": f"job-{agent}",
            "episode_id": "review", "round_index": 0, "agent_id": agent,
            "candidate_hash": digest, "parent_hash": state.parent_hash,
            "parent_score": state.parent_score, "champion_score": .6,
            "contract_hash": snapshot["contract_hash"], "panel_hash": snapshot["panel_hash"],
            "target": plan.target, "mod_type": plan.mod_type,
            "plan_version_id": plan.version_id, "phase_epoch": 0, "purpose": "MAIN"})
    ledger.set_round_phase("review", 0, "ADMISSION")
    return ledger, tickets


@pytest.mark.parametrize("field,replacement", [
    ("contract_hash", "f" * 64), ("panel_hash", "f" * 64),
    ("parent_hash", "f" * 64), ("plan_version_id", "another-plan"),
    ("parent_score", .1), ("champion_score", .1)])
def test_admission_rechecks_frozen_plan_contract_and_baselines(tmp_path, field, replacement):
    ledger, tickets = prepare_admission(tmp_path)
    try:
        bad = {**tickets[0], field: replacement}
        with pytest.raises((AuthorizationError, IntegrityError)):
            ledger.admit(bad, Decimal(3), 1)
        assert ledger.balance("review", "a0") == 2000
    finally:
        ledger.close()


def test_admission_requires_current_owner_precheck_not_just_registered_candidate(tmp_path):
    ledger, tickets = prepare_admission(tmp_path)
    try:
        candidate = "f" * 64
        ledger.record_candidate(candidate, {"candidate_hash": candidate, "kind": "mock"})
        with pytest.raises((AuthorizationError, IntegrityError)):
            ledger.admit({**tickets[0], "candidate_hash": candidate}, Decimal(3), 1)
    finally:
        ledger.close()


def test_caller_cannot_expand_episode_slot_budget(tmp_path):
    ledger, tickets = prepare_admission(tmp_path)
    try:
        ledger.admit(tickets[0], Decimal(3), 1)
        with pytest.raises((AuthorizationError, IntegrityError)):
            ledger.admit(tickets[1], Decimal(3), 999)
    finally:
        ledger.close()


def test_unknown_or_running_result_is_not_a_terminal_score(tmp_path):
    ledger, tickets = prepare_admission(tmp_path)
    try:
        ledger.admit(tickets[0], Decimal(3), 1)
        ledger.set_round_phase("review", 0, "EVALUATING")
        claimed = ledger.claim_ticket(tickets[0]["ticket_id"], tickets[0]["job_id"])
        for status in ("UNKNOWN", "RUNNING", "CLAIMED", "ADMITTED"):
            with pytest.raises((AuthorizationError, IntegrityError)):
                ledger.record_result(EvaluationResult(tickets[0]["job_id"], tickets[0]["candidate_hash"],
                                                     .99, True, status), claimed["fence"])
    finally:
        ledger.close()


@pytest.mark.parametrize("name,value", [("generation_window_seconds", float("nan")),
    ("instance_cpu_seconds", float("inf")), ("initial_wealth", "Infinity"),
    ("reward_scale", "Infinity"), ("memory_bytes", True)])
def test_configuration_rejects_nonfinite_or_noninteger_resource_limits(name, value):
    with pytest.raises(ValueError):
        RunConfig(**{name: value})


@pytest.mark.parametrize("table,column", [("checkpoints", "payload"),
                                          ("plan_versions", "payload")])
def test_replay_checks_immutable_payload_hashes_not_only_existing_cas_blobs(tmp_path, table, column):
    config = RunConfig(rounds=1, warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path, config) as engine:
        warm = engine.bootstrap()
        engine.run(warm["checkpoint"], "first_price")
        row = engine.ledger.connection.execute(f"SELECT rowid,{column} FROM {table} LIMIT 1").fetchone()
        payload = json.loads(row[column])
        if table == "checkpoints":
            first = next(iter(payload["states"]))
            payload["states"][first]["parent_score"] = .999
        else:
            payload["change"] = "mutated after admission"
        engine.ledger.connection.execute(f"UPDATE {table} SET {column}=? WHERE rowid=?",
            (canonical_json(payload), row["rowid"]))
    with pytest.raises(IntegrityError):
        verify_ledger(tmp_path)


def test_next_round_context_retains_public_verified_formal_summaries(tmp_path):
    config = RunConfig(rounds=2, warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path, config) as engine:
        warm = engine.bootstrap()
        run = engine.run(warm["checkpoint"], "first_price")
        submission = engine.ledger.connection.execute(
            "SELECT payload FROM round_submissions WHERE episode_id=? AND round_index=1 LIMIT 1",
            (run["episode_id"],)).fetchone()
        context_hash = json.loads(submission[0])["context_hash"]
        context = engine.artifacts.get_json(context_hash)
        summaries = context["board"].get("results", [])
        assert summaries, "H_t must contain already-published formal outcomes"
        previous_jobs = {row[0] for row in engine.ledger.connection.execute(
            "SELECT job_id FROM eval_tickets WHERE episode_id=? AND round_index=0",
            (run["episode_id"],))}
        assert previous_jobs <= {row["experiment_id"] for row in summaries}


def test_confirmed_not_started_compensates_once_without_new_formal_samples(tmp_path):
    config = RunConfig(rounds=1, warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path, config) as engine:
        warm = engine.bootstrap()
        evaluator = engine.evaluator
        def unavailable(ticket):
            raise RuntimeError("confirmed isolated executor never launched")
        engine.dispatcher.evaluator = type("Offline", (), {"evaluate": staticmethod(unavailable)})()
        with pytest.raises(RuntimeError):
            engine.run(warm["checkpoint"], "first_price", episode_id="never-started")
        tickets = engine.ledger.connection.execute(
            "SELECT id FROM eval_tickets WHERE episode_id='never-started'").fetchall()
        for row in tickets:
            engine.ledger.void_never_started(row[0], {"confirmed_not_started": True,
                                                     "executor_receipt": "operator-reviewed-test"})
            engine.ledger.void_never_started(row[0], {"confirmed_not_started": True,
                                                     "executor_receipt": "operator-reviewed-test"})
        before_calls = len(evaluator.calls)
        engine.dispatcher.evaluator = evaluator
        engine.resume("never-started")
        assert len(evaluator.calls) == before_calls
        for row in engine.ledger.connection.execute(
                "SELECT state_json FROM agents WHERE episode_id='never-started'"):
            assert len(json.loads(row[0])["history"]) == 1
        assert engine.ledger.connection.execute(
            "SELECT count(*) FROM wallet_entries WHERE episode_id='never-started' AND kind='COMPENSATION'").fetchone()[0] == len(tickets)
    assert verify_ledger(tmp_path)["verified"]


def test_replay_uses_frozen_round_baseline_even_when_zero_reward_is_self_consistent(tmp_path):
    failures = {f"agent-{i}:0": {"formal": "invalid"} for i in range(1, 5)}
    config = RunConfig(rounds=1, warm_opportunities_per_agent=1, failures=failures)
    with ExperimentEngine(tmp_path, config) as engine:
        warm = engine.bootstrap()
        run = engine.run(warm["checkpoint"], "first_price")
        row = engine.ledger.connection.execute(
            "SELECT rowid,payload FROM settlements WHERE episode_id=? LIMIT 1",
            (run["episode_id"],)).fetchone()
        payload = json.loads(row["payload"])
        payload["parent_before"] = 0
        engine.ledger.connection.execute("UPDATE settlements SET payload=? WHERE rowid=?",
            (canonical_json(payload), row["rowid"]))
    with pytest.raises(IntegrityError):
        verify_ledger(tmp_path)


def test_replay_anchors_round_snapshot_to_round_opened_event(tmp_path):
    config = RunConfig(rounds=1, warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path, config) as engine:
        warm = engine.bootstrap()
        run = engine.run(warm["checkpoint"], "first_price")
        row = engine.ledger.connection.execute(
            "SELECT rowid,snapshot_json FROM rounds WHERE episode_id=? AND round_index=0",
            (run["episode_id"],)).fetchone()
        snapshot = json.loads(row["snapshot_json"])
        snapshot["seed"] = 12345678
        engine.ledger.connection.execute("UPDATE rounds SET snapshot_json=?,snapshot_hash=? WHERE rowid=?",
            (canonical_json(snapshot), stable_hash(snapshot), row["rowid"]))
    with pytest.raises(IntegrityError):
        verify_ledger(tmp_path)


def test_formally_consumed_plan_version_cannot_be_resubmitted_as_pending(tmp_path):
    ledger, tickets = prepare_admission(tmp_path, rounds=2)
    try:
        ticket = tickets[0]
        ledger.admit(ticket, Decimal(3), 1)
        ledger.set_round_phase("review", 0, "EVALUATING")
        claim = ledger.claim_ticket(ticket["ticket_id"], ticket["job_id"])
        ledger.record_result(EvaluationResult(ticket["job_id"], ticket["candidate_hash"],
                                             0, False, "INVALID"), claim["fence"])
        frozen = json.loads(ledger.connection.execute(
            "SELECT snapshot_json FROM rounds WHERE episode_id='review' AND round_index=0").fetchone()[0])
        ledger.open_round("review", 1, frozen)
        state = AgentState("a0", "a" * 64, .6)
        old = mock_plan(state, 0, 4)
        with pytest.raises((AuthorizationError, IntegrityError)):
            ledger.submit("review", 1, old, Submission("a0", old.version_id, Decimal(3), Decimal(3)), {})
    finally:
        ledger.close()


def test_past_evaluated_candidate_cannot_consume_another_formal_slot(tmp_path):
    config = RunConfig(rounds=1, warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path, config) as engine:
        warm = engine.bootstrap()
        old = engine.ledger.connection.execute(
            "SELECT c.payload FROM candidates c JOIN verified_results v ON v.candidate_hash=c.hash LIMIT 1").fetchone()
        existing = json.loads(old[0])
        def repeat_candidate(e, r, a, state, plan, purpose, deadline):
            with engine.ledger.transaction() as db:
                db.execute("INSERT INTO prechecks VALUES(?,?,?,?,?,?,?)", (
                    f"repeat-{e}-{r}-{a}", e, r, a, existing["candidate_hash"], 1, "{}"))
            return existing
        engine._generate = repeat_candidate
        run = engine.run(warm["checkpoint"], "first_price")
        assert engine.ledger.connection.execute(
            "SELECT count(*) FROM eval_tickets WHERE episode_id=?", (run["episode_id"],)).fetchone()[0] == 0
