"""Injected controller tests; these do not launch or certify a real runtime."""
from dataclasses import replace
import json
import time

from maestro.config import RunConfig
from maestro.loop import ExperimentEngine
from maestro.types import AgentState
from maestro.agents.planner import mock_plan


def controller(tmp_path, backend_factory):
    # Construct mock authority explicitly, then inject only the unit-under-test's
    # live callback branch. No production pace run or benchmark is represented.
    engine = ExperimentEngine(tmp_path, RunConfig(rounds=1, warm_opportunities_per_agent=1))
    engine.config = replace(engine.config, execution_mode="real")
    engine.backend = backend_factory(engine)
    state = AgentState("a", "p", .6)
    return engine, state, mock_plan(state, 0, 1)


def test_generation_resource_report_uses_actual_provider_usage_not_one_call_per_revision(tmp_path):
    class ProviderBacked:
        supports_runtime_only_repair = True
        def __init__(self, engine):
            self.engine = engine
        def generate(self, *args, **kwargs):
            self.last_generation_usage = {"calls": 2, "known_output_tokens": 11,
                "cpu_seconds": .2, "wall_seconds": .4, "measurement": "trusted_provider"}
            digest = self.engine.artifacts.put_json({"unit": "candidate"})
            return {"candidate_hash": digest, "kind": "injected-test"}
        def precheck(self, candidate):
            return True, "PASS"
    engine, state, plan = controller(tmp_path, ProviderBacked)
    try:
        candidate = engine._implement_proposal("test-live", 0, "a", state, plan, "MAIN", time.monotonic() + 5)
        assert candidate
        usage = engine.ledger.connection.execute(
            "SELECT * FROM resource_usage WHERE phase='generation'").fetchone()
        assert usage["llm_calls"] == 2
        assert usage["tokens"] >= 11
        assert usage["cpu_seconds"] >= .2
    finally:
        engine.close()


def test_known_generation_timeout_revokes_all_edit_grants_and_returns_pre_eval_failure(tmp_path):
    class TimeoutBeforeCandidate:
        supports_runtime_only_repair = True
        def __init__(self, engine):
            pass
        def generate(self, *args, **kwargs):
            self.last_generation_usage = {"calls": 1, "output_tokens": 9,
                "cpu_seconds": .1, "wall_seconds": .2, "measurement": "trusted_provider"}
            raise TimeoutError("bounded phase expired before a frozen candidate")
        def precheck(self, candidate):
            raise AssertionError("no candidate exists")
    engine, state, plan = controller(tmp_path, TimeoutBeforeCandidate)
    try:
        result = engine._implement_proposal("test-live", 0, "a", state, plan, "MAIN", time.monotonic() + 5)
        assert result is None
        grants = [json.loads(row[0])["grant_id"] for row in engine.ledger.connection.execute(
            "SELECT payload FROM edit_grants")]
        assert grants and all(grant in engine.grants._revoked for grant in grants)
        assert engine.ledger.connection.execute(
            "SELECT count(*) FROM resource_usage WHERE phase='generation'").fetchone()[0] > 0
    finally:
        engine.close()
