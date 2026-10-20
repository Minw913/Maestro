"""Policy-independent simulated outcomes. This never executes candidate code."""
from __future__ import annotations

import hashlib
from maestro.types import EvaluationResult


class MockEvaluator:
    def __init__(self, seed: int = 0):
        self.seed = seed
        self.calls: list[str] = []

    def evaluate(self, ticket: dict) -> EvaluationResult:
        self.calls.append(ticket["job_id"])
        # Do not include policy, bid, wealth, global call index, or winner order.
        key = "|".join(str(ticket.get(k, "")) for k in (
            "agent_id", "round_index", "semantic_plan_key", "target", "mod_type"))
        raw = int(hashlib.sha256(f"{self.seed}|{key}".encode()).hexdigest()[:16], 16)
        unit = raw / (2**64 - 1)
        parent = float(ticket.get("parent_score", 0.2))
        fault = ticket.get("simulated_failure", ticket.get("failure"))
        if fault in ("crash", "timeout", "invalid"):
            return EvaluationResult(ticket["job_id"], ticket["candidate_hash"], 0.0,
                                    False, status=str(fault).upper(), cpu_seconds=30.0,
                                    wall_seconds=31.0, diagnostics={"simulated": True})
        gain = (unit - 0.28) * 0.13
        score = min(1.0, max(0.0, parent + gain))
        cpu = round(60 + unit * 480, 6)
        return EvaluationResult(ticket["job_id"], ticket["candidate_hash"], round(score, 9),
                                True, cpu_seconds=cpu, wall_seconds=cpu + 1,
                                diagnostics={"simulated": True, "outcome_key": hashlib.sha256(key.encode()).hexdigest()})
