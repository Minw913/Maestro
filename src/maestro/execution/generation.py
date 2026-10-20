"""Bounded generation helpers. Invitation/backfill ownership stays in RoundLoop."""
from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable


def precheck_mock(agent_id: str, round_index: int, failures: dict, revision: int = 0) -> tuple[bool, str]:
    """Deterministic fault injection, explicitly simulation-only.

    Keys accept ``agent:round`` and values ``precheck``, ``generation``,
    ``duplicate``; lists permit bounded repair to recover at a later revision.
    """
    value = failures.get(f"{agent_id}:{round_index}", failures.get(agent_id))
    if isinstance(value, list):
        value = value[min(revision, len(value) - 1)] if value else None
    if isinstance(value, dict):
        value = value.get("precheck", value.get("generation"))
    if value in (True, "precheck", "generation", "invalid", "timeout", "duplicate"):
        return False, str(value)
    return True, "PASS"


@dataclass(frozen=True)
class GenerationOutcome:
    passed: bool
    candidate: object | None
    attempts: int
    reason: str


def bounded_generate(generate: Callable[[int], object], precheck: Callable[[object], tuple[bool, str]],
                     *, max_calls: int = 3, deadline: float, clock=time.monotonic) -> GenerationOutcome:
    """Repair consumes the same invitation budget and common absolute deadline."""
    if not 1 <= max_calls <= 3:
        raise ValueError("V1 permits one to three generation requests per invitation")
    reason = "DEADLINE"
    for index in range(max_calls):
        if clock() >= deadline:
            return GenerationOutcome(False, None, index, "DEADLINE")
        candidate = generate(index)
        if clock() >= deadline:
            return GenerationOutcome(False, None, index + 1, "DEADLINE")
        passed, reason = precheck(candidate)
        if passed:
            return GenerationOutcome(True, candidate, index + 1, reason)
    return GenerationOutcome(False, None, max_calls, reason)
