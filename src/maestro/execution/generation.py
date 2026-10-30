"""Deterministic fault injection for simulated post-allocation checks."""
from __future__ import annotations


def precheck_mock(agent_id: str, round_index: int, failures: dict, revision: int = 0) -> tuple[bool, str]:
    """Simulation-only failures; only ``runtime`` permits an isolated repair.

    Keys accept ``agent:round``. Lists select the failure at each revision;
    legacy failure names remain useful for testing definite rejection/backfill.
    """
    value = failures.get(f"{agent_id}:{round_index}", failures.get(agent_id))
    if isinstance(value, list):
        value = value[min(revision, len(value) - 1)] if value else None
    if isinstance(value, dict):
        value = value.get("precheck", value.get("generation"))
    if value in (True, "precheck", "generation", "invalid", "timeout", "duplicate", "runtime"):
        return False, str(value)
    return True, "PASS"
