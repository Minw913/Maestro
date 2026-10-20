"""Fail-closed submission validation before frozen priority computation."""
from dataclasses import dataclass
from decimal import Decimal
from maestro.agents.planner import validate_plan, validate_complete_evidence
from maestro.market.credit import decimal


@dataclass(frozen=True)
class EligibilityDecision:
    eligible: bool
    reasons: tuple[str, ...] = ()


def check_eligibility(plan, state, *, bid=Decimal(0), holds=Decimal(0),
                      executed_versions=(), invalidated_versions=()):
    reasons = []
    if plan.agent_id != state.agent_id:
        reasons.append("WRONG_OWNER")
    try:
        validate_plan(plan, state.parent_hash, [r.experiment_id for r in state.history])
        validate_complete_evidence(plan, state.history)
    except (TypeError, ValueError):
        reasons.append("INVALID_PLAN")
    if plan.version_id in set(executed_versions):
        reasons.append("PLAN_ALREADY_EXECUTED")
    if plan.version_id in set(invalidated_versions):
        reasons.append("PLAN_INVALIDATED")
    try:
        amount, held, wealth = decimal(bid), decimal(holds), decimal(state.wealth)
    except (TypeError, ValueError):
        return EligibilityDecision(False, (*reasons, "INVALID_RESERVATION"))
    if amount < 0 or held < 0:
        reasons.append("INVALID_RESERVATION")
    elif amount + held > wealth:
        reasons.append("INSUFFICIENT_AVAILABLE_WEALTH")
    return EligibilityDecision(not reasons, tuple(reasons))
