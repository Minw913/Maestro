"""Pure settlement calculation. Persistence and exactly-once writes are external."""
from dataclasses import dataclass
from decimal import Decimal
from maestro.market.credit import decimal, money, reward, score


@dataclass(frozen=True)
class AgentSettlement:
    admitted: bool
    payment: Decimal
    reward: Decimal
    wealth_before: Decimal
    wealth_after: Decimal
    parent_score_before: float
    parent_score_after: float


def settle_agent(*, wealth, parent, champion, admitted, bid=Decimal(0),
                 result=None, policy="first_price_naive", credited_duplicate=False,
                 scale=Decimal("100"), catchup=Decimal("0.25")):
    """Use pre-admission wealth and frozen baselines; never debit in storage here.

    Passing already-debited wealth would double charge and is caller error.
    Unadmitted generation failures have ``result=None``, not a synthetic Y=0.
    """
    before = money(wealth)
    p = score(parent)
    if policy not in {"uniform", "ucb", "first_price_naive", "first_price", "adjusted_valuation", "agent_adjusted"}:
        raise ValueError("unsupported V1 policy")
    if not admitted and result is not None:
        raise ValueError("unadmitted agent cannot have a formal result")
    if admitted and result is None:
        raise ValueError("all admitted jobs require an explicit terminal result")
    payment = money(bid) if admitted and policy in ("first_price_naive","first_price","adjusted_valuation","agent_adjusted") else money(0)
    if payment < 0 or payment > before:
        raise ValueError("payment exceeds pre-admission available wealth")
    credited = money(0)
    after_parent = p
    if admitted:
        if result.status in {"UNKNOWN", "RUNNING", "CLAIMED", "ADMITTED"}:
            raise ValueError("cannot settle an unresolved formal job")
        if result.valid:
            after_parent = max(p, score(result.score))
        if result.valid and not credited_duplicate:
            credited = reward(result.score, p, champion, scale, catchup)
    return AgentSettlement(bool(admitted), payment, credited, before,
                           money(before - payment + credited),
                           float(p), float(after_parent))
