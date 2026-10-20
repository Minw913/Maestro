"""Opt-in full-spec multi-plan mechanisms; the V1 loop does not import these."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import hashlib
import math
from maestro.market.credit import decimal, money
from maestro.market.reputation import TrackRecord, metrics, require_full_profile
from maestro.market.audits import audit_slots, sample_audits


@dataclass(frozen=True)
class FullPlan:
    plan_id: str
    agent_id: str
    parent_hash: str
    target: str
    mod_type: str
    change: str
    bid: Decimal
    order: int = 0
    eligible: bool = True
    q: Decimal = Decimal(1)


@dataclass(frozen=True)
class FullAssignment:
    plan_id: str
    agent_id: str
    bid: Decimal
    payment: Decimal
    audit: bool
    priority: float


@dataclass(frozen=True)
class FullAllocation:
    assignments: tuple[FullAssignment, ...]
    rejected_ids: tuple[str, ...]
    virtual_counts: dict[str, Decimal]
    cutoff: Decimal
    audit_reserve: int


def allocate(plans, records, wealth, *, slots, rule="M_0", gamma=Decimal(1),
             a0=Decimal(1), epsilon=Decimal(0), seed=0, payment_rule="first_price",
             bid_cap=Decimal(100), q_max=None, profile):
    """Fill priority slots sequentially, then draw audits from unselected plans.

    ``records`` are frozen A/B/N snapshots; only virtual exploration counts change
    during clearing. M_U/M_H preserve each agent's supplied own plan order.
    Wallet accounting and actual sandbox dispatch remain the caller's jobs.
    """
    require_full_profile(profile)
    if rule not in {"M_U", "M_H", "M_0", "M_L", "M_C"}:
        raise ValueError("unknown full-spec mechanism")
    if payment_rule not in {"zero", "first_price", "uniform_price"}:
        raise ValueError("unknown payment rule")
    if payment_rule == "uniform_price" and rule != "M_0":
        raise ValueError("uniform critical pricing is only defined for M_0")
    if not isinstance(slots, int) or slots < 0 or decimal(gamma) < 0 or decimal(bid_cap) < 0:
        raise ValueError("invalid slot/exploration/bid cap")
    items = list(plans)
    if len({p.plan_id for p in items}) != len(items):
        raise ValueError("plan IDs must be distinct")
    if q_max is not None and (not isinstance(q_max, int) or q_max < 1):
        raise ValueError("q_max must be a positive integer")
    seen, owner_counts = set(), {}
    for plan in items:
        if not plan.plan_id or plan.agent_id not in wealth:
            raise ValueError("plan identity/owner missing")
        if not 0 <= decimal(plan.bid) <= decimal(bid_cap) or not 0 <= decimal(plan.q) <= 1:
            raise ValueError("bid or credibility outside contract")
        identity = (plan.agent_id, plan.target, plan.change)
        if identity in seen:
            raise ValueError("agent plans are distinct only if target/change differ")
        seen.add(identity)
        owner_counts[plan.agent_id] = owner_counts.get(plan.agent_id, 0) + 1
        if q_max is not None and owner_counts[plan.agent_id] > q_max:
            raise ValueError("agent exceeded q_max")
    accounts = {agent: decimal(value) for agent, value in wealth.items()}
    if any(value < 0 for value in accounts.values()):
        raise ValueError("negative starting wealth")
    records = {agent: records.get(agent, TrackRecord()) for agent in accounts}
    virtual = {agent: record.N for agent, record in records.items()}
    reserve = audit_slots(slots, epsilon, profile=profile)
    if payment_rule == "uniform_price":
        if reserve:
            raise ValueError("uniform critical pricing requires an audit-free top-K allocation")
        if any(sum((decimal(p.bid) for p in items if p.agent_id == agent and p.eligible),
                   Decimal(0)) > value for agent, value in accounts.items()):
            raise ValueError("uniform pricing requires nonbinding aggregate bid budgets")
    regular_count = slots - reserve
    available = [p for p in items if p.eligible]
    selected, spent = [], {agent: Decimal(0) for agent in accounts}
    tie = lambda plan: hashlib.sha256(repr((seed, "full-tie", plan.plan_id)).encode()).hexdigest()
    for assignment_index in range(regular_count):
        candidates = [p for p in available if spent[p.agent_id] + decimal(p.bid) <= accounts[p.agent_id]]
        if rule in {"M_U", "M_H"}:
            first_by_owner = {}
            for plan in sorted(candidates, key=lambda p: (p.order, tie(p))):
                first_by_owner.setdefault(plan.agent_id, plan)
            candidates = list(first_by_owner.values())
        if not candidates:
            break
        total = sum(virtual.values(), Decimal(0))
        def priority(plan):
            m = metrics(records[plan.agent_id], total, a0=a0,
                        virtual_count=virtual[plan.agent_id], profile=profile)
            # gamma=0 deliberately disables cold-start exploration; avoid 0*inf.
            bonus = 0.0 if decimal(gamma) == 0 else float(decimal(gamma)) * m.exploration
            if rule == "M_U":
                return int(hashlib.sha256(repr((seed, "uniform", assignment_index,
                           plan.agent_id)).encode()).hexdigest(), 16) / (2 ** 256)
            if rule == "M_H":
                return float(m.mean_credit) + bonus
            if rule == "M_0":
                return float(decimal(plan.bid))
            weight = m.zeta * (decimal(plan.q) if rule == "M_C" else Decimal(1))
            return float(weight * decimal(plan.bid)) + bonus
        winner = min(candidates, key=lambda p: (-priority(p), tie(p)))
        selected.append((winner, priority(winner), False))
        available.remove(winner)
        spent[winner.agent_id] += decimal(winner.bid)
        virtual[winner.agent_id] += 1
    selected_ids = {p.plan_id for p, _, _ in selected}
    chosen_audits = sample_audits([p.plan_id for p in items if p.eligible],
                                 selected_ids, reserve, seed, profile=profile)
    by_id = {p.plan_id: p for p in items}
    for identity in chosen_audits:
        plan = by_id[identity]
        selected.append((plan, 0.0, True))
        virtual[plan.agent_id] += 1
    all_selected = {p.plan_id for p, _, _ in selected}
    rejected = tuple(p.plan_id for p in items if p.eligible and p.plan_id not in all_selected)
    cutoff = money(max((decimal(by_id[identity].bid) for identity in rejected), default=Decimal(0)))
    assignments = []
    for plan, priority_value, audit in selected:
        payment = (money(0) if audit or payment_rule == "zero"
                   else cutoff if payment_rule == "uniform_price" else money(plan.bid))
        assignments.append(FullAssignment(plan.plan_id, plan.agent_id, money(plan.bid),
                                          payment, audit, priority_value))
    for agent, value in accounts.items():
        if sum((p.payment for p in assignments if p.agent_id == agent), Decimal(0)) > value:
            raise ValueError("clearing cannot satisfy wealth under the requested pricing rule")
    return FullAllocation(tuple(assignments), rejected, virtual, cutoff, reserve)
