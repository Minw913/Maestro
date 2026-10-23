"""Frozen full priority orders used for admission and precheck backfill."""
from __future__ import annotations

import hashlib
import math
from maestro.market.credit import decimal


def _tie(seed, *parts):
    return hashlib.sha256(repr((seed, *parts)).encode()).hexdigest()


def rank(policy, submissions, states, round_index, seed, *, slots):
    """Return every eligible agent once; callers may take successive backfills.

    ``slots`` is the configured K (``RunConfig.slots``). UCB never mutates
    counts here: only finalized formal outcomes at settlement update them.
    """
    if round_index < 0 or slots <= 0:
        raise ValueError("nonnegative round and positive slots required")
    by_agent = {}
    for submission in submissions:
        if submission.agent_id in by_agent:
            raise ValueError("at most one submission per agent per round")
        by_agent[submission.agent_id] = submission
    state_map = states if isinstance(states, dict) else {s.agent_id: s for s in states}
    unknown = set(by_agent) - set(state_map)
    if unknown:
        raise ValueError(f"submission has unknown agent: {sorted(unknown)}")
    eligible = [a for a, submission in by_agent.items() if submission.eligible]
    tie = lambda a: _tie(seed, "policy-tie", round_index, a)
    if policy == "first_price":
        for agent in eligible:
            if decimal(by_agent[agent].bid) < 0:
                raise ValueError("negative bid")
        return sorted(eligible, key=lambda a: (-decimal(by_agent[a].bid), tie(a)))
    if policy == "uniform":
        # Rotate the entire population, then remove ineligible agents. Rotating
        # only eligible agents would silently give abstainers extra priority.
        population = sorted(state_map, key=lambda a: _tie(seed, "rr-order", a))
        if not population:
            return []
        offset = (round_index * slots) % len(population)
        priority = population[offset:] + population[:offset]
        return [a for a in priority if a in eligible]
    if policy == "ucb":
        if any(s.ucb_n < 0 for s in state_map.values()):
            raise ValueError("UCB counts cannot be negative")
        total_n = sum(s.ucb_n for s in state_map.values())
        def ucb(agent):
            s = state_map[agent]
            if s.ucb_n == 0:
                return math.inf
            return float(decimal(s.ucb_total) / s.ucb_n) + math.sqrt(
                2 * math.log(1 + total_n) / s.ucb_n)
        return sorted(eligible, key=lambda a: (-ucb(a), tie(a)))
    raise ValueError(f"unsupported V1 policy: {policy}")
