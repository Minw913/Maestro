"""Balanced opportunity accounting without market bids or payments."""
from __future__ import annotations

from collections import Counter
import hashlib
from maestro.agents.valuation import BootstrapIncomplete


def balanced_schedule(agent_ids, slots, opportunities, seed):
    """Batch equal opportunities using warm concurrency, not auction slots.

    A short last wave is valid. An agent occurs at most once in each wave, so
    its next refinement always follows the previous wave's result barrier.
    """
    ids = list(agent_ids)
    if len(ids) != len(set(ids)) or not ids:
        raise ValueError("unique nonempty agent population required")
    if type(slots) is not int or not 1 <= slots <= len(ids):
        raise ValueError("slots must be in [1, number of agents]")
    if type(opportunities) is not int or opportunities < 1:
        raise ValueError("positive opportunities per agent required")
    order = sorted(ids, key=lambda agent: hashlib.sha256(
        repr((seed, "warm-order", agent)).encode()).hexdigest())
    sequence = order * opportunities
    waves = [sequence[i:i + slots] for i in range(0, len(sequence), slots)]
    assert all(len(wave) == len(set(wave)) for wave in waves)
    assert Counter(a for wave in waves for a in wave) == Counter({a: opportunities for a in ids})
    return waves


def validate_warm_completion(states, opportunity_counts, expected_opportunities):
    """Precheck failures count as opportunities, never invented history rows."""
    states = states.values() if isinstance(states, dict) else states
    errors = []
    for state in states:
        if opportunity_counts.get(state.agent_id, 0) != expected_opportunities:
            errors.append(f"{state.agent_id}: unequal opportunity count")
        if not state.history:
            errors.append(f"{state.agent_id}: no formal evaluation record")
        if any(record.agent_id != state.agent_id for record in state.history):
            errors.append(f"{state.agent_id}: foreign history")
        if state.ucb_n != len(state.history):
            errors.append(f"{state.agent_id}: UCB count does not include complete formal warm history")
    if errors:
        raise BootstrapIncomplete("; ".join(errors))
    return True
