"""Stage-specific copies of controller state supplied to an agent."""
from copy import deepcopy
from dataclasses import replace


def agent_state_view(state, purpose):
    """Warm agents see their own outcomes, without the global reward baseline.

    The controller retains the original state for settlement and the Market
    checkpoint. Credits and champion scores can disclose the shared champion,
    so neither is supplied during Warm Start.
    """
    view = deepcopy(state)
    if purpose == 'WARM':
        view.history = [replace(record, champion_score=None, credit=None)
                        for record in view.history]
    return view
