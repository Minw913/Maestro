"""Semantic warm snapshots; native Coral session IDs are deliberately absent."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, is_dataclass
from decimal import Decimal
import hashlib
import json
from maestro.types import AgentState, HistoryRecord, Plan
from maestro.market.credit import money


def _jsonable(value):
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(v) for v in value]
    return value


def serialize_state(state):
    return _jsonable(state)


def deserialize_state(payload):
    data = deepcopy(payload)
    data["wealth"] = money(data.get("wealth", "2000"))
    data["ucb_total"] = money(data.get("ucb_total", "0"))
    histories = []
    for row in data.get("history", []):
        row["credit"] = money(row.get("credit", "0"))
        histories.append(HistoryRecord(**row))
    data["history"] = histories
    if data.get("pending_plan") is not None:
        row = data["pending_plan"]
        row["evidence_run_ids"] = tuple(row.get("evidence_run_ids", ()))
        data["pending_plan"] = Plan(**row)
    return AgentState(**data)


def make_snapshot(states, board=None, manifests=None, *, settled=True):
    if not settled:
        raise ValueError("only a fully settled boundary can be snapshotted")
    state_map = states if isinstance(states, dict) else {s.agent_id: s for s in states}
    body = {"schema_version": 1, "settled": True,
            "states": {agent: serialize_state(state_map[agent]) for agent in sorted(state_map)},
            "board": _jsonable(board or {}), "manifests": _jsonable(manifests or {})}
    canonical = json.dumps(body, sort_keys=True, ensure_ascii=False,
                           separators=(",", ":")).encode()
    return {**body, "snapshot_hash": hashlib.sha256(canonical).hexdigest()}


def verify_snapshot(snapshot):
    body = deepcopy(snapshot)
    expected = body.pop("snapshot_hash", None)
    actual = hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False,
                                       separators=(",", ":")).encode()).hexdigest()
    if expected != actual or body.get("schema_version") != 1 or body.get("settled") is not True:
        raise ValueError("snapshot hash/version/settled boundary invalid")
    return True


def clone_states(snapshot, initial_wealth=Decimal("2000"), *, fresh_main=True):
    """Copy all real histories/UCB stats; reset only the main wallet and phase.

    A warm pending plan is invalidated at the main boundary because it was not
    prepared against a main round snapshot. Every arm receives the same rule.
    Native sessions are recreated from these private semantic facts.
    """
    verify_snapshot(snapshot)
    states = {agent: deserialize_state(data) for agent, data in snapshot["states"].items()}
    if fresh_main:
        for state in states.values():
            state.wealth = money(initial_wealth)
            state.pending_plan = None
            state.last_planning_round = -999
            state.needs_refresh = False
    return states


def clone_arms(snapshot, policies=("uniform", "ucb", "first_price"),
               initial_wealth=Decimal("2000")):
    if len(set(policies)) != len(policies):
        raise ValueError("duplicate policy arm")
    return {policy: clone_states(snapshot, initial_wealth) for policy in policies}


# Explicit aliases for callers that prefer state-oriented names.
state_to_dict = serialize_state
state_from_dict = deserialize_state
