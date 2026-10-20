"""One startup SearchDirector, separate from market or bidding strategy."""
from __future__ import annotations

import hashlib
import json
from maestro.agents.planner import TARGETS

_DIRECTIONS = (
    ("structural_reduction", ("reduction", "initialization"),
     "Explore safe reduction rules and seed construction.",
     "Improve the initial dominating set without changing validity."),
    ("local_improvement", ("local_search", "postprocess"),
     "Explore bounded local moves and postprocessing.",
     "Improve solution quality through verified local exchanges."),
    ("search_diversification", ("restart", "tie_break"),
     "Explore restarts and deterministic tie handling.",
     "Reduce sensitivity to an unproductive search trajectory."),
    ("execution_efficiency", ("data_structure", "time_management"),
     "Explore incremental data structures and time allocation.",
     "Perform more useful search within the fixed CPU contract."),
)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def make_direction_briefs(agent_ids, seed, *, provider=None, context=None):
    """Offline deterministic defaults or exactly one explicitly injected call.

    A live provider must itself be a budgeted single-call client. This function
    never retries or silently falls back when live output fails validation.
    """
    ids = list(agent_ids)
    if len(ids) != len(set(ids)) or any(not isinstance(a, str) or not a for a in ids):
        raise ValueError("unique nonempty agent identities required")
    if not ids:
        raise ValueError("at least one agent required")
    public_context = dict(context or {})
    prompt_hash = _hash({"kind": "search_director_v1", "agent_ids": ids,
                         "seed": seed, "context": public_context})
    if provider is not None:
        payload = provider({"agent_ids": ids, "seed": seed,
                            "task": public_context, "allowed_targets": list(TARGETS)})
        if isinstance(payload, str):
            from maestro.agents.planner import parse_json_object
            parsed, _ = parse_json_object(payload)
            payload = parsed.get("briefs")
        elif isinstance(payload, dict):
            payload = payload.get("briefs")
        if not isinstance(payload, list):
            raise ValueError("director must return a list of briefs")
        briefs = [dict(item) for item in payload]
    else:
        order = sorted(range(len(_DIRECTIONS)), key=lambda i: _hash([seed, "direction", i]))
        briefs = []
        for index, agent in enumerate(ids):
            name, focus, objective, hypothesis = _DIRECTIONS[order[index % len(order)]]
            variant = index // len(order)
            if variant:
                objective += f" Independently test bounded variant {variant + 1}."
            briefs.append({"direction_id": f"direction-{name}-{index + 1}",
                           "agent_id": agent, "objective": objective,
                           "allowed_focus": list(focus),
                           "initial_hypotheses": [hypothesis],
                           "diversity_rationale": f"Independent route {index + 1}: {name}.",
                           "seed_artifact": public_context.get("seed_artifact", "unspecified")})
    if len(briefs) != len(ids) or {b.get("agent_id") for b in briefs} != set(ids):
        raise ValueError("director must assign every agent exactly once")
    seen_directions, seen_objectives = set(), set()
    by_agent = {}
    for brief in briefs:
        required = {"direction_id", "agent_id", "objective", "allowed_focus",
                    "initial_hypotheses", "diversity_rationale"}
        if not required <= brief.keys():
            raise ValueError("director brief missing required fields")
        if any(not isinstance(brief[k], str) or not brief[k].strip()
               for k in ("direction_id", "objective", "diversity_rationale")):
            raise ValueError("empty direction identity/objective/rationale")
        focus = brief["allowed_focus"]
        if not isinstance(focus, (list, tuple)) or not focus or not set(focus) <= set(TARGETS):
            raise ValueError("director focus outside frozen taxonomy")
        hypotheses = brief["initial_hypotheses"]
        if not isinstance(hypotheses, list) or not hypotheses or any(
                not isinstance(h, str) or not h.strip() for h in hypotheses):
            raise ValueError("director requires initial hypotheses")
        if brief["direction_id"] in seen_directions or brief["objective"] in seen_objectives:
            raise ValueError("director duplicated a direction/objective")
        seen_directions.add(brief["direction_id"])
        seen_objectives.add(brief["objective"])
        brief["seed_artifact"] = public_context.get("seed_artifact", brief.get("seed_artifact", "unspecified"))
        brief["prompt_hash"] = prompt_hash
        brief["source"] = "live_director" if provider else "predeclared_deterministic"
        by_agent[brief["agent_id"]] = brief
    return [by_agent[agent] for agent in ids]
