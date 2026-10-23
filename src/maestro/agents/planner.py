"""Plan-first preparation and persistent pending-plan lifecycle.

This module cannot edit a candidate. A successful schema check is not an
EditGrant or an EvalTicket, and caller-provided owner/parent IDs are rechecked.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any

from maestro.types import Plan

from maestro.tasks.taxonomy import TARGETS, MOD_TYPES

PLAN_FIELDS = {"plan_id", "version_id", "agent_id", "parent_hash", "target",
               "mod_type", "diagnosis", "change", "expected_effect",
               "evidence_run_ids", "category_detail"}


def needs_refresh(state, round_index, refresh_interval=3) -> bool:
    if refresh_interval <= 0:
        raise ValueError("refresh interval must be positive")
    if state.pending_plan is None or state.needs_refresh:
        return True
    return (state.pending_plan.parent_hash != state.parent_hash
            or round_index - state.last_planning_round >= refresh_interval)


def validate_plan(plan, parent_hash, allowed_evidence_ids):
    if not isinstance(plan, Plan):
        raise TypeError("Plan required")
    for field in ("plan_id", "version_id", "agent_id", "parent_hash", "diagnosis",
                  "change", "expected_effect"):
        value = getattr(plan, field)
        if not isinstance(value, str) or not value.strip() or len(value) > 16000:
            raise ValueError(f"invalid {field}")
    if plan.parent_hash != parent_hash:
        raise ValueError("plan parent differs from the frozen parent")
    if plan.target not in TARGETS or plan.mod_type not in MOD_TYPES:
        raise ValueError("unknown target or modification type")
    if not isinstance(plan.category_detail, str) or len(plan.category_detail) > 4000:
        raise ValueError("invalid category_detail")
    if not isinstance(plan.evidence_run_ids, (tuple, list)):
        raise ValueError("evidence_run_ids must be a sequence")
    ids = list(plan.evidence_run_ids)
    if any(not isinstance(item, str) or not item for item in ids) or len(ids) != len(set(ids)):
        raise ValueError("evidence IDs must be distinct nonempty strings")
    if not set(ids) <= set(allowed_evidence_ids):
        raise ValueError("plan evidence contains inaccessible experiment IDs")
    return plan


def required_evidence(history, target, mod_type):
    """All matching own evidence, including failed and zero-gain outcomes."""
    return tuple(r.experiment_id for r in history
                 if r.target == target and r.mod_type == mod_type)


def validate_complete_evidence(plan, history):
    required = set(required_evidence(history, plan.target, plan.mod_type))
    if not required <= set(plan.evidence_run_ids):
        raise ValueError("matching own evidence was omitted, including failures")
    return plan


def _hash(*parts):
    return hashlib.sha256(json.dumps(parts, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


def mock_plan(state, round_index, seed) -> Plan:
    """Deterministic simulation plan; does not claim a model was called."""
    key = int(_hash(seed, "plan", state.agent_id, round_index)[:16], 16)
    target = TARGETS[key % len(TARGETS)]
    mod_type = MOD_TYPES[(key // len(TARGETS)) % len(MOD_TYPES)]
    evidence = required_evidence(state.history, target, mod_type)
    # Fallback evidence supplies context but does not change exact-match value.
    if not evidence:
        evidence = tuple(r.experiment_id for r in state.history)
    plan_id = "plan-" + _hash(seed, state.agent_id, round_index, state.parent_hash)[:20]
    return Plan(plan_id=plan_id, version_id=plan_id + "-v1", agent_id=state.agent_id,
                parent_hash=state.parent_hash, target=target, mod_type=mod_type,
                diagnosis="Use verified own history to test a bounded refinement.",
                change=f"Apply one {mod_type} refinement to {target}.",
                expected_effect="Improve verified score within the unchanged execution contract.",
                evidence_run_ids=evidence,
                category_detail="deterministic mock proposal; not measured evidence")


def _distinct_object(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError(f"duplicate JSON field: {key}")
        obj[key] = value
    return obj


def repair_json_once(raw: str) -> str:
    """Only strip an enclosing JSON fence and trailing commas outside strings.

    Never invent values, quote bare keys, use eval, or make another model call.
    A malformed payload after this one deterministic repair must abstain.
    """
    value = raw.strip()
    if value.startswith("```") and value.endswith("```"):
        first = value.find("\n")
        if first == -1 or value[:first].strip().lower() not in {"```", "```json"}:
            raise ValueError("unsupported code fence")
        value = value[first + 1:-3].strip()
    chars = []
    in_string = escaped = False
    for i, char in enumerate(value):
        if in_string:
            chars.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
            chars.append(char)
        elif char == ",":
            following = value[i + 1:].lstrip()
            if not following.startswith(("}", "]")):
                chars.append(char)
        else:
            chars.append(char)
    return "".join(chars)


def parse_json_object(raw: str) -> tuple[dict[str, Any], bool]:
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > 128 * 1024:
        raise ValueError("planner response missing or too large")
    def load(value):
        def invalid_constant(value):
            raise ValueError(f"nonfinite JSON value: {value}")
        result = json.loads(value, object_pairs_hook=_distinct_object,
                            parse_constant=invalid_constant)
        if not isinstance(result, dict):
            raise ValueError("planner JSON object required")
        return result
    try:
        return load(raw), False
    except json.JSONDecodeError:
        return load(repair_json_once(raw)), True


@dataclass(frozen=True)
class PlanDecision:
    action: str
    plan: Plan | None
    lessons: tuple[dict, ...] = ()
    repaired: bool = False


def parse_plan_decision(raw, state, round_index, allowed_evidence_ids=None, *, namespace=""):
    """Parse one model answer. Caller records its one-call receipt separately."""
    payload, repaired = parse_json_object(raw)
    if set(payload) - {"action", "plan", "lessons"}:
        raise ValueError("unknown planning decision fields")
    action = payload.get("action")
    if action not in {"retain", "revise", "withdraw"}:
        raise ValueError("action must be retain, revise, or withdraw")
    lessons = payload.get("lessons", [])
    if not isinstance(lessons, list) or len(lessons) > 5 or any(not isinstance(x, dict) for x in lessons):
        raise ValueError("lessons must contain at most five objects")
    if action == "withdraw":
        if payload.get("plan") is not None:
            raise ValueError("withdraw cannot contain a plan")
        return PlanDecision(action, None, tuple(lessons), repaired)
    if action == "retain":
        if state.pending_plan is None or state.pending_plan.parent_hash != state.parent_hash:
            raise ValueError("cannot retain a missing plan or rebind its parent")
        # A failed precheck requires a NEW planning call, but that call may
        # explicitly retain the still-unexecuted hypothesis. Formal consumption
        # and parent changes remove the pending plan entirely.
        if payload.get("plan") is not None:
            raise ValueError("retain references the existing version without editing it")
        return PlanDecision(action, state.pending_plan, tuple(lessons), repaired)
    supplied = payload.get("plan")
    if not isinstance(supplied, dict) or set(supplied) - PLAN_FIELDS:
        raise ValueError("revise requires a plan object with known fields")
    data = dict(supplied)
    for key, expected in (("agent_id", state.agent_id), ("parent_hash", state.parent_hash)):
        if key in data and data[key] != expected:
            raise ValueError(f"planner cannot override {key}")
        data[key] = expected
    content = {k: v for k, v in data.items() if k not in {"plan_id", "version_id"}}
    version = "pv-" + _hash(namespace, state.agent_id, state.parent_hash, round_index, content)[:24]
    # A revision retains logical plan_id, but NEVER overwrites its old version.
    data["plan_id"] = (state.pending_plan.plan_id if state.pending_plan is not None
                       else "plan-" + _hash(namespace, state.agent_id, round_index, state.parent_hash)[:24])
    data["version_id"] = version
    if isinstance(data.get("evidence_run_ids", ()), list):
        data["evidence_run_ids"] = tuple(data["evidence_run_ids"])
    try:
        plan = Plan(**data)
    except TypeError as exc:
        raise ValueError("missing or invalid plan fields") from exc
    allowed = ([r.experiment_id for r in state.history]
               if allowed_evidence_ids is None else allowed_evidence_ids)
    validate_plan(plan, state.parent_hash, allowed)
    validate_complete_evidence(plan, state.history)
    return PlanDecision(action, plan, tuple(lessons), repaired)


def apply_decision(state, decision, round_index):
    """Apply one validated planning call; no LLM or solver execution occurs."""
    if decision.action not in {"retain", "revise", "withdraw"}:
        raise ValueError("invalid planning action")
    if (decision.action == "withdraw") != (decision.plan is None):
        raise ValueError("planning action and payload disagree")
    if decision.plan is not None:
        validate_plan(decision.plan, state.parent_hash,
                      [r.experiment_id for r in state.history])
        if decision.plan.agent_id != state.agent_id:
            raise ValueError("wrong plan owner")
    from maestro.agents.memory import merge_lessons
    lessons = merge_lessons(state.lessons, decision.lessons,
                            [r.experiment_id for r in state.history])
    state.pending_plan = decision.plan
    state.last_planning_round = round_index
    state.needs_refresh = False
    state.lessons = [dict(lesson_id=lesson.lesson_id, claim=lesson.claim,
                          supporting_ids=list(lesson.supporting_ids),
                          opposing_ids=list(lesson.opposing_ids), status=lesson.status,
                          revision=lesson.revision) for lesson in lessons]
    return state


def invalidate_pending(state, reason):
    """Generation/precheck failure or formal use makes the old version unusable."""
    if reason not in {"FORMAL_EVALUATION", "GENERATION_FAILURE", "PRECHECK_FAILURE",
                      "PARENT_CHANGED", "DUPLICATE", "EXPIRED"}:
        raise ValueError("unknown pending-plan invalidation reason")
    state.needs_refresh = True
    if reason in {"FORMAL_EVALUATION", "PARENT_CHANGED", "DUPLICATE"}:
        state.pending_plan = None
    return reason
