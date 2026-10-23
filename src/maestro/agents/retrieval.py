"""Deterministic private retrieval and explicit, reproducible truncation."""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from decimal import Decimal
import hashlib
import json
from maestro.tasks.taxonomy import TARGETS, MOD_TYPES, TAXONOMY_VERSION



def _jsonable(value):
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(x) for x in value]
    return value


def _encode(value):
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _public_board(board):
    """Only verified-board contract fields; arbitrary private/raw fields drop."""
    if board is None:
        return {}
    if not isinstance(board, dict):
        raise ValueError("public board must be an explicit verified projection")
    keys = {"version", "round_index", "champion_hash", "champion_score",
            "champion_artifact", "panel_hash", "contract_hash"}
    result = {k: board[k] for k in keys if k in board}
    rows = []
    for row in board.get("results", []):
        if not isinstance(row, dict) or not row.get("verified", False):
            continue
        allowed = {"experiment_id", "candidate_hash", "agent_id", "score", "valid",
                   "status", "verified", "target", "mod_type", "round_index"}
        rows.append({k: row[k] for k in allowed if k in row})
    if rows:
        result["results"] = rows
    return _jsonable(result)


def build_context(state, board, token_budget=8192):
    """Own measured examples plus public verified facts, never private peers.

    The estimate uses UTF-8 bytes as a conservative token upper bound for the
    provider-independent context. Exact provider token use is additionally
    reserved at the gateway. Context truncation NEVER truncates valuation.
    """
    if token_budget < 1:
        raise ValueError("positive context token budget required")
    records = list(state.history)
    if any(r.agent_id != state.agent_id for r in records):
        raise ValueError("private history contains another agent's records")
    pending = state.pending_plan
    preferred = ((pending.target, pending.mod_type) if pending else None)
    indexed = list(enumerate(records))
    # Prefer exact categories; preserve a failed/zero-gain counterexample before
    # successful examples, then recency. Numeric estimation separately uses all.
    indexed.sort(key=lambda pair: (
        0 if preferred == (pair[1].target, pair[1].mod_type) else 1,
        0 if not pair[1].valid or pair[1].gain <= 0 else 1,
        -pair[0]))
    own = [{"kind": "measured", **_jsonable(record)} for _, record in indexed]
    evidence_catalog = {}
    for record in records:
        category = f"{record.target}/{record.mod_type}"
        evidence_catalog.setdefault(category, []).append(record.experiment_id)
    lessons = _jsonable(state.lessons[-5:])
    public = _public_board(board)
    history_hash = hashlib.sha256(_encode(records)).hexdigest()
    context = {
        "agent_id": state.agent_id,
        "parent_hash": state.parent_hash,
        "parent_score": state.parent_score,
        "pending_plan": _jsonable(pending),
        "taxonomy": {"targets": list(TARGETS), "mod_types": list(MOD_TYPES)},
        # Never truncate this compact identity index. A model can cite every
        # matching failed/zero record even when verbose examples were omitted.
        "evidence_catalog": evidence_catalog,
        "history": own,
        "lessons": lessons,
        "board": public,
        "schema_examples": [{
            "kind": "synthetic_schema_only", "target": "local_search",
            "mod_type": "parameter_change",
            "change": "Change one search parameter; this is not measured evidence."}],
        "manifest": {},
    }
    def manifest():
        included = [r["experiment_id"] for r in context["history"]]
        context["manifest"] = {
            "taxonomy_version": TAXONOMY_VERSION,
            "selection_rule": "own_exact_then_counterexample_then_recent",
            "included_experiment_ids": included,
            "available_record_count": len(records),
            "omitted_record_count": len(records) - len(included),
            "history_content_hash": history_hash,
            "token_budget": token_budget,
            "token_estimator": "utf8_byte_upper_bound",
            "numeric_estimation_uses_untruncated_history": True,
        }
    manifest()
    while len(_encode(context)) > token_budget:
        if context["history"]:
            context["history"].pop()
        elif context["lessons"]:
            context["lessons"].pop(0)
        elif context["board"].get("results"):
            context["board"]["results"].pop(0)
        elif context["schema_examples"]:
            context["schema_examples"] = []
        elif context["board"]:
            context["board"] = {}
        else:
            raise ValueError("context budget is too small for its identity and manifest")
        manifest()
    # Self hash excludes itself, avoiding a circular content-hash definition.
    context["manifest"]["context_content_hash"] = hashlib.sha256(_encode(context)).hexdigest()
    while len(_encode(context)) > token_budget:
        if context["history"]:
            context["history"].pop()
        elif context["lessons"]:
            context["lessons"].pop(0)
        elif context["board"].get("results"):
            context["board"]["results"].pop(0)
        elif context["schema_examples"]:
            context["schema_examples"] = []
        elif context["board"]:
            context["board"] = {}
        else:
            raise ValueError("context budget too small for hashed manifest")
        manifest()
        context["manifest"]["context_content_hash"] = hashlib.sha256(_encode(context)).hexdigest()
    return context
