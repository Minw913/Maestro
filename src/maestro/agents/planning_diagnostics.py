"""Bounded, data-free diagnostics for rejected planning responses."""
from __future__ import annotations

import json


_VALIDATION_MESSAGES = {
    "planner response missing or too large": "RESPONSE_MISSING_OR_TOO_LARGE",
    "planner JSON object required": "JSON_OBJECT_REQUIRED",
    "unsupported code fence": "UNSUPPORTED_CODE_FENCE",
    "unknown planning decision fields": "UNKNOWN_DECISION_FIELDS",
    "action must be retain, revise, or withdraw": "INVALID_ACTION",
    "lessons must contain at most five objects": "INVALID_LESSONS",
    "withdraw cannot contain a plan": "WITHDRAW_HAS_PLAN",
    "cannot retain a missing plan or rebind its parent": "RETAIN_UNAVAILABLE",
    "retain references the existing version without editing it": "RETAIN_HAS_PLAN",
    "revise requires a plan object with known fields": "INVALID_REVISED_PLAN",
    "missing or invalid plan fields": "MISSING_PLAN_FIELDS",
    "plan parent differs from the frozen parent": "PARENT_MISMATCH",
    "unknown target or modification type": "INVALID_TAXONOMY",
    "unknown target": "INVALID_TARGET",
    "unknown modification type": "INVALID_MOD_TYPE",
    "invalid target": "INVALID_TARGET",
    "invalid mod_type": "INVALID_MOD_TYPE",
    "invalid category_detail": "INVALID_CATEGORY_DETAIL",
    "evidence_run_ids must be a sequence": "INVALID_EVIDENCE_TYPE",
    "evidence IDs must be distinct nonempty strings": "INVALID_EVIDENCE_IDS",
    "plan evidence contains inaccessible experiment IDs": "INACCESSIBLE_EVIDENCE",
    "matching own evidence was omitted, including failures": "INCOMPLETE_EVIDENCE",
}
_PLAN_FIELDS = frozenset({"plan_id", "version_id", "agent_id", "parent_hash",
    "diagnosis", "change", "expected_effect"})
_KNOWN_EXCEPTION_TYPES = frozenset({"ValueError", "TypeError", "TimeoutError",
    "PermissionError", "RuntimeError", "JSONDecodeError", "IndexError"})
_REQUEST_CODES = frozenset({"REQUEST_COUNT_MISMATCH", "PLANNING_DEADLINE_EXCEEDED",
    "PROTOCOL_VIOLATION", "PROVIDER_TERMINAL_FAILURE", "PROXY_RESOLUTION_FAILED",
    "DEADLINE_OR_TIMEOUT", "REQUEST_FAILED"})
_VALIDATION_CODES = frozenset(_VALIDATION_MESSAGES.values()) | {
    "JSON_DECODE_ERROR", "UNCLASSIFIED_VALIDATION_ERROR", "DUPLICATE_JSON_FIELD",
    "NONFINITE_JSON_VALUE",
} | {"INVALID_PLAN_FIELD_" + field.upper() for field in _PLAN_FIELDS} | {
    "IDENTITY_OVERRIDE_AGENT_ID", "IDENTITY_OVERRIDE_PARENT_HASH"}


def planning_rejection_diagnostic(error: Exception, step: str) -> dict[str, str]:
    """Describe a failure with fixed labels only; never persist exception text."""
    if step == "validation":
        if isinstance(error, json.JSONDecodeError):
            code = "JSON_DECODE_ERROR"
        else:
            message = str(error)
            code = _VALIDATION_MESSAGES.get(message)
            if code is None and message.startswith("invalid "):
                field = message.removeprefix("invalid ")
                if field in _PLAN_FIELDS:
                    code = "INVALID_PLAN_FIELD_" + field.upper()
            if code is None and message.startswith("planner cannot override "):
                field = message.removeprefix("planner cannot override ")
                if field in {"agent_id", "parent_hash"}:
                    code = "IDENTITY_OVERRIDE_" + field.upper()
            if code is None and message.startswith("duplicate JSON field: "):
                code = "DUPLICATE_JSON_FIELD"
            if code is None and message.startswith("nonfinite JSON value: "):
                code = "NONFINITE_JSON_VALUE"
            if code is None:
                code = "UNCLASSIFIED_VALIDATION_ERROR"
        category = "PLAN_VALIDATION"
    elif step == "request_count":
        category, code = "REQUEST_ADMISSION", "REQUEST_COUNT_MISMATCH"
    elif step == "deadline":
        category, code = "REQUEST_ADMISSION", "PLANNING_DEADLINE_EXCEEDED"
    elif step == "protocol_violation":
        category, code = "REQUEST_PROTOCOL", "PROTOCOL_VIOLATION"
    elif step == "terminal_failure":
        category, code = "REQUEST_PROTOCOL", "PROVIDER_TERMINAL_FAILURE"
    elif step == "proxy_resolution":
        category, code = "REQUEST_PROTOCOL", "PROXY_RESOLUTION_FAILED"
    else:
        category = "REQUEST_EXECUTION"
        code = "DEADLINE_OR_TIMEOUT" if isinstance(error, TimeoutError) else "REQUEST_FAILED"
    exception_type = type(error).__name__
    return {"category": category, "code": code,
            "exception_type": exception_type if exception_type in _KNOWN_EXCEPTION_TYPES else "Other"}


def safe_planning_diagnostic(value) -> dict[str, str] | None:
    """Accept only registered labels before writing a backend diagnostic to the ledger."""
    if not isinstance(value, dict):
        return None
    category, code, exception_type = (value.get(key) for key in
        ("category", "code", "exception_type"))
    if not all(isinstance(item, str) for item in (category, code, exception_type)):
        return None
    allowed = _VALIDATION_CODES if category == "PLAN_VALIDATION" else (
        _REQUEST_CODES if category in {"REQUEST_ADMISSION", "REQUEST_PROTOCOL",
                                  "REQUEST_EXECUTION"} else ())
    if (code not in allowed or exception_type not in _KNOWN_EXCEPTION_TYPES | {"Other"}
            or set(value) != {"category", "code", "exception_type"}):
        return None
    return {"category": category, "code": code, "exception_type": exception_type}
