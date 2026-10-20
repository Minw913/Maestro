"""Untrusted Coral outbox requests, separated from trusted ticket admission.

Stock ``coral eval`` is not the experiment launcher in this architecture. A
coding agent may request an evaluation, but cannot mint a ticket, call the
dispatcher, read benchmark panels, or open the SQLite authority database.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import uuid


@dataclass(frozen=True)
class EvalRequest:
    agent_id: str
    plan_version_id: str
    parent_hash: str
    candidate_hash: str
    round_index: int


def write_eval_request(outbox: str | Path, request: EvalRequest) -> Path:
    """A request file conveys no authority; the controller revalidates every field."""
    outbox = Path(outbox)
    outbox.mkdir(parents=True, exist_ok=True)
    identity = uuid.uuid4().hex
    temp, target = outbox / f".{identity}.tmp", outbox / f"eval-request-{identity}.json"
    with temp.open("x", encoding="utf-8") as handle:
        json.dump(asdict(request), handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    temp.replace(target)
    return target


def write_bound_eval_request(*, message: str = "") -> Path:
    """Optional phase-bound file outbox; no grader launch, wait, or ticket minting."""
    from .workspace import candidate_tree_hash
    required = ("MAESTRO_EVAL_OUTBOX", "MAESTRO_AGENT_ID", "MAESTRO_PLAN_VERSION_ID",
                "MAESTRO_PARENT_HASH", "MAESTRO_ROUND_INDEX", "MAESTRO_SOLVER_PATH")
    if any(not os.environ.get(key) for key in required):
        raise PermissionError("No phase-bound MAESTRO evaluation outbox is available")
    return write_eval_request(os.environ["MAESTRO_EVAL_OUTBOX"], EvalRequest(
        os.environ["MAESTRO_AGENT_ID"], os.environ["MAESTRO_PLAN_VERSION_ID"],
        os.environ["MAESTRO_PARENT_HASH"], candidate_tree_hash(os.environ["MAESTRO_SOLVER_PATH"]),
        int(os.environ["MAESTRO_ROUND_INDEX"])))


class SubmissionAdapter:
    """Controller-side bridge for a previously issued canonical EvalTicket."""
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher

    def evaluate(self, ticket_id: str, job_id: str, epoch: int = 0):
        return self.dispatcher.dispatch(ticket_id, job_id, expected_epoch=epoch)

    def native_eval(self, *args, **kwargs):
        raise PermissionError("Native pending queues do not authorize formal MAESTRO evaluation")
