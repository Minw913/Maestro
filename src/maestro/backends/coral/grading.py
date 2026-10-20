"""Real Coral Task.metadata adapter and ticket-guarded TaskGrader entrypoint."""
from __future__ import annotations

from pathlib import Path
from maestro.backends.base import BackendUnavailable
from maestro.execution.dispatcher import Dispatcher
from .runtime import load_factory
from .results import to_score_bundle
from .workspace import candidate_tree_hash

try:
    from coral.grader.task_grader import TaskGrader as _TaskGrader
except ImportError:
    class _TaskGrader:
        """Import-time compatibility only; cannot execute without real Coral."""
        def __init__(self, config):
            self.config, self.tasks = config, []


_BINDINGS = ("episode_id", "candidate_hash", "contract_hash", "panel_hash", "plan_version_id",
             "parent_hash", "purpose", "agent_id", "round_index")


def task_for_ticket(ticket: dict):
    """Metadata survives Coral SubprocessGrader's Task.to_dict/from_dict boundary.

    Arbitrary grader kwargs do not survive that worker; do not place a ticket
    solely in ``grade(...,ticket_id=...)``.
    """
    try:
        from coral.types import Task
    except ImportError as error:
        raise BackendUnavailable("Real Coral Task class is unavailable") from error
    metadata = {key: ticket.get(key) for key in _BINDINGS}
    metadata.update(ticket_id=ticket["ticket_id"], job_id=ticket["job_id"],
                    phase_epoch=ticket.get("phase_epoch", 0))
    return Task(id=ticket["job_id"], name="MAESTRO formal evaluation",
                description="Execution requires a trusted admitted EvalTicket",
                metadata={"maestro": metadata})


class TicketedGradeBridge:
    def __init__(self, ledger, evaluator):
        self.ledger, self.evaluator = ledger, evaluator

    def grade_request(self, codebase_path: str, tasks: list):
        if len(tasks) != 1:
            raise PermissionError("V1 requires exactly one ticket-bound candidate per grade request")
        meta = tasks[0].metadata.get("maestro")
        if not isinstance(meta, dict) or not meta.get("ticket_id") or not meta.get("job_id"):
            raise PermissionError("No trusted ticket binding in Task.metadata")
        ticket = self.ledger.ticket(meta["ticket_id"])
        for key in _BINDINGS:
            if meta.get(key) != ticket.get(key):
                raise PermissionError(f"Task metadata is not bound to the canonical ticket: {key}")
        if ticket.get("job_id") != meta["job_id"]:
            raise PermissionError("Task job ID mismatch")
        if hasattr(tasks[0], 'id') and tasks[0].id != ticket['job_id']:
            raise PermissionError("Coral Task ID differs from the ticket-bound job")
        if Path(codebase_path).resolve() != Path(ticket["candidate_path"]).resolve():
            raise PermissionError("Grader checkout is not the admitted immutable artifact")
        if candidate_tree_hash(codebase_path) != ticket["candidate_hash"]:
            raise PermissionError("Admitted candidate content has changed")
        return Dispatcher(self.ledger, self.evaluator).dispatch(
            meta["ticket_id"], meta["job_id"], expected_epoch=meta.get("phase_epoch", 0))


class TicketedGrader(_TaskGrader):
    """Configure Coral grader.entrypoint as maestro.backends.coral.grading:TicketedGrader.

    ``config.args`` contains trusted ledger_path and evaluator_factory. Candidate
    code runs only in that evaluator's containment runner, never the grader venv.
    This entrypoint is a last gate; the default architecture does not launch the
    stock daemon or its worker pool for unfunded pending submissions.
    """
    def evaluate(self):
        from maestro.storage.ledger import Ledger
        args = self.config.args
        if not args.get("ledger_path") or not args.get("evaluator_factory"):
            raise BackendUnavailable("TicketedGrader requires authority ledger and trusted evaluator factory")
        evaluator = load_factory(args["evaluator_factory"])(args)
        with Ledger(args["ledger_path"]) as ledger:
            result = TicketedGradeBridge(ledger, evaluator).grade_request(self.codebase_path, self.tasks)
        return to_score_bundle(result)
