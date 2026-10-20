"""Ticket-enforced formal experiment launch. No caller-supplied metadata is trusted."""
from __future__ import annotations

from maestro.types import EvaluationResult
from .authorization import execution_scope


class Dispatcher:
    def __init__(self, ledger, evaluator):
        self.ledger = ledger
        self.evaluator = evaluator

    def dispatch(self, ticket_id: str, job_id: str, expected_epoch: int = 0) -> EvaluationResult:
        # Completed replay is read-only. A claimed/running job has no automatic
        # rerun path: physical exactly-once cannot be guaranteed by SQLite alone.
        completed = self.ledger.existing_result(job_id)
        if completed is not None:
            trusted = self.ledger.ticket(ticket_id)
            if (trusted.get("job_id") != job_id or trusted["candidate_hash"] != completed.candidate_hash
                    or trusted["phase_epoch"] != expected_epoch):
                raise PermissionError("Completed result belongs to another ticket/candidate")
            return completed
        trusted = self.ledger.ticket(ticket_id)
        if trusted.get("candidate_path"):
            from maestro.backends.coral.workspace import candidate_tree_hash
            if candidate_tree_hash(trusted["candidate_path"]) != trusted["candidate_hash"]:
                raise PermissionError("Admitted immutable candidate content has changed before launch")
        ticket = self.ledger.claim_ticket(ticket_id, job_id, expected_epoch=expected_epoch)
        fence = ticket["fence"]
        try:
            # A Python call is not proof that an OS process started. The trusted
            # evaluator may record RUNNING only with a real executor receipt.
            with execution_scope(self.ledger, ticket):
                result = self.evaluator.evaluate(ticket)
            if result.job_id != job_id or result.candidate_hash != ticket["candidate_hash"]:
                raise PermissionError("Evaluator returned an unbound result")
            self.ledger.record_result(result, fence)
            return result
        except BaseException as error:
            self.ledger.mark_unknown(job_id, f"{type(error).__name__}: {error}")
            raise
