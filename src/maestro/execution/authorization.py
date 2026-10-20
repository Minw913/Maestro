"""Ephemeral dispatcher authority, never serialized into agent-visible tickets.

A ticket dictionary is evidence, not permission. Only a live dispatcher claim
opens this context; PACE checks it again immediately before every runner call.
The context is process local and cannot be obtained through Coral Task metadata.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar

from maestro.types import AuthorizationError


_active_claim = ContextVar("maestro_active_execution_claim", default=None)
@contextmanager
def execution_scope(ledger, claimed_ticket):
    """Trusted dispatcher-only scope; it grants no new claim or launch itself."""
    token = _active_claim.set((ledger, dict(claimed_ticket), {"evaluation_entered": False}))
    try:
        yield
    finally:
        _active_claim.reset(token)


def require_execution_authority(ticket, *, begin=False, launch=False):
    claim = _active_claim.get()
    if claim is None:
        raise AuthorizationError("Formal evaluation requires a live dispatcher ticket claim")
    ledger, expected, scope_state = claim
    if any(ticket.get(key) != value for key, value in expected.items() if key != "status"):
        raise AuthorizationError("Evaluator request differs from its dispatcher claim")
    ledger.validate_execution_claim(expected)
    if begin:
        if scope_state['evaluation_entered']:
            raise AuthorizationError('This dispatcher claim already entered a formal evaluator')
        scope_state['evaluation_entered'] = True
    if launch:
        # This records an attempt *before* calling the executor. It deliberately
        # does not assert that an operating-system process physically started.
        ledger.mark_launch_intent(expected["job_id"], expected["fence"])
