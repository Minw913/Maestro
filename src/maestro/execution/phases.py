"""Synchronous phase state machine with monotonic deadlines and fencing epochs."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import time


class PhaseError(RuntimeError):
    pass


class Phase(str, Enum):
    PREPARE = "PREPARE"
    PLAN = "PLAN"
    CLEAR = "CLEAR"
    GENERATE = "GENERATE"
    EVALUATE = "EVALUATE"
    SETTLE = "SETTLE"
    COMPLETE = "COMPLETE"


_NEXT = {Phase.PREPARE: Phase.PLAN, Phase.PLAN: Phase.CLEAR,
         Phase.CLEAR: Phase.GENERATE, Phase.GENERATE: Phase.EVALUATE,
         Phase.EVALUATE: Phase.SETTLE, Phase.SETTLE: Phase.COMPLETE}


@dataclass
class PhaseController:
    """In-memory round barrier; the ledger remains the recovery authority.

    The supervisor must stop all previous handles before invoking ``advance``.
    A deadline is absolute: restarting a CLI never buys more phase time.
    """
    round_index: int
    phase: Phase = Phase.PREPARE
    epoch: int = 0
    deadline: float | None = None
    pending: set[str] = field(default_factory=set)
    clock: object = time.monotonic

    def begin(self, participants: list[str], duration: float) -> None:
        if duration <= 0 or self.deadline is not None:
            raise PhaseError("Phase already started or invalid duration")
        self.pending = set(participants)
        self.deadline = self.clock() + duration

    def arrive(self, agent_id: str, epoch: int) -> None:
        if epoch != self.epoch:
            raise PhaseError("Stale phase epoch")
        self.pending.discard(agent_id)

    @property
    def expired(self) -> bool:
        return self.deadline is not None and self.clock() >= self.deadline

    def advance(self, *, processes_stopped: bool, force_deadline: bool = False) -> Phase:
        if not processes_stopped:
            raise PhaseError("Old process trees must stop before permissions change")
        if self.pending and not (force_deadline and self.expired):
            raise PhaseError("Participants have not reached the barrier")
        if self.phase not in _NEXT:
            raise PhaseError("Round already complete")
        self.phase = _NEXT[self.phase]
        self.epoch += 1
        self.deadline = None
        self.pending.clear()
        return self.phase
