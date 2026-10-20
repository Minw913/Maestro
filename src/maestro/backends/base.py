from __future__ import annotations
from typing import Protocol
from maestro.types import EvaluationResult


class Evaluator(Protocol):
    def evaluate(self, ticket: dict) -> EvaluationResult: ...


class BackendUnavailable(RuntimeError):
    """A requested real backend lacks a verified prerequisite."""
