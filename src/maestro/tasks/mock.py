"""Compatibility import; simulation is never a fallback for PACE."""

from maestro.backends.mock import MockEvaluator

__all__ = ["MockEvaluator"]
