"""The evaluator boundary receives trusted, admitted ticket data only."""

from typing import Any, Protocol


class Evaluator(Protocol):
    def evaluate(self, ticket: dict[str, Any]) -> Any:
        """Evaluate one immutable candidate under its bound task contract."""
        ...
