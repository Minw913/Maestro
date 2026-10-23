"""The evaluator boundary receives trusted, admitted ticket data only."""

from importlib import import_module
from typing import Any, Callable, Protocol


class Evaluator(Protocol):
    def evaluate(self, ticket: dict[str, Any]) -> Any:
        """Evaluate one immutable candidate under its bound task contract."""
        ...


class Benchmark(Protocol):
    """Everything the benchmark-neutral core needs from one task family."""
    name: str
    directions: tuple  # (label, focus targets, exploration text, objective text) per startup brief

    def settings(self, config) -> dict:
        """The benchmark's own config section."""
        ...

    def live(self, config):
        """Frozen panel/contract for real execution, with `panel_hash`, `contract_hash` and
        `evaluator(runner=, artifact_store=, cpu_pool=)`; raises when prerequisites are missing."""
        ...

    def image(self, config) -> str:
        """Pinned candidate container image."""
        ...

    def seed_solver(self, config) -> str:
        """Path of the common seed candidate directory."""
        ...

    def solver_argv(self, config) -> tuple[str, ...]:
        """Command that runs a candidate inside its container, reading the instance on stdin."""
        ...

    def smoke_check(self, config) -> Callable:
        """`check(candidate_path, runner)` for the fixed pre-evaluation toy run, input disjoint from the panel."""
        ...

    def evaluate_final(self, engine, source_episode_id: str, manifest_path: str) -> dict:
        """Held-out evaluation of a completed episode's champion."""
        ...


BENCHMARKS = {
    'pace': 'maestro.tasks.pace.benchmark:PACE',
    'ale': 'maestro.tasks.ale.benchmark:ALE',
}


def benchmark_for(name: str) -> Benchmark:
    """The registered adapter for a benchmark name."""
    if name not in BENCHMARKS:
        raise ValueError(f'unknown benchmark {name!r}; registered: {sorted(BENCHMARKS)}')
    module, attribute = BENCHMARKS[name].split(':')
    return getattr(import_module(module), attribute)
