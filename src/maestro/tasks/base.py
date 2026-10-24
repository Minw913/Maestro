"""The evaluator boundary receives trusted, admitted ticket data only."""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from maestro.config import RunConfig
    from maestro.execution.cpu_pool import GlobalCpuPool
    from maestro.execution.sandbox import ContainmentRunner
    from maestro.loop import ExperimentEngine
    from maestro.types import EvaluationResult

# (label, focus targets, exploration text, objective text) for one startup brief.
Direction = tuple[str, tuple[str, ...], str, str]
Ticket = dict[str, object]
ArtifactStore = Callable[[bytes], str]
# (candidate_path, runner) -> (passed, reason); a RuntimePrecheckFailure is such a tuple too
SmokeCheck = Callable[['str | Path', 'ContainmentRunner'], tuple[bool, str]]


class Evaluator(Protocol):
    def evaluate(self, ticket: Ticket) -> EvaluationResult:
        """Evaluate one immutable candidate under its bound task contract."""
        ...


class LiveTask(Protocol):
    """A frozen panel and contract for real execution."""

    @property
    def panel(self) -> object: ...

    @property
    def contract(self) -> object: ...

    @property
    def panel_hash(self) -> str: ...

    @property
    def contract_hash(self) -> str: ...

    def evaluator(self, *, runner: ContainmentRunner | None, artifact_store: ArtifactStore | None,
                  cpu_pool: GlobalCpuPool) -> Evaluator:
        """The formal evaluator bound to this panel and contract."""
        ...


class Benchmark(Protocol):
    """Everything the benchmark-neutral core needs from one task family."""

    name: str
    directions: tuple[Direction, ...]
    scratch_exec: bool  # candidates compile binaries into /scratch and run them there

    def settings(self, config: RunConfig) -> dict[str, object]:
        """The benchmark's own config section."""
        ...

    def validate(self, config: RunConfig) -> None:
        """Raise ValueError when the benchmark's settings in `config` are malformed."""
        ...

    def live(self, config: RunConfig) -> LiveTask:
        """Frozen panel/contract for real execution; raises when prerequisites are missing."""
        ...

    def image(self, config: RunConfig) -> str:
        """Pinned candidate container image."""
        ...

    def seed_solver(self, config: RunConfig) -> str:
        """Path of the common seed candidate directory."""
        ...

    def solver_argv(self, config: RunConfig) -> tuple[str, ...]:
        """Command that runs a candidate inside its container, reading the instance on stdin."""
        ...

    def smoke_check(self, config: RunConfig, *, require_file: bool = False,
                    missing_message: str = ...) -> SmokeCheck:
        """`check(candidate_path, runner)` for the fixed pre-evaluation toy run, input disjoint from the panel."""
        ...

    def evaluate_final(self, engine: ExperimentEngine, source_episode_id: str,
                       manifest_path: str) -> dict[str, object]:
        """Held-out evaluation of a completed episode's champion."""
        ...


BENCHMARKS: dict[str, str] = {
    'pace': 'maestro.tasks.pace.benchmark:PACE',
    'ale': 'maestro.tasks.ale.benchmark:ALE',
    'frontieror': 'maestro.tasks.frontieror.benchmark:FRONTIEROR',
}


def benchmark_for(name: str) -> Benchmark:
    """The registered adapter for a benchmark name."""
    if name not in BENCHMARKS:
        raise ValueError(f'unknown benchmark {name!r}; registered: {sorted(BENCHMARKS)}')
    module, attribute = BENCHMARKS[name].split(':')
    return cast('Benchmark', getattr(import_module(module), attribute))
