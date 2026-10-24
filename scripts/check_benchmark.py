"""Checks a benchmark experiment end to end without any model: the seed's precheck, then a formal score of the
seed through the engine's own path (ticket, dispatcher, evaluator). Writes only under OUTPUT_ROOT."""
import argparse
import json
import time

from maestro.backends.coral.workspace import WorkspaceStore
from maestro.config import RunConfig, load_config
from maestro.execution.sandbox import DockerRunner
from maestro.loop import ExperimentEngine
from maestro.tasks import fields
from maestro.tasks.base import benchmark_for
from maestro.types import AgentState


class NoModelBackend:
    """Supplies only the contained runner; no agent, planner or model is involved."""

    def __init__(self, runner: DockerRunner) -> None:
        self.candidate_runner: DockerRunner = runner

    def preflight(self, config: RunConfig) -> dict[str, object]:
        del config
        return {}


def main(experiment: str, root: str) -> None:
    config = load_config(experiment, stage='warm_start')
    benchmark = benchmark_for(config.benchmark)
    runner = DockerRunner(benchmark.image(config))
    started = time.monotonic()
    passed, reason = benchmark.smoke_check(config)(benchmark.seed_solver(config), runner)
    print(json.dumps({'precheck': passed, 'reason': reason, 'seconds': round(time.monotonic() - started, 1)}))
    if not passed:
        raise SystemExit('the seed fails its own precheck')
    engine = ExperimentEngine(root, config, backend=NoModelBackend(runner))
    try:
        candidate = WorkspaceStore(engine.root / 'work', engine.root / 'cas').freeze(benchmark.seed_solver(config))
        candidate_hash = fields.text(candidate, 'candidate_hash')
        states = {'agent-1': AgentState('agent-1', candidate_hash, 0.0, config.initial_wealth)}
        engine.ledger.create_episode('check', 'warm', config, states)
        engine.ledger.record_candidate(candidate_hash, candidate)
        started = time.monotonic()
        score = engine._evaluate_seed('check', states, candidate)  # pyright: ignore[reportPrivateUsage]  # the engine's own setup path
        print(json.dumps({'formal_F': score, 'seconds': round(time.monotonic() - started, 1)}))
    finally:
        engine.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument('experiment', help='layered experiment config (execution_mode: real)')
    _ = parser.add_argument('output_root', help='fresh directory for the check ledger')
    options = fields.mapping(vars(parser.parse_args()), 'arguments')
    main(fields.text(options, 'experiment'), fields.text(options, 'output_root'))
