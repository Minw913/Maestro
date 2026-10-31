"""Finding a finished episode's champion for a benchmark's own final evaluation."""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

from maestro.tasks import fields
from maestro.types import IntegrityError, MaestroError

if TYPE_CHECKING:
    from maestro.loop import ExperimentEngine


def champion_path(engine: ExperimentEngine, episode_id: str) -> tuple[str, Path]:
    """(champion hash, frozen candidate directory) from the episode's last settled checkpoint."""
    connection = engine.ledger.connection
    row = cast(tuple[str] | None, connection.execute(
        'SELECT payload FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',
        (episode_id,)).fetchone())
    if row is None:
        raise MaestroError('source episode has no settled checkpoint')
    board = fields.section(fields.parse_object(row[0], 'checkpoint'), 'board')
    champion = fields.text(board, 'champion_hash')
    candidate = cast(tuple[str] | None, connection.execute(
        'SELECT payload FROM candidates WHERE hash=?', (champion,)).fetchone())
    if candidate is None:
        raise IntegrityError('champion has no recorded candidate')
    return champion, Path(fields.text(fields.parse_object(candidate[0], 'candidate'), 'candidate_path'))


def evaluate_frozen_final(root: str | Path, episode_id: str, manifest_path: str) -> dict[str, object]:
    """Dispatch final evaluation from the frozen task contract without an agent backend."""
    import json
    from types import SimpleNamespace
    from maestro.config import RunConfig
    from maestro.execution.pipeline import read_episode
    from maestro.execution.cpu_pool import GlobalCpuPool
    from maestro.execution.sandbox import DockerRunner
    from maestro.storage import Ledger
    from maestro.tasks.base import benchmark_for
    from maestro.types import stable_hash

    source = read_episode(root, episode_id)
    raw = json.loads(source['config_json'])
    if (source['status'] != 'COMPLETED' or source['execution_mode'] != 'real'
            or source['policy'] in ('warm', 'heldout')):
        raise MaestroError('final evaluation requires a completed real main episode')
    if stable_hash(raw) != source['config_hash']:
        raise IntegrityError('source episode configuration is corrupted')
    if raw.get('benchmark', 'pace') == 'pace':
        from maestro.tasks.pace.heldout import evaluate_frozen_final as evaluate_pace
        return evaluate_pace(root, episode_id, manifest_path)
    # Scheduling and wallet fields from retired searches do not govern task-only
    # evaluation. Keep the original record untouched, as in the PACE entry.
    wanted = {'benchmark', 'task', 'pace', 'coral', 'cpu_workers', 'instance_workers',
              'evaluation_instance_count', 'cpu_seconds_per_instance',
              'verification_cpu_seconds', 'memory_bytes', 'pids_max', 'output_bytes', 'seed'}
    config = RunConfig(**{key: value for key, value in raw.items() if key in wanted},
                       execution_mode='real')
    benchmark = benchmark_for(config.benchmark)
    live = benchmark.live(config)
    evaluator = live.evaluator(runner=DockerRunner(benchmark.image(config)),
                              artifact_store=None, cpu_pool=GlobalCpuPool(config.cpu_workers))
    preflight = getattr(evaluator, 'preflight', None)
    if not callable(preflight):
        raise MaestroError('Final evaluation requires benchmark preflight')
    preflight()
    root = Path(root).resolve()
    with Ledger(root / 'authority' / 'ledger.sqlite') as ledger:
        context = SimpleNamespace(root=root, config=config, ledger=ledger)
        return benchmark.evaluate_final(context, episode_id, manifest_path)
