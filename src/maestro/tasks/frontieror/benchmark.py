"""FrontierOR adapter: large-scale OR algorithm design, each instance solved and checked in a contained run."""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from maestro.backends.base import BackendUnavailable
from maestro.types import EvaluationResult, IntegrityError, MaestroError, stable_hash

if TYPE_CHECKING:
    from maestro.config import RunConfig

MANIFEST_FIELDS = {'schema_version', 'paper_id', 'direction', 'frontieror_revision', 'time_limit',
                   'smoke_time_limit', 'panel', 'heldout', 'smoke', 'checker_sha256', 'reference_source'}
ENTRY = 'code.py'  # FrontierOR's candidate entry point
# FrontierOR's CLI contract: the program reads --instance_path and writes --solution_path within --time_limit.
_SOLVE = ('cat > /scratch/instance.json && python3 /candidate/code.py --instance_path /scratch/instance.json '
          '--solution_path /scratch/solution.json --time_limit {limit} >&2 && cat /scratch/solution.json')
_CHECK = ('cat > /scratch/solution.json && python3 /input/checker/feasibility_check.py --instance_path '
          '/input/instances/{name} --solution_path /scratch/solution.json --result_path /scratch/result.json >&2 '
          '&& cat /scratch/result.json')


def instance_score(objective: float, reference: float, direction: str) -> float:
    """Reference-normalized score in [0,1]: r/s when minimizing, s/r when maximizing; 1 once the reference is matched."""
    beats = objective <= reference if direction == 'min' else objective >= reference
    if beats:
        return 1.0
    if objective <= 0:
        return 0.0
    return reference / objective if direction == 'min' else objective / reference


@dataclass(frozen=True)
class FrontierManifest:
    schema_version: int
    paper_id: str
    direction: str
    frontieror_revision: str
    time_limit: int
    smoke_time_limit: int
    panel: tuple[dict[str, Any], ...]  # ({instance, file, input_sha256, reference_objective}, ...)
    heldout: tuple[dict[str, Any], ...]
    smoke: dict[str, Any]
    checker_sha256: str
    reference_source: str

    @classmethod
    def load(cls, path: str | Path) -> FrontierManifest:
        raw = json.loads(Path(path).read_text(encoding='utf-8'))
        if not isinstance(raw, dict) or set(raw) != MANIFEST_FIELDS or raw['schema_version'] != 1:
            raise ValueError(f'FrontierOR manifest needs exactly {sorted(MANIFEST_FIELDS)} with schema_version 1')
        if raw['direction'] not in ('min', 'max'):
            raise ValueError('direction must be min or max')
        for key in ('panel', 'heldout'):
            if any(set(e) != {'instance', 'file', 'input_sha256', 'reference_objective'}
                   or e['reference_objective'] <= 0 for e in raw[key]):
                raise ValueError(f'every {key} entry needs a positive reference_objective')
            raw[key] = tuple(raw[key])
        if not raw['panel']:
            raise ValueError('FrontierOR panel needs at least one instance')
        return cls(**raw)

    @property
    def panel_hash(self) -> str:
        return stable_hash({'benchmark': 'frontieror', **asdict(self)})


@dataclass(frozen=True)
class LiveFrontier:
    panel: FrontierManifest
    contract: dict[str, Any]
    config: RunConfig

    @property
    def panel_hash(self) -> str:
        return self.panel.panel_hash

    @property
    def contract_hash(self) -> str:
        return stable_hash(self.contract)

    def evaluator(self, *, runner: Any, artifact_store: Any, cpu_pool: Any) -> FrontierEvaluator:
        return FrontierEvaluator(self, runner=runner, cpu_pool=cpu_pool)


def _data_dir(config: RunConfig) -> Path:
    return Path(config.task['paths']['manifest']).parent


def solve_and_check(candidate: Path, entry: dict[str, Any], time_limit: int, *, data_dir: Path, image: str,
                    runner: Any, contract: dict[str, Any], config: RunConfig) -> dict[str, Any]:
    """Run the candidate on one instance, then the trusted checker on its answer; both contained."""
    from maestro.execution.sandbox import Mount, ResourceLimits, RunRequest
    instance_text = (data_dir / 'instances' / entry['file']).read_text(encoding='utf-8')
    if hashlib.sha256(instance_text.encode()).hexdigest() != entry['input_sha256']:
        raise IntegrityError(f"instance {entry['instance']} changed after the panel froze")
    solve = runner.run(RunRequest(('/bin/sh', '-c', _SOLVE.format(limit=time_limit)), candidate, instance_text,
                                  (Mount(candidate, '/candidate', True),), image),
                       ResourceLimits(cpu_seconds=time_limit, wall_seconds=time_limit,
                                      memory_mb=contract['memory_mb'], pids=config.pids_max,
                                      output_bytes=contract['solution_bytes'], cpus=1))
    outcome = {'instance': entry['instance'], 'cpu_seconds': solve.cpu_seconds, 'wall_seconds': solve.wall_seconds,
               'timed_out': solve.timed_out, 'valid': False, 'objective': None}
    if solve.timed_out or solve.returncode != 0 or not solve.stdout.strip():
        return {**outcome, 'reason': 'candidate failed, timed out or wrote no solution', 'stderr': solve.stderr[-500:]}
    check = runner.run(RunRequest(('/bin/sh', '-c', _CHECK.format(name=entry['file'])), candidate, solve.stdout,
                                  (Mount(data_dir / 'instances', '/input/instances', True),
                                   Mount(data_dir / 'checker', '/input/checker', True)), image),
                       ResourceLimits(cpu_seconds=contract['checker_seconds'], wall_seconds=contract['checker_seconds'],
                                      memory_mb=contract['memory_mb'], pids=config.pids_max,
                                      output_bytes=config.output_bytes, cpus=1))
    outcome['cpu_seconds'] += check.cpu_seconds
    try:
        verdict = json.loads(check.stdout)
        objective = json.loads(solve.stdout).get('objective_value')
    except ValueError:
        return {**outcome, 'reason': 'checker or solution output is not JSON', 'stderr': check.stderr[-500:]}
    if check.returncode != 0 or verdict.get('feasible') is not True or not isinstance(objective, (int, float)):
        return {**outcome, 'reason': 'infeasible: ' + ', '.join(map(str, verdict.get('violated_constraints', [])))}
    return {**outcome, 'valid': True, 'objective': float(objective), 'reason': 'feasible'}


class FrontierEvaluator:
    def __init__(self, live: LiveFrontier, *, runner: Any, cpu_pool: Any):
        self.live, self.runner, self.cpu_pool = live, runner, cpu_pool
        self.data_dir = _data_dir(live.config)

    def preflight(self) -> dict[str, Any]:
        checker = self.data_dir / 'checker' / 'feasibility_check.py'
        if hashlib.sha256(checker.read_bytes()).hexdigest() != self.live.panel.checker_sha256:
            raise IntegrityError('FrontierOR checker differs from the one the manifest froze')
        if self.runner is None or not hasattr(self.runner, 'preflight'):
            raise MaestroError('FrontierOR evaluation requires a contained candidate runner')
        self.runner.preflight(image=self.live.contract['image'])
        return {'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash}

    def evaluate(self, ticket: dict[str, Any]) -> EvaluationResult:
        from concurrent.futures import ThreadPoolExecutor
        from contextvars import copy_context

        from maestro.backends.coral.workspace import candidate_tree_hash
        from maestro.execution.authorization import require_execution_authority

        require_execution_authority(ticket, begin=True)
        for key in ('job_id', 'candidate_hash', 'panel_hash', 'contract_hash', 'purpose', 'candidate_path'):
            if not ticket.get(key):
                raise ValueError(f'trusted evaluation ticket is missing {key}')
        if ticket['panel_hash'] != self.live.panel_hash or ticket['contract_hash'] != self.live.contract_hash:
            raise ValueError('ticket panel/contract binding mismatch')
        if ticket['purpose'] not in {'SETUP', 'WARM', 'MAIN'}:
            raise ValueError('FrontierOR training evaluation accepts SETUP, WARM or MAIN only')
        candidate = Path(ticket['candidate_path']).resolve(strict=True)
        if candidate_tree_hash(candidate) != ticket['candidate_hash']:
            raise ValueError('candidate content differs from ticket-bound immutable hash')
        manifest, started = self.live.panel, time.monotonic()

        def one(entry: dict[str, Any]) -> dict[str, Any]:
            with self.cpu_pool.slot():
                require_execution_authority(ticket, launch=True)
                return solve_and_check(candidate, entry, manifest.time_limit, data_dir=self.data_dir,
                                       image=self.live.contract['image'], runner=self.runner,
                                       contract=self.live.contract, config=self.live.config)

        width = ticket.get('effective_instance_workers', self.live.config.instance_workers)
        # The dispatcher's ticket claim is a context variable; worker threads must carry it.
        with ThreadPoolExecutor(max_workers=width) as pool:
            outcomes = [f.result() for f in [pool.submit(copy_context().run, one, entry) for entry in manifest.panel]]
        for outcome, entry in zip(outcomes, manifest.panel, strict=True):
            outcome['reference_objective'] = entry['reference_objective']
            outcome['score'] = (instance_score(outcome['objective'], entry['reference_objective'], manifest.direction)
                                if outcome['valid'] else 0.0)
        valid = all(o['valid'] for o in outcomes)
        score = sum(o['score'] for o in outcomes) / len(outcomes) if valid else 0.0
        return EvaluationResult(
            job_id=ticket['job_id'], candidate_hash=ticket['candidate_hash'], score=score, valid=valid,
            status='COMPLETED' if valid else 'INVALID', cpu_seconds=sum(o['cpu_seconds'] for o in outcomes),
            wall_seconds=time.monotonic() - started,
            diagnostics={'label': 'frontieror_gurobi_normalized', 'purpose': ticket['purpose'],
                         'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash,
                         'paper_id': manifest.paper_id, 'time_limit': manifest.time_limit,
                         'invalids': sum(not o['valid'] for o in outcomes),
                         'matched_reference': sum(o['score'] >= 1.0 for o in outcomes)},
            instance_results=tuple(outcomes))


class FrontierBenchmark:
    name = 'frontieror'
    scratch_exec = False  # candidates are Python run from /candidate
    # Same four routes as the other heuristic-search benchmarks, phrased for OR algorithm design.
    directions = (
        ("constructive_start", ("initialization", "reduction"),
         "Explore greedy, dispatching-rule and decomposition constructions.",
         "Raise the quality of the first feasible solution before any search."),
        ("local_improvement", ("local_search", "initialization"),
         "Explore neighbourhoods, local search and metaheuristics.",
         "Improve the objective through accepted local moves."),
        ("search_diversification", ("restart", "initialization"),
         "Explore restarts, perturbation and population diversity.",
         "Escape plateaus that trap a single search trajectory."),
        ("execution_efficiency", ("time_management", "initialization"),
         "Explore incremental evaluation and use of the time limit.",
         "Fit more useful search into the fixed time limit."),
    )

    def settings(self, config: RunConfig) -> dict[str, Any]:
        return config.task

    def validate(self, config: RunConfig) -> None:
        if config.pace:
            raise ValueError('benchmark=frontieror takes its settings from task:, not pace:')
        if config.execution_mode != 'real':
            return
        paths, contract = config.task.get('paths', {}), config.task.get('contract', {})
        missing = [f'task.paths.{k}' for k in ('manifest', 'seed_solver') if not paths.get(k)]
        missing += [f'task.contract.{k}' for k in ('image', 'memory_mb', 'solution_bytes', 'checker_seconds')
                    if not contract.get(k)]
        if missing:
            raise ValueError('benchmark=frontieror with execution_mode=real requires ' + ', '.join(missing))

    def live(self, config: RunConfig) -> LiveFrontier:
        manifest = FrontierManifest.load(config.task['paths']['manifest'])
        if config.evaluation_instance_count != len(manifest.panel):
            raise IntegrityError('evaluation_instance_count differs from the FrontierOR panel size')
        if config.cpu_seconds_per_instance != manifest.time_limit:
            raise IntegrityError('cpu_seconds_per_instance differs from the manifest time_limit')
        if manifest.smoke_time_limit + config.task['contract']['checker_seconds'] >= config.generation_window_seconds:
            raise IntegrityError('the tiny precheck and its checker must fit inside generation_window_seconds')
        contract = {'benchmark': 'frontieror', 'panel_hash': manifest.panel_hash, **config.task['contract'],
                    'cpu_workers': config.cpu_workers, 'instance_workers': config.instance_workers}
        return LiveFrontier(manifest, contract, config)

    def image(self, config: RunConfig) -> str:
        return config.task.get('contract', {}).get('image', '')

    def seed_solver(self, config: RunConfig) -> str:
        return config.task.get('paths', {}).get('seed_solver', '')

    def solver_argv(self, config: RunConfig) -> tuple[str, ...]:
        manifest = FrontierManifest.load(config.task['paths']['manifest'])
        return ('/bin/sh', '-c', _SOLVE.format(limit=manifest.time_limit))

    def smoke_check(self, config: RunConfig, *, require_file: bool = False,
                    missing_message: str = 'Configured FrontierOR smoke instance is missing'):
        manifest = FrontierManifest.load(config.task['paths']['manifest'])
        data_dir, entry = _data_dir(config), manifest.smoke
        if not (data_dir / 'instances' / entry['file']).is_file():
            raise BackendUnavailable(missing_message)
        if entry['input_sha256'] in {e['input_sha256'] for e in (*manifest.panel, *manifest.heldout)}:
            raise IntegrityError('FrontierOR smoke instance is part of the panel or held-out set')
        image, contract = self.image(config), config.task['contract']

        def check(candidate_path: Any, runner: Any) -> tuple[bool, str]:
            if not (Path(candidate_path) / ENTRY).is_file():
                return False, f'candidate has no {ENTRY}'
            outcome = solve_and_check(Path(candidate_path).resolve(strict=True), entry, manifest.smoke_time_limit,
                                      data_dir=data_dir, image=image, runner=runner, contract=contract, config=config)
            return (True, 'pass') if outcome['valid'] else (False, outcome['reason'])
        return check

    def evaluate_final(self, engine: Any, source_episode_id: str, manifest_path: str) -> dict[str, Any]:
        # shortcut: scores the final champion on the held-out instances as one ledger event, without a
        # held-out ticket; upgrade to PACE's separate held-out episode before publishing held-out results.
        from maestro.execution.sandbox import DockerRunner
        config = engine.config
        manifest = FrontierManifest.load(config.task['paths']['manifest'])
        row = engine.ledger.connection.execute(
            'SELECT payload FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',
            (source_episode_id,)).fetchone()
        if row is None:
            raise MaestroError('source episode has no settled checkpoint')
        champion = json.loads(row[0])['board']['champion_hash']
        candidate = Path(json.loads(engine.ledger.connection.execute(
            'SELECT payload FROM candidates WHERE hash=?', (champion,)).fetchone()[0])['candidate_path'])
        runner = DockerRunner(self.image(config))
        outcomes = [solve_and_check(candidate, entry, manifest.time_limit, data_dir=_data_dir(config),
                                    image=self.image(config), runner=runner, contract=config.task['contract'],
                                    config=config) for entry in manifest.heldout]
        scores = [instance_score(o['objective'], e['reference_objective'], manifest.direction) if o['valid'] else 0.0
                  for o, e in zip(outcomes, manifest.heldout, strict=True)]
        summary = {'source_episode_id': source_episode_id, 'champion_hash': champion,
                   'heldout_score': sum(scores) / len(scores) if scores else None,
                   'instances': [{'instance': o['instance'], 'valid': o['valid'], 'objective': o['objective'],
                                  'score': s} for o, s in zip(outcomes, scores, strict=True)]}
        engine.ledger.event('FRONTIEROR_HELDOUT_EVAL', summary, source_episode_id)
        return summary


FRONTIEROR = FrontierBenchmark()
