"""FrontierOR adapter: large-scale OR algorithm design, each instance solved and checked in a contained run."""
from __future__ import annotations

import hashlib
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast, override, runtime_checkable

from maestro.backends.base import BackendUnavailable
from maestro.tasks import fields
from maestro.types import EvaluationResult, IntegrityError, MaestroError, stable_hash

if TYPE_CHECKING:
    from maestro.config import RunConfig
    from maestro.execution.cpu_pool import GlobalCpuPool
    from maestro.execution.sandbox import ContainmentRunner
    from maestro.loop import ExperimentEngine
    from maestro.tasks.base import ArtifactStore, SmokeCheck, Ticket

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
class InstanceEntry:
    instance: str
    file: str
    input_sha256: str

    @classmethod
    def parse(cls, raw: fields.Fields) -> InstanceEntry:
        return cls(fields.text(raw, 'instance'), fields.text(raw, 'file'), fields.text(raw, 'input_sha256'))


@dataclass(frozen=True)
class PanelEntry(InstanceEntry):
    reference_objective: float  # Gurobi's best objective within one hour

    @override
    @classmethod
    def parse(cls, raw: fields.Fields) -> PanelEntry:
        if set(raw) != {'instance', 'file', 'input_sha256', 'reference_objective'}:
            raise ValueError('a panel entry needs instance, file, input_sha256 and reference_objective')
        reference = fields.number(raw, 'reference_objective')
        if reference <= 0:
            raise ValueError('reference_objective must be positive')
        return cls(fields.text(raw, 'instance'), fields.text(raw, 'file'), fields.text(raw, 'input_sha256'), reference)


@dataclass(frozen=True)
class FrontierManifest:
    schema_version: int
    paper_id: str
    direction: str
    frontieror_revision: str
    time_limit: int
    smoke_time_limit: int
    panel: tuple[PanelEntry, ...]
    heldout: tuple[PanelEntry, ...]
    smoke: InstanceEntry
    checker_sha256: str
    reference_source: str

    @classmethod
    def load(cls, path: str | Path) -> FrontierManifest:
        raw = fields.parse_object(Path(path).read_text(encoding='utf-8'), 'FrontierOR manifest')
        if set(raw) != MANIFEST_FIELDS or raw['schema_version'] != 1:
            raise ValueError(f'FrontierOR manifest needs exactly {sorted(MANIFEST_FIELDS)} with schema_version 1')
        direction = fields.text(raw, 'direction')
        if direction not in ('min', 'max'):
            raise ValueError('direction must be min or max')
        panel = tuple(PanelEntry.parse(e) for e in fields.objects(raw, 'panel'))
        if not panel:
            raise ValueError('FrontierOR panel needs at least one instance')
        return cls(fields.integer(raw, 'schema_version'), fields.text(raw, 'paper_id'), direction,
                   fields.text(raw, 'frontieror_revision'), fields.integer(raw, 'time_limit'),
                   fields.integer(raw, 'smoke_time_limit'), panel,
                   tuple(PanelEntry.parse(e) for e in fields.objects(raw, 'heldout')),
                   InstanceEntry.parse(fields.section(raw, 'smoke')), fields.text(raw, 'checker_sha256'),
                   fields.text(raw, 'reference_source'))

    @property
    def panel_hash(self) -> str:
        return stable_hash({'benchmark': 'frontieror', **asdict(self)})


@dataclass(frozen=True)
class FrontierContract:
    """Per-instance container limits from the experiment's task.contract."""
    image: str
    memory_mb: int
    solution_bytes: int
    checker_seconds: float

    @classmethod
    def of(cls, config: RunConfig) -> FrontierContract:
        contract = _contract(config)
        return cls(fields.text(contract, 'image'), fields.integer(contract, 'memory_mb'),
                   fields.integer(contract, 'solution_bytes'), fields.number(contract, 'checker_seconds'))


@dataclass(frozen=True)
class Outcome:
    """One instance: the candidate's run and the trusted checker's verdict."""
    instance: str
    cpu_seconds: float
    wall_seconds: float
    timed_out: bool
    valid: bool
    objective: float | None
    reason: str
    stderr: str = ''

    def record(self, reference: float, score: float) -> dict[str, object]:
        """The ledger's per-instance result."""
        return {'instance': self.instance, 'cpu_seconds': self.cpu_seconds, 'wall_seconds': self.wall_seconds,
                'timed_out': self.timed_out, 'valid': self.valid, 'objective': self.objective,
                'reason': self.reason, 'stderr': self.stderr, 'reference_objective': reference, 'score': score}


def _paths(config: RunConfig) -> dict[str, object]:
    return fields.section(config.task, 'paths')


def _contract(config: RunConfig) -> dict[str, object]:
    return fields.section(config.task, 'contract')


def _data_dir(config: RunConfig) -> Path:
    return Path(fields.text(_paths(config), 'manifest')).parent


@runtime_checkable
class _Preflighted(Protocol):
    def preflight(self, image: str | None = None) -> object: ...


@dataclass(frozen=True)
class LiveFrontier:
    panel: FrontierManifest
    contract: dict[str, object]
    config: RunConfig

    @property
    def panel_hash(self) -> str:
        return self.panel.panel_hash

    @property
    def contract_hash(self) -> str:
        return stable_hash(self.contract)

    def evaluator(self, *, runner: ContainmentRunner | None, artifact_store: ArtifactStore | None,
                  cpu_pool: GlobalCpuPool) -> FrontierEvaluator:
        del artifact_store  # solutions are scored, not archived
        if runner is None:
            raise MaestroError('FrontierOR evaluation requires a contained candidate runner')
        return FrontierEvaluator(self, runner=runner, cpu_pool=cpu_pool)


def solve_and_check(candidate: Path, entry: InstanceEntry, time_limit: int, *, data_dir: Path,
                    runner: ContainmentRunner, limits: FrontierContract, config: RunConfig) -> Outcome:
    """Run the candidate on one instance, then the trusted checker on its answer; both contained."""
    from maestro.execution.sandbox import Mount, ResourceLimits, RunRequest
    instance_text = (data_dir / 'instances' / entry.file).read_text(encoding='utf-8')
    if hashlib.sha256(instance_text.encode()).hexdigest() != entry.input_sha256:
        raise IntegrityError(f'instance {entry.instance} changed after the panel froze')
    solve = runner.run(RunRequest(('/bin/sh', '-c', _SOLVE.format(limit=time_limit)), candidate, instance_text,
                                  (Mount(candidate, '/candidate', True),), limits.image),
                       ResourceLimits(cpu_seconds=time_limit, wall_seconds=time_limit, memory_mb=limits.memory_mb,
                                      pids=config.pids_max, output_bytes=limits.solution_bytes, cpus=1))
    failed = Outcome(entry.instance, solve.cpu_seconds, solve.wall_seconds, solve.timed_out, False, None, '')
    if solve.timed_out or solve.returncode != 0 or not solve.stdout.strip():
        return replace(failed, reason='candidate failed, timed out or wrote no solution', stderr=solve.stderr[-500:])
    check = runner.run(RunRequest(('/bin/sh', '-c', _CHECK.format(name=entry.file)), candidate, solve.stdout,
                                  (Mount(data_dir / 'instances', '/input/instances', True),
                                   Mount(data_dir / 'checker', '/input/checker', True)), limits.image),
                       ResourceLimits(cpu_seconds=limits.checker_seconds, wall_seconds=limits.checker_seconds,
                                      memory_mb=limits.memory_mb, pids=config.pids_max,
                                      output_bytes=config.output_bytes, cpus=1))
    cpu = solve.cpu_seconds + check.cpu_seconds
    try:
        verdict = fields.parse_object(check.stdout, 'checker result')
        objective = fields.parse_object(solve.stdout, 'solution').get('objective_value')
    except ValueError:
        return Outcome(entry.instance, cpu, solve.wall_seconds, False, False, None,
                       'checker or solution output is not JSON', check.stderr[-500:])
    violated = verdict.get('violated_constraints', [])
    violations = cast(list[object], violated) if isinstance(violated, list) else [violated]
    if (check.returncode != 0 or verdict.get('feasible') is not True or isinstance(objective, bool)
            or not isinstance(objective, int | float)):
        return Outcome(entry.instance, cpu, solve.wall_seconds, False, False, None,
                       'infeasible: ' + ', '.join(map(str, violations)))
    return Outcome(entry.instance, cpu, solve.wall_seconds, False, True, float(objective), 'feasible')


class FrontierEvaluator:
    def __init__(self, live: LiveFrontier, *, runner: ContainmentRunner, cpu_pool: GlobalCpuPool):
        self.live: LiveFrontier = live
        self.runner: ContainmentRunner = runner
        self.cpu_pool: GlobalCpuPool = cpu_pool
        self.data_dir: Path = _data_dir(live.config)
        self.limits: FrontierContract = FrontierContract.of(live.config)

    def preflight(self) -> dict[str, str]:
        checker = self.data_dir / 'checker' / 'feasibility_check.py'
        if hashlib.sha256(checker.read_bytes()).hexdigest() != self.live.panel.checker_sha256:
            raise IntegrityError('FrontierOR checker differs from the one the manifest froze')
        if not isinstance(self.runner, _Preflighted):
            raise MaestroError('FrontierOR evaluation requires a runner that can verify its containment')
        _ = self.runner.preflight(image=self.limits.image)
        return {'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash}

    def evaluate(self, ticket: Ticket) -> EvaluationResult:
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
        purpose = fields.text(ticket, 'purpose')
        if purpose not in {'SETUP', 'WARM', 'MAIN'}:
            raise ValueError('FrontierOR training evaluation accepts SETUP, WARM or MAIN only')
        candidate = Path(fields.text(ticket, 'candidate_path')).resolve(strict=True)
        if candidate_tree_hash(candidate) != ticket['candidate_hash']:
            raise ValueError('candidate content differs from ticket-bound immutable hash')
        manifest, started = self.live.panel, time.monotonic()

        def one(entry: PanelEntry) -> Outcome:
            with self.cpu_pool.slot():
                require_execution_authority(ticket, launch=True)
                return solve_and_check(candidate, entry, manifest.time_limit, data_dir=self.data_dir,
                                       runner=self.runner, limits=self.limits, config=self.live.config)

        width = (fields.integer(ticket, 'effective_instance_workers') if 'effective_instance_workers' in ticket
                 else self.live.config.instance_workers)
        # The dispatcher's ticket claim is a context variable; worker threads must carry it.
        with ThreadPoolExecutor(max_workers=width) as pool:
            outcomes = [f.result() for f in [pool.submit(copy_context().run, one, entry) for entry in manifest.panel]]
        scores = [instance_score(o.objective, e.reference_objective, manifest.direction)
                  if o.valid and o.objective is not None else 0.0 for o, e in zip(outcomes, manifest.panel, strict=True)]
        valid = all(o.valid for o in outcomes)
        return EvaluationResult(
            job_id=fields.text(ticket, 'job_id'), candidate_hash=fields.text(ticket, 'candidate_hash'),
            score=sum(scores) / len(scores) if valid else 0.0, valid=valid,
            status='COMPLETED' if valid else 'INVALID', cpu_seconds=sum(o.cpu_seconds for o in outcomes),
            wall_seconds=time.monotonic() - started,
            diagnostics={'label': 'frontieror_gurobi_normalized', 'purpose': purpose,
                         'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash,
                         'paper_id': manifest.paper_id, 'time_limit': manifest.time_limit,
                         'invalids': sum(not o.valid for o in outcomes),
                         'matched_reference': sum(score >= 1.0 for score in scores)},
            instance_results=tuple(o.record(e.reference_objective, s)
                                   for o, e, s in zip(outcomes, manifest.panel, scores, strict=True)))


class FrontierBenchmark:
    name: str = 'frontieror'
    scratch_exec: bool = False  # candidates are Python run from /candidate
    # Same four routes as the other heuristic-search benchmarks, phrased for OR algorithm design.
    directions: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
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

    def settings(self, config: RunConfig) -> dict[str, object]:
        return fields.mapping(config.task, 'task')

    def validate(self, config: RunConfig) -> None:
        if config.pace:
            raise ValueError('benchmark=frontieror takes its settings from task:, not pace:')
        if config.execution_mode != 'real':
            return
        paths, contract = _paths(config), _contract(config)
        missing = [f'task.paths.{k}' for k in ('manifest', 'seed_solver') if not paths.get(k)]
        missing += [f'task.contract.{k}' for k in ('image', 'memory_mb', 'solution_bytes', 'checker_seconds')
                    if not contract.get(k)]
        if missing:
            raise ValueError('benchmark=frontieror with execution_mode=real requires ' + ', '.join(missing))

    def live(self, config: RunConfig) -> LiveFrontier:
        manifest = FrontierManifest.load(fields.text(_paths(config), 'manifest'))
        if config.evaluation_instance_count != len(manifest.panel):
            raise IntegrityError('evaluation_instance_count differs from the FrontierOR panel size')
        if config.cpu_seconds_per_instance != manifest.time_limit:
            raise IntegrityError('cpu_seconds_per_instance differs from the manifest time_limit')
        if manifest.smoke_time_limit + FrontierContract.of(config).checker_seconds >= config.generation_window_seconds:
            raise IntegrityError('the tiny precheck and its checker must fit inside generation_window_seconds')
        contract: dict[str, object] = {'benchmark': 'frontieror', 'panel_hash': manifest.panel_hash,
                                       **_contract(config), 'cpu_workers': config.cpu_workers,
                                       'instance_workers': config.instance_workers}
        return LiveFrontier(manifest, contract, config)

    def image(self, config: RunConfig) -> str:
        image = _contract(config).get('image', '')
        return image if isinstance(image, str) else ''

    def seed_solver(self, config: RunConfig) -> str:
        seed = _paths(config).get('seed_solver', '')
        return seed if isinstance(seed, str) else ''

    def solver_argv(self, config: RunConfig) -> tuple[str, ...]:
        manifest = FrontierManifest.load(fields.text(_paths(config), 'manifest'))
        return ('/bin/sh', '-c', _SOLVE.format(limit=manifest.time_limit))

    def smoke_check(self, config: RunConfig, *, require_file: bool = False,
                    missing_message: str = 'Configured FrontierOR smoke instance is missing') -> SmokeCheck:
        del require_file  # the smoke instance is part of the frozen panel, so it is always required
        manifest = FrontierManifest.load(fields.text(_paths(config), 'manifest'))
        data_dir, entry, limits = _data_dir(config), manifest.smoke, FrontierContract.of(config)
        if not (data_dir / 'instances' / entry.file).is_file():
            raise BackendUnavailable(missing_message)
        if entry.input_sha256 in {e.input_sha256 for e in (*manifest.panel, *manifest.heldout)}:
            raise IntegrityError('FrontierOR smoke instance is part of the panel or held-out set')

        def check(candidate_path: str | Path, runner: ContainmentRunner) -> tuple[bool, str]:
            if not (Path(candidate_path) / ENTRY).is_file():
                return False, f'candidate has no {ENTRY}'
            outcome = solve_and_check(Path(candidate_path).resolve(strict=True), entry, manifest.smoke_time_limit,
                                      data_dir=data_dir, runner=runner, limits=limits, config=config)
            return (True, 'pass') if outcome.valid else (False, outcome.reason)
        return check

    def evaluate_final(self, engine: ExperimentEngine, source_episode_id: str,
                       manifest_path: str) -> dict[str, object]:
        # shortcut: scores the final champion on the held-out instances as one ledger event, without a
        # held-out ticket; upgrade to PACE's separate held-out episode before publishing held-out results.
        from maestro.execution.sandbox import DockerRunner
        from maestro.tasks.final import champion_path
        del manifest_path  # the held-out instances are frozen in the training manifest
        config = engine.config
        manifest = FrontierManifest.load(fields.text(_paths(config), 'manifest'))
        champion, candidate = champion_path(engine, source_episode_id)
        limits = FrontierContract.of(config)
        runner = DockerRunner(limits.image)
        outcomes = [solve_and_check(candidate, entry, manifest.time_limit, data_dir=_data_dir(config),
                                    runner=runner, limits=limits, config=config) for entry in manifest.heldout]
        scores = [instance_score(o.objective, e.reference_objective, manifest.direction)
                  if o.valid and o.objective is not None else 0.0
                  for o, e in zip(outcomes, manifest.heldout, strict=True)]
        summary: dict[str, object] = {
            'source_episode_id': source_episode_id, 'champion_hash': champion,
            'heldout_score': sum(scores) / len(scores) if scores else None,
            'instances': [{'instance': o.instance, 'valid': o.valid, 'objective': o.objective, 'score': s}
                          for o, s in zip(outcomes, scores, strict=True)]}
        engine.ledger.event('FRONTIEROR_HELDOUT_EVAL', summary, source_episode_id)
        return summary


FRONTIEROR = FrontierBenchmark()
