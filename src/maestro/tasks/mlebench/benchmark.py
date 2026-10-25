"""MLE-bench adapter: train a model for a Kaggle competition, scored by the fraction of the real leaderboard beaten.

Formal evaluations grade a validation split carved from the public training data; the private test is graded once,
on the final champion, so neither agents nor the market ever see a test score.
"""
from __future__ import annotations

import hashlib
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from maestro.tasks import fields
from maestro.types import EvaluationResult, IntegrityError, MaestroError, stable_hash

if TYPE_CHECKING:
    from maestro.config import RunConfig
    from maestro.execution.cpu_pool import GlobalCpuPool
    from maestro.execution.sandbox import ContainmentRunner
    from maestro.loop import ExperimentEngine
    from maestro.tasks.base import ArtifactStore, SmokeCheck, Ticket

MANIFEST_FIELDS = {'schema_version', 'competition', 'mlebench_revision', 'image', 'timeout_seconds', 'cpus',
                   'memory_mb', 'answers', 'hashes'}
SPLITS = ('train', 'final')  # train: validation split graded during the run; final: MLE-bench's private test
HASHED = ('train/inputs', 'train/answers', 'final/inputs', 'final/answers', 'leaderboard.csv')
# The solution writes its submission into scratch; stdout carries it back because host mounts are read-only.
_RUN = 'python3 /candidate/solution.py /input /scratch/submission.csv >&2 && cat /scratch/submission.csv'


@dataclass(frozen=True)
class MleBenchManifest:
    schema_version: int
    competition: str
    mlebench_revision: str
    image: str  # agents' tools and formal evaluations run here; it holds no competition data
    timeout_seconds: float
    cpus: float
    memory_mb: int
    answers: str  # the answers file name inside each split's answers directory
    hashes: dict[str, str]  # HASHED path -> its content hash, frozen when the panel was built

    @classmethod
    def load(cls, path: str | Path) -> MleBenchManifest:
        raw = fields.parse_object(Path(path).read_text(encoding='utf-8'), 'MLE-bench manifest')
        if set(raw) != MANIFEST_FIELDS or raw['schema_version'] != 1:
            raise ValueError(f'MLE-bench manifest needs exactly {sorted(MANIFEST_FIELDS)} with schema_version 1')
        hashes = fields.section(raw, 'hashes')
        if set(hashes) != set(HASHED):
            raise ValueError(f'hashes must name exactly {list(HASHED)}')
        return cls(fields.integer(raw, 'schema_version'), fields.text(raw, 'competition'),
                   fields.text(raw, 'mlebench_revision'), fields.text(raw, 'image'),
                   fields.number(raw, 'timeout_seconds'), fields.number(raw, 'cpus'), fields.integer(raw, 'memory_mb'),
                   fields.text(raw, 'answers'), {k: fields.text(hashes, k) for k in HASHED})

    @property
    def panel_hash(self) -> str:
        return stable_hash({'benchmark': 'mlebench', **asdict(self)})


def content_hash(path: Path) -> str:
    """sha256 of a file, or the tree hash of a directory."""
    from maestro.backends.coral.workspace import candidate_tree_hash
    return candidate_tree_hash(path) if path.is_dir() else hashlib.sha256(path.read_bytes()).hexdigest()


def _paths(config: RunConfig) -> dict[str, object]:
    return fields.section(config.task, 'paths')


def _contract(config: RunConfig) -> dict[str, object]:
    return fields.section(config.task, 'contract')


def _data_dir(config: RunConfig) -> Path:
    return Path(fields.text(_paths(config), 'manifest')).parent


def _python(config: RunConfig) -> str:
    # A venv directory, not its python: resolving the interpreter's symlink would leave the venv.
    return str(Path(fields.text(_paths(config), 'mlebench_venv')) / 'bin' / 'python')


def _cores(manifest: MleBenchManifest) -> int:
    return max(1, round(manifest.cpus))


@dataclass(frozen=True)
class Grade:
    """One graded run: the competition's own score and the leaderboard fraction it beats."""
    valid: bool
    score: float | None
    beaten: float
    any_medal: bool
    above_median: bool
    summary: str
    cpu_seconds: float
    wall_seconds: float
    timed_out: bool


def grade_submission(submission: str, split: str, *, data_dir: Path, manifest: MleBenchManifest,
                     mlebench_python: str) -> dict[str, object]:
    """MLE-bench's own grader on `submission`, run in MLE-bench's venv."""
    data_dir = data_dir.resolve()  # the grader runs from a throwaway directory
    with tempfile.TemporaryDirectory(prefix='mlebench-grade-') as scratch:
        path = Path(scratch) / 'submission.csv'
        _ = path.write_text(submission, encoding='utf-8')
        tool = Path(__file__).resolve().parents[4] / 'scripts' / 'mlebench_tool.py'
        try:
            done = subprocess.run([mlebench_python, '-I', str(tool), 'grade', '--competition', manifest.competition,
                                   '--data-dir', scratch, '--submission', str(path),
                                   '--answers', str(data_dir / split / 'answers' / manifest.answers),
                                   '--leaderboard', str(data_dir / 'leaderboard.csv')],
                                  # MLE-bench opens a disk cache in its working directory, so give it a throwaway one.
                                  capture_output=True, text=True, timeout=manifest.timeout_seconds, check=False,
                                  cwd=scratch)
        except subprocess.TimeoutExpired as exc:
            raise MaestroError('MLE-bench grader exceeded the task timeout') from exc
    if done.returncode != 0:
        raise MaestroError('MLE-bench grader failed: ' + done.stderr[-500:])
    return fields.parse_object(done.stdout.strip().splitlines()[-1], 'MLE-bench grade')


def run_and_grade(candidate: Path, split: str, *, data_dir: Path, manifest: MleBenchManifest,
                  runner: ContainmentRunner, pids: int, mlebench_python: str) -> Grade:
    """Run the candidate on `split`'s inputs, contained, and grade what it submits."""
    from maestro.execution.sandbox import Mount, ResourceLimits, RunRequest
    inputs = data_dir / split / 'inputs'
    # Inputs can be many GB, so they are hashed once per run (preflight, final); the small grading files every time.
    for key in (f'{split}/answers', 'leaderboard.csv'):
        if content_hash(data_dir / key) != manifest.hashes[key]:
            raise IntegrityError(f'{key} changed after the panel froze')
    receipt = runner.run(
        RunRequest(('/bin/sh', '-c', _RUN), candidate, '',
                   (Mount(candidate, '/candidate', True), Mount(inputs, '/input', True)), manifest.image),
        # The submission is written to the scratch tmpfs, which the runner sizes to memory_mb, so that bounds it.
        ResourceLimits(cpu_seconds=manifest.cpus * manifest.timeout_seconds, wall_seconds=manifest.timeout_seconds,
                       memory_mb=manifest.memory_mb, pids=pids, output_bytes=manifest.memory_mb * 2**20,
                       cpus=manifest.cpus))
    if receipt.returncode != 0 or receipt.timed_out:
        return Grade(False, None, 0.0, False, False, 'solution failed: ' + receipt.stderr[-300:], receipt.cpu_seconds,
                     receipt.wall_seconds, receipt.timed_out)
    result = grade_submission(receipt.stdout, split, data_dir=data_dir, manifest=manifest,
                              mlebench_python=mlebench_python)
    score = result.get('score')
    if not isinstance(score, int | float):
        return Grade(False, None, 0.0, False, False, 'the grader rejected the submission', receipt.cpu_seconds,
                     receipt.wall_seconds, False)
    beaten = fields.number(result, 'beaten')
    return Grade(True, float(score), beaten, fields.flag(result, 'any_medal'), fields.flag(result, 'above_median'),
                 f'score {score}, beats {beaten:.1%} of the leaderboard', receipt.cpu_seconds, receipt.wall_seconds,
                 False)


@dataclass(frozen=True)
class LiveMleBench:
    panel: MleBenchManifest
    contract: dict[str, object]
    config: RunConfig

    @property
    def panel_hash(self) -> str:
        return self.panel.panel_hash

    @property
    def contract_hash(self) -> str:
        return stable_hash(self.contract)

    def evaluator(self, *, runner: ContainmentRunner | None, artifact_store: ArtifactStore | None,
                  cpu_pool: GlobalCpuPool) -> MleBenchEvaluator:
        del artifact_store  # the grade is the whole result
        if runner is None:
            raise MaestroError('MLE-bench evaluation requires a contained runner')
        return MleBenchEvaluator(self, runner=runner, cpu_pool=cpu_pool)


class MleBenchEvaluator:
    def __init__(self, live: LiveMleBench, *, runner: ContainmentRunner, cpu_pool: GlobalCpuPool):
        self.live: LiveMleBench = live
        self.runner: ContainmentRunner = runner
        self.cpu_pool: GlobalCpuPool = cpu_pool
        self.data_dir: Path = _data_dir(live.config)

    def preflight(self) -> dict[str, str]:
        for key in HASHED:
            if content_hash(self.data_dir / key) != self.live.panel.hashes[key]:
                raise IntegrityError(f'{key} changed after the panel froze')
        return {'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash}

    def evaluate(self, ticket: Ticket) -> EvaluationResult:
        from maestro.backends.coral.workspace import candidate_tree_hash
        from maestro.execution.authorization import require_execution_authority
        from maestro.tasks.slots import held

        require_execution_authority(ticket, begin=True)
        for key in ('job_id', 'candidate_hash', 'panel_hash', 'contract_hash', 'purpose', 'candidate_path'):
            if not ticket.get(key):
                raise ValueError(f'trusted evaluation ticket is missing {key}')
        if ticket['panel_hash'] != self.live.panel_hash or ticket['contract_hash'] != self.live.contract_hash:
            raise ValueError('ticket panel/contract binding mismatch')
        purpose = fields.text(ticket, 'purpose')
        if purpose not in {'SETUP', 'WARM', 'MAIN'}:
            raise ValueError('MLE-bench training evaluation accepts SETUP, WARM or MAIN only')
        candidate = Path(fields.text(ticket, 'candidate_path')).resolve(strict=True)
        if candidate_tree_hash(candidate) != ticket['candidate_hash']:
            raise ValueError('candidate content differs from ticket-bound immutable hash')
        manifest, contract, started = self.live.panel, self.live.contract, time.monotonic()
        with held(self.cpu_pool, _cores(manifest)):
            require_execution_authority(ticket, launch=True)
            grade = run_and_grade(candidate, 'train', data_dir=self.data_dir, manifest=manifest, runner=self.runner,
                                  pids=fields.integer(contract, 'pids'),
                                  mlebench_python=_python(self.live.config))
        return EvaluationResult(
            job_id=fields.text(ticket, 'job_id'), candidate_hash=fields.text(ticket, 'candidate_hash'),
            score=grade.beaten, valid=grade.valid, status='COMPLETED' if grade.valid else 'INVALID',
            cpu_seconds=grade.cpu_seconds, wall_seconds=time.monotonic() - started,
            diagnostics={'label': 'mlebench_validation', 'purpose': purpose, 'competition': manifest.competition,
                         'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash,
                         'competition_score': grade.score, 'summary': grade.summary, 'timed_out': grade.timed_out},
            instance_results=({'instance': 'validation', 'valid': grade.valid, 'competition_score': grade.score,
                               'beaten': grade.beaten, 'summary': grade.summary, 'cpu_seconds': grade.cpu_seconds,
                               'wall_seconds': grade.wall_seconds},))


class MleBenchBenchmark:
    name: str = 'mlebench'
    scratch_exec: bool = False
    # shortcut: ML work mapped onto the shared heuristic-search targets, since taxonomy.TARGETS is core;
    # give MLE-bench its own vocabulary once the taxonomy is per benchmark.
    directions: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
        ("strong_baseline", ("initialization", "reduction"),
         "Explore a well-tuned standard model for this data type, end to end.",
         "Replace the sample submission with a real model that beats most of the leaderboard."),
        ("feature_engineering", ("data_structure", "initialization"),
         "Explore better input representations and features.",
         "Lift the validation score through what the model sees."),
        ("model_search", ("restart", "local_search"),
         "Explore different model families and their hyperparameters.",
         "Find the model family that fits this competition best."),
        ("ensembling", ("postprocess", "time_management"),
         "Explore blending, stacking and calibration within the time budget.",
         "Combine models for a score no single one reaches."),
    )

    def settings(self, config: RunConfig) -> dict[str, object]:
        return fields.mapping(config.task, 'task')

    def validate(self, config: RunConfig) -> None:
        if config.pace:
            raise ValueError('benchmark=mlebench takes its settings from task:, not pace:')
        if config.execution_mode != 'real':
            return
        paths, contract = _paths(config), _contract(config)
        missing = [f'task.paths.{k}' for k in ('manifest', 'seed_solver', 'mlebench_venv') if not paths.get(k)]
        missing += [f'task.contract.{k}' for k in ('pids',) if not contract.get(k)]
        if missing:
            raise ValueError('benchmark=mlebench with execution_mode=real requires ' + ', '.join(missing))

    def live(self, config: RunConfig) -> LiveMleBench:
        manifest = MleBenchManifest.load(fields.text(_paths(config), 'manifest'))
        if config.evaluation_instance_count != 1:
            raise IntegrityError('an MLE-bench evaluation is one graded run: evaluation_instance_count must be 1')
        if _cores(manifest) > config.cpu_workers:
            raise IntegrityError(f'the task needs {_cores(manifest)} cores but cpu_workers is {config.cpu_workers}')
        if config.cpu_seconds_per_instance != manifest.cpus * manifest.timeout_seconds:
            raise IntegrityError('cpu_seconds_per_instance must equal the task cpus x timeout_seconds')
        contract: dict[str, object] = {'benchmark': 'mlebench', 'panel_hash': manifest.panel_hash,
                                       **_contract(config), 'cpu_workers': config.cpu_workers}
        return LiveMleBench(manifest, contract, config)

    def image(self, config: RunConfig) -> str:
        return MleBenchManifest.load(fields.text(_paths(config), 'manifest')).image

    def seed_solver(self, config: RunConfig) -> str:
        seed = _paths(config).get('seed_solver', '')
        return seed if isinstance(seed, str) else ''

    def solver_argv(self, config: RunConfig) -> tuple[str, ...]:
        del config  # every competition runs the same entry point on its own inputs
        return ('python3', '/candidate/solution.py', '/input', '/scratch/submission.csv')

    def smoke_check(self, config: RunConfig, *, require_file: bool = False,
                    missing_message: str = 'Configured MLE-bench panel is missing') -> SmokeCheck:
        from maestro.backends.base import BackendUnavailable
        del require_file  # the frozen inputs are always required
        if not (_data_dir(config) / 'train' / 'inputs').is_dir():
            raise BackendUnavailable(missing_message)

        def check(candidate_path: str | Path, runner: ContainmentRunner) -> tuple[bool, str]:
            del runner  # training runs are reserved for formal evaluations
            if not (Path(candidate_path) / 'solution.py').is_file():
                return False, 'the candidate has no solution.py'
            return True, 'pass'
        return check

    def evaluate_final(self, engine: ExperimentEngine, source_episode_id: str,
                       manifest_path: str) -> dict[str, object]:
        # shortcut: the private test graded once on the final champion, recorded as one ledger event without a
        # held-out ticket; upgrade to PACE's separate held-out episode before publishing held-out results.
        from maestro.execution.sandbox import DockerRunner
        from maestro.tasks.final import champion_path
        del manifest_path  # the private test is frozen in the training manifest
        config = engine.config
        manifest = MleBenchManifest.load(fields.text(_paths(config), 'manifest'))
        champion, candidate = champion_path(engine, source_episode_id)
        if content_hash(_data_dir(config) / 'final' / 'inputs') != manifest.hashes['final/inputs']:
            raise IntegrityError('final/inputs changed after the panel froze')
        grade = run_and_grade(candidate, 'final', data_dir=_data_dir(config), manifest=manifest,
                              runner=DockerRunner(manifest.image), pids=fields.integer(_contract(config), 'pids'),
                              mlebench_python=_python(config))
        summary: dict[str, object] = {'source_episode_id': source_episode_id, 'champion_hash': champion,
                                      'competition': manifest.competition, 'valid': grade.valid,
                                      'competition_score': grade.score, 'beaten': grade.beaten,
                                      'any_medal': grade.any_medal, 'above_median': grade.above_median,
                                      'summary': grade.summary}
        engine.ledger.event('MLEBENCH_HELDOUT_EVAL', summary, source_episode_id)
        return summary


MLEBENCH = MleBenchBenchmark()
