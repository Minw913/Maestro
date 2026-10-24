"""ALE-Bench (AtCoder Heuristic Contest) adapter; official judge via ALE's own interpreter."""
from __future__ import annotations

import hashlib
import json
import subprocess
import threading
import time
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from maestro.backends.base import BackendUnavailable
from maestro.tasks import fields
from maestro.types import EvaluationResult, IntegrityError, MaestroError, stable_hash

if TYPE_CHECKING:
    from maestro.config import RunConfig
    from maestro.execution.cpu_pool import GlobalCpuPool
    from maestro.execution.sandbox import ContainmentRunner
    from maestro.loop import ExperimentEngine
    from maestro.tasks.base import ArtifactStore, SmokeCheck, Ticket

WORKER = Path(__file__).with_name('worker.py')
MANIFEST_FIELDS = {'schema_version', 'problem_id', 'lite_version', 'judge_version', 'code_language',
                   'score_type', 'time_limit', 'memory_limit', 'compile_timeout', 'compile_command', 'run_command',
                   'submission_file', 'ale_revision', 'reference_candidate_hash', 'cases', 'smoke_input'}
_ACQUIRE = threading.Lock()  # takes an evaluation's CPU slots atomically so concurrent evaluations cannot deadlock


def worker_call(python: str, request: dict[str, object], *, timeout: float | None) -> dict[str, object]:
    """The ALE worker's JSON reply; raises MaestroError with its stderr tail on failure."""
    process = subprocess.run([python, '-I', str(WORKER)], input=json.dumps(request), capture_output=True,
                             text=True, timeout=timeout, check=False)
    if process.returncode:
        raise MaestroError(f"ALE worker {request['operation']} failed: {process.stderr[-2000:]}")
    return fields.parse_object(process.stdout, 'ALE worker reply')


def case_score(score: int, reference: int, score_type: str, accepted: bool) -> float:
    """Reference-normalized case score in [0,1]: s/r when maximizing, r/s when minimizing, 0 unless accepted."""
    if not accepted or score <= 0:
        return 0.0
    ratio = score / reference if score_type == 'maximize' else reference / score
    return min(1.0, ratio)


def ale_python(checkout: str | Path) -> str:
    """Interpreter of the checkout's own venv; the symlink is kept, since resolving it leaves the venv."""
    return str(Path(checkout) / '.venv' / 'bin' / 'python')


def ale_revision(checkout: str | Path) -> str:
    """Git HEAD of the ALE-Bench checkout the worker imports."""
    return subprocess.run(['git', '-C', str(checkout), 'rev-parse', 'HEAD'], capture_output=True,
                          text=True, check=True).stdout.strip()


@dataclass(frozen=True)
class AleCase:
    input_sha256: str
    reference_score: int


@dataclass(frozen=True)
class JudgedCase:
    """One public case as the official judge reported it."""
    absolute_score: int
    judge_result: str
    execution_time: float
    memory_usage: int
    message: str
    input_sha256: str

    @classmethod
    def parse(cls, raw: fields.Fields) -> JudgedCase:
        return cls(fields.integer(raw, 'absolute_score'), fields.text(raw, 'judge_result'),
                   fields.number(raw, 'execution_time'), fields.integer(raw, 'memory_usage'),
                   fields.text(raw, 'message'), fields.text(raw, 'input_sha256'))


@dataclass(frozen=True)
class AleManifest:
    schema_version: int
    problem_id: str
    lite_version: bool
    judge_version: str
    code_language: str
    score_type: str
    time_limit: float
    memory_limit: int
    compile_timeout: float
    compile_command: str
    run_command: str
    submission_file: str
    ale_revision: str
    reference_candidate_hash: str
    cases: tuple[AleCase, ...]  # in public-case order
    smoke_input: str  # file name next to the manifest; generated from a seed outside the public panel

    @classmethod
    def load(cls, path: str | Path) -> AleManifest:
        raw = fields.parse_object(Path(path).read_text(encoding='utf-8'), 'ALE manifest')
        if set(raw) != MANIFEST_FIELDS or raw['schema_version'] != 1:
            raise ValueError(f'ALE manifest needs exactly {sorted(MANIFEST_FIELDS)} with schema_version 1')
        score_type = fields.text(raw, 'score_type')
        if score_type not in ('maximize', 'minimize'):
            raise ValueError('score_type must be maximize or minimize')
        cases = tuple(AleCase(fields.text(c, 'input_sha256'), fields.integer(c, 'reference_score'))
                      for c in fields.objects(raw, 'cases'))
        if not cases or any(case.reference_score <= 0 for case in cases):
            raise ValueError('every case needs input_sha256 and a positive reference_score')
        return cls(fields.integer(raw, 'schema_version'), fields.text(raw, 'problem_id'),
                   fields.flag(raw, 'lite_version'), fields.text(raw, 'judge_version'),
                   fields.text(raw, 'code_language'), score_type, fields.number(raw, 'time_limit'),
                   fields.integer(raw, 'memory_limit'), fields.number(raw, 'compile_timeout'),
                   fields.text(raw, 'compile_command'), fields.text(raw, 'run_command'),
                   fields.text(raw, 'submission_file'), fields.text(raw, 'ale_revision'),
                   fields.text(raw, 'reference_candidate_hash'), cases, fields.text(raw, 'smoke_input'))

    @property
    def panel_hash(self) -> str:
        return stable_hash({'benchmark': 'ale', **asdict(self)})


def _paths(config: RunConfig) -> dict[str, object]:
    return fields.section(config.task, 'paths')


def _contract(config: RunConfig) -> dict[str, object]:
    return fields.section(config.task, 'contract')


@dataclass(frozen=True)
class LiveAle:
    panel: AleManifest
    contract: dict[str, object]
    config: RunConfig

    @property
    def panel_hash(self) -> str:
        return self.panel.panel_hash

    @property
    def contract_hash(self) -> str:
        return stable_hash(self.contract)

    def evaluator(self, *, runner: ContainmentRunner | None, artifact_store: ArtifactStore | None,
                  cpu_pool: GlobalCpuPool) -> AleEvaluator:
        del runner, artifact_store  # the official judge runs and stores the cases itself
        return AleEvaluator(self, cpu_pool=cpu_pool)


class AleEvaluator:
    def __init__(self, live: LiveAle, *, cpu_pool: GlobalCpuPool):
        self.live: LiveAle = live
        self.cpu_pool: GlobalCpuPool = cpu_pool
        self.checkout: str = fields.text(_paths(live.config), 'ale_checkout')
        self.python: str = ale_python(self.checkout)

    def preflight(self) -> dict[str, str]:
        if ale_revision(self.checkout) != self.live.panel.ale_revision:
            raise IntegrityError('ALE-Bench checkout differs from the revision the manifest was frozen with')
        return {'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash}

    def evaluate(self, ticket: Ticket) -> EvaluationResult:
        from maestro.backends.coral.workspace import candidate_tree_hash
        from maestro.execution.authorization import require_execution_authority

        require_execution_authority(ticket, begin=True)
        for key in ('job_id', 'candidate_hash', 'panel_hash', 'contract_hash', 'purpose', 'candidate_path'):
            if not ticket.get(key):
                raise ValueError(f'trusted evaluation ticket is missing {key}')
        if ticket['panel_hash'] != self.live.panel_hash or ticket['contract_hash'] != self.live.contract_hash:
            raise ValueError('ticket panel/contract binding mismatch')
        purpose, job_id = fields.text(ticket, 'purpose'), fields.text(ticket, 'job_id')
        candidate_hash = fields.text(ticket, 'candidate_hash')
        if purpose not in {'SETUP', 'WARM', 'MAIN'}:
            raise ValueError('ALE training evaluation accepts SETUP, WARM or MAIN only')
        candidate = Path(fields.text(ticket, 'candidate_path')).resolve(strict=True)
        if candidate_tree_hash(candidate) != candidate_hash:
            raise ValueError('candidate content differs from ticket-bound immutable hash')
        manifest = self.live.panel
        source = candidate / manifest.submission_file
        if not source.is_file():
            return EvaluationResult(job_id=job_id, candidate_hash=candidate_hash, score=0.0, valid=False,
                                    status='INVALID', diagnostics={'label': 'ale_reference_normalized',
                                                                   'reason': f'candidate has no {manifest.submission_file}'})
        width = (fields.integer(ticket, 'effective_instance_workers') if 'effective_instance_workers' in ticket
                 else self.live.config.instance_workers)
        started = time.monotonic()
        with ExitStack() as held:
            with _ACQUIRE:
                for _ in range(width):
                    held.enter_context(self.cpu_pool.slot())
            require_execution_authority(ticket, launch=True)
            reply = worker_call(self.python, {
                'operation': 'public_eval', 'problem_id': manifest.problem_id,
                'lite_version': manifest.lite_version, 'num_workers': width,
                'code': source.read_text(encoding='utf-8'), 'code_language': manifest.code_language,
                'judge_version': manifest.judge_version},
                timeout=fields.number(self.live.contract, 'evaluation_wall_seconds'))
        cases = [JudgedCase.parse(raw) for raw in fields.objects(reply, 'cases')]
        if [c.input_sha256 for c in cases] != [c.input_sha256 for c in manifest.cases]:
            raise IntegrityError('ALE public cases differ from the frozen panel')
        instance_results: list[dict[str, object]] = []
        scores: list[float] = []
        for index, (case, frozen) in enumerate(zip(cases, manifest.cases, strict=True)):
            accepted = case.judge_result == 'ACCEPTED'
            scores.append(case_score(case.absolute_score, frozen.reference_score, manifest.score_type, accepted))
            instance_results.append({
                'instance_id': f'{manifest.problem_id}-{index:04d}', 'valid': accepted,
                'absolute_score': case.absolute_score, 'reference_score': frozen.reference_score,
                'score': scores[-1], 'judge_result': case.judge_result, 'message': case.message[:500],
                'cpu_seconds': case.execution_time, 'memory_usage': case.memory_usage})
        accepted_all = all(case.judge_result == 'ACCEPTED' for case in cases)
        return EvaluationResult(
            job_id=job_id, candidate_hash=candidate_hash, score=sum(scores) / len(scores) if accepted_all else 0.0,
            valid=accepted_all, status='COMPLETED' if accepted_all else 'INVALID',
            cpu_seconds=sum(case.execution_time for case in cases), wall_seconds=time.monotonic() - started,
            diagnostics={'label': 'ale_reference_normalized', 'purpose': purpose,
                         'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash,
                         'overall_absolute_score': fields.integer(reply, 'overall_absolute_score'),
                         'num_workers': width,
                         'invalids': sum(case.judge_result != 'ACCEPTED' for case in cases),
                         'better_than_reference': sum(score >= 1.0 for score in scores)},
            instance_results=tuple(instance_results))


class AleBenchmark:
    name: str = 'ale'
    scratch_exec: bool = True
    # The shared seed solves nothing, so every route may also build its own initial solution first.
    directions: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
        ("constructive_start", ("initialization", "reduction"),
         "Explore greedy and constructive initial solutions.",
         "Raise the score of the first feasible answer before any search."),
        ("local_improvement", ("local_search", "initialization"),
         "Explore neighbourhoods and annealing-style local moves.",
         "Improve the answer through accepted local changes."),
        ("search_diversification", ("restart", "initialization"),
         "Explore restarts, beam widths and tie handling.",
         "Escape plateaus that trap a single search trajectory."),
        ("execution_efficiency", ("time_management", "initialization"),
         "Explore incremental evaluation and use of the time limit.",
         "Fit more useful search iterations into the fixed time limit."),
    )

    def settings(self, config: RunConfig) -> dict[str, object]:
        return fields.mapping(config.task, 'task')

    def validate(self, config: RunConfig) -> None:
        if config.pace:
            raise ValueError('benchmark=ale takes its settings from task:, not pace:')
        if config.execution_mode != 'real':
            return
        paths, contract = _paths(config), _contract(config)
        missing = [f'task.paths.{k}' for k in ('manifest', 'seed_solver', 'ale_checkout') if not paths.get(k)]
        missing += [f'task.contract.{k}' for k in ('image', 'evaluation_wall_seconds') if not contract.get(k)]
        if missing:
            raise ValueError('benchmark=ale with execution_mode=real requires ' + ', '.join(missing))

    def live(self, config: RunConfig) -> LiveAle:
        manifest = AleManifest.load(fields.text(_paths(config), 'manifest'))
        if config.evaluation_instance_count != len(manifest.cases):
            raise IntegrityError('evaluation_instance_count differs from the ALE panel size')
        contract: dict[str, object] = {'benchmark': 'ale', 'panel_hash': manifest.panel_hash, **_contract(config),
                                       'cpu_workers': config.cpu_workers, 'instance_workers': config.instance_workers,
                                       'worker_sha256': hashlib.sha256(WORKER.read_bytes()).hexdigest()}
        return LiveAle(manifest, contract, config)

    def image(self, config: RunConfig) -> str:
        image = _contract(config).get('image', '')
        return image if isinstance(image, str) else ''

    def seed_solver(self, config: RunConfig) -> str:
        seed = _paths(config).get('seed_solver', '')
        return seed if isinstance(seed, str) else ''

    def solver_argv(self, config: RunConfig) -> tuple[str, ...]:
        manifest = AleManifest.load(fields.text(_paths(config), 'manifest'))
        # Compiler chatter goes to stderr so stdout carries only the answer.
        return ('/bin/bash', '--noprofile', '--norc', '-c',
                f'cp /candidate/{manifest.submission_file} . && {manifest.compile_command} >&2 && {manifest.run_command}')

    def smoke_check(self, config: RunConfig, *, require_file: bool = False,
                    missing_message: str = 'Configured ALE smoke input is missing') -> SmokeCheck:
        from maestro.execution.sandbox import Mount, ResourceLimits, RunRequest
        del require_file  # the smoke input is part of the frozen panel, so it is always required
        manifest_path = Path(fields.text(_paths(config), 'manifest'))
        manifest = AleManifest.load(manifest_path)
        toy = manifest_path.parent / manifest.smoke_input
        if not toy.is_file():
            raise BackendUnavailable(missing_message)
        data = toy.read_text(encoding='utf-8')
        if hashlib.sha256(data.encode()).hexdigest() in {c.input_sha256 for c in manifest.cases}:
            raise IntegrityError('ALE smoke input is one of the public panel cases')
        image, argv = self.image(config), self.solver_argv(config)
        budget = manifest.compile_timeout + manifest.time_limit  # the judge's own compile and run allowances
        # cpus=1 matches the judge's per-case container (cpu_quota of one CPU).
        limits = ResourceLimits(cpu_seconds=budget, wall_seconds=budget, memory_mb=manifest.memory_limit // 2**20,
                                pids=config.pids_max, output_bytes=config.output_bytes, cpus=1)

        def check(candidate_path: str | Path, runner: ContainmentRunner) -> tuple[bool, str]:
            candidate = Path(candidate_path).resolve(strict=True)
            receipt = runner.run(RunRequest(argv, candidate, data, (Mount(candidate, '/candidate', True),), image,
                                            scratch_exec=True), limits)
            if receipt.timed_out or receipt.returncode != 0:
                return False, 'compile or toy run failed: ' + receipt.stderr[-500:]
            return (True, 'pass') if receipt.stdout.strip() else (False, 'toy run produced no output')
        return check

    def evaluate_final(self, engine: ExperimentEngine, source_episode_id: str,
                       manifest_path: str) -> dict[str, object]:
        # shortcut: private eval of the final champion, recorded as one ledger event without a held-out
        # ticket; upgrade to PACE's separate held-out episode before publishing held-out results.
        from maestro.tasks.final import champion_path
        del manifest_path  # the private set is ALE's own; there is no separate held-out manifest
        config = engine.config
        manifest = AleManifest.load(fields.text(_paths(config), 'manifest'))
        champion, candidate = champion_path(engine, source_episode_id)
        code = (candidate / manifest.submission_file).read_text(encoding='utf-8')
        reply = worker_call(ale_python(fields.text(_paths(config), 'ale_checkout')), {
            'operation': 'private_eval', 'problem_id': manifest.problem_id, 'lite_version': manifest.lite_version,
            'num_workers': config.instance_workers, 'code': code, 'code_language': manifest.code_language,
            'judge_version': manifest.judge_version}, timeout=None)  # operator command; private panels run long
        cases = [JudgedCase.parse(raw) for raw in fields.objects(reply, 'cases')]
        summary: dict[str, object] = {
            'source_episode_id': source_episode_id, 'champion_hash': champion,
            'rank': fields.integer(reply, 'rank'), 'performance': fields.integer(reply, 'performance'),
            'overall_absolute_score': fields.integer(reply, 'overall_absolute_score'),
            'accepted': sum(case.judge_result == 'ACCEPTED' for case in cases), 'cases': len(cases)}
        engine.ledger.event('ALE_PRIVATE_EVAL', summary, source_episode_id)
        return summary


ALE = AleBenchmark()
