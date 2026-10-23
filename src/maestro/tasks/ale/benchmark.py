"""ALE-Bench (AtCoder Heuristic Contest) adapter; official judge via ALE's own interpreter."""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import subprocess
import threading
import time

from maestro.backends.base import BackendUnavailable
from maestro.types import EvaluationResult, IntegrityError, MaestroError, stable_hash

WORKER = Path(__file__).with_name('worker.py')
MANIFEST_FIELDS = {'schema_version', 'problem_id', 'lite_version', 'judge_version', 'code_language',
                   'score_type', 'time_limit', 'memory_limit', 'compile_timeout', 'compile_command', 'run_command',
                   'submission_file', 'ale_revision', 'reference_candidate_hash', 'cases', 'smoke_input'}
_ACQUIRE = threading.Lock()  # takes an evaluation's CPU slots atomically so concurrent evaluations cannot deadlock


def worker_call(python: str, request: dict, *, timeout: float) -> dict:
    """The ALE worker's JSON reply; raises MaestroError with its stderr tail on failure."""
    process = subprocess.run([python, '-I', str(WORKER)], input=json.dumps(request), capture_output=True,
                             text=True, timeout=timeout, check=False)
    if process.returncode:
        raise MaestroError(f"ALE worker {request['operation']} failed: {process.stderr[-2000:]}")
    return json.loads(process.stdout)


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
    cases: tuple  # ({input_sha256, reference_score}, ...) in public-case order
    smoke_input: str  # file name next to the manifest; generated from a seed outside the public panel

    @classmethod
    def load(cls, path: str | Path) -> 'AleManifest':
        raw = json.loads(Path(path).read_text(encoding='utf-8'))
        if not isinstance(raw, dict) or set(raw) != MANIFEST_FIELDS or raw['schema_version'] != 1:
            raise ValueError(f'ALE manifest needs exactly {sorted(MANIFEST_FIELDS)} with schema_version 1')
        if raw['score_type'] not in ('maximize', 'minimize'):
            raise ValueError('score_type must be maximize or minimize')
        cases = tuple(raw.pop('cases'))
        if not cases or any(set(c) != {'input_sha256', 'reference_score'} or c['reference_score'] <= 0
                            for c in cases):
            raise ValueError('every case needs input_sha256 and a positive reference_score')
        return cls(cases=cases, **raw)

    @property
    def panel_hash(self) -> str:
        return stable_hash({'benchmark': 'ale', **asdict(self)})


@dataclass(frozen=True)
class LiveAle:
    panel: AleManifest
    contract: dict
    config: object

    @property
    def panel_hash(self) -> str:
        return self.panel.panel_hash

    @property
    def contract_hash(self) -> str:
        return stable_hash(self.contract)

    def evaluator(self, *, runner, artifact_store, cpu_pool):
        return AleEvaluator(self, cpu_pool=cpu_pool, artifact_store=artifact_store)


class AleEvaluator:
    def __init__(self, live: LiveAle, *, cpu_pool, artifact_store=None):
        self.live, self.cpu_pool, self.artifact_store = live, cpu_pool, artifact_store
        self.checkout = live.config.task['paths']['ale_checkout']
        self.python = ale_python(self.checkout)

    def preflight(self):
        manifest = self.live.panel
        if ale_revision(self.checkout) != manifest.ale_revision:
            raise IntegrityError('ALE-Bench checkout differs from the revision the manifest was frozen with')
        return {'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash}

    def _width(self, ticket):
        return ticket.get('effective_instance_workers', self.live.config.instance_workers)

    def evaluate(self, ticket: dict):
        from maestro.backends.coral.workspace import candidate_tree_hash
        from maestro.execution.authorization import require_execution_authority

        require_execution_authority(ticket, begin=True)
        for key in ('job_id', 'candidate_hash', 'panel_hash', 'contract_hash', 'purpose', 'candidate_path'):
            if not ticket.get(key):
                raise ValueError(f'trusted evaluation ticket is missing {key}')
        if ticket['panel_hash'] != self.live.panel_hash or ticket['contract_hash'] != self.live.contract_hash:
            raise ValueError('ticket panel/contract binding mismatch')
        if ticket['purpose'] not in {'SETUP', 'WARM', 'MAIN'}:
            raise ValueError('ALE training evaluation accepts SETUP, WARM or MAIN only')
        candidate = Path(ticket['candidate_path']).resolve(strict=True)
        if candidate_tree_hash(candidate) != ticket['candidate_hash']:
            raise ValueError('candidate content differs from ticket-bound immutable hash')
        manifest = self.live.panel
        source = candidate / manifest.submission_file
        if not source.is_file():
            return self._invalid(ticket, f'candidate has no {manifest.submission_file}')
        width = self._width(ticket)
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
                timeout=self.live.contract['evaluation_wall_seconds'])
        cases = reply['cases']
        if [c['input_sha256'] for c in cases] != [c['input_sha256'] for c in manifest.cases]:
            raise IntegrityError('ALE public cases differ from the frozen panel')
        instance_results = []
        for index, (case, frozen) in enumerate(zip(cases, manifest.cases, strict=True)):
            accepted = case['judge_result'] == 'ACCEPTED'
            instance_results.append({
                'instance_id': f'{manifest.problem_id}-{index:04d}', 'valid': accepted,
                'absolute_score': case['absolute_score'], 'reference_score': frozen['reference_score'],
                'score': case_score(case['absolute_score'], frozen['reference_score'], manifest.score_type, accepted),
                'judge_result': case['judge_result'], 'message': case['message'][:500],
                'cpu_seconds': case['execution_time'], 'memory_usage': case['memory_usage']})
        valid = all(r['valid'] for r in instance_results)
        score = sum(r['score'] for r in instance_results) / len(instance_results) if valid else 0.0
        cpu = sum(r['cpu_seconds'] for r in instance_results)
        return EvaluationResult(
            job_id=ticket['job_id'], candidate_hash=ticket['candidate_hash'], score=score, valid=valid,
            status='COMPLETED' if valid else 'INVALID', cpu_seconds=cpu, wall_seconds=time.monotonic() - started,
            diagnostics={'label': 'ale_reference_normalized', 'purpose': ticket['purpose'],
                         'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash,
                         'overall_absolute_score': reply['overall_absolute_score'], 'num_workers': width,
                         'invalids': sum(not r['valid'] for r in instance_results),
                         'better_than_reference': sum(r['score'] >= 1.0 for r in instance_results)},
            instance_results=tuple(instance_results))

    def _invalid(self, ticket, reason):
        return EvaluationResult(job_id=ticket['job_id'], candidate_hash=ticket['candidate_hash'], score=0.0,
                                valid=False, status='INVALID', diagnostics={'label': 'ale_reference_normalized',
                                                                            'reason': reason})


class AleBenchmark:
    name = 'ale'
    # Same shape as PACE's briefs; texts name AtCoder heuristic-contest routes.
    directions = (
        ("constructive_start", ("initialization", "reduction"),
         "Explore greedy and constructive initial solutions.",
         "Raise the score of the first feasible answer before any search."),
        ("local_improvement", ("local_search", "postprocess"),
         "Explore neighbourhoods and annealing-style local moves.",
         "Improve the answer through accepted local changes."),
        ("search_diversification", ("restart", "tie_break"),
         "Explore restarts, beam widths and tie handling.",
         "Escape plateaus that trap a single search trajectory."),
        ("execution_efficiency", ("data_structure", "time_management"),
         "Explore incremental evaluation and use of the time limit.",
         "Fit more useful search iterations into the fixed time limit."),
    )

    def settings(self, config) -> dict:
        return config.task

    def validate(self, config) -> None:
        if config.pace:
            raise ValueError('benchmark=ale takes its settings from task:, not pace:')
        if config.execution_mode != 'real':
            return
        paths, contract = config.task.get('paths', {}), config.task.get('contract', {})
        missing = [f'task.paths.{k}' for k in ('manifest', 'seed_solver', 'ale_checkout') if not paths.get(k)]
        missing += [f'task.contract.{k}' for k in ('image', 'evaluation_wall_seconds') if not contract.get(k)]
        if missing:
            raise ValueError('benchmark=ale with execution_mode=real requires ' + ', '.join(missing))

    def live(self, config) -> LiveAle:
        manifest = AleManifest.load(config.task['paths']['manifest'])
        if config.evaluation_instance_count != len(manifest.cases):
            raise IntegrityError('evaluation_instance_count differs from the ALE panel size')
        contract = {'benchmark': 'ale', 'panel_hash': manifest.panel_hash, **config.task['contract'],
                    'cpu_workers': config.cpu_workers, 'instance_workers': config.instance_workers,
                    'worker_sha256': hashlib.sha256(WORKER.read_bytes()).hexdigest()}
        return LiveAle(manifest, contract, config)

    def image(self, config) -> str:
        return config.task.get('contract', {}).get('image', '')

    def seed_solver(self, config) -> str:
        return config.task.get('paths', {}).get('seed_solver', '')

    def solver_argv(self, config) -> tuple[str, ...]:
        manifest = AleManifest.load(config.task['paths']['manifest'])
        # Compiler chatter goes to stderr so stdout carries only the answer.
        return ('/bin/bash', '--noprofile', '--norc', '-c',
                f'cp /candidate/{manifest.submission_file} . && {manifest.compile_command} >&2 && {manifest.run_command}')

    def smoke_check(self, config, *, require_file=False, missing_message='Configured ALE smoke input is missing'):
        from maestro.execution.sandbox import Mount, ResourceLimits, RunRequest
        manifest_path = Path(config.task['paths']['manifest'])
        manifest = AleManifest.load(manifest_path)
        toy = manifest_path.parent / manifest.smoke_input
        if not toy.is_file():
            raise BackendUnavailable(missing_message)
        data = toy.read_text(encoding='utf-8')
        if hashlib.sha256(data.encode()).hexdigest() in {c['input_sha256'] for c in manifest.cases}:
            raise IntegrityError('ALE smoke input is one of the public panel cases')
        image, argv = self.image(config), self.solver_argv(config)
        budget = manifest.compile_timeout + manifest.time_limit  # the judge's own compile and run allowances
        # cpus=1 matches the judge's per-case container (cpu_quota of one CPU).
        limits = ResourceLimits(cpu_seconds=budget, wall_seconds=budget, memory_mb=manifest.memory_limit // 2**20,
                                pids=config.pids_max, output_bytes=config.output_bytes, cpus=1)

        def check(candidate_path, runner):
            candidate = Path(candidate_path).resolve(strict=True)
            receipt = runner.run(RunRequest(argv, candidate, data, (Mount(candidate, '/candidate', True),), image),
                                 limits)
            if receipt.timed_out or receipt.returncode != 0:
                return False, 'compile or toy run failed: ' + receipt.stderr[-500:]
            return (True, 'pass') if receipt.stdout.strip() else (False, 'toy run produced no output')
        return check

    def evaluate_final(self, engine, source_episode_id, manifest_path):
        # shortcut: private eval of the final champion, recorded as one ledger event without a held-out
        # ticket; upgrade to PACE's separate held-out episode before publishing held-out results.
        config = engine.config
        manifest = AleManifest.load(config.task['paths']['manifest'])
        row = engine.ledger.connection.execute(
            'SELECT payload FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',
            (source_episode_id,)).fetchone()
        if row is None:
            raise MaestroError('source episode has no settled checkpoint')
        champion = json.loads(row[0])['board']['champion_hash']
        candidate = json.loads(engine.ledger.connection.execute(
            'SELECT payload FROM candidates WHERE hash=?', (champion,)).fetchone()[0])
        code = (Path(candidate['candidate_path']) / manifest.submission_file).read_text(encoding='utf-8')
        reply = worker_call(ale_python(config.task['paths']['ale_checkout']), {
            'operation': 'private_eval', 'problem_id': manifest.problem_id, 'lite_version': manifest.lite_version,
            'num_workers': config.instance_workers, 'code': code, 'code_language': manifest.code_language,
            'judge_version': manifest.judge_version}, timeout=None)  # operator command; private panels run long
        summary = {'source_episode_id': source_episode_id, 'champion_hash': champion, 'rank': reply['rank'],
                   'performance': reply['performance'], 'overall_absolute_score': reply['overall_absolute_score'],
                   'accepted': sum(c['judge_result'] == 'ACCEPTED' for c in reply['cases']),
                   'cases': len(reply['cases'])}
        engine.ledger.event('ALE_PRIVATE_EVAL', summary, source_episode_id)
        return summary


ALE = AleBenchmark()
