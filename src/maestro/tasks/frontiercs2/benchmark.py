"""FrontierCS-2 adapter: beat a published method on a research repo, scored by the task's own verifier."""
from __future__ import annotations

import hashlib
import io
import shutil
import subprocess
import tarfile
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

from maestro.tasks import fields
from maestro.types import EvaluationResult, IntegrityError, MaestroError, stable_hash

if TYPE_CHECKING:
    from maestro.config import RunConfig
    from maestro.execution.cpu_pool import GlobalCpuPool
    from maestro.execution.sandbox import ContainmentRunner
    from maestro.loop import ExperimentEngine
    from maestro.tasks.base import ArtifactStore, SmokeCheck, Ticket

MANIFEST_FIELDS = {'schema_version', 'task', 'problem_id', 'frontiercs_revision', 'verifier_image', 'agent_image',
                   'allow_paths', 'allow_empty', 'max_changed_files', 'max_patch_bytes', 'timeout_seconds', 'cpus',
                   'memory_mb', 'datasets', 'base_hash'}
PATCH_NAME = 'payload/solution.patch'  # FrontierCS-2's submission contract
# The verifier reads its inputs from FRONTIER_* variables; the root is read-only, so all writes go to /scratch.
_VERIFY = ('FRONTIER_PAYLOAD_PATH=/input/payload/answer.tar FRONTIER_DATASET_PATH=/input/dataset/{dataset} '
           'FRONTIER_VERIFICATION_MODE={mode} FRONTIER_RESULT_PATH=/scratch/result.json '
           'FRONTIER_EVIDENCE_PATH=/scratch/evidence.tar FRONTIER_VERIFIER_SCRATCH=/scratch TMPDIR=/scratch '
           '/opt/frontier/verifier >&2; cat /scratch/result.json')


@dataclass(frozen=True)
class FrontierCSManifest:
    schema_version: int
    task: str  # e.g. Database/join_tree_rewriting
    problem_id: str
    frontiercs_revision: str
    verifier_image: str  # holds the private suites; only formal evaluations run in it
    agent_image: str  # the task's agent-base image; agents' tools run here
    allow_paths: tuple[str, ...]
    allow_empty: bool
    max_changed_files: int
    max_patch_bytes: int
    timeout_seconds: float
    cpus: float
    memory_mb: int
    datasets: dict[str, str]  # verification mode -> sha256 of its dataset selector file
    base_hash: str  # tree hash of the pristine repo every patch is taken against

    @classmethod
    def load(cls, path: str | Path) -> FrontierCSManifest:
        raw = fields.parse_object(Path(path).read_text(encoding='utf-8'), 'FrontierCS-2 manifest')
        if set(raw) != MANIFEST_FIELDS or raw['schema_version'] != 1:
            raise ValueError(f'FrontierCS-2 manifest needs exactly {sorted(MANIFEST_FIELDS)} with schema_version 1')
        datasets = fields.section(raw, 'datasets')
        if set(datasets) != {'train', 'eval'}:
            raise ValueError('datasets must name exactly the train and eval selectors')
        allow = raw.get('allow_paths')
        if not isinstance(allow, list) or not allow:
            raise fields.FieldError('allow_paths must be a non-empty list')
        return cls(fields.integer(raw, 'schema_version'), fields.text(raw, 'task'), fields.text(raw, 'problem_id'),
                   fields.text(raw, 'frontiercs_revision'), fields.text(raw, 'verifier_image'),
                   fields.text(raw, 'agent_image'), tuple(str(p) for p in cast(list[object], allow)),
                   fields.flag(raw, 'allow_empty'), fields.integer(raw, 'max_changed_files'),
                   fields.integer(raw, 'max_patch_bytes'), fields.number(raw, 'timeout_seconds'),
                   fields.number(raw, 'cpus'), fields.integer(raw, 'memory_mb'),
                   {mode: fields.text(datasets, mode) for mode in ('train', 'eval')}, fields.text(raw, 'base_hash'))

    @property
    def panel_hash(self) -> str:
        return stable_hash({'benchmark': 'frontiercs2', **asdict(self)})


def _paths(config: RunConfig) -> dict[str, object]:
    return fields.section(config.task, 'paths')


def _contract(config: RunConfig) -> dict[str, object]:
    return fields.section(config.task, 'contract')


def _data_dir(config: RunConfig) -> Path:
    return Path(fields.text(_paths(config), 'manifest')).parent


def _cores(manifest: FrontierCSManifest) -> int:
    return max(1, round(manifest.cpus))


def _allowed(relative: Path, allow_paths: tuple[str, ...]) -> bool:
    text = relative.as_posix()
    return any(text == p.rstrip('/') or text.startswith(p.rstrip('/') + '/') for p in allow_paths)


def _files(root: Path) -> dict[Path, bytes]:
    return {p.relative_to(root): p.read_bytes() for p in sorted(root.rglob('*'))
            if p.is_file() and '.git' not in p.relative_to(root).parts}


def make_patch(base: Path, candidate: Path, manifest: FrontierCSManifest) -> str:
    """The candidate's change to the pristine repo, made exactly as FrontierCS-2's make_submission.sh does."""
    with tempfile.TemporaryDirectory(prefix='fcs2-patch-') as scratch:
        work = Path(scratch) / 'repo'
        _ = shutil.copytree(base, work)
        git = ['git', '-C', str(work), '-c', 'user.name=maestro', '-c', 'user.email=maestro@localhost']
        _ = subprocess.run([*git, 'init', '-q'], check=True)
        _ = subprocess.run([*git, 'add', '-A'], check=True)
        _ = subprocess.run([*git, 'commit', '-q', '-m', 'pristine'], check=True)
        for relative, content in _files(candidate).items():
            if _allowed(relative, manifest.allow_paths):
                (work / relative).parent.mkdir(parents=True, exist_ok=True)
                _ = (work / relative).write_bytes(content)
        for relative in _files(base):
            if _allowed(relative, manifest.allow_paths) and not (candidate / relative).is_file():
                (work / relative).unlink()
        # Staging first makes new files under an allowed directory part of the patch, not silently dropped.
        _ = subprocess.run([*git, 'add', '-A'], check=True)
        return subprocess.run([*git, 'diff', '--no-ext-diff', '--no-color', '--cached', '--'], capture_output=True,
                              text=True, check=True).stdout


def patch_problems(base: Path, candidate: Path, manifest: FrontierCSManifest, patch: str) -> list[str]:
    """Contract violations a verifier would reject, checked before spending an evaluation."""
    problems: list[str] = []
    ignored = {Path('PROBLEM.md')}
    pristine, current = _files(base), _files(candidate)
    changed = {r for r, c in current.items() if pristine.get(r) != c and r not in ignored}
    changed |= {r for r in pristine if r not in current}
    outside = sorted(r.as_posix() for r in changed if not _allowed(r, manifest.allow_paths))
    if outside:
        problems.append('only ' + ', '.join(manifest.allow_paths) + ' may change; also changed: ' + ', '.join(outside))
    if not patch and not manifest.allow_empty:
        problems.append('the submission patch is empty')
    if len(patch.encode()) > manifest.max_patch_bytes:
        problems.append(f'the patch exceeds {manifest.max_patch_bytes} bytes')
    if patch.count('\ndiff --git ') + patch.startswith('diff --git ') > manifest.max_changed_files:
        problems.append(f'at most {manifest.max_changed_files} files may change')
    return problems


def payload_tar(patch: str) -> bytes:
    """The uncompressed POSIX tar FrontierCS-2 accepts: payload/ holding exactly payload/solution.patch."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w', format=tarfile.USTAR_FORMAT) as archive:
        directory = tarfile.TarInfo('payload')
        directory.type, directory.mode = tarfile.DIRTYPE, 0o755
        archive.addfile(directory)
        data = patch.encode()
        member = tarfile.TarInfo(PATCH_NAME)
        member.size, member.mode = len(data), 0o644
        archive.addfile(member, io.BytesIO(data))
    return buffer.getvalue()


@dataclass(frozen=True)
class Verdict:
    """One verifier run: its own status and 0-100 score."""
    valid: bool
    raw_score: float
    summary: str
    cpu_seconds: float
    wall_seconds: float
    timed_out: bool


def verify(candidate: Path, mode: str, *, data_dir: Path, manifest: FrontierCSManifest, runner: ContainmentRunner,
           pids: int) -> Verdict:
    """Run the task's verifier on the candidate's patch in `mode` (train or eval), contained."""
    from maestro.execution.sandbox import Mount, ResourceLimits, RunRequest
    dataset = data_dir / 'datasets' / f'{mode}.json'
    if hashlib.sha256(dataset.read_bytes()).hexdigest() != manifest.datasets[mode]:
        raise IntegrityError(f'the {mode} dataset selector changed after the panel froze')
    with tempfile.TemporaryDirectory(prefix='fcs2-payload-') as scratch:
        payload = Path(scratch)
        _ = (payload / 'answer.tar').write_bytes(payload_tar(make_patch(data_dir / 'base', candidate, manifest)))
        payload.chmod(0o755)
        (payload / 'answer.tar').chmod(0o644)
        receipt = runner.run(
            RunRequest(('/bin/sh', '-c', _VERIFY.format(dataset=dataset.name, mode=mode)), payload, '',
                       (Mount(payload, '/input/payload', True), Mount(dataset.parent, '/input/dataset', True)),
                       manifest.verifier_image, scratch_exec=True),
            # The verifier copies its whole task bundle into scratch, so files may be as large as the scratch
            # tmpfs, which the runner sizes to memory_mb; that tmpfs also bounds the total written.
            ResourceLimits(cpu_seconds=manifest.cpus * manifest.timeout_seconds, wall_seconds=manifest.timeout_seconds,
                           memory_mb=manifest.memory_mb, pids=pids, output_bytes=manifest.memory_mb * 2**20,
                           cpus=manifest.cpus))
    try:
        result = fields.parse_object(receipt.stdout, 'verifier result')
        passed = fields.text(result, 'status') == 'passed'
        score = fields.number(result, 'raw_score') if passed else 0.0
        summary = fields.text(result, 'summary')
    except ValueError:
        passed, score, summary = False, 0.0, 'verifier produced no result: ' + receipt.stderr[-300:]
    return Verdict(passed and not receipt.timed_out, score, summary[:500], receipt.cpu_seconds, receipt.wall_seconds,
                   receipt.timed_out)


@dataclass(frozen=True)
class LiveFrontierCS:
    panel: FrontierCSManifest
    contract: dict[str, object]
    config: RunConfig

    @property
    def panel_hash(self) -> str:
        return self.panel.panel_hash

    @property
    def contract_hash(self) -> str:
        return stable_hash(self.contract)

    def evaluator(self, *, runner: ContainmentRunner | None, artifact_store: ArtifactStore | None,
                  cpu_pool: GlobalCpuPool) -> FrontierCSEvaluator:
        del artifact_store  # the verifier keeps its own evidence inside the run
        if runner is None:
            raise MaestroError('FrontierCS-2 evaluation requires a contained runner')
        return FrontierCSEvaluator(self, runner=runner, cpu_pool=cpu_pool)


class FrontierCSEvaluator:
    def __init__(self, live: LiveFrontierCS, *, runner: ContainmentRunner, cpu_pool: GlobalCpuPool):
        self.live: LiveFrontierCS = live
        self.runner: ContainmentRunner = runner
        self.cpu_pool: GlobalCpuPool = cpu_pool
        self.data_dir: Path = _data_dir(live.config)

    def preflight(self) -> dict[str, str]:
        from maestro.backends.coral.workspace import candidate_tree_hash
        if candidate_tree_hash(self.data_dir / 'base') != self.live.panel.base_hash:
            raise IntegrityError('the pristine FrontierCS-2 repo changed after the panel froze')
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
            raise ValueError('FrontierCS-2 training evaluation accepts SETUP, WARM or MAIN only')
        candidate = Path(fields.text(ticket, 'candidate_path')).resolve(strict=True)
        if candidate_tree_hash(candidate) != ticket['candidate_hash']:
            raise ValueError('candidate content differs from ticket-bound immutable hash')
        manifest, started = self.live.panel, time.monotonic()
        with held(self.cpu_pool, _cores(manifest)):  # the verifier runs on the task's own core count
            require_execution_authority(ticket, launch=True)
            verdict = verify(candidate, 'train', data_dir=self.data_dir, manifest=manifest, runner=self.runner,
                             pids=fields.integer(self.live.contract, 'pids'))
        return EvaluationResult(
            job_id=fields.text(ticket, 'job_id'), candidate_hash=fields.text(ticket, 'candidate_hash'),
            score=verdict.raw_score / 100 if verdict.valid else 0.0, valid=verdict.valid,
            status='COMPLETED' if verdict.valid else 'INVALID', cpu_seconds=verdict.cpu_seconds,
            wall_seconds=time.monotonic() - started,
            diagnostics={'label': 'frontiercs2_verifier', 'purpose': purpose, 'task': manifest.task,
                         'panel_hash': self.live.panel_hash, 'contract_hash': self.live.contract_hash,
                         'raw_score': verdict.raw_score, 'summary': verdict.summary, 'timed_out': verdict.timed_out},
            instance_results=({'instance': 'train', 'valid': verdict.valid, 'raw_score': verdict.raw_score,
                               'summary': verdict.summary, 'cpu_seconds': verdict.cpu_seconds,
                               'wall_seconds': verdict.wall_seconds},))


class FrontierCSBenchmark:
    name: str = 'frontiercs2'
    scratch_exec: bool = True  # many tasks compile and run their candidate from scratch space
    # shortcut: the shared heuristic-search vocabulary; give FrontierCS-2 research-task routes before
    # comparing plan statistics across benchmarks.
    directions: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
        ("constructive_start", ("initialization", "reduction"),
         "Explore a different algorithm or plan shape from the published method's starting point.",
         "Make every workload pass before optimizing the continuous score."),
        ("local_improvement", ("local_search", "initialization"),
         "Explore targeted refinements of the current method on the failing workloads.",
         "Turn failing or slow workloads into passing ones."),
        ("search_diversification", ("restart", "initialization"),
         "Explore alternative heuristics and decision rules the paper did not try.",
         "Escape a plateau the current method is stuck on."),
        ("execution_efficiency", ("time_management", "initialization"),
         "Explore lower overhead, caching and cheaper decisions.",
         "Win on the measured latency and resource metrics."),
    )

    def settings(self, config: RunConfig) -> dict[str, object]:
        return fields.mapping(config.task, 'task')

    def validate(self, config: RunConfig) -> None:
        if config.pace:
            raise ValueError('benchmark=frontiercs2 takes its settings from task:, not pace:')
        if config.execution_mode != 'real':
            return
        paths, contract = _paths(config), _contract(config)
        missing = [f'task.paths.{k}' for k in ('manifest', 'seed_solver') if not paths.get(k)]
        missing += [f'task.contract.{k}' for k in ('pids',) if not contract.get(k)]
        if missing:
            raise ValueError('benchmark=frontiercs2 with execution_mode=real requires ' + ', '.join(missing))

    def live(self, config: RunConfig) -> LiveFrontierCS:
        manifest = FrontierCSManifest.load(fields.text(_paths(config), 'manifest'))
        if config.evaluation_instance_count != 1:
            raise IntegrityError('a FrontierCS-2 evaluation is one verifier run: evaluation_instance_count must be 1')
        if _cores(manifest) > config.cpu_workers:
            raise IntegrityError(f'the task needs {_cores(manifest)} cores but cpu_workers is {config.cpu_workers}')
        if config.cpu_seconds_per_instance != manifest.cpus * manifest.timeout_seconds:
            raise IntegrityError('cpu_seconds_per_instance must equal the task cpus x timeout_seconds')
        contract: dict[str, object] = {'benchmark': 'frontiercs2', 'panel_hash': manifest.panel_hash,
                                       **_contract(config), 'cpu_workers': config.cpu_workers}
        return LiveFrontierCS(manifest, contract, config)

    def image(self, config: RunConfig) -> str:
        return FrontierCSManifest.load(fields.text(_paths(config), 'manifest')).agent_image

    def seed_solver(self, config: RunConfig) -> str:
        seed = _paths(config).get('seed_solver', '')
        return seed if isinstance(seed, str) else ''

    def solver_argv(self, config: RunConfig) -> tuple[str, ...]:
        del config  # every task runs only inside its own verifier, on the patch against the pristine repo
        return ('/opt/frontier/verifier',)

    def smoke_check(self, config: RunConfig, *, require_file: bool = False,
                    missing_message: str = 'Configured FrontierCS-2 pristine repo is missing') -> SmokeCheck:
        from maestro.backends.base import BackendUnavailable
        del require_file  # the pristine repo is part of the frozen panel, so it is always required
        data_dir = _data_dir(config)
        manifest = FrontierCSManifest.load(data_dir / 'manifest.json')
        if not (data_dir / 'base').is_dir():
            raise BackendUnavailable(missing_message)

        def check(candidate_path: str | Path, runner: ContainmentRunner) -> tuple[bool, str]:
            del runner  # the patch contract is checked on the host; the verifier is reserved for formal runs
            candidate = Path(candidate_path).resolve(strict=True)
            problems = patch_problems(data_dir / 'base', candidate, manifest,
                                      make_patch(data_dir / 'base', candidate, manifest))
            return (False, '; '.join(problems)) if problems else (True, 'pass')
        return check

    def evaluate_final(self, engine: ExperimentEngine, source_episode_id: str,
                       manifest_path: str) -> dict[str, object]:
        # shortcut: the eval-mode verifier on the final champion, recorded as one ledger event without a
        # held-out ticket; upgrade to PACE's separate held-out episode before publishing held-out results.
        from maestro.execution.sandbox import DockerRunner
        from maestro.tasks.final import champion_path
        del manifest_path  # the eval selector is frozen in the training manifest
        config = engine.config
        manifest = FrontierCSManifest.load(fields.text(_paths(config), 'manifest'))
        champion, candidate = champion_path(engine, source_episode_id)
        verdict = verify(candidate, 'eval', data_dir=_data_dir(config), manifest=manifest,
                         runner=DockerRunner(manifest.verifier_image), pids=fields.integer(_contract(config), 'pids'))
        summary: dict[str, object] = {'source_episode_id': source_episode_id, 'champion_hash': champion,
                                      'task': manifest.task, 'valid': verdict.valid, 'raw_score': verdict.raw_score,
                                      'summary': verdict.summary}
        engine.ledger.event('FRONTIERCS2_HELDOUT_EVAL', summary, source_episode_id)
        return summary


FRONTIERCS2 = FrontierCSBenchmark()
