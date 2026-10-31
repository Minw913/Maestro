"""One ticket runs a full frozen panel through an injected containment runner.

No subprocess executes directly in this module. The default runner refuses to
execute; a real runner must satisfy the execution.sandbox containment protocol.
The caller/dispatcher supplies ticket data read from the trusted ledger, never
an agent-authored request. Candidate paths must identify already-frozen assets.
"""

from dataclasses import asdict, dataclass
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextvars import copy_context
import hashlib
import math
from pathlib import Path
from threading import Event
import time
from typing import TYPE_CHECKING, Callable

from .manifest import EvaluationContract, ManifestError, PanelManifest

if TYPE_CHECKING:
    from maestro.execution.cpu_pool import GlobalCpuPool
    from maestro.execution.sandbox import ContainmentRunner
from .parser import parse_graph
from .scoring import aggregate_score
from .verifier import verify_solution
from .verification import ContainerVerificationWorker, SimulatedVerificationWorker


@dataclass(frozen=True)
class InstanceResult:
    instance_id: str
    reference_size: int
    result_size: int | None
    score: float
    valid: bool | None
    timeout: bool
    status: str
    cpu_seconds: float
    wall_seconds: float
    reason: str = ""
    better_than_reference: bool = False
    stdout_artifact: str | None = None
    stderr_artifact: str | None = None
    resource_observations: dict | None = None


class PaceEvaluator:
    def __init__(self, panel: PanelManifest, contract: EvaluationContract, runner: 'ContainmentRunner | None' = None,
                 candidate_resolver: Callable[[dict], Path] | None = None,
                 artifact_store: Callable[[bytes], str] | None = None,
                 *, simulate_containment: bool = False, verification_worker=None,
                 instance_workers: int | None = None, cpu_workers: int | None = None,
                 cpu_pool: 'GlobalCpuPool | None' = None, concurrent_candidates: int = 1):
        from maestro.execution.cpu_pool import GlobalCpuPool
        from maestro.execution.sandbox import FailClosedRunner

        if len(panel.instances) != contract.instances:
            raise ManifestError("ordered panel length differs from execution contract")
        if panel.split == "smoke":
            raise ManifestError("smoke panels cannot produce formal task scores")
        self.panel = panel
        self.contract = contract
        self.cpu_workers = contract.cpu_workers if cpu_workers is None else cpu_workers
        self.instance_workers = contract.instance_workers if instance_workers is None else instance_workers
        for name, value in (("cpu_workers", self.cpu_workers), ("instance_workers", self.instance_workers),
                            ("concurrent_candidates", concurrent_candidates)):
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if (self.cpu_workers, self.instance_workers) != (contract.cpu_workers, contract.instance_workers):
            raise ValueError("worker settings differ from the frozen evaluation contract")
        self.cpu_pool = cpu_pool if cpu_pool is not None else GlobalCpuPool(self.cpu_workers)
        if self.cpu_pool.capacity != self.cpu_workers:
            raise ValueError("CPU pool capacity differs from the frozen evaluation contract")
        self.concurrent_candidates = concurrent_candidates
        self.effective_instance_workers = self._worker_width(concurrent_candidates)
        self.runner = runner if runner is not None else FailClosedRunner()
        self.candidate_resolver = candidate_resolver or (lambda ticket: Path(ticket["candidate_path"]))
        self.artifact_store = artifact_store
        self.simulate_containment = simulate_containment
        self.verification_worker = verification_worker or (
            SimulatedVerificationWorker() if simulate_containment else ContainerVerificationWorker(self.runner))

    def _worker_width(self, candidate_batch_width):
        fair_ceiling = (self.cpu_workers + candidate_batch_width - 1) // candidate_batch_width
        return min(self.instance_workers, len(self.panel.instances), fair_ceiling)

    def _ticket_workers(self, ticket):
        names = ("admission_capacity", "cpu_workers", "instance_workers", "evaluation_batch_width", "effective_instance_workers")
        if not any(name in ticket for name in names):
            # Compatibility for trusted standalone dispatcher fixtures. Engine
            # tickets always carry the round's complete frozen resource plan.
            return self.concurrent_candidates, self.effective_instance_workers
        if any(type(ticket.get(name)) is not int or ticket[name] < 1 for name in names):
            raise ValueError("ticket has incomplete or invalid worker settings")
        batch = ticket["evaluation_batch_width"]
        if (ticket["cpu_workers"] != self.cpu_workers or ticket["instance_workers"] != self.instance_workers
                or batch != min(self.cpu_workers, ticket["admission_capacity"])
                or ticket["effective_instance_workers"] != self._worker_width(batch)):
            raise ValueError("ticket workers differ from the frozen resource plan")
        return batch, self._worker_width(batch)

    def preflight(self):
        """Reject incomplete physical execution prerequisites before dispatch."""
        from maestro.execution.sandbox import SandboxUnavailable

        if not self.simulate_containment:
            if not self.contract.image or "@sha256:" not in self.contract.image:
                raise SandboxUnavailable("PACE requires an explicitly digest-pinned container image")
            if self.contract.hardware_id == "UNSPECIFIED" or self.contract.toolchain_digest == "UNSPECIFIED":
                raise SandboxUnavailable("PACE requires frozen hardware and toolchain identifiers")
            if self.artifact_store is None:
                raise SandboxUnavailable("PACE requires an immutable private stdout/stderr artifact store")
            checker = getattr(self.runner, "preflight", None)
            if checker is None:
                raise SandboxUnavailable("PACE runner has no verifiable physical containment preflight")
            checker(image=self.contract.image)
            if not getattr(self.verification_worker, "enforces_hard_limits", False):
                raise SandboxUnavailable("formal PACE requires a hard-bounded trusted verifier worker")
            self.verification_worker.preflight(self.contract)
        return {"panel_hash": self.panel.panel_hash, "contract_hash": self.contract.contract_hash,
                "reserved_cpu_seconds": self.contract.reserved_cpu_seconds,
                "cpu_workers": self.cpu_workers, "instance_workers": self.instance_workers,
                "execution_kind": "simulated_containment" if self.simulate_containment else "isolated_container"}

    def evaluate(self, ticket: dict):
        from maestro.execution.authorization import require_execution_authority
        from maestro.execution.sandbox import Mount, ResourceLimits, RunRequest
        from maestro.types import EvaluationResult

        require_execution_authority(ticket, begin=True)
        self.preflight()
        for key in ("job_id", "candidate_hash", "panel_hash", "contract_hash", "purpose"):
            if not ticket.get(key):
                raise ValueError(f"trusted evaluation ticket is missing {key}")
        if ticket["panel_hash"] != self.panel.panel_hash or ticket["contract_hash"] != self.contract.contract_hash:
            raise ValueError("ticket panel/contract binding mismatch")
        if ticket["purpose"] not in {"SETUP", "WARM", "MAIN", "HELDOUT"}:
            raise ValueError("unknown evaluation purpose")
        if (self.panel.split == "heldout") != (ticket["purpose"] == "HELDOUT"):
            raise ValueError("held-out panel requires separate HELDOUT authority")
        candidate_batch_width, worker_width = self._ticket_workers(ticket)
        candidate_input = Path(self.candidate_resolver(ticket))
        candidate = candidate_input.resolve(strict=True)
        if not candidate.is_dir() or candidate_input.is_symlink():
            raise ValueError("frozen candidate must be a directory")
        if candidate != Path(ticket['candidate_path']).resolve(strict=True):
            raise ValueError("candidate resolver differs from ticket-bound immutable path")
        from maestro.backends.coral.workspace import candidate_tree_hash
        if candidate_tree_hash(candidate) != ticket['candidate_hash']:
            raise ValueError("candidate content differs from ticket-bound immutable hash")
        started = time.monotonic()
        # Validate the whole panel before granting even the first launch. Recheck
        # bytes in each worker after waiting for a permit as well.
        for entry in self.panel.instances:
            if hashlib.sha256(entry.path.read_bytes()).hexdigest() != entry.sha256:
                raise ManifestError(f"instance {entry.instance_id} changed after panel freeze")
        cancelled = Event()

        def run_instance(entry):
            try:
                with self.cpu_pool.slot():
                    if cancelled.is_set():
                        raise RuntimeError("instance launch cancelled after an earlier executor failure")
                    data = entry.path.read_bytes()
                    if hashlib.sha256(data).hexdigest() != entry.sha256:
                        raise ManifestError(f"instance {entry.instance_id} changed after panel freeze")
                    # Only one graph enters the container; reference sizes and
                    # the panel manifest are never mounted into the candidate.
                    request = RunRequest(
                        argv=tuple(self.contract.solver_argv), workdir=candidate,
                        stdin=data.decode("utf-8"), mounts=(Mount(candidate, "/candidate", True),),
                        image=self.contract.image, container_workdir="/scratch")
                    limits = ResourceLimits(
                        cpu_seconds=self.contract.cpu_seconds_per_instance,
                        wall_seconds=self.contract.wall_seconds_per_instance,
                        memory_mb=self.contract.memory_mb, pids=self.contract.pids,
                        output_bytes=self.contract.output_bytes, cpus=1)
                    # Recheck phase/fence after queueing, immediately before the
                    # physical launch. A copied ContextVar preserves authority.
                    require_execution_authority(ticket, launch=True)
                    receipt = self.runner.run(request, limits)
                    if (not math.isfinite(receipt.cpu_seconds) or not math.isfinite(receipt.wall_seconds)
                            or receipt.cpu_seconds < 0 or receipt.wall_seconds < 0):
                        raise ValueError("containment returned invalid resource accounting")
                    return receipt
            except BaseException:
                cancelled.set()
                raise

        receipts = [None] * len(self.panel.instances)
        if worker_width == 1:
            for index, entry in enumerate(self.panel.instances):
                receipts[index] = run_instance(entry)
        else:
            # Rolling submission keeps at most worker_width futures in flight.
            # On failure, stop replenishing and drain running work before the
            # dispatcher can mark UNKNOWN or settlement can observe the job.
            with ThreadPoolExecutor(max_workers=worker_width, thread_name_prefix="pace-instance") as executor:
                pending = {}
                next_index = 0

                def submit(index):
                    context = copy_context()  # A separate Context per future.
                    future = executor.submit(context.run, run_instance, self.panel.instances[index])
                    pending[future] = index

                for _ in range(worker_width):
                    submit(next_index)
                    next_index += 1
                try:
                    while pending:
                        done, _ = wait(pending, return_when=FIRST_COMPLETED)
                        # Inspect all completions before launching replacements.
                        for future in done:
                            receipts[pending.pop(future)] = future.result()
                        if cancelled.is_set():
                            # Another worker may have just failed. Surface its
                            # original exception, without submitting more work.
                            for future in pending:
                                future.result()
                            raise RuntimeError("instance executor cancelled")
                        while next_index < len(receipts) and len(pending) < worker_width:
                            submit(next_index)
                            next_index += 1
                except BaseException:
                    cancelled.set()
                    for future in pending:
                        future.cancel()
                    # Executor.__exit__ waits for all started workers. No live
                    # task escapes into another phase or a closed ledger.
                    raise
        run_cpu = 0.0
        output_refs = []
        private_outputs = {}
        for entry, receipt in zip(self.panel.instances, receipts, strict=True):
            run_cpu += receipt.cpu_seconds
            stdout_ref = self.artifact_store(receipt.stdout.encode("utf-8")) if self.artifact_store else None
            stderr_ref = self.artifact_store(receipt.stderr.encode("utf-8")) if self.artifact_store else None
            output_refs.append((stdout_ref, stderr_ref))
            if self.artifact_store is None:
                # Used only in explicit simulated containment tests. Production
                # preflight requires durable artifacts and never inline outputs.
                private_outputs[entry.instance_id] = {"stdout": receipt.stdout, "stderr": receipt.stderr}
        # One separate trusted container now verifies the entire panel under
        # the single 20-second CPU reserve and its independent wall deadline.
        with self.cpu_pool.slot():
            require_execution_authority(ticket, launch=True)
            if isinstance(self.verification_worker, ContainerVerificationWorker):
                verification = self.verification_worker.verify(self.panel, receipts, self.contract,
                    before_launch=lambda: require_execution_authority(ticket, launch=True))
            else:
                verification = self.verification_worker.verify(self.panel, receipts, self.contract)
        instance_results = []
        for index, (entry, receipt) in enumerate(zip(self.panel.instances, receipts, strict=True)):
            assessed = (verification.records[index] if verification.status == "COMPLETED" else
                        {"result_size": None, "score": 0.0, "valid": None,
                         "status": "UNVERIFIED_INFRA_FAILURE", "reason": "trusted verification failed",
                         "better_than_reference": False})
            instance_results.append(InstanceResult(
                instance_id=entry.instance_id, reference_size=entry.reference_size,
                result_size=assessed["result_size"], score=assessed["score"], valid=assessed["valid"],
                timeout=receipt.timed_out, status=assessed["status"], cpu_seconds=receipt.cpu_seconds,
                wall_seconds=receipt.wall_seconds, reason=assessed["reason"],
                better_than_reference=assessed["better_than_reference"],
                stdout_artifact=output_refs[index][0], stderr_artifact=output_refs[index][1],
                resource_observations={key:receipt.diagnostics.get(key) for key in (
                    'reason','memory_peak_bytes','pids_peak','sampled_pids_peak',
                    'stdout_file_bytes','stderr_file_bytes')}))
        candidate_valid = verification.status == "COMPLETED" and all(x.valid is not False for x in instance_results)
        if verification.status != "COMPLETED":
            status = "INFRA_FAILURE"
        elif not candidate_valid:
            status = "INVALID"
        elif any(item.timeout for item in instance_results):
            status = "COMPLETED_WITH_TIMEOUTS"
        else:
            status = "COMPLETED"
        score = aggregate_score([x.score for x in instance_results], candidate_valid=candidate_valid)
        diagnostics = {
            "label": "pace_style_internal", "purpose": ticket["purpose"],
            "panel_hash": self.panel.panel_hash, "contract_hash": self.contract.contract_hash,
            "execution_kind": "simulated_containment" if self.simulate_containment else "isolated_container",
            "run_cpu_seconds": run_cpu, "verification_cpu_seconds": verification.cpu_seconds,
            "verification_wall_seconds": verification.wall_seconds,
            "verification_code_hash": self.contract.verification_code_hash,
            "verification_worker": verification.diagnostics,
            "reserved_cpu_seconds": self.contract.reserved_cpu_seconds,
            "cpu_workers": self.cpu_workers, "instance_workers": self.instance_workers,
            "evaluation_batch_width": candidate_batch_width,
            "effective_instance_workers": worker_width,
            "instance_wall_seconds_sum": sum(receipt.wall_seconds for receipt in receipts),
            "reference_review": [x.instance_id for x in instance_results if x.better_than_reference],
            "timeouts": sum(x.timeout for x in instance_results),
            "invalids": sum(x.valid is False for x in instance_results),
            "hardware_id": self.contract.hardware_id, "toolchain_digest": self.contract.toolchain_digest,
            "seed": self.contract.seed,
        }
        if private_outputs:
            diagnostics["private_simulated_outputs"] = private_outputs
        return EvaluationResult(
            job_id=ticket["job_id"], candidate_hash=ticket["candidate_hash"],
            score=score, valid=candidate_valid, status=status,
            cpu_seconds=run_cpu + verification.cpu_seconds, wall_seconds=time.monotonic() - started,
            diagnostics=diagnostics, instance_results=tuple(asdict(x) for x in instance_results))


def smoke_test(candidate_path: str | Path, graph_path: str | Path, runner: 'ContainmentRunner', *, image: str,
               solver_argv: tuple[str, ...] = ("/candidate/run",)) -> tuple[bool, str]:
    """A fixed toy graph produces pass/fail only, never a training score."""
    from maestro.execution.sandbox import Mount, ResourceLimits, RunRequest

    candidate = Path(candidate_path).resolve(strict=True)
    data = Path(graph_path).read_text(encoding="utf-8")
    graph = parse_graph(data)
    limits = ResourceLimits(cpu_seconds=1.0, wall_seconds=3.0, memory_mb=128,
                            pids=8, output_bytes=65536, cpus=1)
    receipt = runner.run(
        RunRequest(tuple(solver_argv), candidate, data, (Mount(candidate, "/candidate", True),), image),
        limits)
    if receipt.timed_out or receipt.returncode != 0:
        from maestro.backends.coral.workspace import candidate_tree_hash
        from hashlib import sha256
        from maestro.tasks.runtime_failure import RuntimePrecheckFailure, classify_runtime_failure
        evidence = classify_runtime_failure(candidate_tree_hash(candidate), receipt,
            test_context={'kind':'fixed_sanity_only','stdin':data,
                'input_sha256':sha256(data.encode('utf-8')).hexdigest(),
                'solver_argv':list(solver_argv),'limits':asdict(limits),
                'receipt':asdict(receipt),
                'problem_contract':
                    'PACE Dominating Set: stdin is p ds n m followed by undirected edges u v, '
                    'vertices 1..n. Output k alone on the first non-comment line, '
                    'then k distinct vertex IDs (normally one per line); '
                    'every vertex must be selected or adjacent to a selected vertex.'})
        if evidence is not None:
            return RuntimePrecheckFailure("runtime exception in fixed precheck", evidence)
        return False, "smoke execution failed or timed out"
    verified = verify_solution(graph, receipt.stdout)
    return verified.valid, "pass" if verified.valid else verified.reason
