"""Hard-bounded trusted verification and an explicit simulation-only adapter."""

from dataclasses import dataclass, field
import json
import math
from pathlib import Path
import tempfile
import time

from .manifest import EvaluationContract, PanelManifest, VERIFIER_SOURCE_FILES, verifier_source_digest
from .scoring import instance_score
from .verification_worker import assess_payload


@dataclass(frozen=True)
class VerificationOutcome:
    status: str
    records: tuple[dict, ...] = ()
    cpu_seconds: float = 0.0
    wall_seconds: float = 0.0
    diagnostics: dict = field(default_factory=dict)


def _exception_in_chain(error: BaseException, matches):
    """Find critical failures even when temporary cleanup masks an exception."""
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if matches(current):
            return current
        for previous in (current.__cause__, current.__context__):
            if previous is not None:
                pending.append(previous)
    return None


def _payload(panel: PanelManifest, outputs: list) -> dict:
    if len(panel.instances) != len(outputs):
        raise ValueError("verifier requires one receipt per ordered panel instance")
    return {"schema_version": 1, "instances": [
        {"instance_id": entry.instance_id, "graph_path": f"/input/graphs/{index}.gr",
         "sha256": entry.sha256, "n": entry.n, "reference_size": entry.reference_size,
         "stdout": output.stdout, "returncode": output.returncode, "timed_out": output.timed_out}
        for index, (entry, output) in enumerate(zip(panel.instances, outputs, strict=True))]}


def _validated_records(payload: dict, panel: PanelManifest) -> tuple[dict, ...]:
    if payload.get("schema_version") != 1:
        raise ValueError("invalid verifier result schema")
    records = payload.get("instances")
    if not isinstance(records, list) or len(records) != len(panel.instances):
        raise ValueError("verifier result does not cover the complete frozen panel")
    for entry, record in zip(panel.instances, records, strict=True):
        if record.get("instance_id") != entry.instance_id:
            raise ValueError("verifier result instance order/identity mismatch")
        if record.get("valid") is not None and type(record["valid"]) is not bool:
            raise ValueError("verifier validity must be true, false or null")
        if type(record.get("score")) not in (int, float) or not 0 <= record["score"] <= 1:
            raise ValueError("verifier score is not finite and normalized")
        if record.get("status") not in {"COMPLETED", "TIMEOUT", "INVALID", "FAILED"}:
            raise ValueError("invalid verifier instance status")
        if record["valid"] is True:
            size = record.get("result_size")
            if type(size) is not int or not 0 <= size <= entry.n or record["status"] != "COMPLETED":
                raise ValueError("inconsistent valid verifier result")
            if abs(record["score"] - instance_score(size, entry.n, entry.reference_size)) > 1e-12:
                raise ValueError("verifier score differs from the frozen reference formula")
        elif record["score"] != 0 or record.get("result_size") is not None:
            raise ValueError("unverified or illegal results cannot contain a score/size")
        if (record["valid"] is None) != (record["status"] == "TIMEOUT"):
            raise ValueError("inconsistent timeout verifier result")
    return tuple(records)


class ContainerVerificationWorker:
    """One independent container verifies the complete panel within one reserve.

    Candidate containers have already terminated before this worker launches.
    The worker mounts trusted source and individual graphs, without any candidate
    executable, credentials, ledger, or other data. CPU is a cgroup tree budget;
    a distinct wall deadline and host watchdog prevent a hung trusted parser.
    """

    enforces_hard_limits = True

    def __init__(self, runner, *, temporary_root: str | Path | None = None):
        self.runner = runner
        self.temporary_root = Path(temporary_root).resolve() if temporary_root else None

    def preflight(self, contract: EvaluationContract):
        from maestro.execution.sandbox import SandboxUnavailable
        if verifier_source_digest() != contract.verification_code_hash:
            raise SandboxUnavailable("trusted verifier source differs from the frozen contract")
        checker = getattr(self.runner, "preflight", None)
        if not checker:
            raise SandboxUnavailable("trusted verification requires physical containment")
        return checker(image=contract.image)

    def verify(self, panel: PanelManifest, outputs: list, contract: EvaluationContract,
               *, before_launch=None) -> VerificationOutcome:
        from maestro.execution.sandbox import (
            ExecutionQuiescenceUnknown, Mount, ResourceLimits, RunRequest)
        from maestro.types import AuthorizationError

        self.preflight(contract)
        limits = ResourceLimits(
            cpu_seconds=contract.verification_cpu_seconds,
            wall_seconds=contract.verification_wall_seconds,
            memory_mb=contract.memory_mb, pids=8, output_bytes=1_000_000, cpus=1)
        started = time.monotonic()
        try:
            # Protected host parent + readable bind targets preserve private
            # 0600 source graphs while allowing the container's UID65534 worker
            # to read only these immutable copies. Capture the minimal trusted
            # package too, preventing code changes or unrelated package files
            # from entering an in-flight verifier.
            original_source = Path(__file__).resolve().parents[3]
            if self.temporary_root:
                self.temporary_root.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix="maestro-verify-", dir=self.temporary_root) as temporary:
                private = Path(temporary)
                source_root = private / "source"
                for name in VERIFIER_SOURCE_FILES:
                    target = source_root / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes((original_source / name).read_bytes())
                    target.chmod(0o444)
                for directory in source_root.rglob("*"):
                    if directory.is_dir():
                        directory.chmod(0o555)
                source_root.chmod(0o555)
                if verifier_source_digest(source_root) != contract.verification_code_hash:
                    raise ValueError("trusted verifier source changed during capture")
                mounts = [Mount(source_root, "/input/source", True)]
                for index, entry in enumerate(panel.instances):
                    graph = private / f"graph-{index}.gr"
                    graph.write_bytes(entry.path.read_bytes())
                    graph.chmod(0o444)
                    mounts.append(Mount(graph, f"/input/graphs/{index}.gr", True))
                request = RunRequest(
                    argv=("python3", "-I", "/input/source/maestro/tasks/pace/verification_worker.py"),
                    workdir=source_root, stdin=json.dumps(_payload(panel, outputs), allow_nan=False),
                    mounts=tuple(mounts), image=contract.image, container_workdir="/scratch")
                # Source capture and payload construction can take time. The
                # dispatcher must revalidate its claim at the physical boundary.
                if before_launch is not None:
                    before_launch()
                receipt = self.runner.run(request, limits)
        except ExecutionQuiescenceUnknown:
            # A live/unknown container cannot become a terminal, settleable
            # verifier failure. Nor may a revoked claim turn into a task score.
            raise
        except BaseException as exc:
            unknown = _exception_in_chain(exc, lambda error: getattr(error, "execution_unknown", False))
            if unknown is not None:
                if not isinstance(exc, Exception):
                    # Preserve KeyboardInterrupt/SystemExit rather than swallow
                    # them, while letting the CPU pool retain the uncertainty.
                    exc.execution_unknown = True
                    raise
                raise ExecutionQuiescenceUnknown(str(unknown)) from exc
            if not isinstance(exc, Exception):
                raise
            revoked = _exception_in_chain(exc, lambda error: isinstance(error, AuthorizationError))
            if revoked is not None:
                if revoked is exc:
                    raise
                raise AuthorizationError(str(revoked)) from exc
            return VerificationOutcome("INFRA_FAILURE", wall_seconds=time.monotonic() - started,
                                       diagnostics={"reason": "VERIFIER_RUNNER_FAILED", "error": str(exc)})
        accounting = {"cpu_seconds": receipt.cpu_seconds, "wall_seconds": receipt.wall_seconds}
        if any(not math.isfinite(value) or value < 0 for value in accounting.values()):
            return VerificationOutcome("INFRA_FAILURE", diagnostics={"reason": "VERIFIER_INVALID_METERING"})
        failure = None
        if receipt.timed_out:
            failure = "VERIFIER_LIMIT_EXCEEDED"
        elif receipt.returncode != 0:
            failure = "VERIFIER_FAILED"
        elif receipt.diagnostics.get("cpu_measurement_incomplete"):
            failure = "VERIFIER_INCOMPLETE_METERING"
        elif receipt.cpu_seconds > contract.verification_cpu_seconds + 0.05:
            failure = "VERIFIER_CPU_CONTRACT_EXCEEDED"
        if failure:
            return VerificationOutcome("INFRA_FAILURE", **accounting,
                diagnostics={"reason": failure, "worker": receipt.diagnostics, "stderr": receipt.stderr})
        try:
            records = _validated_records(json.loads(receipt.stdout), panel)
        except (ValueError, KeyError, TypeError) as exc:
            return VerificationOutcome("INFRA_FAILURE", **accounting,
                                       diagnostics={"reason": "VERIFIER_PROTOCOL_ERROR", "error": str(exc)})
        return VerificationOutcome("COMPLETED", records, **accounting,
                                   diagnostics={"worker": receipt.diagnostics,
                                                "verification_code_hash": contract.verification_code_hash})


class SimulatedVerificationWorker:
    """Only for explicitly selected deterministic containment tests."""

    enforces_hard_limits = False

    def verify(self, panel: PanelManifest, outputs: list, contract: EvaluationContract) -> VerificationOutcome:
        started, before = time.monotonic(), time.process_time()
        graph_bytes = {entry.instance_id: entry.path.read_bytes() for entry in panel.instances}
        try:
            result = assess_payload(_payload(panel, outputs),
                                    read_graph=lambda item: graph_bytes[item["instance_id"]])
            records = _validated_records(result, panel)
        except Exception as exc:
            return VerificationOutcome("INFRA_FAILURE", diagnostics={"reason": "SIMULATED_VERIFIER_FAILED", "error": str(exc)})
        return VerificationOutcome("COMPLETED", records, time.process_time() - before,
                                   time.monotonic() - started, {"simulated": True})
