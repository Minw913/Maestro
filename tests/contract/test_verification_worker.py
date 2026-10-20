from dataclasses import replace
import json

import pytest

from maestro.execution.sandbox import RunReceipt, SandboxUnavailable
from maestro.tasks.pace.runner import PaceEvaluator
from maestro.tasks.pace.verification import ContainerVerificationWorker, VerificationOutcome
from maestro.tasks.pace.verification_worker import assess_payload

from test_pace_runner import receipt, setup, evaluate_under_claim


class VerifierRunner:
    """Simulates the containment transport, not the verifier implementation."""

    def __init__(self, mode="normal"):
        self.mode = mode
        self.calls = []

    def preflight(self, image=None):
        return {"image": image, "simulated_test_transport": True}

    def run(self, request, limits):
        self.calls.append((request, limits))
        if self.mode == "timeout":
            return RunReceipt(-9, "", "", 20, 30, True, {"reason": "CPU_LIMIT"})
        if self.mode == "crash":
            return RunReceipt(1, "", "worker crashed", 0.1, 0.2)
        if self.mode == "bad_protocol":
            return RunReceipt(0, "{}", "", 0.1, 0.2)
        if self.mode == "transport_failure":
            raise SandboxUnavailable("test transport lost")
        if self.mode == "cpu_breach":
            return RunReceipt(0, "{}", "", 25, 30)
        payload = json.loads(request.stdin)
        mount_paths = {mount.target: mount.source for mount in request.mounts}
        output = assess_payload(payload, read_graph=lambda item: mount_paths[item["graph_path"]].read_bytes())
        return RunReceipt(0, json.dumps(output), "", 0.1, 0.2)


def test_verifier_is_separate_and_one_reserve_covers_full_panel(tmp_path):
    evaluator, candidate_runner, ticket, panel = setup(tmp_path, [receipt(), receipt()])
    transport = VerifierRunner()
    evaluator.verification_worker = ContainerVerificationWorker(transport, temporary_root=tmp_path)
    result = evaluate_under_claim(evaluator, ticket)
    assert result.valid and result.score == 1
    assert len(candidate_runner.calls) == 2
    assert len(transport.calls) == 1
    request, limits = transport.calls[0]
    assert limits.cpu_seconds == 20
    assert limits.wall_seconds == 30
    assert limits.cpus == 1
    assert all(mount.read_only for mount in request.mounts)
    assert not any(mount.target.startswith("/candidate") for mount in request.mounts)
    assert request.argv[:2] == ("python3", "-I")
    assert result.diagnostics["verification_cpu_seconds"] == 0.1
    assert len(result.diagnostics["verification_code_hash"]) == 64


@pytest.mark.parametrize("mode", ["timeout", "crash", "bad_protocol", "transport_failure", "cpu_breach"])
def test_verifier_failures_complete_as_infra_failure_without_rewards(tmp_path, mode):
    evaluator, _, ticket, _ = setup(tmp_path, [receipt(), receipt()])
    evaluator.verification_worker = ContainerVerificationWorker(VerifierRunner(mode), temporary_root=tmp_path)
    result = evaluate_under_claim(evaluator, ticket)
    assert result.status == "INFRA_FAILURE"
    assert not result.valid and result.score == 0
    assert all(instance["status"] == "UNVERIFIED_INFRA_FAILURE" for instance in result.instance_results)
    assert result.diagnostics["verification_worker"]["reason"].startswith("VERIFIER_")


def test_verifier_source_hash_is_frozen_before_worker_launch(tmp_path):
    evaluator, _, _, panel = setup(tmp_path, [receipt(), receipt()])
    runner = VerifierRunner()
    worker = ContainerVerificationWorker(runner, temporary_root=tmp_path)
    altered = replace(evaluator.contract, verification_code_hash="0" * 64)
    with pytest.raises(SandboxUnavailable, match="source"):
        worker.preflight(altered)
    assert not runner.calls


def test_formal_mode_rejects_inline_simulated_verifier(tmp_path):
    evaluator, candidate_runner, _, panel = setup(tmp_path, [receipt(), receipt()])
    candidate_runner.preflight = lambda **kwargs: {}
    contract = replace(evaluator.contract, image="test@sha256:" + "1" * 64,
                       hardware_id="test", toolchain_digest="test")
    # Explicit simulation workers cannot silently be installed in formal mode.
    formal = PaceEvaluator(panel, contract, candidate_runner, artifact_store=lambda data: "test",
                           verification_worker=evaluator.verification_worker)
    with pytest.raises(SandboxUnavailable, match="hard-bounded"):
        formal.preflight()


def test_verifier_worker_checks_graph_checksum_independently(tmp_path):
    evaluator, _, _, panel = setup(tmp_path, [receipt(), receipt()])
    payload = {"schema_version": 1, "instances": [{
        "instance_id": "g0", "n": 3, "reference_size": 1,
        "sha256": "bad", "stdout": "1\n2\n", "returncode": 0, "timed_out": False,
    }]}
    with pytest.raises(ValueError, match="checksum"):
        assess_payload(payload, read_graph=lambda item: panel.instances[0].path.read_bytes())
