"""Cleanup acknowledgement contracts using only fake Docker transports."""

import json
import subprocess

import pytest

from maestro.execution.sandbox import (
    DockerRunner, ExecutionQuiescenceUnknown, ResourceLimits, RunRequest,
    SandboxUnavailable)
from maestro.tasks.pace.verification import ContainerVerificationWorker
from maestro.types import AuthorizationError


CONTAINER_ID = "a" * 64


def _runner(monkeypatch, *, removal=1, listing=CONTAINER_ID, listing_fails=False,
            timeout=False, kill_fails=False):
    runner = DockerRunner("test@sha256:" + "1" * 64)
    commands = []
    monkeypatch.setattr(runner, "preflight", lambda image=None: {})

    def docker(args, **kwargs):
        commands.append(args)
        if args[0] == "create":
            return subprocess.CompletedProcess(args, 0, CONTAINER_ID, "")
        if args[0] == "kill":
            if kill_fails:
                raise SandboxUnavailable("kill response lost")
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "rm":
            if removal is None:
                raise SandboxUnavailable("remove response lost")
            return subprocess.CompletedProcess(args, removal, "", "remove failed")
        if args[:2] == ["container", "ls"]:
            if listing_fails:
                raise SandboxUnavailable("daemon unavailable")
            assert args[-1] == "id=" + CONTAINER_ID
            return subprocess.CompletedProcess(args, 0, listing, "")
        raise AssertionError(f"Unexpected Docker command: {args}")

    def start(args, **kwargs):
        assert args[:2] == ["docker", "start"]
        if timeout:
            raise subprocess.TimeoutExpired(args, kwargs["timeout"])
        return subprocess.CompletedProcess(args, 0, json.dumps({
            "returncode": 0, "stdout": "", "stderr": "", "cpu_seconds": 0.1,
            "wall_seconds": 0.1, "timed_out": False, "diagnostics": {}}), "")

    monkeypatch.setattr(runner, "_docker", docker)
    monkeypatch.setattr("maestro.execution.sandbox.subprocess.run", start)
    return runner, commands


def _request(tmp_path):
    return RunRequest(("python3", "-c", "pass"), tmp_path)


def test_live_container_after_cleanup_is_unknown_execution(tmp_path, monkeypatch):
    runner, commands = _runner(monkeypatch)
    with pytest.raises(ExecutionQuiescenceUnknown) as error:
        runner.run(_request(tmp_path), ResourceLimits())
    assert error.value.execution_unknown is True
    assert [args[0] for args in commands] == ["create", "rm", "container"]


def test_daemon_error_is_not_evidence_of_absence(tmp_path, monkeypatch):
    runner, _ = _runner(monkeypatch, listing_fails=True)
    with pytest.raises(ExecutionQuiescenceUnknown):
        runner.run(_request(tmp_path), ResourceLimits())


def test_lost_removal_response_requires_successful_empty_listing(tmp_path, monkeypatch):
    runner, commands = _runner(monkeypatch, removal=None, listing="")
    receipt = runner.run(_request(tmp_path), ResourceLimits())
    assert receipt.returncode == 0
    assert commands[-1][:2] == ["container", "ls"]


def test_failed_watchdog_kill_and_cleanup_cannot_return_timeout_receipt(tmp_path, monkeypatch):
    runner, commands = _runner(monkeypatch, timeout=True, kill_fails=True)
    with pytest.raises(ExecutionQuiescenceUnknown):
        runner.run(_request(tmp_path), ResourceLimits())
    assert [args[0] for args in commands] == ["create", "kill", "rm", "container"]


def test_failed_watchdog_kill_is_safe_only_after_acknowledged_removal(tmp_path, monkeypatch):
    runner, commands = _runner(monkeypatch, timeout=True, kill_fails=True, removal=0)
    receipt = runner.run(_request(tmp_path), ResourceLimits())
    assert receipt.timed_out and receipt.diagnostics["cpu_measurement_incomplete"]
    assert receipt.diagnostics["watchdog_kill_confirmed"] is False
    assert [args[0] for args in commands] == ["create", "kill", "rm"]


def test_verifier_does_not_settle_unknown_execution_as_infra_failure(tmp_path):
    from test_pace_runner import receipt, setup

    evaluator, _, _, panel = setup(tmp_path, [receipt(), receipt()])

    class UncertainTransport:
        def preflight(self, image=None):
            return {}

        def run(self, request, limits):
            raise ExecutionQuiescenceUnknown("verifier container may still run")

    worker = ContainerVerificationWorker(UncertainTransport(), temporary_root=tmp_path)
    with pytest.raises(ExecutionQuiescenceUnknown, match="may still run"):
        worker.verify(panel, [receipt(), receipt()], evaluator.contract)


@pytest.mark.parametrize("cleanup_error", [OSError, KeyboardInterrupt])
def test_unknown_execution_survives_temporary_cleanup_error(tmp_path, monkeypatch, cleanup_error):
    from maestro.tasks.pace import verification
    from test_pace_runner import receipt, setup

    evaluator, _, _, panel = setup(tmp_path, [receipt(), receipt()])
    original_temporary_directory = verification.tempfile.TemporaryDirectory

    class BrokenCleanup:
        def __init__(self, *args, **kwargs):
            self.inner = original_temporary_directory(*args, **kwargs)

        def __enter__(self):
            return self.inner.__enter__()

        def __exit__(self, *args):
            self.inner.__exit__(*args)
            raise cleanup_error("temporary cleanup interrupted")

    class UncertainTransport:
        def preflight(self, image=None):
            return {}

        def run(self, request, limits):
            raise ExecutionQuiescenceUnknown("verifier container may still run")

    monkeypatch.setattr(verification.tempfile, "TemporaryDirectory", BrokenCleanup)
    worker = ContainerVerificationWorker(UncertainTransport(), temporary_root=tmp_path)
    expected = KeyboardInterrupt if cleanup_error is KeyboardInterrupt else ExecutionQuiescenceUnknown
    with pytest.raises(expected) as error:
        worker.verify(panel, [receipt(), receipt()], evaluator.contract)
    assert error.value.execution_unknown is True


def test_verifier_preserves_unrelated_keyboard_interrupt(tmp_path):
    from test_pace_runner import receipt, setup

    evaluator, _, _, panel = setup(tmp_path, [receipt(), receipt()])

    class InterruptedTransport:
        def preflight(self, image=None):
            return {}

        def run(self, request, limits):
            raise KeyboardInterrupt("operator interrupted")

    worker = ContainerVerificationWorker(InterruptedTransport(), temporary_root=tmp_path)
    with pytest.raises(KeyboardInterrupt, match="operator interrupted") as error:
        worker.verify(panel, [receipt(), receipt()], evaluator.contract)
    assert not getattr(error.value, "execution_unknown", False)


def test_unknown_execution_poisons_cpu_pool_before_any_later_slot():
    from maestro.execution.cpu_pool import CpuPoolUnavailable, GlobalCpuPool

    pool = GlobalCpuPool(1)
    with pytest.raises(ExecutionQuiescenceUnknown):
        with pool.slot():
            raise ExecutionQuiescenceUnknown("executor cleanup unconfirmed")
    with pytest.raises(CpuPoolUnavailable, match="reconciliation"):
        with pool.slot():
            raise AssertionError("a poisoned CPU pool must reject every later launch")


def test_verifier_rechecks_authority_after_capture_and_before_runner(tmp_path):
    from test_pace_runner import receipt, setup

    evaluator, _, _, panel = setup(tmp_path, [receipt(), receipt()])
    calls = []

    class NeverStartedTransport:
        def preflight(self, image=None):
            return {}

        def run(self, request, limits):
            calls.append("runner")
            raise AssertionError("revoked authority must prevent verifier launch")

    def revoked():
        calls.append("authority")
        raise AuthorizationError("claim revoked while preparing verifier")

    worker = ContainerVerificationWorker(NeverStartedTransport(), temporary_root=tmp_path)
    with pytest.raises(AuthorizationError, match="revoked"):
        worker.verify(panel, [receipt(), receipt()], evaluator.contract, before_launch=revoked)
    assert calls == ["authority"]


def test_verifier_launch_callback_precedes_transport_call(tmp_path):
    from test_pace_runner import receipt, setup
    from test_verification_worker import VerifierRunner

    evaluator, _, _, panel = setup(tmp_path, [receipt(), receipt()])
    calls = []

    class OrderedTransport(VerifierRunner):
        def run(self, request, limits):
            calls.append("runner")
            return super().run(request, limits)

    worker = ContainerVerificationWorker(OrderedTransport(), temporary_root=tmp_path)
    result = worker.verify(panel, [receipt(), receipt()], evaluator.contract,
                           before_launch=lambda: calls.append("authority"))
    assert result.status == "COMPLETED"
    assert calls == ["authority", "runner"]


def test_cleanup_cannot_mask_verifier_authorization_refusal(tmp_path, monkeypatch):
    from maestro.tasks.pace import verification
    from test_pace_runner import receipt, setup

    evaluator, _, _, panel = setup(tmp_path, [receipt(), receipt()])
    original_temporary_directory = verification.tempfile.TemporaryDirectory
    calls = []

    class BrokenCleanup:
        def __init__(self, *args, **kwargs):
            self.inner = original_temporary_directory(*args, **kwargs)

        def __enter__(self):
            return self.inner.__enter__()

        def __exit__(self, *args):
            self.inner.__exit__(*args)
            raise OSError("cleanup failed after authorization refusal")

    class NeverStartedTransport:
        def preflight(self, image=None):
            return {}

        def run(self, request, limits):
            calls.append("runner")
            raise AssertionError("authorization refusal must prevent launch")

    def revoked():
        calls.append("authority")
        raise AuthorizationError("claim revoked before verifier launch")

    monkeypatch.setattr(verification.tempfile, "TemporaryDirectory", BrokenCleanup)
    worker = ContainerVerificationWorker(NeverStartedTransport(), temporary_root=tmp_path)
    with pytest.raises(AuthorizationError, match="claim revoked") as error:
        worker.verify(panel, [receipt(), receipt()], evaluator.contract, before_launch=revoked)
    assert isinstance(error.value.__cause__, OSError)
    assert calls == ["authority"]
