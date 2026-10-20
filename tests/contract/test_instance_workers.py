"""Concurrency regressions for ticket-bound PACE instance workers.

These tests use real threads and SQLite claims, with simulated containment only.
They never require Docker, model inference, or external credentials.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
import threading

import pytest

from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.config import RunConfig
from maestro.execution.authorization import execution_scope, require_execution_authority
from maestro.execution.cpu_pool import GlobalCpuPool
from maestro.execution.dispatcher import Dispatcher
from maestro.execution.sandbox import RunReceipt
from maestro.storage.ledger import Ledger
from maestro.tasks.pace.manifest import EvaluationContract, load_panel
from maestro.tasks.pace.runner import PaceEvaluator
from maestro.tasks.pace.verification import SimulatedVerificationWorker
from maestro.types import AgentState, AuthorizationError


def _instance_index(request):
    return int(request.stdin.splitlines()[0].split()[-1])


def _receipt(index):
    return RunReceipt(0, "1\n2\n", "", (index + 1) * 0.25, 0.5)


class Meter:
    """Count physical solver and verifier entries under one shared lock."""

    def __init__(self):
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.entries = []

    @contextmanager
    def entered(self, kind):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.entries.append(kind)
        try:
            yield
        finally:
            with self.lock:
                self.active -= 1


def _setup(tmp_path, runner, *, count=4, verifier=None, ticket_overrides=None,
           **worker_settings):
    tmp_path.mkdir(parents=True, exist_ok=True)
    instances = []
    for index in range(count):
        n = index + 3
        graph = f"c instance {index}\np ds {n} {n - 1}\n" + "".join(
            f"2 {vertex}\n" for vertex in range(1, n + 1) if vertex != 2)
        path = tmp_path / f"graph-{index}.gr"
        path.write_bytes(graph.encode())
        instances.append({"instance_id": f"g{index}", "path": path.name,
                          "sha256": hashlib.sha256(graph.encode()).hexdigest(),
                          "reference_size": 1})
    manifest = tmp_path / "panel.json"
    manifest.write_text(json.dumps({"schema_version": 1, "panel_id": "instance-workers",
                                    "split": "training", "instances": instances}))
    panel = load_panel(manifest)
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    contract_settings = {key: worker_settings[key] for key in ("cpu_workers", "instance_workers")
                         if worker_settings.get(key) is not None}
    contract = EvaluationContract(instances=count, **contract_settings)
    evaluator = PaceEvaluator(panel, contract, runner, simulate_containment=True,
                              verification_worker=verifier, **worker_settings)
    candidate_hash = candidate_tree_hash(candidate)
    ticket = {"ticket_id": "ticket1", "job_id": "job1", "episode_id": "workers",
              "round_index": 0, "agent_id": "a", "plan_version_id": "setup", "phase_epoch": 0,
              "candidate_hash": candidate_hash, "candidate_path": str(candidate),
              "panel_hash": panel.panel_hash, "contract_hash": contract.contract_hash,
              "purpose": "SETUP"}
    ticket.update(ticket_overrides or {})
    ledger = Ledger(tmp_path / "authority.sqlite")
    ledger.create_episode("workers", "warm",
                          RunConfig(slots=1, cpu_workers=contract.cpu_workers,
                                    instance_workers=contract.instance_workers),
                          {"a": AgentState("a", candidate_hash, 0)})
    ledger.open_round("workers", 0, {key: ticket[key] for key in
                      ("purpose", "candidate_hash", "panel_hash", "contract_hash")})
    ledger.record_candidate(candidate_hash, {key: ticket[key] for key in
                                             ("candidate_hash", "candidate_path")})
    ledger.invite("workers", 0, "a", 0)
    ledger.set_round_phase("workers", 0, "ADMISSION")
    ledger.admit(ticket, 0, 1)
    ledger.set_round_phase("workers", 0, "EVALUATING")
    return evaluator, ledger, ticket


def _evaluate(evaluator, ledger, ticket):
    claimed = ledger.claim_ticket(ticket["ticket_id"], ticket["job_id"])
    bound = {**ticket, "fence": claimed["fence"]}
    with execution_scope(ledger, claimed):
        return evaluator.evaluate(bound)


@pytest.mark.parametrize("settings", [
    {"instance_workers": 0}, {"instance_workers": True}, {"instance_workers": 1.5},
    {"cpu_workers": 0}, {"cpu_workers": True}, {"concurrent_candidates": 0},
])
def test_invalid_worker_settings_are_rejected_before_launch(tmp_path, settings):
    calls = []

    class Runner:
        def run(self, request, limits):
            calls.append(True)
            return _receipt(0)

    with pytest.raises(ValueError):
        _setup(tmp_path, Runner(), **settings)
    assert not calls


@pytest.mark.parametrize(("requested", "cpus", "candidates", "expected"), [
    (1, 8, 4, 1), (2, 8, 4, 2), (10, 8, 4, 2), (10, 2, 4, 1),
    (10, 5, 4, 2), (10, 64, 1, 4),
])
def test_worker_count_respects_requested_panel_and_cpu_limits(
        tmp_path, requested, cpus, candidates, expected):
    evaluator, _, _ = _setup(tmp_path, object(), instance_workers=requested,
                             cpu_workers=cpus, concurrent_candidates=candidates)
    assert evaluator.effective_instance_workers == expected


@pytest.mark.parametrize("workers", [0, -1, True, 1.5])
def test_cpu_pool_rejects_nonpositive_or_noninteger_capacity(workers):
    with pytest.raises(ValueError):
        GlobalCpuPool(workers)


def test_evaluator_rejects_pool_capacity_different_from_frozen_cpu_setting(tmp_path):
    with pytest.raises(ValueError):
        _setup(tmp_path, object(), cpu_workers=2, cpu_pool=GlobalCpuPool(3))


def test_parallel_workers_preserve_ticket_authority_panel_order_and_cpu_sum(tmp_path):
    two_workers = threading.Barrier(2)
    second_finished = threading.Event()
    completed = []
    threads = set()
    main_thread = threading.get_ident()
    bound_ticket = {}

    class Runner:
        def run(self, request, limits):
            # A fresh Context must be copied into each thread. A plain ticket
            # dictionary cannot substitute for the process-local claim.
            require_execution_authority(bound_ticket)
            index = _instance_index(request)
            threads.add(threading.get_ident())
            assert limits.cpus == 1
            if index < 2:
                two_workers.wait(timeout=5)
            if index == 0:
                assert second_finished.wait(timeout=5)
            completed.append(index)
            if index == 1:
                second_finished.set()
            return _receipt(index)

    class Verifier:
        def verify(self, panel, outputs, contract):
            assert [item.cpu_seconds for item in outputs] == [0.25, 0.5, 0.75, 1.0]
            assessed = SimulatedVerificationWorker().verify(panel, outputs, contract)
            return replace(assessed, cpu_seconds=0.125)

    evaluator, ledger, ticket = _setup(tmp_path, Runner(), verifier=Verifier(),
                                      instance_workers=2, cpu_workers=8,
                                      concurrent_candidates=4)
    claimed = ledger.claim_ticket("ticket1", "job1")
    bound_ticket.update(ticket, fence=claimed["fence"])
    with execution_scope(ledger, claimed):
        result = evaluator.evaluate(bound_ticket)
    assert len(threads) == 2 and main_thread not in threads
    assert completed.index(1) < completed.index(0)
    assert [item["instance_id"] for item in result.instance_results] == ["g0", "g1", "g2", "g3"]
    assert result.valid and result.score == 1
    assert result.cpu_seconds == pytest.approx(2.625)
    assert result.diagnostics["run_cpu_seconds"] == pytest.approx(2.5)


def test_instance_workers_cannot_launch_without_dispatcher_claim(tmp_path):
    calls = []

    class Runner:
        def run(self, request, limits):
            calls.append(True)
            return _receipt(0)

    evaluator, _, ticket = _setup(tmp_path, Runner(), instance_workers=4, cpu_workers=8)
    with pytest.raises(AuthorizationError, match="dispatcher"):
        evaluator.evaluate({**ticket, "fence": 1, "status": "CLAIMED"})
    assert not calls


def test_shared_cpu_pool_limits_multiple_candidates_and_verifier(tmp_path):
    pool = GlobalCpuPool(2)
    meter = Meter()
    first_pair = threading.Barrier(2)
    count_lock = threading.Lock()
    solver_entries = 0

    class Runner:
        def run(self, request, limits):
            nonlocal solver_entries
            with meter.entered("solver"):
                with count_lock:
                    solver_entries += 1
                    number = solver_entries
                if number <= 2:
                    first_pair.wait(timeout=5)
                return _receipt(_instance_index(request))

    class Verifier:
        def verify(self, panel, outputs, contract):
            with meter.entered("verifier"):
                return SimulatedVerificationWorker().verify(panel, outputs, contract)

    cases = [_setup(tmp_path / name, Runner(), verifier=Verifier(),
                     instance_workers=2, cpu_workers=2, concurrent_candidates=1,
                     cpu_pool=pool) for name in ("a", "b")]
    start = threading.Barrier(2)

    def run(case):
        start.wait(timeout=5)
        return _evaluate(*case)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run, case) for case in cases]
        results = [future.result(timeout=10) for future in futures]
    assert all(result.valid for result in results)
    assert meter.peak == 2 and meter.active == 0
    assert meter.entries.count("solver") == 8
    assert meter.entries.count("verifier") == 2


def test_worker_failure_joins_active_work_and_stops_new_launches(tmp_path):
    second_entered = threading.Event()
    first_failed = threading.Event()
    release_second = threading.Event()
    second_finished = threading.Event()
    calls = []
    verifier_calls = []

    class Runner:
        def run(self, request, limits):
            index = _instance_index(request)
            calls.append(index)
            if index == 0:
                assert second_entered.wait(timeout=5)
                first_failed.set()
                raise RuntimeError("lost instance executor receipt")
            if index == 1:
                second_entered.set()
                assert release_second.wait(timeout=5)
                second_finished.set()
                return _receipt(index)
            raise AssertionError("failure must stop unstarted instance work")

    class Verifier:
        def verify(self, *args):
            verifier_calls.append(True)
            raise AssertionError("incomplete panel must not reach verification")

    evaluator, ledger, _ = _setup(tmp_path, Runner(), count=8, verifier=Verifier(),
                                 instance_workers=2, cpu_workers=2)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(Dispatcher(ledger, evaluator).dispatch, "ticket1", "job1")
        try:
            assert first_failed.wait(timeout=5)
            # Failure must not release the dispatch scope while another
            # physical instance call is still alive.
            assert not future.done()
        finally:
            release_second.set()
        with pytest.raises(RuntimeError, match="lost instance executor receipt"):
            future.result(timeout=10)
    assert second_finished.is_set()
    assert set(calls) == {0, 1}
    assert not verifier_calls
    assert ledger.ticket("ticket1")["status"] == "UNKNOWN"


def test_worker_rechecks_ticket_after_waiting_for_a_cpu_slot(tmp_path):
    waiting = threading.Event()
    calls = []

    class ObservablePool(GlobalCpuPool):
        @contextmanager
        def slot(self):
            waiting.set()
            with super().slot():
                yield

    class Runner:
        def run(self, request, limits):
            calls.append(True)
            return _receipt(0)

    pool = ObservablePool(1)
    evaluator, ledger, _ = _setup(tmp_path, Runner(), instance_workers=1,
                                 cpu_workers=1, cpu_pool=pool)
    with ThreadPoolExecutor(max_workers=1) as executor:
        # Hold the only physical slot until the ticket's phase is revoked.
        with GlobalCpuPool.slot(pool):
            future = executor.submit(Dispatcher(ledger, evaluator).dispatch, "ticket1", "job1")
            assert waiting.wait(timeout=5)
            ledger.set_round_phase("workers", 0, "RECOVERY_REQUIRED")
        with pytest.raises(AuthorizationError, match="phase"):
            future.result(timeout=10)
    assert not calls


def test_solver_and_verifier_both_hold_the_shared_cpu_slot(tmp_path):
    held = threading.local()
    observed = []

    class ObservablePool(GlobalCpuPool):
        @contextmanager
        def slot(self):
            with super().slot():
                held.depth = getattr(held, "depth", 0) + 1
                try:
                    yield
                finally:
                    held.depth -= 1

    class Runner:
        def run(self, request, limits):
            assert getattr(held, "depth", 0) == 1
            observed.append("solver")
            return _receipt(_instance_index(request))

    class Verifier:
        def verify(self, panel, outputs, contract):
            assert getattr(held, "depth", 0) == 1
            observed.append("verifier")
            return SimulatedVerificationWorker().verify(panel, outputs, contract)

    evaluator, ledger, ticket = _setup(tmp_path, Runner(), verifier=Verifier(),
                                      instance_workers=2, cpu_workers=2,
                                      cpu_pool=ObservablePool(2))
    assert _evaluate(evaluator, ledger, ticket).valid
    assert observed.count("solver") == 4 and observed.count("verifier") == 1


def test_concurrent_jobs_compute_workers_locally_without_mutating_shared_evaluator(tmp_path):
    # Two different batch contracts can overlap on one shared evaluator. Their
    # job-local limits must not overwrite a mutable shared worker setting.
    targets = {"a": 4, "b": 2}
    first_wave = threading.Barrier(sum(targets.values()))
    lock = threading.Lock()
    active = {"a": 0, "b": 0}
    peaks = dict(active)

    class Runner:
        def run(self, request, limits):
            name = request.workdir.parent.name
            index = _instance_index(request)
            with lock:
                active[name] += 1
                peaks[name] = max(peaks[name], active[name])
            try:
                if index < targets[name]:
                    first_wave.wait(timeout=5)
                return _receipt(index)
            finally:
                with lock:
                    active[name] -= 1

    def resource_binding(batch_width, effective):
        return {"cpu_workers": 8, "instance_workers": 4,
                "evaluation_batch_width": batch_width,
                "effective_instance_workers": effective,
                "admission_capacity": batch_width}

    runner = Runner()
    first = _setup(tmp_path / "a", runner, instance_workers=4, cpu_workers=8,
                   concurrent_candidates=4, ticket_overrides=resource_binding(1, 4))
    _, second_ledger, second_ticket = _setup(
        tmp_path / "b", runner, instance_workers=4, cpu_workers=8,
        concurrent_candidates=4, ticket_overrides=resource_binding(4, 2))
    evaluator, first_ledger, first_ticket = first
    assert evaluator.effective_instance_workers == 2
    start = threading.Barrier(2)

    def evaluate(ledger, ticket):
        start.wait(timeout=5)
        return _evaluate(evaluator, ledger, ticket)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(evaluate, ledger, ticket) for ledger, ticket in
                   ((first_ledger, first_ticket), (second_ledger, second_ticket))]
        results = [future.result(timeout=10) for future in futures]
    assert all(result.valid for result in results)
    assert peaks == targets and active == {"a": 0, "b": 0}
    assert evaluator.effective_instance_workers == 2


@pytest.mark.parametrize(("field", "replacement"), [
    ("cpu_workers", 8), ("instance_workers", 3),
    ("evaluation_batch_width", 0), ("evaluation_batch_width", 5),
    ("evaluation_batch_width", True), ("effective_instance_workers", 1),
    ("admission_capacity", 0),
])
def test_ticket_worker_resource_tampering_is_rejected_before_runner(tmp_path, field, replacement):
    calls = []

    class Runner:
        def run(self, request, limits):
            calls.append(True)
            return _receipt(_instance_index(request))

    resource_binding = {"cpu_workers": 4, "instance_workers": 2,
                        "evaluation_batch_width": 2, "effective_instance_workers": 2,
                        "admission_capacity": 2}
    resource_binding[field] = replacement
    evaluator, ledger, ticket = _setup(tmp_path, Runner(), instance_workers=2,
                                      cpu_workers=4, ticket_overrides=resource_binding)
    # Legacy fixture admission accepts its exact payload; this exercises the
    # evaluator's independent frozen-contract validation after a genuine claim.
    with pytest.raises((AuthorizationError, ValueError)):
        _evaluate(evaluator, ledger, ticket)
    assert not calls


@pytest.mark.parametrize("missing", ["cpu_workers", "instance_workers",
                                    "evaluation_batch_width", "effective_instance_workers",
                                    "admission_capacity"])
def test_partial_ticket_worker_binding_is_rejected_before_runner(tmp_path, missing):
    calls = []

    class Runner:
        def run(self, request, limits):
            calls.append(True)
            return _receipt(_instance_index(request))

    resource_binding = {"cpu_workers": 4, "instance_workers": 2,
                        "evaluation_batch_width": 2, "effective_instance_workers": 2,
                        "admission_capacity": 2}
    del resource_binding[missing]
    evaluator, ledger, ticket = _setup(tmp_path, Runner(), instance_workers=2,
                                      cpu_workers=4, ticket_overrides=resource_binding)
    with pytest.raises((AuthorizationError, ValueError)):
        _evaluate(evaluator, ledger, ticket)
    assert not calls
