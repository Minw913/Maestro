from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from maestro.execution.budgets import BudgetStore, BudgetExceeded
from maestro.execution.gateway import BudgetGateway, GatewayHTTPServer
from maestro.execution.grants import GrantAuthority, GrantError
from maestro.execution.phases import PhaseController, PhaseError, Phase
from maestro.execution.generation import bounded_generate, precheck_mock
from maestro.execution.sandbox import FailClosedRunner, ResourceLimits, RunRequest, SandboxUnavailable, DockerRunner


def account(store, name="phase", calls=1):
    store.create(name, max_calls=calls, max_request_bytes=100_000,
                 max_output_tokens=1000, deadline=time.time()+60)


def test_budget_survives_restart_and_cannot_expand(tmp_path):
    path = tmp_path / "quota.sqlite"
    store = BudgetStore(path); account(store)
    reservation = store.reserve("phase", "request", request_bytes=10, output_tokens=100)
    store.close()
    store = BudgetStore(path)
    with pytest.raises(BudgetExceeded):
        store.reserve("phase", "retry", request_bytes=10, output_tokens=100)
    with pytest.raises(BudgetExceeded):
        account(store, calls=3)
    store.finish(reservation, actual_output_tokens=20)
    assert store.snapshot("phase")["calls"] == 1
    assert store.snapshot("phase")["output_tokens"] == 20
    store.close()


def test_budget_atomic_concurrent_claims(tmp_path):
    store = BudgetStore(tmp_path / "quota.sqlite"); account(store)
    def reserve(index):
        try:
            store.reserve("phase", str(index), request_bytes=10, output_tokens=10)
            return True
        except BudgetExceeded:
            return False
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(reserve, range(8))) == 1
    store.close()


def test_transport_error_does_not_refund_a_call(tmp_path):
    store = BudgetStore(tmp_path / "quota.sqlite"); account(store)
    def failure(payload):
        raise ConnectionError("Ambiguous provider response")
    gateway = BudgetGateway(store, failure)
    token = gateway.credential("phase", "a")
    with pytest.raises(ConnectionError):
        gateway.complete(token, "r1", {"max_tokens": 100})
    with pytest.raises(BudgetExceeded):
        gateway.complete(token, "r2", {"max_tokens": 100})
    assert store.snapshot("phase")["output_tokens"] == 100
    store.close()


def test_gateway_http_rejects_before_transport(tmp_path):
    store = BudgetStore(tmp_path / "quota.sqlite"); account(store)
    calls = []
    gateway = BudgetGateway(store, lambda payload: calls.append(payload) or {"usage": {"completion_tokens": 3}})
    token = gateway.credential("phase", "a")
    server = GatewayHTTPServer(gateway).start()
    try:
        request = lambda: Request(server.url+"/v1/chat/completions", json.dumps({"max_tokens": 10}).encode(),
                                 {"Authorization": "Bearer "+token}, method="POST")
        with urlopen(request()) as response:
            assert response.status == 200
        with pytest.raises(HTTPError) as error:
            urlopen(request())
        assert error.value.code == 429
        assert len(calls) == 1
    finally:
        server.close(); store.close()


def test_quota_cannot_be_bypassed_by_stream_or_multiple_completions(tmp_path):
    store = BudgetStore(tmp_path / "quota.sqlite"); account(store)
    gateway = BudgetGateway(store, lambda payload: {})
    token = gateway.credential("phase", "a")
    for payload in ({"stream": True, "max_tokens": 10}, {"n": 2, "max_tokens": 10}, {}):
        with pytest.raises(BudgetExceeded):
            gateway.complete(token, "id", payload)
    assert store.snapshot("phase")["calls"] == 0
    store.close()


def test_edit_grant_is_plan_parent_epoch_bound_and_revocable():
    authority = GrantAuthority(b"k"*32, clock=lambda: 100)
    grant = authority.issue(episode_id="e", round_index=0, agent_id="a", plan_version_id="p",
                            parent_hash="h", epoch=2, expires_at=200)
    authority.verify(grant, agent_id="a", plan_version_id="p", parent_hash="h", epoch=2)
    for forged in (replace(grant, parent_hash="other"), replace(grant, expires_at=1000)):
        with pytest.raises(GrantError):
            authority.verify(forged, agent_id="a", plan_version_id="p", parent_hash="h", epoch=2)
    with pytest.raises(GrantError):
        authority.verify(grant, agent_id="a", plan_version_id="p", parent_hash="h", epoch=1)
    authority.revoke(grant)
    with pytest.raises(GrantError):
        authority.verify(grant, agent_id="a", plan_version_id="p", parent_hash="h", epoch=2)


def test_phase_waits_for_barrier_and_process_tree_stop():
    clock = [0]
    controller = PhaseController(0, clock=lambda: clock[0])
    controller.begin(["a", "b"], 10)
    controller.arrive("a", 0)
    with pytest.raises(PhaseError):
        controller.advance(processes_stopped=True)
    clock[0] = 11
    with pytest.raises(PhaseError):
        controller.advance(processes_stopped=False, force_deadline=True)
    assert controller.advance(processes_stopped=True, force_deadline=True) == Phase.PLAN


def test_generation_repair_is_bounded_and_shared_deadline():
    generated = []
    result = bounded_generate(lambda i: generated.append(i) or i,
                              lambda candidate: (candidate == 2, "syntax"), deadline=100,
                              clock=lambda: 0)
    assert result.passed and result.attempts == 3 and generated == [0, 1, 2]
    assert not bounded_generate(lambda i: i, lambda c: (True, "PASS"),
                                deadline=1, clock=lambda: 2).passed
    assert precheck_mock("a", 1, {"a:1": ["precheck", None]}, revision=0)[0] is False
    assert precheck_mock("a", 1, {"a:1": ["precheck", None]}, revision=1)[0] is True


def test_no_unsafe_native_fallback(tmp_path):
    with pytest.raises(SandboxUnavailable):
        FailClosedRunner().run(RunRequest(("python", "candidate.py"), tmp_path), ResourceLimits())
    with pytest.raises(SandboxUnavailable):
        DockerRunner().preflight()
