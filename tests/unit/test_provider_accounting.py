import hashlib
import json
import time

import pytest

from maestro.execution.budgets import BudgetExceeded, BudgetStore
from maestro.execution.gateway import BudgetGateway


HASH = hashlib.sha256(b"provider-request").hexdigest()


@pytest.fixture
def store(tmp_path):
    budget = BudgetStore(tmp_path / "budgets.sqlite")
    budget.create("generation", max_calls=3, max_request_bytes=10000,
                  max_output_tokens=100, deadline=time.time() + 60)
    yield budget
    budget.close()


def test_anthropic_cache_tokens_are_included_once_in_total_input(store):
    reservation = store.reserve("generation", "one", request_bytes=10, output_tokens=20)
    raw = {"input_tokens": 10, "cache_read_input_tokens": 100,
           "cache_creation_input_tokens": 20, "output_tokens": 5}
    normalized = store.save_receipt(reservation, raw, HASH, "provider-one", protocol="/v1/messages")
    store.finish(reservation, actual_output_tokens=5)
    assert normalized["input_tokens"] == 130
    usage = store.usage("generation")
    assert usage["input_tokens"] == 130 and usage["output_tokens"] == 5
    assert usage["cache_read_input_tokens"] == 100
    assert usage["cache_creation_input_tokens"] == 20
    assert usage["reserved_output_tokens"] == 20
    assert usage["charged_output_tokens"] == 5
    assert usage["usage_complete"]
    row = store.connection.execute("SELECT usage_json,provider_request_id FROM provider_receipts").fetchone()
    assert json.loads(row[0]) == raw and row[1] == "provider-one"


def test_openai_cached_subset_is_not_added_twice(store):
    reservation = store.reserve("generation", "one", request_bytes=10, output_tokens=20)
    store.save_receipt(reservation, {"prompt_tokens": 100, "completion_tokens": 7,
        "prompt_tokens_details": {"cached_tokens": 80}}, HASH, protocol="/v1/chat/completions")
    store.finish(reservation, actual_output_tokens=7)
    usage = store.usage("generation")
    assert usage["input_tokens"] == 100
    assert usage["cache_read_input_tokens"] == 80
    assert usage["output_tokens"] == 7


def test_failed_or_missing_usage_remains_unknown_and_conservative(store):
    reservation = store.reserve("generation", "one", request_bytes=10, output_tokens=20)
    store.finish(reservation, failed=True)
    usage = store.usage("generation")
    assert not usage["usage_complete"] and usage["unknown_requests"] == 1
    assert usage["input_tokens"] is None and usage["output_tokens"] is None
    assert usage["known_output_tokens"] == 0
    assert usage["reserved_output_tokens"] == usage["charged_output_tokens"] == 20
    assert store.snapshot("generation")["active"] == 1
    row = store.connection.execute("SELECT status,actual_output_tokens FROM inference_reservations").fetchone()
    assert row["status"] == "UNKNOWN" and row["actual_output_tokens"] is None
    with pytest.raises(BudgetExceeded):
        store.reserve("generation", "second-request", request_bytes=10, output_tokens=20)


def test_transport_exception_cannot_release_concurrency_even_with_unused_calls(store):
    def disconnected(payload):
        raise ConnectionError("provider execution status is unknown")
    gateway = BudgetGateway(store, disconnected)
    token = gateway.credential("generation", "agent")
    with pytest.raises(ConnectionError):
        gateway.complete(token, "first", {"max_tokens": 20})
    assert store.snapshot("generation")["calls"] == 1
    assert store.snapshot("generation")["max_calls"] == 3
    with pytest.raises(BudgetExceeded):
        gateway.complete(token, "second", {"max_tokens": 20})
    assert store.snapshot("generation")["calls"] == 1


def test_only_authoritative_reconciliation_releases_unknown_request(store):
    reservation = store.reserve("generation", "one", request_bytes=10, output_tokens=20)
    store.finish(reservation, failed=True)
    with pytest.raises(BudgetExceeded, match="evidence"):
        store.confirm_finished(reservation, proof={"confirmed_finished": True})
    store.confirm_finished(reservation, proof={"confirmed_finished": True, "provider_evidence": "terminal request status"},
                           actual_output_tokens=3)
    assert store.snapshot("generation")["active"] == 0
    assert store.snapshot("generation")["output_tokens"] == 3
    assert store.snapshot("generation")["calls"] == 1
    store.reserve("generation", "second", request_bytes=10, output_tokens=20)


def test_partial_usage_does_not_fabricate_input_cost(store):
    reservation = store.reserve("generation", "one", request_bytes=10, output_tokens=20)
    store.save_receipt(reservation, {"output_tokens": 7}, HASH)
    store.finish(reservation, actual_output_tokens=7)
    usage = store.usage("generation")
    assert usage["input_tokens"] is None and usage["output_tokens"] is None
    assert usage["known_input_tokens"] == 0 and usage["known_output_tokens"] == 7
    assert not usage["usage_complete"]


def test_provider_receipt_idempotency_and_conflicting_write(store):
    reservation = store.reserve("generation", "one", request_bytes=10, output_tokens=20)
    raw = {"input_tokens": 3, "output_tokens": 4}
    store.save_receipt(reservation, raw, HASH)
    store.save_receipt(reservation, raw, HASH)
    assert store.connection.execute("SELECT count(*) FROM provider_receipts").fetchone()[0] == 1
    with pytest.raises(BudgetExceeded, match="Conflicting"):
        store.save_receipt(reservation, {"input_tokens": 30, "output_tokens": 4}, HASH)


def test_gateway_persists_usage_before_refunding_output_reservation(store):
    transport = lambda payload: {"usage": {"input_tokens": 3, "output_tokens": 4},
                                 "provider_request_id": "provider-one"}
    gateway = BudgetGateway(store, transport, allowed_model="model")
    token = gateway.credential("generation", "agent")
    gateway.complete(token, "request-one", {"model": "model", "max_tokens": 20,
                                           "_maestro_http_path": "/v1/messages"})
    usage = store.usage("generation")
    assert usage["calls"] == 1 and usage["usage_complete"]
    assert usage["input_tokens"] == 3 and usage["output_tokens"] == 4
    assert usage["reserved_output_tokens"] == 20 and usage["charged_output_tokens"] == 4


@pytest.mark.parametrize("bad", [-1, 1.5, True])
def test_malformed_token_counts_never_become_authoritative_measurements(store, bad):
    reservation = store.reserve("generation", "one", request_bytes=10, output_tokens=20)
    store.save_receipt(reservation, {"input_tokens": bad, "output_tokens": 3}, HASH)
    assert not store.usage("generation")["usage_complete"]
    with pytest.raises(BudgetExceeded):
        store.finish(reservation, actual_output_tokens=bad)
