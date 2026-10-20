"""Network-free native request gate tests; no Codex or provider is launched."""
from __future__ import annotations

import http.client
import json
import ssl
import threading
import time
from urllib.parse import urlsplit

import pytest

from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy, _https_upstream
from maestro.execution.budgets import BudgetExceeded, BudgetStore


def event(value):
    return b"data: " + json.dumps(value).encode() + b"\n\n"


def terminal(*, usage=True, output=None):
    value = {"id": "resp_mock", "status": "completed", "output": output or []}
    if usage:
        value["usage"] = {"input_tokens": 10, "output_tokens": 2,
                          "input_tokens_details": {"cached_tokens": 3}}
    return event({"type": "response.completed", "response": value})


class FakeResponse:
    def __init__(self, chunks=None, *, status=200, content_type="text/event-stream"):
        self.chunks = [terminal()] if chunks is None else chunks
        self.status = status
        self.headers = {"Content-Type": content_type}
        self.closed = False

    def iter_bytes(self, deadline):
        for chunk in self.chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk

    def close(self):
        self.closed = True


@pytest.fixture
def gateway(tmp_path):
    store = BudgetStore(tmp_path / "requests.sqlite")
    proxies = []
    def make(upstream, *, account="episode:1:a:GENERATE", **kwargs):
        config = {"deadline": time.monotonic()+15, "max_calls": 3,
                  "max_request_bytes": 131072, "max_output_tokens": 300}
        config.update(kwargs)
        proxy = CodexQuotaProxy(store, account, "test-model", upstream=upstream, **config)
        proxies.append(proxy)
        return proxy, proxy.start()
    yield store, make
    for proxy in proxies:
        proxy.close()
    store.close()


def request(base, payload=None, *, suffix="/responses", method="POST", headers=None):
    url = urlsplit(base)
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=5)
    raw = json.dumps({"model": "test-model", "input": "test"} if payload is None else payload).encode()
    actual_headers = {"Authorization": "Bearer only-in-memory-secret", "ChatGPT-Account-ID": "private-account",
                      "Content-Type": "application/json"}
    if headers:
        actual_headers.update(headers)
    connection.request(method, url.path+suffix, body=raw, headers=actual_headers)
    response = connection.getresponse()
    status, body = response.status, response.read()
    connection.close()
    return status, body


def test_each_actual_request_reserved_before_forward_and_capped(gateway):
    store, make = gateway
    calls = []
    def upstream(path, headers, body, timeout):
        snapshot = store.snapshot("episode:1:a:GENERATE")
        assert snapshot["calls"] == len(calls)+1
        assert snapshot["active"] == 1
        calls.append((path, dict(headers), json.loads(body)))
        return FakeResponse()
    proxy, base = make(upstream)
    for _ in range(3):
        assert request(base)[0] == 200
    assert request(base)[0] == 429
    assert len(calls) == 3
    assert all("max_output_tokens" not in item[2] for item in calls)
    usage = proxy.usage()
    assert usage["actual_requests"] == 3
    assert usage["request_count_unknown"] is False
    assert usage["known_input_tokens"] == 30
    assert usage["known_output_tokens"] == 6
    assert usage["inflight_unknown"] is False
    assert usage["quota_denied"] is True
    assert usage["output_token_limit_enforced"] is False


def test_compaction_consumes_same_phase_budget(gateway):
    _store, make = gateway
    paths = []
    def upstream(path, *_args):
        paths.append(path)
        if path.endswith("/compact"):
            value = {"id": "cmp_1", "object": "response.compaction", "output": [{"type": "compaction"}],
                     "usage": {"input_tokens": 4, "output_tokens": 1}}
            return FakeResponse([json.dumps(value).encode()], content_type="application/json")
        return FakeResponse()
    proxy, base = make(upstream, max_calls=1)
    assert request(base, suffix="/responses/compact")[0] == 200
    assert request(base)[0] == 429
    assert paths == ["/responses/compact"]
    assert proxy.usage()["calls"] == 1


def test_compaction_does_not_gain_unsupported_tool_fields(gateway):
    _store, make = gateway
    received = []
    def upstream(_path, _headers, body, _timeout):
        received.append(json.loads(body))
        return FakeResponse([json.dumps({"object": "response.compaction", "output": []}).encode()],
                            content_type="application/json")
    _proxy, base = make(upstream, allowed_tool_names=("maestro_tool",))
    original = {"model": "test-model", "input": [{"type": "message", "role": "user", "content": []}]}
    assert request(base, original, suffix="/responses/compact")[0] == 200
    assert received == [original]


@pytest.mark.parametrize("chunks", [[], [b"data: [DONE]\n\n"], [event({"type": "response.created"}), OSError("transport lost")]])
def test_ambiguous_transport_blocks_retries_and_reopening(gateway, chunks):
    store, make = gateway
    calls = []
    def upstream(*_args):
        calls.append(1)
        return FakeResponse(chunks)
    proxy, base = make(upstream)
    assert request(base)[0] == 502
    assert request(base)[0] == 429
    assert len(calls) == 1
    assert proxy.usage()["inflight_unknown"] is True
    assert proxy.quota_denied is False
    with pytest.raises(BudgetExceeded):
        proxy.assert_resolved()
    proxy.close()
    with pytest.raises(BudgetExceeded):
        make(upstream)
    rows = store.connection.execute("SELECT status FROM inference_reservations").fetchall()
    assert [row[0] for row in rows] == ["UNKNOWN"]


def test_missing_usage_is_unknown_metering_not_unknown_execution(gateway):
    _store, make = gateway
    proxy, base = make(lambda *_args: FakeResponse([terminal(usage=False)]))
    assert request(base)[0] == 200
    proxy.assert_resolved()
    usage = proxy.usage()
    assert usage["usage_complete"] is False
    assert usage["unresolved_requests"] == 0
    assert usage["unknown_requests"] == 1
    assert request(base)[0] == 200


def test_excess_observed_tokens_preserve_receipt_without_unknown_execution(gateway):
    _store, make = gateway
    response = {"id": "resp_1", "status": "completed", "output": [],
                "usage": {"input_tokens": 4, "output_tokens": 200}}
    proxy, base = make(lambda *_args: FakeResponse([event({"type": "response.completed", "response": response})]))
    assert request(base)[0] == 200
    usage = proxy.usage()
    assert usage["known_output_tokens"] == 200
    assert usage["charged_output_tokens"] == 100
    assert usage["token_ceiling_exceeded"] == 1
    assert usage["inflight_unknown"] is False


def test_only_approved_dynamic_tool_manifest_reaches_upstream(gateway):
    _store, make = gateway
    bodies = []
    def upstream(_path, _headers, body, _timeout):
        bodies.append(json.loads(body))
        return FakeResponse()
    _proxy, base = make(upstream, allowed_tool_names=("maestro_tool",))
    payload = {"model": "test-model", "tools": [
        {"type": "function", "name": "maestro_tool", "parameters": {}},
        {"type": "function", "name": "exec_command", "parameters": {}},
        {"type": "web_search"}, {"type": "local_shell"}]}
    assert request(base, payload)[0] == 200
    assert [tool["name"] for tool in bodies[0]["tools"]] == ["maestro_tool"]
    assert bodies[0]["tools"][0]["parameters"]["required"] == ["action"]
    assert bodies[0]["tools"][0]["strict"] is False


@pytest.mark.parametrize("allowed", [(), ("maestro_tool",)])
def test_embedded_native_namespaces_are_replaced_with_trusted_broker(gateway, allowed):
    _store, make = gateway
    received = []
    def upstream(_path, _headers, body, _timeout):
        received.append(json.loads(body))
        return FakeResponse()
    _proxy, base = make(upstream, allowed_tool_names=allowed)
    native = {"model": "test-model", "store": False, "stream": True,
        "input": [
            {"type": "additional_tools", "role": "developer", "id": "at_native", "tools": [
                {"type": "namespace", "name": "functions", "tools": [
                    {"type": "custom", "name": "exec", "description": "native host shell"},
                    {"type": "function", "name": "request_user_input", "parameters": {}}]},
                {"type": "namespace", "name": "clock", "tools": [{"type": "function", "name": "sleep"}]}]},
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Keep this context"}]},
        ]}
    assert request(base, native)[0] == 200
    forwarded = received[0]
    assert "tools" not in forwarded
    assert forwarded["store"] is False and forwarded["stream"] is True
    assert native["input"][1] in forwarded["input"]
    embedded = [item for item in forwarded["input"] if item.get("type") == "additional_tools"]
    if allowed:
        assert len(embedded) == 1
        assert embedded[0]["id"].startswith("at_") and len(embedded[0]["id"]) == 39
        assert [tool["name"] for tool in embedded[0]["tools"]] == ["maestro_tool"]
        assert embedded[0]["tools"][0]["parameters"]["required"] == ["action"]
    else:
        assert embedded == []
    assert b'"name": "exec"' not in json.dumps(forwarded).encode()
    assert b'"name": "clock"' not in json.dumps(forwarded).encode()


@pytest.mark.parametrize("item", [
    {"type": "local_shell_call", "action": {"command": "echo forbidden"}},
    {"type": "function_call", "name": "exec_command", "arguments": "{}"},
    {"type": "custom_tool_call", "name": "unknown", "input": "anything"},
    {"type": "web_search_call"}, {"type": "computer_call"},
])
def test_forbidden_tool_events_never_reach_native_client(gateway, item):
    _store, make = gateway
    chunks = [event({"type": "response.output_item.added", "item": item}), terminal(output=[item])]
    proxy, base = make(lambda *_args: FakeResponse(chunks), allowed_tool_names=("maestro_tool",))
    status, body = request(base)
    assert status == 502
    assert b"echo forbidden" not in body
    assert b"function_call" not in body
    assert proxy.usage()["protocol_violations"] == 1
    assert proxy.usage()["unresolved_requests"] == 0


def test_allowed_broker_tool_roundtrip_and_terminal_truncation(gateway):
    _store, make = gateway
    item = {"type": "function_call", "name": "maestro_tool", "call_id": "call_1", "arguments": "{}"}
    safe = event({"type": "response.output_item.added", "item": item})+terminal(output=[item])
    unsafe_tail = event({"type": "response.output_item.added", "item": {"type": "local_shell_call"}})
    _proxy, base = make(lambda *_args: FakeResponse([safe+unsafe_tail]), allowed_tool_names=("maestro_tool",))
    status, body = request(base)
    assert status == 200
    assert body == safe


def test_forbidden_tool_in_nonterminal_response_envelope_is_blocked(gateway):
    _store, make = gateway
    item = {"type": "function_call", "name": "exec_command", "arguments": "{}"}
    chunks = [event({"type": "response.created", "response": {"output": [item]}}), terminal()]
    proxy, base = make(lambda *_args: FakeResponse(chunks), allowed_tool_names=("maestro_tool",))
    status, body = request(base)
    assert status == 502
    assert b"exec_command" not in body
    assert proxy.usage()["protocol_violations"] == 1
    assert proxy.usage()["unresolved_requests"] == 0


@pytest.mark.parametrize("suffix", ["/models", "/responses?upstream=evil", "/responses/../models", "/responses/compact/", "//responses"])
def test_arbitrary_routes_are_rejected_without_dispatch(gateway, suffix):
    _store, make = gateway
    def forbidden(*_args):
        pytest.fail("Unauthorized route forwarded")
    proxy, base = make(forbidden)
    assert request(base, suffix=suffix)[0] == 403
    assert proxy.usage()["calls"] == 0


@pytest.mark.parametrize("payload", [
    {"model": "different-model"},
    {"model": "test-model", "tool_choice": {"type": "web_search"}},
    {"model": "test-model", "tool_choice": {"type": "function", "name": "exec_command"}},
    {"model": "test-model", "max_output_tokens": True},
])
def test_conflicting_model_or_tool_choice_is_rejected(gateway, payload):
    _store, make = gateway
    def forbidden(*_args):
        pytest.fail("Unauthorized model/tool choice forwarded")
    proxy, base = make(forbidden)
    assert request(base, payload)[0] in {400, 403}
    assert proxy.usage()["calls"] == 0


def test_upgrade_is_rejected_and_credentials_never_persist(gateway):
    store, make = gateway
    captures = []
    def upstream(_path, headers, _body, _timeout):
        captures.append(dict(headers))
        return FakeResponse()
    proxy, base = make(upstream)
    assert request(base, headers={"Connection": "Upgrade", "Upgrade": "websocket"})[0] == 403
    assert request(base, headers={"Cookie": "must-not-forward", "X-Other": "must-not-forward"})[0] == 200
    forwarded = {key.lower(): value for key, value in captures[0].items()}
    assert forwarded["authorization"] == "Bearer only-in-memory-secret"
    assert forwarded["chatgpt-account-id"] == "private-account"
    assert "cookie" not in forwarded
    dump = "\n".join(store.connection.iterdump())
    assert "only-in-memory-secret" not in dump
    assert "private-account" not in dump
    assert "must-not-forward" not in dump
    assert proxy.usage()["calls"] == 1


def test_completed_account_reuse_does_not_reset_calls_or_deadline(gateway):
    store, make = gateway
    proxy, base = make(lambda *_args: FakeResponse(), max_calls=1)
    assert request(base)[0] == 200
    old = store.snapshot(proxy.account)
    proxy.close()
    replacement, again = make(lambda *_args: FakeResponse(), max_calls=1)
    assert request(again)[0] == 429
    assert store.snapshot(proxy.account)["deadline"] == old["deadline"]
    assert replacement.usage()["calls"] == 1
    with pytest.raises(BudgetExceeded):
        make(lambda *_args: FakeResponse(), max_calls=4)


def test_completed_protocol_violation_is_durable_across_proxy_reuse(gateway):
    _store, make = gateway
    item = {"type": "local_shell_call", "action": {"command": "never execute"}}
    proxy, base = make(lambda *_args: FakeResponse([terminal(output=[item])]))
    assert request(base)[0] == 502
    proxy.close()
    replacement, _base = make(lambda *_args: FakeResponse())
    assert replacement.usage()["protocol_violations"] == 1
    assert replacement.usage()["inflight_unknown"] is False


def test_non_success_upstream_response_is_not_forwarded_or_retried(gateway):
    _store, make = gateway
    proxy, base = make(lambda *_args: FakeResponse([b"private upstream diagnostics"], status=302))
    status, body = request(base)
    assert status == 502
    assert b"private upstream" not in body
    assert proxy.usage()["unresolved_requests"] == 1
    assert request(base)[0] == 429


@pytest.mark.parametrize("status", [400, 401, 403, 404, 405, 413, 415, 422, 429])
def test_explicit_upstream_rejection_finishes_and_persists_only_safe_diagnostics(gateway, status):
    store, make = gateway
    error = {"error": {"type": "invalid_request_error", "code": "unsupported_parameter",
                       "message": "Bearer private-provider-secret; private-account; unsupported field details"}}
    proxy, base = make(lambda *_args: FakeResponse([json.dumps(error).encode()], status=status,
                                                  content_type="application/json"))
    code, body = request(base)
    assert code == 502
    assert b"private-provider-secret" not in body
    usage = proxy.usage()
    assert usage["unresolved_requests"] == 0
    assert usage["calls"] == 1
    assert usage["terminal_failures"] == 1
    assert usage["transport_errors"] == [{
        "http_status": status, "error_type": "invalid_request_error", "error_code": "unsupported_parameter",
        "exception_class": "QuotaProxyError", "stage": "upstream_status", "response_format": "json",
        "known_rejected": True,
    }]
    proxy.assert_resolved()
    dump = "\n".join(store.connection.iterdump())
    assert "private-provider-secret" not in dump
    assert "private-account" not in dump


def test_tls_failure_persists_exception_class_without_message_and_remains_unknown(gateway):
    store, make = gateway
    def upstream(*_args):
        raise ssl.SSLError("Bearer private-TLS-diagnostic-secret")
    proxy, base = make(upstream)
    assert request(base)[0] == 502
    usage = proxy.usage()
    assert usage["unresolved_requests"] == 1
    assert usage["transport_errors"] == [{
        "http_status": None, "error_type": "", "error_code": "", "exception_class": "SSLError",
        "stage": "upstream_connect", "response_format": "", "known_rejected": False,
    }]
    assert "private-TLS-diagnostic-secret" not in "\n".join(store.connection.iterdump())


def test_error_labels_with_credentials_or_messages_are_not_persisted(gateway):
    store, make = gateway
    error = {"error": {"type": "Bearer private-secret", "code": "https://credentials.example/private"}}
    proxy, base = make(lambda *_args: FakeResponse([json.dumps(error).encode()], status=403,
                                                  content_type="application/json"))
    assert request(base)[0] == 502
    diagnostic = proxy.usage()["transport_errors"][0]
    assert diagnostic["error_type"] == diagnostic["error_code"] == ""
    assert "private-secret" not in "\n".join(store.connection.iterdump())


def test_upstream_server_error_keeps_request_unresolved(gateway):
    _store, make = gateway
    body = json.dumps({"error": {"type": "server_error", "code": "internal_error"}}).encode()
    proxy, base = make(lambda *_args: FakeResponse([body], status=503, content_type="application/json"))
    assert request(base)[0] == 502
    usage = proxy.usage()
    assert usage["unresolved_requests"] == 1
    assert usage["transport_errors"][0]["http_status"] == 503
    assert usage["transport_errors"][0]["known_rejected"] is False


@pytest.mark.parametrize("kind,status", [
    ("response.failed", "failed"), ("response.incomplete", "incomplete"),
    ("response.completed", "failed"),
])
def test_known_unsuccessful_terminal_is_finished_execution(gateway, kind, status):
    _store, make = gateway
    value = {"id": "resp_failed", "status": status, "output": [],
             "usage": {"input_tokens": 4, "output_tokens": 1},
             "error": {"message": "private upstream diagnostics"}}
    proxy, base = make(lambda *_args: FakeResponse([event({"type": kind, "response": value})]))
    code, body = request(base)
    assert code == 502
    assert b"private upstream" not in body
    usage = proxy.usage()
    assert usage["calls"] == 1
    assert usage["unresolved_requests"] == 0
    assert usage["known_input_tokens"] == 4
    assert usage["known_output_tokens"] == 1
    assert usage["terminal_failures"] == 1
    assert usage["protocol_violations"] == 0
    proxy.assert_resolved()
    proxy.close()
    replacement, _base = make(lambda *_args: FakeResponse())
    assert replacement.usage()["terminal_failures"] == 1


def test_close_drains_handler_before_store_can_close(gateway):
    _store, make = gateway
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    class BlockingResponse(FakeResponse):
        def iter_bytes(self, deadline):
            entered.set()
            assert release.wait(5)
            raise OSError("transport stopped")
            yield b""  # pragma: no cover
        def close(self):
            release.set()
            super().close()
    proxy, base = make(lambda *_args: BlockingResponse())
    requester = threading.Thread(target=lambda: (request(base), finished.set()))
    requester.start()
    assert entered.wait(5)
    proxy.close()
    requester.join(5)
    assert finished.is_set()
    assert proxy.usage()["unresolved_requests"] == 1


def test_production_upstream_uses_fixed_tls_origin_and_disables_redirect(monkeypatch):
    calls = []
    class Connection:
        def __init__(self, host, port, timeout):
            calls.append((host, port, timeout))
            self.sock = None
        def request(self, method, path, body, headers):
            calls.append((method, path))
        def getresponse(self):
            class Response:
                status = 302
                def getheaders(self):
                    return [("Location", "https://untrusted.example")]
                def close(self):
                    pass
            return Response()
        def close(self):
            pass
    monkeypatch.setattr(http.client, "HTTPSConnection", Connection)
    response = _https_upstream("/responses", {"Authorization": "Bearer never-log"}, b"{}", 1)
    assert response.status == 302
    assert calls == [("chatgpt.com", 443, 1), ("POST", "/backend-api/codex/responses")]
    response.close()
