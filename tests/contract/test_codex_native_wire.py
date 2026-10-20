"""Opt-in installed-Codex wire qualification; all inference responses are local.

This test writes a fabricated, unprivileged ChatGPT auth fixture in a temporary
CODEX_HOME. It never reads/copies the user's auth file or contacts a provider.
It proves the installed binary routes its native requests and tool loop through
the production gateway, not model entitlement or real inference quality.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from urllib.parse import urlsplit

import pytest

from maestro.backends.codex_session.autonomous_client import AutonomousCodexClient
from maestro.backends.codex_session.client import resolve_codex
from maestro.backends.codex_session.quota_proxy import CodexQuotaProxy
from maestro.execution.budgets import BudgetStore


pytestmark = pytest.mark.skipif(os.environ.get("MAESTRO_RUN_NATIVE_WIRE") != "1",
                               reason="Opt-in local Codex binary wire qualification")


def _event(value):
    return b"data: " + json.dumps(value).encode() + b"\n\n"


class _Response:
    status = 200
    headers = {"Content-Type": "text/event-stream"}

    def __init__(self, index, item):
        response = {"id": f"resp_wire_{index}", "object": "response", "created_at": int(time.time()),
            "status": "in_progress", "model": "gpt-6-astra", "output": []}
        completed = {**response, "status": "completed", "output": [item],
            "usage": {"input_tokens": 30, "output_tokens": 5, "total_tokens": 35,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens_details": {"reasoning_tokens": 0}}}
        self.chunks = [_event({"type": "response.created", "response": response}),
            _event({"type": "response.output_item.added", "output_index": 0, "item": item}),
            _event({"type": "response.output_item.done", "output_index": 0, "item": item}),
            _event({"type": "response.completed", "response": completed})]

    def iter_bytes(self, deadline):
        yield from self.chunks

    def close(self):
        pass


def _fake_chatgpt_home(root):
    root.mkdir()
    def encoded(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()
    claims = {"sub": "synthetic-user", "email": "synthetic@example.invalid",
        "exp": int(time.time())+86400,
        "https://api.openai.com/auth": {"chatgpt_account_id": "synthetic-account",
            "chatgpt_user_id": "synthetic-user", "chatgpt_plan_type": "pro"}}
    token = encoded({"alg": "none"}) + "." + encoded(claims) + ".synthetic-unprivileged"
    (root/"auth.json").write_text(json.dumps({"auth_mode": "chatgpt", "OPENAI_API_KEY": None,
        "tokens": {"id_token": token, "access_token": token,
            "refresh_token": "synthetic-unprivileged-refresh", "account_id": "synthetic-account"},
        "last_refresh": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}), encoding="utf-8")
    (root/"config.toml").write_text('cli_auth_credentials_store="file"\n', encoding="utf-8")


def _assert_native_backend_contract(value):
    """A forced fake tool response must not conceal a broken model manifest."""
    assert value.get("store") is False
    assert value.get("stream") is True
    forbidden = {"background", "conversation", "max_output_tokens", "max_tool_calls", "metadata",
        "moderation", "multi_agent", "prompt", "prompt_cache_retention", "safety_identifier",
        "temperature", "top_logprobs", "top_p", "truncation", "user", "previous_response_id"}
    assert not forbidden.intersection(value)
    declarations = list(value.get("tools", []))
    for item in value["input"]:
        assert item.get("role") != "system"
        if item.get("type") == "additional_tools":
            declarations.extend(item.get("tools", []))
    def names(tools):
        result = []
        for tool in tools:
            if tool.get("type") == "namespace":
                result.extend(names(tool.get("tools", [])))
            else:
                assert tool.get("type") == "function"
                result.append(tool.get("name"))
        return result
    assert names(declarations) == ["maestro_tool"]


@pytest.mark.parametrize("request_cap", [1, 3])
def test_installed_codex_oauth_provider_dynamic_tools_use_metered_loopback(tmp_path, request_cap):
    try:
        executable = resolve_codex(os.environ.get("MAESTRO_CODEX_EXECUTABLE"))
    except Exception:
        pytest.skip("Native Codex executable is unavailable")
    fake_home = tmp_path / "codex-home"
    _fake_chatgpt_home(fake_home)
    forwarded, tool_calls, local_requests = [], [], []

    def upstream(path, headers, body, timeout):
        value = json.loads(body)
        _assert_native_backend_contract(value)
        # Store only presence booleans. Raw authorization/account header values
        # must never appear in pytest diagnostics or persistent test artifacts.
        forwarded.append({"path": path,
            "auth_present": any(key.lower() == "authorization" and bool(value) for key, value in headers.items()),
            "account_present": any(key.lower() == "chatgpt-account-id" and bool(value) for key, value in headers.items()),
            "body": value})
        if len(forwarded) == 1:
            item = {"id": "fc_wire_1", "type": "function_call", "status": "completed",
                "call_id": "call_wire_1", "name": "maestro_tool",
                "arguments": json.dumps({"action": "list", "path": "solver/"})}
        else:
            item = {"id": "msg_wire_2", "type": "message", "status": "completed", "role": "assistant",
                "content": [{"type": "output_text", "text": "Native broker loop completed.", "annotations": []}]}
        return _Response(len(forwarded), item)

    deadline = time.monotonic()+30
    store = BudgetStore(tmp_path / "request-budget.sqlite")
    proxy = CodexQuotaProxy(store, "wire:1:a:GENERATION", "gpt-6-astra", deadline=deadline,
        max_calls=request_cap, max_request_bytes=2*1024*1024, max_output_tokens=1000,
        allowed_tool_names=("maestro_tool",), upstream=upstream)
    original_request = proxy._request
    def observed_request(handler):
        observation = {"authorization_present": bool(handler.headers.get("Authorization")),
            "content_encoding": handler.headers.get("Content-Encoding"),
            "transfer_encoding": handler.headers.get("Transfer-Encoding"),
            "content_type": handler.headers.get("Content-Type")}
        local_requests.append(observation)
        try:
            return original_request(handler)
        except Exception as error:
            observation["rejected"] = type(error).__name__
            observation["reason"] = str(error)
            raise
    proxy._request = observed_request
    try:
        base_url = proxy.start()
        origin = urlsplit(base_url)
        # Deflect any unexpected external native discovery/refresh traffic to
        # the local server, which rejects CONNECT. The injected upstream above
        # is the sole inference responder; it cannot make a network connection.
        environment = dict(os.environ, CODEX_HOME=str(fake_home),
            HTTPS_PROXY=f"http://{origin.hostname}:{origin.port}",
            HTTP_PROXY=f"http://{origin.hostname}:{origin.port}",
            ALL_PROXY=f"http://{origin.hostname}:{origin.port}", NO_PROXY="127.0.0.1,localhost,::1")
        client = AutonomousCodexClient(model="gpt-6-astra", work_root=tmp_path/"native-host",
            executable=executable, environment=environment, timeout=30)
        def tool(name, arguments):
            tool_calls.append((name, arguments))
            return {"files": ["solver/run"]}
        if request_cap == 1:
            with pytest.raises(Exception):
                client.run_phase("Use maestro_tool to list solver/ once, then report completion.",
                    base_url=base_url, deadline=deadline, tools=tool)
            proxy.assert_resolved()
            assert len(forwarded) == 1
            assert len(tool_calls) == 1
            assert proxy.usage()["actual_requests"] == 1
            assert proxy.quota_denied is True
            return
        try:
            response = client.run_phase("Use maestro_tool to list solver/ once, then report completion.",
                base_url=base_url, deadline=deadline, tools=tool)
        except Exception as error:
            pytest.fail(f"Native loop failed ({type(error).__name__}); local request metadata: {local_requests}; "
                        f"forwarded requests: {len(forwarded)}")
        proxy.assert_resolved()
        assert response.text == "Native broker loop completed."
        assert response.tool_calls == 1
        assert tool_calls == [("maestro_tool", {"action": "list", "path": "solver/"})]
        assert len(forwarded) == 2
        assert all(item["auth_present"] for item in forwarded)
        assert all(item["account_present"] for item in forwarded)
        assert all(item["path"] == "/responses" for item in forwarded)
        assert proxy.usage()["actual_requests"] == 2
        assert proxy.usage()["protocol_violations"] == 0
        assert any(item.get("type") == "function_call_output"
                   for item in forwarded[1]["body"]["input"])
        # A new process and phase capability must resume the same native
        # private history while sending its new request to the NEW gateway.
        proxy.close()
        next_proxy = CodexQuotaProxy(store, "wire:1:a:GENERATION-RESUME", "gpt-6-astra",
            deadline=deadline, max_calls=1, max_request_bytes=2*1024*1024,
            max_output_tokens=1000, allowed_tool_names=("maestro_tool",), upstream=upstream)
        try:
            next_url = next_proxy.start()
            # Keep outbound proxy rejection aligned with the live local server.
            next_origin = urlsplit(next_url)
            for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
                client.env[name] = f"http://{next_origin.hostname}:{next_origin.port}"
            resumed = client.run_phase("Keep the previous private tool evidence; report completion.",
                base_url=next_url, deadline=deadline, tools=tool, session_id=response.thread_id)
            next_proxy.assert_resolved()
            assert resumed.thread_id == response.thread_id
            assert resumed.text == "Native broker loop completed."
            assert len(forwarded) == 3
            assert next_proxy.usage()["actual_requests"] == 1
            assert any(item.get("type") == "function_call_output"
                       for item in forwarded[2]["body"]["input"])
        finally:
            next_proxy.close()
    finally:
        proxy.close()
        store.close()
