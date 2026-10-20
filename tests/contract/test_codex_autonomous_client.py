"""App-server transport contracts; no model, account, or network access."""
from __future__ import annotations

import io
import json
import queue
import threading
import time

import pytest

from maestro.backends.codex_session.autonomous_client import (
    AppServerProtocolError, AutonomousCodexClient, PROVIDER_NAME, TOOL_NAME,
)


class _Lines:
    def __init__(self):
        self.values = queue.Queue()

    def put(self, value):
        self.values.put(json.dumps(value) + "\n")

    def readline(self, size=-1):
        return self.values.get()


class _Writer:
    def __init__(self, process):
        self.process = process

    def write(self, value):
        self.process.receive(json.loads(value))

    def flush(self):
        pass


class _Process:
    def __init__(self, mode="normal", *, session="private-thread", held=False, config=None):
        self.stdout, self.stderr = _Lines(), io.StringIO("")
        self.stdin = _Writer(self)
        self.mode, self.session, self.messages = mode, session, []
        self.returncode, self.terminated, self.held = None, False, held
        self.config = config or {"mcp_servers": {}, "plugins": {}, "features": {"hooks": False}, "notify": []}
        self.config.setdefault("model_providers", {PROVIDER_NAME: {
            "base_url": "http://127.0.0.1:1234/phase/capability", "wire_api": "responses",
            "requires_openai_auth": True, "supports_websockets": False,
            "request_max_retries": 0, "stream_max_retries": 0}})

    def receive(self, message):
        self.messages.append(message)
        method, request_id = message.get("method"), message.get("id")
        if method == "initialize":
            self.stdout.put({"id": request_id, "result": {"userAgent": "test"}})
        elif method == "config/read":
            self.stdout.put({"id": request_id, "result": {"config": self.config}})
        elif method in {"thread/start", "thread/resume"}:
            result = {"thread": {"id": self.session}, "model": "test-model",
                "modelProvider": PROVIDER_NAME, "approvalPolicy": "never",
                "sandbox": {"type": "readOnly"}}
            if self.mode == "wrong-provider":
                result["modelProvider"] = "openai"
            self.stdout.put({"id": request_id, "result": result})
        elif method == "turn/start":
            self.stdout.put({"id": request_id, "result": {"turn": {"id": "turn-1", "status": "inProgress"}}})
            if self.held:
                return
            if self.mode in {"tool", "wrong-thread-tool", "unknown-tool", "malformed-tool", "duplicate-tool"}:
                self.tool_event()
            elif self.mode == "approval":
                self.stdout.put({"id": "approval-1", "method": "item/commandExecution/requestApproval", "params": {}})
            elif self.mode == "native-item":
                self.stdout.put({"method": "item/started", "params": {"threadId": self.session,
                    "item": {"type": "commandExecution", "command": "forbidden"}}})
            else:
                self.finish()
        elif request_id == "tool-request" and "result" in message:
            if self.mode == "duplicate-tool":
                self.tool_event()
            else:
                self.finish()

    def tool_event(self):
        arguments = {"action": "read", "path": "solver/solver.py"}
        if self.mode == "malformed-tool":
            arguments["extra"] = "forbidden"
        self.stdout.put({"id": "tool-request", "method": "item/tool/call", "params": {
            "threadId": "peer-thread" if self.mode == "wrong-thread-tool" else self.session,
            "turnId": "turn-1", "callId": "call-1", "namespace": None,
            "tool": "shell" if self.mode == "unknown-tool" else TOOL_NAME,
            "arguments": arguments}})

    def finish(self):
        self.stdout.put({"method": "item/completed", "params": {"threadId": self.session,
            "item": {"type": "agentMessage", "text": '{"done":true}'}}})
        self.stdout.put({"method": "turn/completed", "params": {"threadId": self.session,
            "turn": {"id": "turn-1", "status": "failed" if self.mode == "failed" else "completed"}}})

    def poll(self):
        return self.returncode

    def terminate(self):
        self.returncode, self.terminated = -15, True
        self.stdout.values.put("")

    def kill(self):
        self.terminate()

    def wait(self, timeout=None):
        return self.returncode


def _client(tmp_path, process, **kwargs):
    executable = tmp_path / "codex.exe"
    executable.write_bytes(b"fake")
    starts = []

    def factory(argv, **options):
        starts.append((argv, options))
        return process

    client = AutonomousCodexClient(model="test-model", work_root=tmp_path / "broker",
        executable=executable, factory=factory, **kwargs)
    return client, starts


def _run(client, **kwargs):
    return client.run_phase("Frozen phase context", base_url="http://127.0.0.1:1234/phase/capability",
        deadline=time.monotonic() + 5, **kwargs)


def test_complete_native_turn_and_isolated_provider(tmp_path):
    process = _Process()
    client, starts = _client(tmp_path, process, environment={"PATH": "safe", "OPENAI_API_KEY": "do-not-use",
        "OPENAI_BASE_URL": "https://unused.example.invalid", "CODEX_CHATGPT_BASE_URL": "https://unused.example.invalid"})
    response = _run(client, output_schema={"type": "object"})
    assert (response.text, response.thread_id, response.turn_id, response.tool_calls) == (
        '{"done":true}', "private-thread", "turn-1", 0)
    argv, options = starts[0]
    assert options["env"] == {"PATH": "safe"}
    assert options["cwd"] == str(client.root)
    assert not options.get("shell", False)
    settings = dict(arg.split("=", 1) for arg in argv if "=" in arg)
    assert settings[f"model_providers.{PROVIDER_NAME}.requires_openai_auth"] == "true"
    assert settings[f"model_providers.{PROVIDER_NAME}.supports_websockets"] == "false"
    assert settings[f"model_providers.{PROVIDER_NAME}.request_max_retries"] == "0"
    assert settings[f"model_providers.{PROVIDER_NAME}.stream_max_retries"] == "0"
    assert settings["features.shell_tool"] == "false"
    assert settings["features.hooks"] == "false"
    assert process.terminated
    start = next(message["params"] for message in process.messages if message.get("method") == "thread/start")
    assert start["environments"] == []
    assert start["ephemeral"] is False
    assert start["dynamicTools"][0]["name"] == TOOL_NAME
    turn = next(message["params"] for message in process.messages if message.get("method") == "turn/start")
    assert turn["outputSchema"] == {"type": "object"}
    assert turn["sandboxPolicy"] == {"type": "readOnly", "networkAccess": False}


def test_brokered_dynamic_tool_loop(tmp_path):
    process = _Process("tool")
    client, _ = _client(tmp_path, process)
    calls = []

    def broker(operation, arguments):
        calls.append((operation, arguments))
        return {"contents": "private workspace content"}

    result = _run(client, tools=broker)
    assert result.tool_calls == 1
    assert calls == [(TOOL_NAME, {"action": "read", "path": "solver/solver.py"})]
    answer = next(message["result"] for message in process.messages if message.get("id") == "tool-request")
    assert answer == {"success": True, "contentItems": [{"type": "inputText",
        "text": '{"contents": "private workspace content"}'}]}


def test_resume_keeps_private_memory_and_updates_phase_provider(tmp_path):
    process = _Process()
    client, starts = _client(tmp_path, process)
    _run(client, session_id="private-thread")
    resume = next(message["params"] for message in process.messages if message.get("method") == "thread/resume")
    assert resume["threadId"] == "private-thread"
    assert resume["modelProvider"] == PROVIDER_NAME
    assert not any(message.get("method") == "thread/start" for message in process.messages)
    assert "http://127.0.0.1:1234/phase/capability" in " ".join(starts[0][0])


@pytest.mark.parametrize("mode", ["wrong-provider", "approval", "native-item", "wrong-thread-tool",
                                  "unknown-tool", "malformed-tool", "duplicate-tool"])
def test_unqualified_capability_fails_closed(tmp_path, mode):
    process = _Process(mode)
    client, _ = _client(tmp_path, process)
    with pytest.raises(AppServerProtocolError):
        _run(client, tools=lambda operation, arguments: {})
    assert process.terminated
    assert not any(message.get("id") == "approval-1" and "result" in message for message in process.messages)


def test_planning_cannot_call_tools(tmp_path):
    process = _Process("tool")
    client, _ = _client(tmp_path, process)
    with pytest.raises(AppServerProtocolError, match="outside its active phase"):
        _run(client)
    assert process.terminated


def test_failed_turn_is_not_success(tmp_path):
    process = _Process("failed")
    client, _ = _client(tmp_path, process)
    with pytest.raises(Exception, match="failed or was interrupted"):
        _run(client)
    assert process.terminated


def test_common_deadline_stops_local_process(tmp_path):
    process = _Process(held=True)
    client, _ = _client(tmp_path, process)
    with pytest.raises(TimeoutError):
        client.run_phase("context", base_url="http://127.0.0.1:1234/phase/capability",
            deadline=time.monotonic() + .05)
    assert process.terminated


@pytest.mark.parametrize("url", ["https://api.openai.com/v1", "http://example.com:123/v1",
    "http://127.0.0.1/v1", "http://secret@127.0.0.1:123/v1", "http://127.0.0.1:123/v1?token=x"])
def test_only_loopback_gateway_is_accepted(tmp_path, url):
    client, starts = _client(tmp_path, _Process())
    with pytest.raises(ValueError):
        client.run_phase("context", base_url=url, deadline=time.monotonic() + 5)
    assert not starts


def test_broker_exception_is_propagated_without_native_fallback(tmp_path):
    process = _Process("tool")
    client, _ = _client(tmp_path, process)

    def broker(operation, arguments):
        raise PermissionError("no EditGrant")

    with pytest.raises(PermissionError, match="no EditGrant"):
        _run(client, tools=broker)
    assert process.terminated
    assert not any("result" in message and message.get("id") == "tool-request" for message in process.messages)


def test_inherited_extensions_disabled_before_agent_thread_exists(tmp_path):
    inherited = {"mcp_servers": {"server.with.dot": {"command": "must-never-start"}},
        "plugins": {"plugin@local": {"enabled": True}}, "features": {"hooks": False}, "notify": []}
    first, actual = _Process(config=inherited), _Process()
    processes, starts = [first, actual], []
    executable = tmp_path/"codex.exe"
    executable.write_bytes(b"fake")
    def factory(argv, **options):
        starts.append(argv)
        return processes.pop(0)
    client = AutonomousCodexClient(model="test-model", work_root=tmp_path/"broker",
        executable=executable, factory=factory)
    assert _run(client).text == '{"done":true}'
    assert first.terminated and actual.terminated
    assert not any(message.get("method") in {"thread/start", "turn/start"} for message in first.messages)
    assert 'mcp_servers={"server.with.dot"={"enabled"=false}}' in starts[1]
    assert 'plugins={"plugin@local"={"enabled"=false}}' in starts[1]


@pytest.mark.parametrize("settings", [{"features": {"hooks": True}, "notify": []},
                                      {"features": {"hooks": False}, "notify": ["untrusted-command"]}])
def test_startup_hooks_and_notifications_are_rejected(tmp_path, settings):
    process = _Process(config=settings)
    client, _ = _client(tmp_path, process)
    with pytest.raises(AppServerProtocolError, match="hooks or command notifications"):
        _run(client)
    assert not any(message.get("method") == "thread/start" for message in process.messages)
    assert process.terminated


@pytest.mark.parametrize("field", ["env_key", "experimental_bearer_token", "http_headers",
                                   "env_http_headers", "query_params"])
def test_inherited_provider_cannot_substitute_credentials_or_headers(tmp_path, field):
    process = _Process()
    process.config["model_providers"][PROVIDER_NAME][field] = {"unsafe": "synthetic"} if field.endswith("headers") or field == "query_params" else "synthetic"
    client, _ = _client(tmp_path, process)
    with pytest.raises(AppServerProtocolError, match="Inherited provider credentials"):
        _run(client)
    assert not any(message.get("method") == "thread/start" for message in process.messages)
