"""Native Codex app-server turns behind MAESTRO's request-counting gateway.

Codex owns its existing ChatGPT login and durable conversation history. This
module never reads credentials. The only model-visible execution capability is
``maestro_tool``; the gateway independently filters both tool definitions and
tool-call responses before Codex receives them. Its pre-dispatch quota is the
accounting boundary, not app-server turns or token usage notifications.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit

from maestro.backends.base import BackendUnavailable
from maestro.backends.codex_session.client import _run, _session_environment, resolve_codex
from maestro.backends.codex_session.tools import MAESTRO_TOOL_SPEC


TOOL_NAME = "maestro_tool"
PROVIDER_NAME = "maestro_quota"
TOOL_SPEC = {"type": "function", **MAESTRO_TOOL_SPEC}
_BASE_ENV = {"OPENAI_BASE_URL", "OPENAI_API_BASE", "CHATGPT_BASE_URL",
             "CODEX_CHATGPT_BASE_URL", "CODEX_BASE_URL"}


@dataclass(frozen=True)
class AutonomousResponse:
    text: str
    thread_id: str
    turn_id: str
    response_hash: str
    tool_calls: int


class AppServerProtocolError(BackendUnavailable):
    """A native capability, malformed event, or unqualified session was seen."""


def _loopback_url(value):
    if not isinstance(value, str):
        raise ValueError("A phase-specific loopback quota gateway URL is required")
    parsed = urlsplit(value)
    if (parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.port is None):
        raise ValueError("Codex must use the local MAESTRO quota gateway")
    return value.rstrip("/")


def _toml_literal(value):
    if isinstance(value, dict):
        return "{" + ",".join(json.dumps(key) + "=" + _toml_literal(item)
                               for key, item in value.items()) + "}"
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def _extension_overrides(config):
    """Keep only extension names; credentials and configuration stay host-side."""
    if not isinstance(config, dict):
        raise AppServerProtocolError("Cannot inspect effective app-server capabilities")
    result = {}
    for kind in ("mcp_servers", "plugins"):
        entries = config.get(kind) or {}
        if not isinstance(entries, dict) or any(not isinstance(value, dict) for value in entries.values()):
            raise AppServerProtocolError("Cannot qualify inherited app-server extensions")
        result[kind] = {name: {"enabled": False} for name, value in entries.items()
                        if value.get("enabled") is not False}
    return result


def _assert_oauth_provider(config, base_url):
    provider = config.get("model_providers", {}).get(PROVIDER_NAME)
    if not isinstance(provider, dict):
        raise AppServerProtocolError("The effective quota provider is unavailable")
    forbidden = ("env_key", "auth", "experimental_bearer_token", "bearer_token", "api_key",
                 "http_headers", "env_http_headers", "env_headers", "query_params")
    if any(provider.get(name) not in (None, "", {}) for name in forbidden):
        raise AppServerProtocolError("Inherited provider credentials, headers, or query parameters are forbidden")
    if (provider.get("base_url") != base_url or provider.get("wire_api") != "responses"
            or provider.get("requires_openai_auth") is not True
            or provider.get("supports_websockets") is not False
            or provider.get("request_max_retries") != 0
            or provider.get("stream_max_retries") != 0):
        raise AppServerProtocolError("The effective provider bypasses the required OAuth request gate")


class _Rpc:
    """Bounded JSON-lines transport; a reader thread makes deadlines enforceable."""
    def __init__(self, process, deadline, max_bytes):
        self.process, self.deadline, self.max_bytes = process, deadline, max_bytes
        self.events, self.request_id, self.bytes_read = queue.Queue(maxsize=2048), 0, 0
        self._closed = threading.Event()
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()
        # Diagnostics may contain account/configuration data. Drain and discard;
        # they are never returned to models, events, or exception messages.
        self._stderr = threading.Thread(target=self._drain_stderr, daemon=True)
        self._stderr.start()

    def _offer(self, event):
        while not self._closed.is_set():
            try:
                self.events.put(event, timeout=.1)
                return
            except queue.Full:
                continue

    def _read(self):
        try:
            while not self._closed.is_set():
                line = self.process.stdout.readline(self.max_bytes + 1)
                if not line:
                    self._offer(AppServerProtocolError("Codex app-server closed before completing its turn"))
                    return
                self.bytes_read += len(line.encode("utf-8"))
                if self.bytes_read > self.max_bytes or not line.endswith("\n"):
                    raise AppServerProtocolError("Codex app-server event stream exceeded its allowance")
                if not line.strip():
                    continue
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise ValueError("event object required")
                self._offer(event)
        except Exception:
            self._offer(AppServerProtocolError("Malformed or oversized Codex app-server event stream"))

    def _drain_stderr(self):
        try:
            while not self._closed.is_set() and self.process.stderr.read(4096):
                pass
        except Exception:
            pass

    def receive(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Codex phase deadline elapsed; dispatched inference may be incomplete")
        try:
            value = self.events.get(timeout=remaining)
        except queue.Empty:
            raise TimeoutError("Codex phase deadline elapsed; dispatched inference may be incomplete") from None
        if time.monotonic() >= self.deadline:
            raise TimeoutError("Codex phase deadline elapsed; dispatched inference may be incomplete")
        if isinstance(value, Exception):
            raise value
        return value

    def send(self, value):
        if time.monotonic() >= self.deadline:
            raise TimeoutError("Codex phase deadline elapsed before RPC dispatch")
        self.process.stdin.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
        self.process.stdin.flush()

    def request(self, method, params):
        self.request_id += 1
        self.send({"id": self.request_id, "method": method, "params": params})
        return self.request_id

    def close(self):
        self._closed.set()


class AutonomousCodexClient:
    """A native tool loop, authenticated by Codex, with brokered host tools.

    Each call starts a separate app-server configured for exactly one gateway
    capability URL. A saved thread ID resumes the same private agent history.
    Callers must never share thread IDs between agents or experimental groups.
    The gateway must remain open until this method returns or raises.
    """
    is_live = True

    def __init__(self, *, model: str, work_root: str | Path, executable=None,
                 factory=None, environment=None, timeout=300,
                 max_response_bytes=8 * 1024 * 1024, max_tool_calls=128):
        if not isinstance(model, str) or not model.strip():
            raise ValueError("An explicit Codex model is required")
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("Positive finite Codex timeout required")
        if type(max_response_bytes) is not int or max_response_bytes < 1024:
            raise ValueError("Invalid accepted event byte allowance")
        if type(max_tool_calls) is not int or max_tool_calls < 1:
            raise ValueError("Positive host tool-call allowance required")
        self.model, self.executable = model, resolve_codex(executable)
        self.root = Path(work_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.factory = factory or subprocess.Popen
        self.env = {key: value for key, value in _session_environment(environment).items()
                    if key.upper() not in _BASE_ENV}
        self.timeout = float(timeout)
        self.max_response_bytes, self.max_tool_calls = max_response_bytes, max_tool_calls

    def login_status(self, *, deadline=None):
        remaining = min(15, self.timeout)
        if deadline is not None:
            remaining = min(remaining, deadline-time.monotonic())
        if remaining <= 0:
            raise TimeoutError("Codex login probe missed the common deadline")
        result = _run([str(self.executable), "login", "status"], input_text="",
            cwd=self.root, env=self.env, timeout=remaining, max_bytes=65536)
        authenticated = (result.returncode == 0 and
            "logged in using chatgpt" in (result.stdout + "\n" + result.stderr).lower())
        return {"authenticated": authenticated,
                "auth_method": "chatgpt" if authenticated else "unavailable_or_other",
                "external_api_key_required": False,
                "strict_v1_request_quota": True}

    def require_chatgpt_login(self, *, deadline=None):
        report = self.login_status(deadline=deadline)
        if not report["authenticated"]:
            raise BackendUnavailable("Existing Codex ChatGPT login is required; run codex login")
        return report

    def preflight(self, *, deadline=None, require_login=True):
        """Qualify local protocol support without starting an inference turn."""
        remaining = min(30, self.timeout)
        if deadline is not None:
            remaining = min(remaining, deadline-time.monotonic())
        if remaining <= 0:
            raise TimeoutError("Codex local preflight missed its deadline")
        report = self.require_chatgpt_login(deadline=deadline) if require_login else {}
        version = _run([str(self.executable), "--version"], input_text="", cwd=self.root,
            env=self.env, timeout=remaining, max_bytes=65536)
        if version.returncode:
            raise BackendUnavailable("Cannot read the installed Codex CLI version")
        with tempfile.TemporaryDirectory(prefix="protocol-", dir=self.root) as directory:
            generated = _run([str(self.executable), "app-server", "generate-json-schema",
                "--experimental", "--out", directory], input_text="", cwd=self.root,
                env=self.env, timeout=remaining, max_bytes=65536)
            if generated.returncode:
                raise BackendUnavailable("Installed Codex lacks experimental app-server schemas")
            try:
                start = json.loads((Path(directory)/"v2"/"ThreadStartParams.json").read_text())
                turn = json.loads((Path(directory)/"v2"/"TurnStartParams.json").read_text())
                dynamic = json.loads((Path(directory)/"DynamicToolCallParams.json").read_text())
                if (not {"dynamicTools", "environments", "modelProvider"} <= set(start["properties"])
                        or not {"outputSchema", "environments", "sandboxPolicy"} <= set(turn["properties"])
                        or not {"arguments", "callId", "threadId", "tool", "turnId"}
                            <= set(dynamic["required"])):
                    raise ValueError("required protocol capabilities missing")
            except (OSError, KeyError, ValueError, TypeError):
                raise BackendUnavailable("Installed Codex app-server protocol is not qualified") from None
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("Codex local preflight completed after its deadline")
        return {**report, "cli_version": version.stdout.strip().splitlines()[0],
                "app_server_dynamic_tools": True, "native_inference_started": False,
                "request_accounting_boundary": "maestro_pre_dispatch_gateway"}

    def _argv(self, base_url, extension_settings=None):
        settings = {
            "model_provider": PROVIDER_NAME,
            f"model_providers.{PROVIDER_NAME}.name": "MAESTRO quota gateway",
            f"model_providers.{PROVIDER_NAME}.base_url": base_url,
            f"model_providers.{PROVIDER_NAME}.wire_api": "responses",
            f"model_providers.{PROVIDER_NAME}.requires_openai_auth": True,
            f"model_providers.{PROVIDER_NAME}.supports_websockets": False,
            f"model_providers.{PROVIDER_NAME}.request_max_retries": 0,
            f"model_providers.{PROVIDER_NAME}.stream_max_retries": 0,
            "forced_login_method": "chatgpt", "approval_policy": "never",
            "sandbox_mode": "read-only", "web_search": "disabled",
            "project_doc_max_bytes": 0, "mcp_servers": {}, "plugins": {},
            "notify": [],
            "apps._default.enabled": False, "tools.view_image": False,
            "agents.enabled": False,
            "features.shell_tool": False, "features.hooks": False,
            "features.memories": False, "features.multi_agent": False,
            "features.remote_plugin": False, "features.apps": False,
            "features.browser_use": False, "features.computer_use": False,
            "features.code_mode": False, "features.code_mode_host": False,
            "features.enable_request_compression": False,
        }
        if extension_settings:
            settings.update(extension_settings)
        argv = [str(self.executable), "app-server", "--listen", "stdio://"]
        for key, value in settings.items():
            argv.extend(["-c", f"{key}={_toml_literal(value)}"])
        return argv

    def _stop(self, process):
        try:
            if process.poll() is not None:
                return
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            if process.poll() is not None:
                return
            error = AppServerProtocolError("Codex process termination could not be confirmed")
            error.local_quiescent = False
            error.inflight_unknown = True
            raise error from None

    def run_phase(self, prompt: str, *, base_url: str, deadline: float,
                  tools=None, output_schema=None, session_id=None) -> AutonomousResponse:
        """Run one native turn; ``tools(tool_name, arguments)`` brokers every action.

        Planning passes ``tools=None`` and a complete frozen input context, so
        its one-request quota cannot be spent on an interactive tool round trip.
        Generation can perform multiple tool calls between gated requests.
        """
        base_url = _loopback_url(base_url)
        if not isinstance(prompt, str) or not prompt or len(prompt.encode()) > 256 * 1024:
            raise ValueError("A bounded, nonempty phase prompt is required")
        if (isinstance(deadline, bool) or not isinstance(deadline, (int, float))
                or not math.isfinite(deadline)):
            raise ValueError("A finite monotonic phase deadline is required")
        deadline = min(float(deadline), time.monotonic() + self.timeout)
        if deadline <= time.monotonic():
            raise TimeoutError("Codex phase deadline elapsed before process startup")
        if tools is not None and not callable(tools):
            raise TypeError("A host tool broker callback is required")
        if output_schema is not None and not isinstance(output_schema, dict):
            raise TypeError("JSON output schema object required")
        if session_id is not None and (not isinstance(session_id, str) or not session_id):
            raise ValueError("A private Codex thread ID is required for resume")
        options = {"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {}
        process = self.factory(self._argv(base_url), cwd=str(self.root), env=self.env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", errors="strict", bufsize=1, **options)
        rpc = _Rpc(process, deadline, self.max_response_bytes)
        phase_done = threading.Event()
        def enforce_deadline():
            while True:
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    break
                if phase_done.wait(remaining):
                    return
            if phase_done.is_set():
                return
            # Also break a blocked stdin pipe write; receive() deadlines alone
            # cannot bound a child that stops consuming its input. Recheck the
            # monotonic clock above because Windows waits can round early.
            try:
                process.kill()
            except OSError:
                pass
        threading.Thread(target=enforce_deadline, daemon=True).start()
        thread_id, turn_id, final_text, tool_calls = "", "", None, 0
        answered_calls = set()

        def notification(event):
            nonlocal final_text
            method, params = event.get("method"), event.get("params", {})
            if not isinstance(params, dict):
                raise AppServerProtocolError("Malformed app-server notification")
            if thread_id and params.get("threadId", thread_id) != thread_id:
                raise AppServerProtocolError("Notification belongs to a different private agent thread")
            if method in {"item/started", "item/completed"}:
                item = params.get("item", {})
                kind = item.get("type") if isinstance(item, dict) else None
                if kind not in {"agentMessage", "reasoning", "dynamicToolCall", "userMessage",
                                "contextCompaction", "plan"}:
                    raise AppServerProtocolError("Unbrokered native tool or unknown item appeared")
                if kind == "dynamicToolCall" and item.get("tool", TOOL_NAME) != TOOL_NAME:
                    raise AppServerProtocolError("Unknown dynamic tool appeared")
                if kind == "agentMessage" and method == "item/completed":
                    if not isinstance(item.get("text"), str):
                        raise AppServerProtocolError("Codex agent message has no text")
                    final_text = item["text"]
            if method in {"error", "turn/failed"}:
                raise BackendUnavailable("Codex reported an unsuccessful autonomous turn")
            if method in {"item/commandExecution/outputDelta", "item/fileChange/outputDelta",
                          "item/mcpToolCall/progress", "item/realtime/started"}:
                raise AppServerProtocolError("Unbrokered native execution appeared")

        def tool_request(event):
            nonlocal tool_calls
            if event.get("method") != "item/tool/call":
                raise AppServerProtocolError("Native approval or unknown server request was rejected")
            params = event.get("params", {})
            if (not isinstance(params, dict) or tools is None or params.get("tool") != TOOL_NAME
                    or params.get("namespace") not in {None, ""}
                    or params.get("threadId") != thread_id
                    or (turn_id and params.get("turnId") != turn_id)):
                raise AppServerProtocolError("Dynamic tool call is outside its active phase")
            call_id = params.get("callId")
            if not isinstance(call_id, str) or not call_id or call_id in answered_calls:
                raise AppServerProtocolError("Missing or duplicate dynamic tool call ID")
            if tool_calls >= self.max_tool_calls:
                raise AppServerProtocolError("Host tool-call allowance exhausted")
            arguments = params.get("arguments")
            if (not isinstance(arguments, dict) or not isinstance(arguments.get("action"), str)
                    or not set(arguments) <= set(MAESTRO_TOOL_SPEC["inputSchema"]["properties"])):
                raise AppServerProtocolError("Malformed MAESTRO tool arguments")
            answered_calls.add(call_id)
            tool_calls += 1
            # The broker is trusted to enforce EditGrant, phase, filesystem,
            # and debug CPU/time constraints, including denied operations.
            result = tools(TOOL_NAME, arguments)
            text = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, allow_nan=False)
            if len(text.encode()) > 256 * 1024:
                raise AppServerProtocolError("Host tool output exceeded its accepted allowance")
            rpc.send({"id": event["id"], "result": {"contentItems": [
                {"type": "inputText", "text": text}], "success": True}})

        def wait_response(request_id):
            while True:
                event = rpc.receive()
                if "method" in event:
                    if "id" in event:
                        tool_request(event)
                    else:
                        notification(event)
                elif event.get("id") == request_id:
                    if "error" in event:
                        raise BackendUnavailable("Codex app-server rejected a phase RPC")
                    result = event.get("result")
                    if not isinstance(result, dict):
                        raise AppServerProtocolError("Codex app-server RPC result object required")
                    return result
                else:
                    raise AppServerProtocolError("Unexpected app-server RPC response")

        try:
            wait_response(rpc.request("initialize", {
                "clientInfo": {"name": "maestro", "title": "MAESTRO", "version": "0.1.0"},
                "capabilities": {"experimentalApi": True}}))
            rpc.send({"method": "initialized", "params": {}})
            effective = wait_response(rpc.request("config/read", {"includeLayers": False})).get("config")
            disabled = _extension_overrides(effective)
            if any(disabled.values()):
                # Empty map overrides merge inherited entries in Codex. Discover
                # their names without starting a thread, then disable every
                # entry before constructing the actual agent process.
                rpc.close()
                self._stop(process)
                process = self.factory(self._argv(base_url, disabled), cwd=str(self.root), env=self.env,
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, encoding="utf-8", errors="strict", bufsize=1, **options)
                rpc = _Rpc(process, deadline, self.max_response_bytes)
                wait_response(rpc.request("initialize", {
                    "clientInfo": {"name": "maestro", "title": "MAESTRO", "version": "0.1.0"},
                    "capabilities": {"experimentalApi": True}}))
                rpc.send({"method": "initialized", "params": {}})
                effective = wait_response(rpc.request("config/read", {"includeLayers": False})).get("config")
                if any(_extension_overrides(effective).values()):
                    raise AppServerProtocolError("Inherited MCP servers or plugins could not be disabled")
            if (effective.get("features", {}).get("hooks") is not False
                    or effective.get("notify") not in (None, [])):
                raise AppServerProtocolError("Inherited hooks or command notifications remain enabled")
            _assert_oauth_provider(effective, base_url)
            params = {"model": self.model, "modelProvider": PROVIDER_NAME,
                "cwd": str(self.root), "approvalPolicy": "never", "sandbox": "read-only",
                "runtimeWorkspaceRoots": [str(self.root)],
                "baseInstructions": ("You are a MAESTRO research coding agent. Use only the supplied "
                    "phase context and maestro_tool. The host enforces plan-first, resource limits, "
                    "private/shared memory boundaries, and formal experiment tickets. Never request "
                    "native host tools or unbrokered execution."),
                "developerInstructions": "The phase prompt and broker define your current permissions."}
            if session_id is None:
                params.update({"dynamicTools": [TOOL_SPEC], "environments": [], "ephemeral": False})
                started = wait_response(rpc.request("thread/start", params))
            else:
                params["threadId"] = session_id
                started = wait_response(rpc.request("thread/resume", params))
            thread_id = started.get("thread", {}).get("id")
            if (not isinstance(thread_id, str) or not thread_id
                    or started.get("model") != self.model
                    or started.get("modelProvider") != PROVIDER_NAME
                    or started.get("approvalPolicy") != "never"
                    or started.get("sandbox", {}).get("type") != "readOnly"
                    or (session_id is not None and thread_id != session_id)):
                raise AppServerProtocolError("Codex did not honor the required phase isolation settings")
            turn_params = {"threadId": thread_id, "input": [{"type": "text", "text": prompt}],
                "model": self.model, "approvalPolicy": "never", "environments": [],
                "sandboxPolicy": {"type": "readOnly", "networkAccess": False}}
            if output_schema is not None:
                turn_params["outputSchema"] = output_schema
            turn = wait_response(rpc.request("turn/start", turn_params)).get("turn", {})
            turn_id = turn.get("id")
            if not isinstance(turn_id, str) or not turn_id:
                raise AppServerProtocolError("Codex did not identify its active turn")
            while True:
                event = rpc.receive()
                if "id" in event and "method" in event:
                    tool_request(event)
                    continue
                notification(event)
                if event.get("method") == "turn/completed":
                    completed = event.get("params", {})
                    value = completed.get("turn", {})
                    if (completed.get("threadId") != thread_id or value.get("id") != turn_id
                            or value.get("status") != "completed"):
                        raise BackendUnavailable("Codex autonomous turn failed or was interrupted")
                    if final_text is None:
                        raise AppServerProtocolError("Codex completed without a final agent message")
                    return AutonomousResponse(final_text, thread_id, turn_id,
                        hashlib.sha256(final_text.encode()).hexdigest(), tool_calls)
        finally:
            phase_done.set()
            rpc.close()
            self._stop(process)
            active_error = sys.exc_info()[1]
            if active_error is not None:
                active_error.local_quiescent = True

    def close(self):
        """Phase processes are stopped synchronously by ``run_phase`` itself."""
