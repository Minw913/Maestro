"""Private actual-request gate for Codex's own ChatGPT OAuth transport.

Codex, not MAESTRO, obtains and refreshes login credentials. This loopback
capability accepts those headers only in memory and forwards to one fixed TLS
origin. Every upstream POST (including compaction and native retries) consumes
a durable reservation before dispatch. No CLI turn count is used as a quota.

The native ChatGPT protocol does not expose a supported output-token limit.
Output ceilings below are conservative accounting reservations, never a claim
of provider-side hard enforcement. A completed response with missing usage is
finished execution with unknown metering, rather than an in-flight request.
"""
from __future__ import annotations

from contextlib import suppress
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import json
import math
import re
import secrets
import socket
import threading
import time
import uuid

from maestro.execution.budgets import BudgetExceeded


_UPSTREAM_HOST = "chatgpt.com"
_UPSTREAM_PATH = "/backend-api/codex"
_HEADER_ALLOWLIST = frozenset({
    "authorization", "chatgpt-account-id", "openai-beta", "originator",
    "version", "session_id", "conversation_id", "x-codex-turn-metadata",
    "x-codex-beta-features", "x-codex-installation-id", "x-client-request-id",
    "x-openai-client-user-agent", "user-agent",
})
_SAFE_OUTPUT = frozenset({"message", "reasoning", "compaction"})
_FUNCTION_OUTPUT = frozenset({"function_call", "custom_tool_call"})
_KNOWN_REJECTIONS = frozenset({400, 401, 403, 404, 405, 413, 415, 422, 429})
_SSE_EVENT_TYPES = frozenset({
    "error", "response.created", "response.in_progress", "response.completed",
    "response.failed", "response.incomplete", "response.output_item.added",
    "response.output_item.done", "response.content_part.added", "response.content_part.done",
    "response.output_text.delta", "response.output_text.done", "response.refusal.delta",
    "response.refusal.done", "response.function_call_arguments.delta",
    "response.function_call_arguments.done", "response.custom_tool_call_input.delta",
    "response.custom_tool_call_input.done", "response.reasoning_summary_part.added",
    "response.reasoning_summary_part.done", "response.reasoning_summary_text.delta",
    "response.reasoning_summary_text.done", "response.reasoning_text.delta",
    "response.reasoning_text.done",
})


class QuotaProxyError(RuntimeError):
    """A deliberately sanitized native transport failure."""


class _UpstreamTransportError(QuotaProxyError):
    """Trusted transport-stage evidence, without exception text or credentials."""
    def __init__(self, stage, cause_class):
        super().__init__("Native upstream transport failed")
        self.stage, self.cause_class = stage, cause_class
        # connect() is explicit: an exception there proves request() was never
        # entered. Failures during/after request() remain ambiguous.
        self.never_dispatched = stage == "upstream_connect"


class _TlsResponse:
    def __init__(self, connection, response):
        self.connection, self.response = connection, response
        self.status = response.status
        self.headers = dict(response.getheaders())
        self._socket = connection.sock or getattr(getattr(getattr(response, "fp", None), "raw", None), "_sock", None)

    def iter_bytes(self, deadline):
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Native inference deadline elapsed")
            if self._socket is not None:
                self._socket.settimeout(remaining)
            chunk = self.response.read1(65536)
            if not chunk:
                return
            yield chunk

    def close(self):
        if self._socket is not None:
            with suppress(OSError):
                self._socket.shutdown(socket.SHUT_RDWR)
        self.response.close()
        self.connection.close()


def _https_upstream(path, headers, body, timeout, *, on_post=None):
    """No proxy environment, redirects, URL parameters, or alternate origin."""
    connection = http.client.HTTPSConnection(_UPSTREAM_HOST, 443, timeout=timeout)
    stage = "upstream_connect"
    try:
        connection.connect()
        stage = "upstream_post"
        if on_post is not None:
            on_post()
        connection.request("POST", _UPSTREAM_PATH + path, body=body, headers=headers)
        stage = "upstream_headers"
        return _TlsResponse(connection, connection.getresponse())
    except Exception as error:
        with suppress(Exception):
            connection.close()
        raise _UpstreamTransportError(stage, type(error).__name__) from None
    except BaseException:
        with suppress(Exception):
            connection.close()
        raise


def _strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON field")
            result[key] = value
        return result
    def constant(_value):
        raise ValueError("Nonfinite JSON value")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def _safe_usage(value):
    """Persist only known numeric usage fields, never opaque upstream objects."""
    if not isinstance(value, dict):
        return {}
    result = {name: value[name] for name in ("input_tokens", "output_tokens", "total_tokens")
              if type(value.get(name)) is int and value[name] >= 0}
    for name, allowed in (("input_tokens_details", ("cached_tokens",)),
                          ("output_tokens_details", ("reasoning_tokens",))):
        details = value.get(name)
        if isinstance(details, dict):
            result[name] = {key: details[key] for key in allowed
                           if type(details.get(key)) is int and details[key] >= 0}
    return result


class CodexQuotaProxy:
    """One phase account and an ephemeral loopback capability.

    ``upstream`` is a trusted test-only injectable callable
    ``(path, headers, body, timeout) -> response``. A response has ``status``,
    ``headers``, ``iter_bytes(monotonic_deadline)`` and ``close()``. Production
    uses the hard-coded ChatGPT TLS origin above, never an operator URL.

    Native tools are filtered before dispatch, and responses are buffered until
    a terminal receipt. A forbidden tool call is never exposed to Codex's
    trusted host process. Only the explicitly brokered dynamic tools survive.
    """
    def __init__(self, store, account, model, *, deadline, max_calls,
                 max_request_bytes, max_output_tokens, allowed_tool_names=(),
                 upstream=None, max_response_bytes=32 * 1024 * 1024, event_sink=None, tool_namespace=None, request_guard=None,
                 diagnostic_sink=None, final_response_on_last_request=False):
        if type(final_response_on_last_request) is not bool:
            raise ValueError("Final-response policy must be boolean")
        self.final_response_on_last_request = final_response_on_last_request
        if diagnostic_sink is not None and not callable(diagnostic_sink):
            raise ValueError("Diagnostic sink must be callable")
        self._diagnostic_sink = diagnostic_sink
        unlimited = (type(max_calls) is int and max_calls == 0
                     and isinstance(account, str) and account.endswith(':PREPARATION'))
        if type(max_calls) is not int or (max_calls <= 0 and not unlimited):
            raise ValueError('Only preparation can omit its request-count cap')
        integers = (max_request_bytes, max_output_tokens, max_response_bytes)
        if any(type(value) is not int or value <= 0 for value in integers):
            raise ValueError("Positive integer native request quotas are required")
        if (not isinstance(account, str) or not account or not isinstance(model, str)
                or not model.strip() or isinstance(deadline, bool)
                or not isinstance(deadline, (int, float)) or not math.isfinite(deadline)
                or deadline <= time.monotonic()):
            raise ValueError("A model, phase identity and future monotonic deadline are required")
        if (not isinstance(allowed_tool_names, (tuple, list, set, frozenset))
                or any(not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", name)
                       for name in allowed_tool_names)):
            raise ValueError("Explicit broker tool names are required")
        if set(allowed_tool_names) - {"maestro_tool"}:
            raise ValueError("Only the trusted MAESTRO tool schema can be exposed")
        self.store, self.account, self.model = store, account, model
        self.deadline, self.max_calls = deadline, max_calls
        self.max_request_bytes, self.max_output_tokens = max_request_bytes, max_output_tokens
        self.max_response_bytes = max_response_bytes
        if tool_namespace not in (None, "mcp__maestro"):
            raise ValueError("Only the controller MCP namespace is supported")
        if request_guard is not None and not callable(request_guard):
            raise ValueError("Request guard must be callable")
        self.request_guard = request_guard
        self.tool_namespace = tool_namespace
        self.allowed_tool_names = frozenset(allowed_tool_names)
        self._event_sink = event_sink
        self._upstream = upstream or (lambda path, headers, body, timeout: _https_upstream(
            path, headers, body, timeout, on_post=lambda: self._emit("provider_post", {})))
        self._capability = secrets.token_urlsafe(32)
        self._server = self._thread = None
        self._closed = False
        self._lock = threading.RLock()
        self._responses = {}
        self._native_tool_envelope = False
        self._violations = self._excess_usage = 0
        self.quota_denied = False
        self.quota_denied_count = 0
        # Reopening a consumed account never creates a new deadline or budget.
        try:
            old = store.snapshot(account)
        except BudgetExceeded:
            store.create(account, max_calls=max_calls, max_request_bytes=max_request_bytes,
                         max_output_tokens=max_output_tokens, max_concurrent=1,
                         deadline=time.time() + deadline - time.monotonic(),
                         allow_unlimited_calls=unlimited)
        else:
            if (old["max_calls"], old["max_request_bytes"], old["max_output_tokens"],
                    old["max_concurrent"]) != (max_calls, max_request_bytes, max_output_tokens, 1):
                raise BudgetExceeded("Native phase budget cannot be reset or expanded")
            if old["active"]:
                raise BudgetExceeded("Unresolved native inference blocks phase reuse")
            self.deadline = min(deadline, time.monotonic() + old["deadline"] - time.time())
        with store._transaction():
            store.connection.execute("""CREATE TABLE IF NOT EXISTS codex_gateway_progress(
              reservation_id TEXT PRIMARY KEY REFERENCES inference_reservations(reservation_id),
              response_id TEXT NOT NULL,request_id TEXT NOT NULL,last_event TEXT NOT NULL,
              observed_utc REAL NOT NULL,observed_monotonic REAL NOT NULL,
              terminal_seen INTEGER NOT NULL,transport_closed INTEGER NOT NULL DEFAULT 0)""")
            progress_columns = {row[1] for row in store.connection.execute("PRAGMA table_info(codex_gateway_progress)")}
            if "stream_stats_json" not in progress_columns:
                store.connection.execute("ALTER TABLE codex_gateway_progress ADD COLUMN stream_stats_json TEXT NOT NULL DEFAULT '{}'")
            store.connection.execute("""CREATE TABLE IF NOT EXISTS codex_gateway_receipts(
              reservation_id TEXT PRIMARY KEY REFERENCES inference_reservations(reservation_id),
              protocol_violation INTEGER NOT NULL,token_estimate_exceeded INTEGER NOT NULL,
              terminal_failure INTEGER NOT NULL DEFAULT 0)""")
            columns = {row[1] for row in store.connection.execute("PRAGMA table_info(codex_gateway_receipts)")}
            if "terminal_failure" not in columns:
                store.connection.execute("ALTER TABLE codex_gateway_receipts ADD COLUMN terminal_failure INTEGER NOT NULL DEFAULT 0")
            store.connection.execute("""CREATE TABLE IF NOT EXISTS codex_gateway_transport(
              reservation_id TEXT PRIMARY KEY REFERENCES inference_reservations(reservation_id),
              http_status INTEGER,error_type TEXT NOT NULL,error_code TEXT NOT NULL,
              exception_class TEXT NOT NULL,stage TEXT NOT NULL,response_format TEXT NOT NULL,
              known_rejected INTEGER NOT NULL)""")
            columns = {row[1] for row in store.connection.execute("PRAGMA table_info(codex_gateway_transport)")}
            for name, definition in (
                    ("response_request_id", "TEXT NOT NULL DEFAULT ''"),
                    ("sse_event_types_json", "TEXT NOT NULL DEFAULT '[]'"),
                    ("never_dispatched", "INTEGER NOT NULL DEFAULT 0"),
                    ("retry_after_seconds", "INTEGER"),
                    ("upstream_media_type", "TEXT NOT NULL DEFAULT ''")):
                if name not in columns:
                    store.connection.execute(f"ALTER TABLE codex_gateway_transport ADD COLUMN {name} {definition}")

    def _emit(self, event, fields):
        if self._event_sink is not None:
            self._event_sink(event, {"account": self.account, **fields})

    def start(self):
        with self._lock:
            if self._closed:
                raise QuotaProxyError("Closed native request gate cannot restart")
            if self._server is None:
                owner = self
                class Handler(BaseHTTPRequestHandler):
                    protocol_version = "HTTP/1.1"

                    def setup(self):
                        super().setup()
                        self.connection.settimeout(max(.01, min(10, owner.deadline-time.monotonic())))

                    def log_message(self, *_args):
                        pass  # Never log capability URLs, headers, or bodies.

                    def log_error(self, *_args):
                        pass

                    def do_POST(self):
                        owner._handle(self)

                    def do_GET(self):
                        owner._error(self, 405, "Only inference POST is permitted")

                    do_HEAD = do_OPTIONS = do_CONNECT = do_TRACE = do_PUT = do_DELETE = do_PATCH = do_GET

                class Server(ThreadingHTTPServer):
                    # server_close joins every request handler, including one
                    # still establishing its deadline-bounded TLS connection.
                    # The caller may close BudgetStore only after close returns.
                    daemon_threads = False
                    block_on_close = True
                    def handle_error(self, *_args):
                        pass
                self._server = Server(("127.0.0.1", 0), Handler)
                self._thread = threading.Thread(target=self._server.serve_forever,
                                                name="maestro-codex-quota", daemon=True)
                self._thread.start()
            return f"http://127.0.0.1:{self._server.server_address[1]}/{self._capability}"

    def _error(self, handler, status, message):
        # Draining a small, unambiguous fixed-length body avoids a TCP reset
        # when rejecting e.g. Upgrade headers on Windows. Never drain an
        # attacker-controlled chunked or unbounded body just to send an error.
        length = handler.headers.get("Content-Length", "")
        if (not getattr(handler, "_maestro_body_consumed", False)
                and len(handler.headers.get_all("Content-Length", [])) == 1
                and not handler.headers.get("Transfer-Encoding") and length.isascii() and length.isdigit()
                and 0 < int(length) <= min(self.max_request_bytes, 1024 * 1024)
                and time.monotonic() < self.deadline):
            with suppress(OSError):
                handler.rfile.read(int(length))
            handler._maestro_body_consumed = True
        body = json.dumps({"error": {"type": "maestro_quota_gate", "message": message}}).encode()
        with suppress(OSError):
            handler.send_response(status)
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Content-Length", str(len(body)))
            handler.send_header("Connection", "close")
            handler.end_headers()
            if handler.command != "HEAD":
                handler.wfile.write(body)
        handler.close_connection = True

    def _request(self, handler):
        if self.request_guard is not None:
            self.request_guard()
        if self._closed or time.monotonic() >= self.deadline:
            raise BudgetExceeded("Native inference phase is closed or expired")
        paths = {f"/{self._capability}{path}": path for path in ("/responses", "/responses/compact")}
        if handler.path not in paths:
            raise PermissionError("Native inference route is not authorized")
        headers = handler.headers
        if (headers.get("Upgrade") or "upgrade" in headers.get("Connection", "").lower()
                or headers.get("Transfer-Encoding") or len(headers.get_all("Content-Length", [])) != 1
                or len(headers.get_all("Authorization", [])) != 1):
            raise PermissionError("Upgrades and ambiguous framing are forbidden")
        length = headers.get("Content-Length", "")
        if not length.isascii() or not length.isdigit() or not 0 < int(length) <= self.max_request_bytes:
            raise ValueError("Native request exceeds its byte allowance")
        authorization = headers.get("Authorization", "")
        if not authorization.startswith("Bearer ") or not authorization[7:].strip():
            raise PermissionError("Codex ChatGPT login authorization is required")
        raw = handler.rfile.read(int(length))
        handler._maestro_body_consumed = True
        if len(raw) != int(length):
            raise ValueError("Incomplete native inference body")
        value = _strict_json(raw)
        if not isinstance(value, dict) or value.get("model") != self.model:
            raise PermissionError("Native request model differs from the frozen phase model")
        tools = value.get("tools", [])
        if not isinstance(tools, list):
            raise ValueError("Native tool manifest is malformed")
        snapshot = self.store.snapshot(self.account)
        final_response = (self.final_response_on_last_request
                          and snapshot['max_calls'] > 0
                          and snapshot['calls'] == snapshot['max_calls']-1)
        if final_response and paths[handler.path] != '/responses':
            raise PermissionError('Last preparation request must return a summary, not compact context')
        handler._maestro_final_preparation = final_response
        admitted_tools = self._normalize_tools(value,
            declare=paths[handler.path] == "/responses" and not final_response,
            final_response=final_response)
        if final_response:
            value['tool_choice'] = 'none'
        choice = value.get("tool_choice", "auto")
        if isinstance(choice, dict):
            if (choice.get("type") not in {"function", "custom"}
                    or choice.get("name") not in self.allowed_tool_names
                    or choice.get("namespace") != self.tool_namespace):
                raise PermissionError("Native tool choice is not broker-authorized")
        elif choice not in {"auto", "none", "required"} or (choice == "required" and not admitted_tools):
            raise PermissionError("Native tool choice is not broker-authorized")
        # Never invent an unsupported max_output_tokens field. If the client
        # already supplies one, validate and reserve it without expanding it.
        output_bound = value.get("max_output_tokens")
        if output_bound is not None and (type(output_bound) is not int or output_bound <= 0):
            raise ValueError("Invalid native output-token reservation")
        body = json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
        forwarded = {key: val for key, val in headers.items() if key.lower() in _HEADER_ALLOWLIST}
        forwarded.update({"Content-Type": "application/json", "Accept": "text/event-stream, application/json"})
        return paths[handler.path], forwarded, body, output_bound

    def _normalize_tools(self, value, *, declare=True, final_response=False):
        """Replace both classic and native embedded manifests authoritatively.

        Codex 0.156 can encode executable namespaces inside
        ``input[type=additional_tools].tools`` rather than top-level ``tools``.
        Filtering only the latter would leave builtin declarations visible and
        omit the dynamic broker altogether. Retain ordinary context/history;
        every executable declaration comes from our trusted schema instead.
        """
        from .tools import MAESTRO_TOOL_SPEC
        admitted = ([{"type": "function", "name": tool_name,
                      "description": MAESTRO_TOOL_SPEC["description"],
                      "parameters": MAESTRO_TOOL_SPEC["inputSchema"], "strict": False}
                    for tool_name in sorted(self.allowed_tool_names)] if declare else [])
        if admitted and self.tool_namespace:
            admitted = [{"type":"namespace", "name":self.tool_namespace,
                         "description":"Controller-authorized workspace operations", "tools":admitted}]
        inputs = value.get("input")
        native_envelope = isinstance(inputs, list) and any(
            isinstance(item, dict) and item.get("type") == "additional_tools" for item in inputs)
        with self._lock:
            self._native_tool_envelope |= native_envelope
            native_envelope |= self._native_tool_envelope and isinstance(inputs, list)
        if isinstance(inputs, list):
            value["input"] = [item for item in inputs
                              if not isinstance(item, dict) or item.get("type") != "additional_tools"]
        value.pop("tools", None)
        if not declare and not final_response:
            return []  # Compact is a separate narrow schema, with no tool loop.
        if admitted:
            if native_envelope:
                value["input"].insert(0, {"type": "additional_tools", "role": "developer",
                                          "id": "at_"+str(uuid.uuid4()), "tools": admitted})
            else:
                value["tools"] = admitted
            instruction = ("Only the controller-admitted workspace function is available. It is an external controller "
                "RPC, with virtual workspace paths separate from the read-only Codex host filesystem. "
                "In GENERATION an admitted EditGrant authorizes its solver/ and private/ writes; the "
                "native host remains read-only and public/ is always read-only. Do not infer denial of "
                "these service writes from the native host sandbox; respect actual broker denials. "
                "Call "+", ".join(sorted(self.allowed_tool_names))+" with its "
                "declared action/path/content/command/operations fields. For related file operations use "
                "action=batch with at most eight operations in one call; nested batches and shell in a batch "
                "are forbidden. Each operation is authorized separately. Native namespaces, functions.exec, shell, "
                "clock, wait, web, approvals, and other host tools are unavailable. Use the broker's "
                "read/list/search for memory, write for persistent changes, and shell for isolated debugging.")
        elif final_response:
            from maestro.backends.preparation import FINAL_PREPARATION_INSTRUCTION
            instruction = FINAL_PREPARATION_INSTRUCTION
        else:
            instruction = ("This is read-only single-request planning. No tools or executable namespaces "
                           "are available; use the frozen context and return the requested plan JSON.")
        if isinstance(value.get("input"), list):
            value["input"].append({"type": "message", "role": "developer",
                                   "content": [{"type": "input_text", "text": instruction}]})
        else:
            previous = value.get("instructions", "")
            value["instructions"] = (previous+"\n" if isinstance(previous, str) and previous else "")+instruction
        return admitted

    def _validate_item(self, item, *, tools_allowed=True):
        if not isinstance(item, dict):
            raise QuotaProxyError("Native response output item is malformed")
        kind = item.get("type")
        if kind in _SAFE_OUTPUT:
            return
        if (tools_allowed and kind in _FUNCTION_OUTPUT and item.get("name") in self.allowed_tool_names
                and item.get("namespace") == self.tool_namespace):
            return
        raise QuotaProxyError("Native response requested a forbidden host tool")

    def _collect(self, response, path, observations=None, *, tools_allowed=True):
        """Return buffered bytes, terminal response and a protocol violation."""
        observations = observations if observations is not None else {}
        observations.setdefault("sse_event_types", [])
        stats = observations.setdefault("stream_stats", {"chunks":0, "bytes":0,
            "events":0, "text_delta_events":0, "text_delta_bytes":0,
            "text_delta_nonwhitespace_bytes":0, "maximum_chunk_gap_seconds":0.0})
        content_type = next((str(v).lower() for k, v in response.headers.items()
                             if k.lower() == "content-type"), "")
        raw, pending, terminal, violation = bytearray(), bytearray(), None, False
        terminal_failure = False
        is_sse = True if "text/event-stream" in content_type else None
        if is_sse:
            observations["detected_format"] = "sse"
        for chunk in response.iter_bytes(self.deadline):
            if time.monotonic() >= self.deadline:
                raise TimeoutError("Native inference deadline elapsed")
            if not isinstance(chunk, bytes):
                raise QuotaProxyError("Invalid native transport bytes")
            now = time.monotonic()
            if "last_chunk_monotonic" in stats:
                stats["maximum_chunk_gap_seconds"] = max(stats["maximum_chunk_gap_seconds"], now-stats["last_chunk_monotonic"])
            stats.setdefault("first_chunk_monotonic", now)
            stats["last_chunk_monotonic"] = now
            stats["chunks"] += 1
            stats["bytes"] += len(chunk)
            raw.extend(chunk)
            if len(raw) > self.max_response_bytes:
                raise QuotaProxyError("Native response exceeded its byte allowance")
            if is_sse is None:
                # The native backend sometimes returns valid SSE with another
                # MIME type. Detect bounded framing rather than passing raw
                # bytes through or treating every HTTP 200 body as JSON.
                is_sse = self._detect_sse(raw)
                if is_sse is None:
                    continue
                observations["detected_format"] = "sse" if is_sse else "json"
                if is_sse:
                    pending.extend(raw)
            elif is_sse:
                pending.extend(chunk)
            if is_sse is not True:
                continue
            while True:
                separator = re.search(br"\r?\n\r?\n", pending)
                if separator is None:
                    break
                event = bytes(pending[:separator.start()])
                del pending[:separator.end()]
                lines = event.splitlines()
                if lines and lines[0].startswith(b"\xef\xbb\xbf"):
                    lines[0] = lines[0][3:]
                data = b"\n".join(line[5:].lstrip(b" ") for line in lines
                                   if line.startswith(b"data:"))
                if not data or data == b"[DONE]":
                    continue
                value = _strict_json(data)
                if not isinstance(value, dict):
                    raise QuotaProxyError("Native SSE event is malformed")
                kind = value.get("type")
                if kind in _SSE_EVENT_TYPES:
                    stats["events"] += 1
                    event_time = time.monotonic()
                    if kind == "response.output_text.delta" and isinstance(value.get("delta"), str):
                        stats.setdefault("first_text_delta_monotonic", event_time)
                        stats["last_text_delta_monotonic"] = event_time
                        delta = value["delta"]
                        stats["text_delta_events"] += 1
                        stats["text_delta_bytes"] += len(delta.encode("utf-8"))
                        stats["text_delta_nonwhitespace_bytes"] += len(''.join(c for c in delta if not c.isspace()).encode("utf-8"))
                    if kind.startswith("response.reasoning"):
                        stats["reasoning_events"] = stats.get("reasoning_events", 0)+1
                        stats.setdefault("first_reasoning_event_monotonic", event_time)
                        stats["last_reasoning_event_monotonic"] = event_time
                    if self._diagnostic_sink is not None:
                        diagnostic = {"type":kind,"monotonic":event_time}
                        if kind == "response.output_text.delta" and isinstance(value.get("delta"),str):
                            diagnostic["text_delta"] = value["delta"]
                        item = value.get("item")
                        if isinstance(item,dict) and item.get("type") in _SAFE_OUTPUT:
                            diagnostic["item_type"] = item["type"]
                        nested = value.get("response")
                        if isinstance(nested,dict):
                            diagnostic["response_id"] = self._safe_request_id(nested.get("id"))
                            if kind in {"response.completed","response.failed","response.incomplete"}:
                                diagnostic["usage"] = _safe_usage(nested.get("usage"))
                        self._diagnostic_sink(diagnostic)
                    nested_response = value.get("response")
                    identity = self._safe_request_id(nested_response.get("id")) if isinstance(nested_response, dict) else ""
                    previous = observations.get("response_id", "")
                    if identity and previous and identity != previous:
                        raise QuotaProxyError("Provider response identity changed within a stream")
                    if identity:
                        observations["response_id"] = identity
                    observations["last_event"] = kind
                    observations["terminal_seen"] = kind in {"response.completed", "response.failed", "response.incomplete"}
                    if observations.get("persist_progress"):
                        observations["persist_progress"]()
                if kind in _SSE_EVENT_TYPES and kind not in observations["sse_event_types"]:
                    observations["sse_event_types"].append(kind)
                if kind == "error":
                    # An error frame alone is diagnostic evidence, not proof
                    # the provider stopped execution. No raw message is kept.
                    error = value.get("error", value)
                    if isinstance(error, dict):
                        observations["error_type"] = self._safe_label(error.get("type"))
                        observations["error_code"] = self._safe_label(error.get("code"))
                if "item" in value or kind in {"response.output_item.added", "response.output_item.done"}:
                    try:
                        self._validate_item(value.get("item"), tools_allowed=tools_allowed)
                    except QuotaProxyError:
                        violation = True
                nested = value.get("response")
                if isinstance(nested, dict) and "output" in nested:
                    if not isinstance(nested["output"], list):
                        violation = True
                    else:
                        for item in nested["output"]:
                            try:
                                self._validate_item(item, tools_allowed=tools_allowed)
                            except QuotaProxyError:
                                violation = True
                if isinstance(kind, str) and kind.endswith("_call"):
                    try:
                        self._validate_item(value, tools_allowed=tools_allowed)
                    except QuotaProxyError:
                        violation = True
                if kind in {"response.completed", "response.failed", "response.incomplete"}:
                    terminal = value.get("response")
                    if not isinstance(terminal, dict):
                        terminal, violation = {}, True
                    terminal_failure = (kind != "response.completed"
                                        or terminal.get("status") != "completed")
                    break
            if terminal is not None:
                # Bytes following the terminal SSE frame were not validated and
                # must never reach a trusted native client.
                if pending:
                    del raw[-len(pending):]
                break
        if is_sse is False:
            value = _strict_json(raw)
            if isinstance(value, dict) and ((path == "/responses/compact" and value.get("object") == "response.compaction")
                                           or value.get("status") in {"completed", "failed", "incomplete"}):
                terminal = value
                terminal_failure = value.get("status") in {"failed", "incomplete"}
        if terminal is None:
            raise QuotaProxyError("Native inference returned no authoritative completion receipt")
        output = terminal.get("output", [])
        if not isinstance(output, list):
            violation = True
        else:
            for item in output:
                try:
                    self._validate_item(item, tools_allowed=tools_allowed)
                except QuotaProxyError:
                    violation = True
        return bytes(raw), terminal, violation, terminal_failure, is_sse

    @staticmethod
    def _detect_sse(raw):
        """Sniff only bounded, unambiguous JSON/SSE prefixes across chunks."""
        prefix = bytes(raw[:8193]).lstrip(b" \t\r\n")
        if prefix and len(prefix) < 3 and b"\xef\xbb\xbf".startswith(prefix):
            return None
        if prefix.startswith(b"\xef\xbb\xbf"):
            prefix = prefix[3:].lstrip(b" \t\r\n")
        if not prefix:
            if len(raw) > 8192:
                raise QuotaProxyError("Native response framing prefix exceeded its allowance")
            return None
        if prefix[:1] in (b"{", b"["):
            return False
        markers = (b":", b"data:", b"event:", b"id:", b"retry:")
        if any(prefix.startswith(marker) for marker in markers):
            return True
        if len(raw) <= 8192 and any(marker.startswith(prefix) for marker in markers):
            return None
        raise QuotaProxyError("Native response framing is not admitted JSON or SSE")

    def _handle(self, handler):
        reservation = response = None
        completed = False
        status, error_type, error_code, exception_class = None, "", "", ""
        stage, response_format, known_rejected = "request_validation", "", False
        response_request_id, never_dispatched, upstream_media_type = "", False, ""
        retry_after_seconds = None
        observations = {"sse_event_types": []}
        def persist_progress(*, closed=False):
            if reservation is not None:
                with self.store._transaction():
                    self.store.connection.execute("""INSERT INTO codex_gateway_progress
                        (reservation_id,response_id,request_id,last_event,observed_utc,observed_monotonic,terminal_seen,transport_closed,stream_stats_json)
                        VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(reservation_id) DO UPDATE SET
                        response_id=excluded.response_id,request_id=excluded.request_id,last_event=excluded.last_event,
                        observed_utc=CASE WHEN excluded.transport_closed=1 THEN observed_utc ELSE excluded.observed_utc END,
                        observed_monotonic=CASE WHEN excluded.transport_closed=1 THEN observed_monotonic ELSE excluded.observed_monotonic END,
                        terminal_seen=excluded.terminal_seen,transport_closed=excluded.transport_closed,
                        stream_stats_json=excluded.stream_stats_json""",
                        (reservation, observations.get("response_id", ""), response_request_id,
                         observations.get("last_event", ""), time.time(), time.monotonic(),
                         int(observations.get("terminal_seen", False)), int(closed),
                         json.dumps(observations.get("stream_stats", {}),separators=(",", ":"))))
        observations["persist_progress"] = persist_progress
        diagnostic_recorded = False
        handler._maestro_body_consumed = False

        def record_diagnostic():
            nonlocal diagnostic_recorded
            if reservation is not None and not diagnostic_recorded:
                with self.store._transaction():
                    self.store.connection.execute("""INSERT INTO codex_gateway_transport(
                        reservation_id,http_status,error_type,error_code,exception_class,stage,
                        response_format,known_rejected,response_request_id,sse_event_types_json,
                        never_dispatched,upstream_media_type,retry_after_seconds) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (reservation, status, error_type or observations.get("error_type", ""),
                         error_code or observations.get("error_code", ""), exception_class,
                         stage, observations.get("detected_format", response_format), int(known_rejected), response_request_id,
                         json.dumps(observations["sse_event_types"], separators=(",", ":")),
                         int(never_dispatched), upstream_media_type, retry_after_seconds))
                diagnostic_recorded = True

        def reject(code, message):
            # Accounting/diagnostics are durable before the caller can observe
            # the local error and decide whether a later phase is permissible.
            if reservation is not None and not completed:
                self.store.finish(reservation, failed=True)
            record_diagnostic()
            self._error(handler, code, message)

        try:
            # Bind the final-request policy and reservation under the same lock:
            # parallel HTTP handlers cannot observe a stale request index.
            with self._lock:
                path, headers, body, output_bound = self._request(handler)
                # Native ChatGPT need not accept an output-token field.
                if self.max_calls == 0:
                    snapshot = self.store.snapshot(self.account)
                    remaining = snapshot['max_output_tokens']-snapshot['output_tokens']
                    if remaining <= 0:
                        self.quota_denied = True
                        self.quota_denied_count += 1
                        raise BudgetExceeded('Preparation output-token quota exhausted')
                    output_bound = min(output_bound, remaining) if output_bound else remaining
                else:
                    output_bound = output_bound or max(1, self.max_output_tokens // self.max_calls)
                try:
                    reservation = self.store.reserve(self.account, uuid.uuid4().hex,
                                                     request_bytes=len(body), output_tokens=output_bound)
                except BudgetExceeded:
                    if not self.store.snapshot(self.account)["active"]:
                        self.quota_denied = True
                        self.quota_denied_count += 1
                    raise
            if self._closed or time.monotonic() >= self.deadline:
                raise TimeoutError("Native phase closed after reservation")
            stage = "upstream_connect"
            response = self._upstream(path, headers, body, self.deadline-time.monotonic())
            # The credentials have no artifact/log/error path. Release our local
            # header mapping as soon as the trusted HTTP transport has consumed it.
            headers.clear()
            with self._lock:
                self._responses[id(response)] = response
            status = response.status if type(response.status) is int and 100 <= response.status <= 599 else None
            retry_headers = [v for k,v in response.headers.items() if k.lower() == 'retry-after']
            if len(retry_headers) == 1:
                retry_value = retry_headers[0]
                if (isinstance(retry_value,str) and retry_value.isascii() and retry_value.isdigit()
                        and len(retry_value)<=8 and int(retry_value)<=604800):
                    retry_after_seconds = int(retry_value)
            request_ids = [value for key, value in response.headers.items() if key.lower() == "x-request-id"]
            if len(request_ids) == 1:
                response_request_id = self._safe_request_id(request_ids[0])
            persist_progress()
            content_type = next((str(value).lower() for key, value in response.headers.items()
                                 if key.lower() == "content-type"), "")
            upstream_media_type = self._safe_media_type(content_type)
            response_format = ("sse" if "text/event-stream" in content_type else
                               "json" if "application/json" in content_type else
                               "html" if "text/html" in content_type else "other")
            if response.status != 200:
                stage = "upstream_status"
                error_type, error_code = self._rejection_fields(response)
                if status in _KNOWN_REJECTIONS:
                    known_rejected = True
                    self._finish_known(reservation, body, {}, "", output_bound,
                                       protocol=path, terminal_failure=True)
                    completed = True
                raise QuotaProxyError("Native upstream did not return an admitted response")
            stage = "upstream_read"
            raw, terminal, violation, terminal_failure, is_sse = self._collect(response, path, observations,
                tools_allowed=not getattr(handler, '_maestro_final_preparation', False))
            stage = "response_validation"
            terminal_error = terminal.get("error")
            if isinstance(terminal_error, dict):
                error_type, error_code = (self._safe_label(terminal_error.get("type")),
                                          self._safe_label(terminal_error.get("code")))
            request_identity = terminal.get("id", "")
            if not isinstance(request_identity, str) or not re.fullmatch(r"[A-Za-z0-9_-]{0,128}", request_identity):
                request_identity = ""
            self._finish_known(reservation, body, terminal.get("usage"), request_identity,
                               output_bound, protocol=path, violation=violation,
                               terminal_failure=terminal_failure)
            completed = True
            if violation:
                with self._lock:
                    self._violations += 1
                raise QuotaProxyError("Native response requested a forbidden host tool")
            if terminal_failure:
                raise QuotaProxyError("Native inference returned a known unsuccessful terminal response")
            stage = "client_delivery"
            # Make transport evidence durable before clients can observe success.
            record_diagnostic()
            handler.send_response(200)
            handler.send_header("Content-Type", "text/event-stream" if is_sse else "application/json")
            handler.send_header("Content-Length", str(len(raw)))
            handler.send_header("Connection", "close")
            handler.end_headers()
            handler.wfile.write(raw)
        except _UpstreamTransportError as error:
            exception_class = self._safe_label(error.cause_class, exception=True)
            stage, never_dispatched = error.stage, error.never_dispatched
            if reservation is not None and never_dispatched:
                # Persist proof before releasing the active slot. Calls, bytes
                # and the conservative token charge stay consumed; this is not
                # a provider completion or a timer-based refund.
                record_diagnostic()
                self.store.finish(reservation)
                completed = True
            reject(502, "Native inference transport did not complete")
        except BudgetExceeded as error:
            exception_class = self._safe_label(type(error).__name__, exception=True)
            reject(429, "Native inference quota is consumed or unresolved")
        except PermissionError as error:
            exception_class = self._safe_label(type(error).__name__, exception=True)
            reject(403, "Native inference request is not authorized")
        except (ValueError, TypeError, UnicodeError) as error:
            exception_class = self._safe_label(type(error).__name__, exception=True)
            reject(400, "Native inference request or response is malformed")
        except Exception as error:
            exception_class = self._safe_label(type(error).__name__, exception=True)
            reject(502, "Native inference did not produce an admitted completion")
        finally:
            record_diagnostic()
            if reservation is not None and not completed:
                self.store.finish(reservation, failed=True)
            if response is not None:
                with self._lock:
                    self._responses.pop(id(response), None)
                with suppress(Exception):
                    response.close()
                # Closing a local transport is NOT a provider cancellation.
                persist_progress(closed=True)
            handler.close_connection = True

    @staticmethod
    def _safe_label(value, *, exception=False):
        pattern = r"[A-Za-z_][A-Za-z0-9_]{0,63}" if exception else r"[a-z][a-z0-9_]{0,63}"
        return value if isinstance(value, str) and re.fullmatch(pattern, value) else ""

    @staticmethod
    def _safe_request_id(value):
        return value if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value) else ""

    @staticmethod
    def _safe_media_type(value):
        media_type = value.split(";", 1)[0].strip().lower() if isinstance(value, str) else ""
        return media_type if len(media_type) <= 128 and re.fullmatch(
            r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+", media_type) else ""

    def _rejection_fields(self, response):
        """Extract only bounded static error labels; never persist raw errors."""
        raw = bytearray()
        try:
            for chunk in response.iter_bytes(min(self.deadline, time.monotonic() + 2)):
                if not isinstance(chunk, bytes) or len(raw) + len(chunk) > 16384:
                    return "", ""
                raw.extend(chunk)
            value = _strict_json(raw)
            error = value.get("error") if isinstance(value, dict) else None
            if isinstance(error, dict):
                return self._safe_label(error.get("type")), self._safe_label(error.get("code"))
        except Exception:
            pass
        return "", ""

    def _finish_known(self, reservation, body, usage, request_identity, output_bound, *,
                      protocol="", violation=False, terminal_failure=False):
        normalized = self.store.save_receipt(reservation, _safe_usage(usage),
            hashlib.sha256(body).hexdigest(), request_identity, protocol=protocol)
        actual = normalized["output_tokens"]
        exceeded = actual is not None and actual > output_bound
        if exceeded:
            actual = None  # Retain ceiling; authoritative actual receipt stays.
        with self.store._transaction():
            self.store.connection.execute("INSERT INTO codex_gateway_receipts VALUES(?,?,?,?)",
                                          (reservation, int(violation), int(exceeded), int(terminal_failure)))
        self.store.finish(reservation, actual_output_tokens=actual)
        self._emit("provider_terminal", {"reservation_id": reservation,
            "provider_request_id": request_identity, "usage": normalized,
            "terminal_failure": bool(terminal_failure)})

    def usage(self):
        value = self.store.usage(self.account)
        with self.store._lock:
            diagnostics = self.store.connection.execute("""SELECT
                coalesce(sum(g.protocol_violation),0),coalesce(sum(g.token_estimate_exceeded),0),
                coalesce(sum(g.terminal_failure),0)
                FROM codex_gateway_receipts g JOIN inference_reservations r
                ON r.reservation_id=g.reservation_id WHERE r.account=?""", (self.account,)).fetchone()
            errors = self.store.connection.execute("""SELECT t.http_status,t.error_type,t.error_code,
                t.exception_class,t.stage,t.response_format,t.known_rejected FROM codex_gateway_transport t
                JOIN inference_reservations r ON r.reservation_id=t.reservation_id
                WHERE r.account=? AND (t.http_status IS NULL OR t.http_status!=200 OR t.exception_class!='')
                ORDER BY r.rowid""", (self.account,)).fetchall()
            evidence = self.store.connection.execute("""SELECT t.http_status,t.error_type,t.error_code,
                t.exception_class,t.stage,t.response_format,t.known_rejected,t.response_request_id,
                t.sse_event_types_json,t.never_dispatched,t.upstream_media_type FROM codex_gateway_transport t
                JOIN inference_reservations r ON r.reservation_id=t.reservation_id
                WHERE r.account=? ORDER BY r.rowid""", (self.account,)).fetchall()
        with self._lock:
            value.update({"actual_requests": value["calls"], "request_count_unknown": False,
                          "budget_unit": "actual_inference_request",
                          "inflight_unknown": value["unresolved_requests"] > 0,
                          "protocol_violations": diagnostics[0],
                          "terminal_failures": diagnostics[2],
                          "quota_denied": self.quota_denied,
                          "quota_denied_count": self.quota_denied_count,
                          "token_ceiling_exceeded": diagnostics[1],
                          "output_token_limit_enforced": False})
            value["transport_errors"] = [{"http_status": row[0], "error_type": row[1], "error_code": row[2],
                "exception_class": row[3], "stage": row[4], "response_format": row[5],
                "known_rejected": bool(row[6])} for row in errors]
            value["transport_evidence"] = [{"http_status": row[0], "error_type": row[1], "error_code": row[2],
                "exception_class": row[3], "stage": row[4], "response_format": row[5],
                "known_rejected": bool(row[6]), "response_request_id": row[7],
                "sse_event_types": json.loads(row[8]), "never_dispatched": bool(row[9]),
                "upstream_media_type": row[10]} for row in evidence]
        return value

    def assert_resolved(self):
        if self.store.snapshot(self.account)["active"]:
            raise BudgetExceeded("Unresolved native inference blocks the next phase")

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            server, thread = self._server, self._thread
            responses = list(self._responses.values())
        for response in responses:
            with suppress(Exception):
                response.close()
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=2)
