"""Inference gateway with quota reservation before outbound network dispatch.

This callable service is deliberately provider-neutral. The injected transport
must enforce the transmitted max_tokens and may not follow redirects. HTTP/CLI
provider integration must route *all* inference through this service under a
network-isolating SandboxProvider; environment variables alone are insufficient.
"""
from __future__ import annotations

import hmac
import hashlib
import json
import secrets
import uuid
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import socket
import time
import socketserver
from pathlib import Path
from urllib.parse import urlsplit
from typing import Callable
from .budgets import BudgetStore, BudgetExceeded


class BudgetGateway:
    def __init__(self, store: BudgetStore, transport: Callable[[dict], dict], *, allow_streaming: bool = False,
                 allowed_model: str | None = None):
        self.store, self.transport = store, transport
        self.allow_streaming = allow_streaming
        self.allowed_model = allowed_model
        self._credentials: dict[str, tuple[str, str]] = {}
        self._credentials_lock = threading.RLock()

    def credential(self, account: str, agent_id: str) -> str:
        self.store.snapshot(account)  # Validate before issuing an identity.
        token = secrets.token_urlsafe(32)
        with self._credentials_lock:
            self._credentials[token] = (account, agent_id)
        return token

    def complete(self, token: str, request_id: str, payload: dict) -> dict:
        with self._credentials_lock:
            identity = next((value for key, value in self._credentials.items()
                             if hmac.compare_digest(key, token)), None)
        if identity is None:
            raise BudgetExceeded("Unrecognized phase credential")
        if self.allowed_model is not None and payload.get("model") != self.allowed_model:
            raise BudgetExceeded("Request model differs from the frozen runtime contract")
        if sum(key in payload for key in ("max_tokens", "max_output_tokens", "max_completion_tokens")) != 1:
            raise BudgetExceeded("Exactly one output-token ceiling field is allowed")
        if payload.get("stream") and not self.allow_streaming:
            raise BudgetExceeded("Streaming transport is not enabled by this quota adapter")
        if payload.get("n", 1) != 1:
            raise BudgetExceeded("Multiple completions per call are not allowed")
        maximum = payload.get("max_tokens", payload.get("max_output_tokens", payload.get("max_completion_tokens")))
        if not isinstance(maximum, int) or isinstance(maximum, bool) or maximum <= 0:
            raise BudgetExceeded("Request must specify a bounded output-token ceiling")
        try:
            canonical_request = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                           ensure_ascii=False, allow_nan=False).encode()
        except (TypeError, ValueError) as exc:
            raise BudgetExceeded("Inference request must contain finite JSON data") from exc
        request_bytes = len(canonical_request)
        request_hash = hashlib.sha256(canonical_request).hexdigest()
        with self._credentials_lock:
            if self._credentials.get(token) != identity:
                raise BudgetExceeded("Revoked phase credential")
            reservation = self.store.reserve(identity[0], request_id,
                                             request_bytes=request_bytes, output_tokens=maximum)
        try:
            response = self.transport(dict(payload))
            usage = response.get("usage", {})
            measured = self.store.save_receipt(reservation, usage, request_hash,
                str(response.get("provider_request_id") or response.get("id") or ""),
                protocol=payload.get("_maestro_http_path", ""))
            actual = measured["output_tokens"]
            self.store.finish(reservation, actual_output_tokens=actual)
            return response
        except BaseException:
            self.store.finish(reservation, failed=True)
            raise

    def revoke(self, token: str) -> None:
        with self._credentials_lock:
            self._credentials.pop(token, None)


class HTTPUpstream:
    """Fixed HTTPS provider transport, bounded response, no redirect following.

    Streaming responses are buffered for accounting before forwarding their SSE
    bytes. This is compatible with clients accepting buffered SSE, but latency
    and client timeouts must be qualified in a real runtime pilot.
    """
    PATHS = {"/v1/messages", "/v1/messages?beta=true", "/v1/chat/completions", "/v1/responses"}

    def __init__(self, base_url: str, api_key: str, *, auth_header: str = "Authorization",
                 timeout: float = 60, max_response_bytes: int = 8_000_000,
                 anthropic_beta: tuple[str, ...] = (), absolute_deadline: float | None = None):
        parts = urlsplit(base_url)
        if parts.scheme != "https" or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment:
            raise ValueError("A fixed HTTPS provider origin is required")
        if parts.path not in ("", "/"):
            raise ValueError("Provider URL must be an origin; request paths are allowlisted")
        self.host, self.port, self.api_key = parts.hostname, parts.port, api_key
        self.auth_header, self.timeout, self.max_response_bytes = auth_header, timeout, max_response_bytes
        self.anthropic_beta = anthropic_beta
        self.absolute_deadline = absolute_deadline

    def __call__(self, payload: dict) -> dict:
        payload = dict(payload)
        path = payload.pop("_maestro_http_path", None)
        if path not in self.PATHS:
            raise BudgetExceeded("Unsupported provider endpoint")
        headers = {"Content-Type": "application/json"}
        headers[self.auth_header] = (f"Bearer {self.api_key}" if self.auth_header.lower() == "authorization" else self.api_key)
        if path.startswith("/v1/messages"):
            headers["anthropic-version"] = "2023-06-01"
            if self.anthropic_beta:
                headers["anthropic-beta"] = ",".join(self.anthropic_beta)
        deadline = self.absolute_deadline or time.monotonic()+self.timeout
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Inference request's absolute deadline elapsed")
        expired = threading.Event()

        class DeadlineConnection(http.client.HTTPSConnection):
            watched_socket = None
            def connect(inner):
                super().connect()
                inner.watched_socket = inner.sock
                if expired.is_set() or time.monotonic() >= deadline:
                    inner.close()
                    raise TimeoutError("Inference deadline elapsed during connection/DNS")
            def send(inner, data):
                if expired.is_set() or time.monotonic() >= deadline:
                    raise TimeoutError("Inference deadline elapsed before HTTP dispatch")
                return super().send(data)

        connection = DeadlineConnection(self.host, self.port, timeout=min(self.timeout,remaining))
        def abort():
            expired.set()
            current = connection.watched_socket
            if current is not None:
                try:
                    current.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            connection.close()
        watchdog = threading.Timer(remaining, abort)
        watchdog.daemon = True
        watchdog.start()
        try:
            connection.request("POST", path, body=json.dumps(payload).encode(), headers=headers)
            response = connection.getresponse()
            raw = response.read(self.max_response_bytes + 1)
            if expired.is_set() or time.monotonic() >= deadline:
                raise TimeoutError("Inference response completed after its absolute deadline")
            if len(raw) > self.max_response_bytes:
                raise BudgetExceeded("Provider response exceeded the gateway byte cap")
            if response.status != 200:
                raise RuntimeError(f"Provider returned HTTP {response.status}")
            content_type = response.getheader("Content-Type", "application/json")
            usage = {}
            provider_request_id = response.getheader("request-id") or response.getheader("x-request-id") or ""
            if "event-stream" in content_type:
                for line in raw.decode("utf-8").splitlines():
                    if not line.startswith("data:") or line[5:].strip() == "[DONE]":
                        continue
                    try:
                        event = json.loads(line[5:].strip())
                    except ValueError:
                        continue
                    usage.update(event.get("usage") or {})
                    usage.update(event.get("message", {}).get("usage") or {})
                    usage.update(event.get("response", {}).get("usage") or {})
                    provider_request_id = (provider_request_id or event.get("message", {}).get("id")
                                           or event.get("response", {}).get("id") or event.get("id") or "")
            else:
                decoded = json.loads(raw)
                usage = decoded.get("usage", {})
                provider_request_id = provider_request_id or decoded.get("id", "")
            return {"usage": usage, "provider_request_id": str(provider_request_id),
                    "_raw_body": raw, "_content_type": content_type}
        finally:
            watchdog.cancel()
            connection.close()


class GatewayHTTPServer:
    """Authenticated local endpoint for runtime clients; quota checks precede I/O.

    A separate network sandbox must ensure this is the agent's only reachable
    inference route. This server does not claim to enforce network isolation.
    """
    def __init__(self, gateway: BudgetGateway, host: str = "127.0.0.1", port: int = 0,
                 max_request_bytes: int = 8_000_000, unix_socket: str | Path | None = None):
        owner = self
        self.gateway, self.max_request_bytes = gateway, max_request_bytes

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                if self.path not in HTTPUpstream.PATHS:
                    self.send_error(404)
                    return
                token = self.headers.get("x-api-key", "")
                auth = self.headers.get("Authorization", "")
                if auth.startswith("Bearer "):
                    token = auth[7:]
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 < length <= owner.max_request_bytes:
                        raise BudgetExceeded("Invalid request size")
                    payload = json.loads(self.rfile.read(length))
                    if not isinstance(payload, dict) or "_maestro_http_path" in payload:
                        raise BudgetExceeded("Invalid request envelope")
                    payload["_maestro_http_path"] = self.path
                    result = gateway.complete(token, self.headers.get("x-request-id") or uuid.uuid4().hex, payload)
                    raw = result.get("_raw_body")
                    if raw is None:
                        raw = json.dumps(result).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", result.get("_content_type", "application/json"))
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers(); self.wfile.write(raw)
                except BudgetExceeded:
                    self.send_error(429, "Missing, expired, or exhausted inference authorization")
                except Exception:
                    self.send_error(502, "Upstream inference failed; reservation remains accounted")

            def log_message(self, *args):
                pass  # Never put phase credentials or prompts in HTTP access logs.

        self.unix_socket = Path(unix_socket) if unix_socket else None
        if self.unix_socket:
            class UnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
                daemon_threads = True
            self.unix_socket.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if self.unix_socket.exists():
                raise RuntimeError("Gateway socket already exists; reconcile the previous service first")
            self.server = UnixServer(str(self.unix_socket), Handler)
            self.unix_socket.chmod(0o666)  # Parent is private; container mounts only this socket.
        else:
            self.server = ThreadingHTTPServer((host, port), Handler)
        self.thread = None

    @property
    def url(self):
        if self.unix_socket:
            raise RuntimeError("A Unix gateway has no external TCP URL")
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}"

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
            self.thread.start()
        return self

    def close(self):
        if self.thread is not None:
            self.server.shutdown(); self.thread.join(timeout=5)
        self.server.server_close()
        if self.unix_socket:
            self.unix_socket.unlink(missing_ok=True)
