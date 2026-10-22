"""Single-request planning clients with no hidden repair/retry inference.

The live coding-agent quota remains the trusted generation gateway's job.
This client does not wrap an autonomous CLI loop and count it as one request.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
import threading
import time
import math
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from maestro.agents.planner import parse_plan_decision
from maestro.agents.retrieval import build_context


class InferenceBudgetExceeded(RuntimeError):
    pass


class InferenceRequestUnknown(RuntimeError):
    """Dispatch was attempted but no trustworthy provider receipt was returned.

    The reservation remains consumed. Remote execution/cost cannot be inferred
    from a local timeout, connection reset, or provider adapter parse failure.
    """
    inflight_unknown = True

    def __init__(self, message, *, request_id, scope_id, context):
        super().__init__(message)
        self.request_id, self.scope_id = request_id, scope_id
        self.context, self.receipt = context, None


class PlanningResponseError(ValueError):
    """A completed provider request failed schema or deadline admission checks."""
    inflight_unknown = False

    def __init__(self, message, *, receipt, context):
        super().__init__(message)
        self.receipt, self.context = receipt, context
        self.request_id, self.scope_id = receipt.request_id, receipt.scope_id


@dataclass(frozen=True)
class ProviderResponse:
    text: str
    provider_request_id: str = ""
    input_tokens: int | None = None
    output_tokens: int | None = None


@dataclass(frozen=True)
class PlanningReceipt:
    request_id: str
    scope_id: str
    context_hash: str
    response_hash: str
    provider_request_id: str
    input_tokens: int | None
    output_tokens: int | None
    actual_requests: int = 1
    logical_requests: int | None = None
    retry_attempts: int = 0
    known_input_tokens: int | None = None
    known_output_tokens: int | None = None


class OpenAICompatibleProvider:
    """Minimal explicit HTTP provider; no retry, streaming, tool loop, or fallback.

    Credentials are read just before dispatch and never returned in receipts.
    A provider-compatible endpoint is a deployment setting, not an assumption
    that every runtime/model supports this API. HTTPS is required off localhost.
    """
    is_live = True

    def __init__(self, endpoint, model, *, api_key_env="MAESTRO_API_KEY", timeout=60,
                 max_output_tokens=2048):
        parsed = urlparse(endpoint)
        if parsed.scheme not in {"https", "http"} or not parsed.hostname:
            raise ValueError("HTTP(S) provider endpoint required")
        if parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("remote provider endpoint requires HTTPS")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("credentials/query/fragment are not allowed in provider endpoint")
        if timeout <= 0 or max_output_tokens < 1 or not model:
            raise ValueError("invalid provider timeout/model/output cap")
        self.endpoint, self.model, self.api_key_env = endpoint, model, api_key_env
        self.timeout, self.max_output_tokens = timeout, max_output_tokens

    def __call__(self, payload):
        key = os.environ.get(self.api_key_env)
        if not key:
            raise RuntimeError(f"provider credential environment variable missing: {self.api_key_env}")
        body = json.dumps({"model": self.model, "messages": payload["messages"],
                           "max_tokens": min(payload["max_output_tokens"], self.max_output_tokens),
                           "temperature": 0, "response_format": {"type": "json_object"}}).encode()
        request = Request(self.endpoint, body, {"Content-Type": "application/json",
                          "Authorization": f"Bearer {key}",
                          "X-Maestro-Request-Id": payload["request_id"]}, method="POST")
        remaining = self.timeout
        if payload.get("deadline") is not None:
            remaining = min(remaining, payload["deadline"] - time.monotonic())
            if remaining <= 0:
                raise TimeoutError("planning deadline elapsed before HTTP dispatch")
        with urlopen(request, timeout=remaining) as response:
            raw = response.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError("provider response exceeds limit")
        decoded = json.loads(raw)
        try:
            text = decoded["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError("provider response is not compatible chat JSON") from exc
        if not isinstance(text, str):
            raise ValueError("provider returned non-text planning content")
        usage = decoded.get("usage", {})
        return ProviderResponse(text, str(decoded.get("id", "")),
                                usage.get("prompt_tokens"), usage.get("completion_tokens"))


class PlannerClient:
    """One inference request per (agent, round), including failed attempts.

    A live provider requires a durable ``reserve`` callback before dispatch:
    ``reserve(scope_id, request_id, input_upper_bound, max_output_tokens)``.
    It must atomically reject any previous used/in-flight request in that scope
    and return exactly True for a new reservation. An ambiguous crashed request
    is not retried here. The scheduler reconciles its receipt separately.
    """
    def __init__(self, provider, *, reserve=None, record_usage=None, episode_id="episode",
                 max_output_tokens=2048):
        if provider is None:
            raise ValueError("planner provider must be explicitly injected")
        if not isinstance(max_output_tokens, int) or max_output_tokens < 1:
            raise ValueError("positive planner output cap required")
        if getattr(provider, "is_live", False) and reserve is None:
            raise ValueError("live planning requires a durable budget reservation callback")
        self.provider, self.reserve, self.record_usage = provider, reserve, record_usage
        self.episode_id, self.max_output_tokens = episode_id, max_output_tokens
        self._used = set()
        self._completed = {}
        self._lock = threading.Lock()

    def plan(self, state, round_index, board, *, request_id, token_budget=8192,
             harness_context=None, deadline=None):
        """Accept bounded source/memory summaries from the trusted FS adapter.

        ``harness_context`` is never agent-controlled tool execution. All bytes
        count toward context and inference reservations; no hidden free prompt.
        """
        if deadline is not None and (not isinstance(deadline, (int, float))
                                     or not math.isfinite(deadline)):
            raise ValueError("planning deadline must be a finite monotonic timestamp")
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("planning deadline elapsed before context assembly")
        extra = harness_context or {}
        allowed = {"parent_source", "direction_brief", "own_private_docs", "diagnostics", "source_manifest"}
        if not isinstance(extra, dict) or set(extra) - allowed:
            raise ValueError("unknown trusted harness context fields")
        try:
            extra_text = json.dumps(extra, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("harness context must be finite JSON data") from exc
        extra_size = len(extra_text.encode()) if extra else 0
        if extra_size > token_budget // 2:
            raise ValueError("harness context exceeds its half-context byte allowance")
        context = build_context(state, board, token_budget - extra_size - (384 if extra else 0))
        if extra:
            context["harness_context"] = extra
            manifest = context["manifest"]
            manifest["retrieval_token_budget"] = manifest["token_budget"]
            manifest["token_budget"] = token_budget
            manifest["harness_context_hash"] = hashlib.sha256(extra_text.encode()).hexdigest()
            manifest.pop("context_content_hash", None)
            unhashed = json.dumps(context, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            manifest["context_content_hash"] = hashlib.sha256(unhashed.encode()).hexdigest()
            if len(json.dumps(context, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()) > token_budget:
                raise ValueError("combined trusted context exceeds total context budget")
        canonical = json.dumps(context, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        context_hash = hashlib.sha256(canonical.encode()).hexdigest()
        scope = f"{self.episode_id}:{round_index}:{state.agent_id}:PLAN_RO"
        cache_key = (scope, request_id)
        with self._lock:
            if cache_key in self._completed:
                old_hash, result = self._completed[cache_key]
                if old_hash != context_hash:
                    raise ValueError("request ID reused with a different context")
                return result
            if scope in self._used:
                raise InferenceBudgetExceeded("one actual planning request per agent/round")
            input_bound = len(canonical.encode()) + 2048
            if self.reserve is not None and self.reserve(
                    scope, request_id, input_bound, self.max_output_tokens) is not True:
                raise InferenceBudgetExceeded("persistent planning budget rejected request")
            self._used.add(scope)
        system = (
            "Return one JSON object with action retain/revise/withdraw, optional plan, and lessons. "
            "Planning is read-only: propose changes before any candidate editing. "
            "A revise plan requires target, mod_type, diagnosis, change, expected_effect, evidence_run_ids. "
            "Use evidence_catalog to include all matching own formal IDs, including failures; "
            "verbose history examples may be truncated but this identity index is complete. "
            "Do not invent scores, perform tests, change code, or choose numeric market bids. "
            "Retain may only reference the existing eligible pending version. "
            "Use only the frozen context; no tools or external data are available.")
        payload = {"request_id": request_id, "scope_id": scope,
                   "max_output_tokens": self.max_output_tokens,
                   "messages": [{"role": "system", "content": system},
                                {"role": "user", "content": canonical}]}
        if deadline is not None:
            payload["deadline"] = deadline
            if time.monotonic() >= deadline:
                raise TimeoutError("planning deadline elapsed before inference dispatch")
        try:
            # Exactly one dispatch. Without a response, transport/provider
            # failures cannot establish that the remote request never ran.
            response = self.provider(payload)
        except Exception as error:
            raise InferenceRequestUnknown(
                f"provider request has no confirmed receipt: {type(error).__name__}: {error}",
                request_id=request_id, scope_id=scope, context=context) from error
        if isinstance(response, str):
            response = ProviderResponse(response)
        if not isinstance(response, ProviderResponse) or not isinstance(response.text, str):
            raise InferenceRequestUnknown("provider returned no usable response receipt",
                request_id=request_id, scope_id=scope, context=context)
        receipt = PlanningReceipt(request_id, scope, context_hash,
                                  hashlib.sha256(response.text.encode()).hexdigest(),
                                  response.provider_request_id, response.input_tokens,
                                  response.output_tokens)
        # Account even a malformed answer before parsing it. Provider exceptions
        # leave the durable reservation unresolved rather than refunding blindly.
        if self.record_usage is not None:
            self.record_usage(receipt)
        if deadline is not None and time.monotonic() >= deadline:
            raise PlanningResponseError("planning response arrived after the common deadline",
                                        receipt=receipt, context=context)
        try:
            decision = parse_plan_decision(response.text, state, round_index, namespace=self.episode_id)
        except (ValueError, TypeError) as error:
            raise PlanningResponseError(f"planning response schema rejected: {error}",
                                        receipt=receipt, context=context) from error
        result = (decision, receipt, context)
        with self._lock:
            self._completed[cache_key] = (context_hash, result)
        return result
