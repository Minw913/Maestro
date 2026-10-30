"""Parallel planning with a common absolute deadline and no worker state writes.

Workers receive deep copies and return immutable outcomes. The market owner
alone applies decisions, records receipts, and freezes submissions. A timeout
does not imply the provider stopped processing or that its cost is refundable.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait
from copy import deepcopy
from dataclasses import dataclass
import math
import threading
import time
from maestro.agents.planner import PlanDecision
from maestro.agents.planning_diagnostics import safe_planning_diagnostic


@dataclass(frozen=True)
class PlanOutcome:
    agent_id: str
    decision: PlanDecision | None = None
    receipt: object | None = None
    context: dict | None = None
    error: str | None = None
    status: str = "ABSTAIN"
    inflight_unknown: bool = False
    started_at: float | None = None
    finished_at: float | None = None
    request_id: str | None = None
    scope_id: str | None = None
    diagnostic: dict[str, str] | None = None


def _capabilities(backend):
    value = getattr(backend, "planning_capabilities", {})
    return value() if callable(value) else value


def parallel_plans(states, contexts, backend, round_index, deadline, *, budget_unit="provider_request"):
    """Call each agent once, concurrently, using one monotonic deadline.

    Live backends MUST expose ``plan_with_receipt(state, round, context,
    deadline=...) -> (PlanDecision, receipt, assembled_context)`` and declare
    ``planning_capabilities`` with single_request=True,
    hard_request_timeout=True, mutates_input_state=False. The callback may use
    its trusted durable budget service; it may not mutate market AgentState or
    the market ledger. It must pass the remaining deadline into its transport.

    A started callback that misses the deadline is quarantined as
    INFLIGHT_UNKNOWN. Its late result is discarded; this function does not wait
    for it, issue a replacement inference, or call it again. The round owner
    must reconcile that budget scope before proceeding to another phase/episode
    for the affected agent. An optional ``cancel_planning(agent, round)`` may
    confirm quiescence; it must be nonblocking and return exactly True only when
    no local callback/provider action can still complete. HTTP cancellation
    cannot establish that a remote provider ceased computation.
    """
    if not isinstance(deadline, (int, float)) or not math.isfinite(deadline):
        raise ValueError("finite absolute monotonic planning deadline required")
    state_map = states if isinstance(states, dict) else {s.agent_id: s for s in states}
    if any(agent != state.agent_id for agent, state in state_map.items()):
        raise ValueError("planning population identity mismatch")
    ids = list(state_map)
    if not ids:
        return {}
    if set(contexts) != set(ids):
        raise ValueError("one private context is required for every planning agent")
    caps = _capabilities(backend)
    if budget_unit not in ("provider_request", "session_turn"):
        raise ValueError("Unknown planning budget unit")
    required_cap = "single_request" if budget_unit == "provider_request" else "single_session_turn"
    bounded_recovery=(isinstance(caps,dict) and budget_unit=='provider_request'
        and caps.get('single_logical_request') is True
        and type(caps.get('max_request_attempts')) is int and 1<=caps['max_request_attempts']<=101)
    allowed = (isinstance(caps, dict)
               and (caps.get(required_cap) is True or bounded_recovery)
               and caps.get("hard_request_timeout") is True
               and caps.get("mutates_input_state") is False
               and callable(getattr(backend, "plan_with_receipt", None)))
    if not allowed:
        return {a: PlanOutcome(a, error="BACKEND_UNBOUNDED: controlled single-request transport required")
                for a in ids}
    if time.monotonic() >= deadline:
        return {a: PlanOutcome(a, error="DEADLINE_BEFORE_DISPATCH") for a in ids}
    inputs = {a: (deepcopy(state_map[a]), deepcopy(contexts[a])) for a in ids}
    started = {}
    cancelled_agents = set()
    lock = threading.Lock()
    gate = threading.Event()

    def execute(agent):
        gate.wait()
        with lock:
            at = time.monotonic()
            if at >= deadline or agent in cancelled_agents:
                return PlanOutcome(agent, error="DEADLINE_BEFORE_DISPATCH", finished_at=at)
            started[agent] = at
        try:
            result = backend.plan_with_receipt(inputs[agent][0], round_index,
                                               inputs[agent][1], deadline=deadline)
            finished = time.monotonic()
            if not isinstance(result, tuple) or len(result) != 3:
                raise ValueError("backend must return independent decision/receipt/context")
            decision, receipt, context = result
            calls = (receipt.get("actual_requests") if isinstance(receipt, dict)
                     else getattr(receipt, "actual_requests", None))
            turns = (receipt.get("session_turns") if isinstance(receipt, dict)
                     else getattr(receipt, "session_turns", None))
            unknown = (receipt.get("request_count_unknown") if isinstance(receipt, dict)
                       else getattr(receipt, "request_count_unknown", False))
            logical=(receipt.get('logical_requests') if isinstance(receipt,dict) else getattr(receipt,'logical_requests',None))
            retries=(receipt.get('retry_attempts') if isinstance(receipt,dict) else getattr(receipt,'retry_attempts',None))
            metered = ((logical==1 and type(calls) is int and 1<=calls<=caps['max_request_attempts']
                        and retries==calls-1) if bounded_recovery else calls==1) if budget_unit == "provider_request" else (calls is None and turns == 1 and unknown is True)
            if not isinstance(decision, PlanDecision) or not metered or not isinstance(context, dict):
                raise ValueError("invalid or unmetered single-request planning result")
            if context.get("agent_id") != agent:
                raise ValueError("planning result context belongs to another agent")
            if finished >= deadline:
                return PlanOutcome(agent, receipt=receipt, context=deepcopy(context),
                    error="LATE_RESPONSE_DISCARDED", started_at=at, finished_at=finished)
            return PlanOutcome(agent, decision, receipt, deepcopy(context), status="PLANNED",
                               started_at=at, finished_at=finished)
        except Exception as error:
            unknown = getattr(error, "inflight_unknown", False) is True
            return PlanOutcome(agent,
                receipt=getattr(error, "receipt", None),
                context=deepcopy(getattr(error, "context", None)),
                error=f"{type(error).__name__}: {error}"[:4000],
                status="INFLIGHT_UNKNOWN" if unknown else "ABSTAIN",
                inflight_unknown=unknown, started_at=at, finished_at=time.monotonic(),
                request_id=getattr(error, "request_id", None), scope_id=getattr(error, "scope_id", None),
                diagnostic=safe_planning_diagnostic(getattr(error, "diagnostic", None)))

    executor = ThreadPoolExecutor(max_workers=len(ids), thread_name_prefix="maestro-plan")
    futures = {executor.submit(execute, agent): agent for agent in ids}
    gate.set()
    try:
        done, pending = wait(futures, timeout=max(0, deadline - time.monotonic()))
        grace=caps.get('cleanup_grace_seconds',0.)
        if pending and bounded_recovery and isinstance(grace,(int,float)) and 0<grace<=5:
            drained,pending=wait(pending,timeout=grace)
            done|=drained  # execute() still rejects results returned after the original deadline.
    except BaseException:
        with lock:
            cancelled_agents.update(ids)
        for future in futures:
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    outcomes = {futures[f]: f.result() for f in done}
    cancel = getattr(backend, "cancel_planning", None)
    for future in pending:
        agent = futures[future]
        with lock:
            cancelled_agents.add(agent)
            at = started.get(agent)
        cancelled_before_start = future.cancel()
        quiescent = cancelled_before_start
        if not quiescent and callable(cancel):
            try:
                quiescent = cancel(agent, round_index) is True
            except Exception:
                quiescent = False
        # Even a callback that just completed after wait's snapshot missed the
        # deadline. It cannot be admitted by racing this result collection.
        if future.done() and not future.cancelled():
            late = future.result()
            outcomes[agent] = PlanOutcome(agent, receipt=late.receipt, context=late.context,
                error=late.error if late.inflight_unknown else "LATE_RESPONSE_DISCARDED",
                status="INFLIGHT_UNKNOWN" if late.inflight_unknown else "ABSTAIN",
                inflight_unknown=late.inflight_unknown,
                started_at=late.started_at, finished_at=late.finished_at,
                request_id=late.request_id, scope_id=late.scope_id,
                diagnostic=late.diagnostic if late.inflight_unknown else None)
        else:
            unknown = not quiescent and at is not None
            outcomes[agent] = PlanOutcome(agent,
                error="INFLIGHT_UNKNOWN" if unknown else "PLANNING_CANCELLED_AT_DEADLINE",
                status="INFLIGHT_UNKNOWN" if unknown else "ABSTAIN",
                inflight_unknown=unknown, started_at=at, finished_at=time.monotonic())
    executor.shutdown(wait=False, cancel_futures=True)
    return {agent: outcomes[agent] for agent in ids}
