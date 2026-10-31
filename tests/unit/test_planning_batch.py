from collections import Counter
import json
import threading
import time
import unittest

from maestro.types import AgentState
from maestro.agents.planner import PlanDecision, mock_plan
from maestro.agents.planning_batch import parallel_plans
from maestro.llm import (PlannerClient, PlanningReceipt, ProviderResponse,
                         InferenceRequestUnknown, PlanningResponseError,
                         InferenceBudgetExceeded)

CAPABILITIES = {"single_request": True, "hard_request_timeout": True,
                "mutates_input_state": False}


def response(state, round_index):
    return (PlanDecision("revise", mock_plan(state, round_index, 1)),
            PlanningReceipt(f"r-{state.agent_id}", f"scope-{state.agent_id}",
                            "context", "response", "provider", 100, 20),
            {"agent_id": state.agent_id})


class ParallelPlanningTests(unittest.TestCase):
    def _client_backend(self, client):
        class Backend:
            planning_capabilities = CAPABILITIES
            def plan_with_receipt(self, state, round_index, context, *, deadline):
                return client.plan(state, round_index, context,
                    request_id=f"{state.agent_id}:{round_index}", deadline=deadline)
        return Backend()

    def test_concurrent_start_single_call_shared_deadline_and_private_input_copies(self):
        states = {a: AgentState(a, "parent", .5) for a in "abc"}
        contexts = {a: {"owner": a} for a in states}
        barrier = threading.Barrier(3)
        calls, deadlines = [], []
        lock = threading.Lock()
        class Backend:
            planning_capabilities = CAPABILITIES
            def plan_with_receipt(self, state, round_index, context, *, deadline):
                with lock:
                    calls.append(state.agent_id)
                    deadlines.append(deadline)
                barrier.wait(timeout=1)
                state.lessons.append({"worker_only": True})
                context["owner"] = "mutated-copy"
                return response(state, round_index)
        deadline = time.monotonic() + 2
        outcomes = parallel_plans(states, contexts, Backend(), 0, deadline)
        self.assertEqual(Counter(calls), {a: 1 for a in states})
        self.assertEqual(deadlines, [deadline] * 3)
        self.assertTrue(all(x.status == "PLANNED" and x.decision for x in outcomes.values()))
        self.assertTrue(all(not s.lessons for s in states.values()))
        self.assertEqual(contexts, {a: {"owner": a} for a in states})

    def test_uncancellable_timeout_is_unknown_no_late_decision_or_state_write(self):
        release, finished, entered = threading.Event(), threading.Event(), threading.Event()
        calls = []
        class Backend:
            planning_capabilities = CAPABILITIES
            def plan_with_receipt(self, state, round_index, context, *, deadline):
                calls.append(state.agent_id)
                entered.set()
                release.wait(1)
                state.parent_score = 1
                finished.set()
                return response(state, round_index)
            def cancel_planning(self, agent, round_index):
                return False
        state = AgentState("a", "parent", .5)
        try:
            start = time.monotonic()
            outcomes = parallel_plans({"a": state}, {"a": {}}, Backend(), 0, start + .08)
            self.assertLess(time.monotonic() - start, .5)
            self.assertTrue(entered.is_set())
            self.assertEqual(outcomes["a"].status, "INFLIGHT_UNKNOWN")
            self.assertTrue(outcomes["a"].inflight_unknown)
            self.assertIsNone(outcomes["a"].decision)
        finally:
            release.set()
            self.assertTrue(finished.wait(1))
        self.assertEqual(state.parent_score, .5)
        self.assertEqual(calls, ["a"])

    def test_expired_deadline_or_uncontrolled_backend_never_calls_model(self):
        calls = []
        class Uncontrolled:
            def plan_with_receipt(self, *args, **kwargs):
                calls.append(1)
        states, contexts = {"a": AgentState("a", "p", .5)}, {"a": {}}
        outcomes = parallel_plans(states, contexts, Uncontrolled(), 0, time.monotonic() + 1)
        self.assertEqual(outcomes["a"].status, "ABSTAIN")
        self.assertIn("BACKEND_UNBOUNDED", outcomes["a"].error)
        class Bounded(Uncontrolled):
            planning_capabilities = CAPABILITIES
        outcomes = parallel_plans(states, contexts, Bounded(), 0, time.monotonic() - 1)
        self.assertEqual(outcomes["a"].error, "DEADLINE_BEFORE_DISPATCH")
        self.assertEqual(calls, [])

    def test_provider_exception_is_per_agent_abstention_without_retry(self):
        calls = []
        class Backend:
            planning_capabilities = CAPABILITIES
            def plan_with_receipt(self, state, round_index, context, *, deadline):
                calls.append(state.agent_id)
                if state.agent_id == "a":
                    raise TimeoutError("transport deadline")
                return response(state, round_index)
        states = {a: AgentState(a, "p", .5) for a in "ab"}
        results = parallel_plans(states, {a: {} for a in states}, Backend(), 2, time.monotonic() + 1)
        self.assertEqual(results["a"].status, "ABSTAIN")
        self.assertEqual(results["b"].status, "PLANNED")
        self.assertEqual(Counter(calls), {"a": 1, "b": 1})

    def test_multi_call_or_foreign_context_receipt_is_rejected(self):
        class Backend:
            planning_capabilities = CAPABILITIES
            def plan_with_receipt(self, state, round_index, context, *, deadline):
                decision, receipt, assembled = response(state, round_index)
                return decision, {"actual_requests": 2}, assembled
        results = parallel_plans({"a": AgentState("a", "p", .5)}, {"a": {}},
                                  Backend(), 0, time.monotonic() + 1)
        self.assertEqual(results["a"].status, "ABSTAIN")
        self.assertIn("unmetered", results["a"].error)

    def test_planner_propagates_deadline_into_transport_without_extra_inference(self):
        calls = []
        def provider(payload):
            calls.append(payload)
            return json.dumps({"action": "revise", "plan": {
                "target": "local_search", "mod_type": "parameter_change", "diagnosis": "bottleneck",
                "change": "one change", "expected_effect": "improve", "evidence_run_ids": []}})
        deadline = time.monotonic() + 1
        client = PlannerClient(provider)
        client.plan(AgentState("a", "p", .5), 0, {}, request_id="one", deadline=deadline)
        self.assertEqual(calls[0]["deadline"], deadline)
        self.assertEqual(len(calls), 1)
        with self.assertRaises(TimeoutError):
            PlannerClient(provider).plan(AgentState("b", "p", .5), 0, {}, request_id="two",
                                         deadline=time.monotonic() - 1)
        self.assertEqual(len(calls), 1)

    def test_dispatched_transport_failure_is_unknown_with_scope_and_no_retry(self):
        calls = []
        def provider(payload):
            calls.append(payload)
            raise TimeoutError("socket timed out after dispatch")
        client = PlannerClient(provider, episode_id="episode")
        state = AgentState("a", "p", .5)
        outcomes = parallel_plans({"a": state}, {"a": {}}, self._client_backend(client),
                                  0, time.monotonic() + 1)
        outcome = outcomes["a"]
        self.assertEqual(outcome.status, "INFLIGHT_UNKNOWN")
        self.assertTrue(outcome.inflight_unknown)
        self.assertEqual(outcome.request_id, "a:0")
        self.assertEqual(outcome.scope_id, "episode:0:a:PLAN_RO")
        self.assertEqual(outcome.context["agent_id"], "a")
        self.assertIsNone(outcome.receipt)
        with self.assertRaises(InferenceBudgetExceeded):
            client.plan(state, 0, {}, request_id="retry")
        self.assertEqual(len(calls), 1)

    def test_schema_abstention_keeps_real_receipt_context_and_token_accounting(self):
        accounted = []
        client = PlannerClient(lambda _: ProviderResponse("not valid JSON", "provider-id", 90, 12),
                               record_usage=accounted.append)
        outcomes = parallel_plans({"a": AgentState("a", "p", .5)}, {"a": {}},
                                  self._client_backend(client), 0, time.monotonic() + 1)
        outcome = outcomes["a"]
        self.assertEqual(outcome.status, "ABSTAIN")
        self.assertFalse(outcome.inflight_unknown)
        self.assertIsNone(outcome.decision)
        self.assertEqual(outcome.receipt.input_tokens, 90)
        self.assertEqual(outcome.receipt.output_tokens, 12)
        self.assertEqual(outcome.context["agent_id"], "a")
        self.assertEqual(accounted, [outcome.receipt])
        self.assertEqual(outcome.receipt.actual_requests, 1)

    def test_safe_rejection_diagnostic_survives_parallel_planning(self):
        class Backend:
            planning_capabilities = CAPABILITIES
            def plan_with_receipt(self, state, round_index, context, *, deadline):
                receipt = response(state, round_index)[1]
                raise PlanningResponseError('Coral plan was not accepted', receipt=receipt,
                    context={'agent_id':state.agent_id}, diagnostic={
                        'category':'PLAN_VALIDATION','code':'INVALID_ACTION',
                        'exception_type':'ValueError'})
        outcome = parallel_plans({'a':AgentState('a','p',.5)}, {'a':{}}, Backend(),
                                 0,time.monotonic()+1)['a']
        self.assertEqual(outcome.status,'ABSTAIN')
        self.assertEqual(outcome.diagnostic['code'],'INVALID_ACTION')
        self.assertEqual(outcome.receipt.actual_requests,1)

    def test_completed_late_response_keeps_usage_receipt_but_no_decision(self):
        accounted = []
        def provider(_):
            time.sleep(.02)
            return ProviderResponse('{"action":"withdraw"}', "late-provider", 20, 3)
        client = PlannerClient(provider, record_usage=accounted.append)
        with self.assertRaises(PlanningResponseError) as caught:
            client.plan(AgentState("a", "p", .5), 0, {}, request_id="late",
                        deadline=time.monotonic() + .005)
        self.assertEqual(caught.exception.receipt.output_tokens, 3)
        self.assertEqual(caught.exception.context["agent_id"], "a")
        self.assertFalse(caught.exception.inflight_unknown)
        self.assertEqual(len(accounted), 1)


if __name__ == "__main__":
    unittest.main()
