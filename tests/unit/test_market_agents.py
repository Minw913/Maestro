from __future__ import annotations

from collections import Counter
from dataclasses import replace
from decimal import Decimal
import json
import unittest

from maestro.types import AgentState, HistoryRecord, Plan, Submission, EvaluationResult, BootstrapIncomplete
from maestro.market.credit import reward, money
from maestro.market.policies import rank
from maestro.market.eligibility import check_eligibility
from maestro.market.settlement import settle_agent
from maestro.agents.valuation import estimate_legacy_value as estimate_value
from maestro.agents.bidding import cooperative_bid
from maestro.agents.planner import (mock_plan, needs_refresh, parse_plan_decision,
    apply_decision, validate_plan, validate_complete_evidence, invalidate_pending)
from maestro.agents.retrieval import build_context
from maestro.agents.memory import Lesson, merge_lessons, strategy_document
from maestro.llm import PlannerClient, InferenceBudgetExceeded


def history(agent="a", gains=(0, 0, .08, .08), target="local_search", mod_type="parameter_change"):
    return [HistoryRecord(f"exp-{agent}-{i}", agent, .50, .60,
                          float(Decimal(".50") + Decimal(str(gain))), True,
                          target, mod_type) for i, gain in enumerate(gains)]


def state(agent="a", gains=(0, 0, .08, .08)):
    return AgentState(agent, "parent-" + agent, .70, history=history(agent, gains))


class CreditTests(unittest.TestCase):
    def test_catchup_frontier_and_no_improvement(self):
        self.assertEqual(reward(.78, .70, .75), Decimal("4.250000"))
        self.assertEqual(reward(score=.74, parent=.70, champion=.75), Decimal("1.000000"))
        self.assertEqual(reward(.69, .70, .75), 0)
        self.assertEqual(reward(None, .70, .75, valid=False), 0)

    def test_decimal_rounding_and_invalid_numbers(self):
        self.assertEqual(money("2.0000005"), Decimal("2.000000"))
        self.assertEqual(money("2.0000015"), Decimal("2.000002"))
        for invalid in ("NaN", "Infinity", float("nan")):
            with self.assertRaises(ValueError):
                money(invalid)
        with self.assertRaises(ValueError):
            reward(1.01, .5, .6)

    def test_legacy_history_rebased_fixture_is_2_125(self):
        value, ids = estimate_value(history(), "local_search", "parameter_change", .70, .75)
        self.assertEqual(value, Decimal("2.125000"))
        self.assertEqual(ids, ["exp-a-0", "exp-a-1", "exp-a-2", "exp-a-3"])
        self.assertEqual(cooperative_bid(value), Decimal("2.125000"))

    def test_legacy_category_fallback_and_failures_not_filtered(self):
        records = history(gains=(.08, 0))
        records[1] = replace(records[1], valid=False, status="TIMEOUT", score=0)
        records.append(HistoryRecord("other", "a", .5, .6, .9, True, "restart", "removal"))
        exact, ids = estimate_value(records, "local_search", "parameter_change", .70, .75)
        self.assertEqual(exact, Decimal("2.125000"))
        self.assertEqual(len(ids), 2)
        fallback, ids = estimate_value(records, "other", "other", .70, .75)
        self.assertEqual(len(ids), 3)
        self.assertGreater(fallback, exact)

    def test_no_prior_and_uncapped_bid(self):
        with self.assertRaises(BootstrapIncomplete):
            estimate_value([], "other", "other", .7, .75)
        self.assertEqual(cooperative_bid(200), Decimal("200.000000"))
        with self.assertRaises(ValueError):
            cooperative_bid(-1)


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.states = {a: state(a) for a in "abcd"}
        self.submissions = [Submission(a, "plan-" + a, Decimal(1), Decimal(1)) for a in "abcd"]

    def test_balanced_rotation_and_eligibility_do_not_reseed(self):
        counts = Counter(a for t in range(20) for a in rank(
            "uniform", self.submissions, self.states, t, 19, slots=2)[:2])
        self.assertEqual(counts, {a: 10 for a in "abcd"})
        complete = rank("uniform", self.submissions, self.states, 0, 19, slots=2)
        removed = complete[1]
        partial = [replace(x, eligible=False) if x.agent_id == removed else x for x in self.submissions]
        self.assertEqual(rank("uniform", partial, self.states, 0, 19, slots=2),
                         [a for a in complete if a != removed])

    def test_first_price_naive_is_seeded_stable_and_never_reorders_by_text(self):
        submissions = [replace(s, bid=Decimal(i)) for i, s in enumerate(self.submissions)]
        self.assertEqual(rank("first_price_naive", submissions, self.states, 0, 5), list("dcba"))
        self.assertEqual(rank("first_price_naive", self.submissions, self.states, 0, 5),
                         rank("first_price_naive", self.submissions[::-1], self.states, 0, 5))

    def test_ucb_uses_warm_history_and_cold_start(self):
        for a, s in self.states.items():
            s.ucb_n = 10
            s.ucb_total = Decimal(10 if a == "a" else 0)
            s.history = [replace(record,credit=Decimal(1 if a=='a' else 0)) for record in s.history]
        self.assertEqual(rank("ucb", self.submissions, self.states, 0, 0, ucb_exploration_coefficient=1)[0], "a")
        self.states["d"].ucb_n = 0
        self.assertEqual(rank("ucb", self.submissions, self.states, 0, 0, ucb_exploration_coefficient=1)[0], "d")

    def test_duplicate_submission_rejected(self):
        with self.assertRaises(ValueError):
            rank("uniform", self.submissions * 2, self.states, 0, 0, slots=2)


class PlanTests(unittest.TestCase):
    def test_pending_refresh_does_not_change_losing_version(self):
        s = state()
        s.pending_plan = mock_plan(s, 0, 1)
        s.last_planning_round = 0
        self.assertFalse(needs_refresh(s, 2))
        self.assertTrue(needs_refresh(s, 3))
        decision = parse_plan_decision('{"action":"retain"}', s, 3)
        old = s.pending_plan
        apply_decision(s, decision, 3)
        self.assertIs(s.pending_plan, old)
        invalidate_pending(s, "PRECHECK_FAILURE")
        self.assertTrue(needs_refresh(s, 4))
        # A fresh call may retain an unexecuted hypothesis after toy failure.
        refreshed = parse_plan_decision('{"action":"retain"}', s, 4)
        apply_decision(s, refreshed, 4)
        self.assertFalse(needs_refresh(s, 5))
        invalidate_pending(s, "FORMAL_EVALUATION")
        with self.assertRaises(ValueError):
            parse_plan_decision('{"action":"retain"}', s, 5)

    def _payload(self, evidence=None):
        return {"action": "revise", "plan": {"target": "local_search", "mod_type": "parameter_change",
                "diagnosis": "Too much repeated work.", "change": "Adjust one local move threshold.",
                "expected_effect": "Higher verified score.",
                "evidence_run_ids": evidence if evidence is not None else [r.experiment_id for r in history()]}}

    def test_one_local_json_repair_no_extra_call_and_revision_namespace(self):
        s = state()
        raw = "```json\n" + json.dumps(self._payload())[:-1] + ",}\n```"
        d = parse_plan_decision(raw, s, 0, namespace="episode1")
        self.assertTrue(d.repaired)
        self.assertEqual(d.plan.agent_id, "a")
        self.assertEqual(d.plan.parent_hash, s.parent_hash)
        d2 = parse_plan_decision(json.dumps(self._payload()), s, 0, namespace="episode2")
        self.assertNotEqual(d.plan.version_id, d2.plan.version_id)

    def test_cannot_omit_failures_rebind_parent_or_inject_evidence(self):
        s = state()
        for evidence in (["exp-a-2"], ["foreign-secret"]):
            with self.assertRaises(ValueError):
                parse_plan_decision(json.dumps(self._payload(evidence)), s, 0)
        payload = self._payload()
        payload["plan"]["parent_hash"] = "new-parent"
        with self.assertRaises(ValueError):
            parse_plan_decision(json.dumps(payload), s, 0)
        with self.assertRaises(ValueError):
            parse_plan_decision('{"action":"withdraw","action":"retain"}', s, 0)

    def test_withdraw_must_state_its_reason(self):
        s = state()
        for raw in ('{"action":"withdraw"}', '{"action":"withdraw","reason":"  "}'):
            with self.assertRaises(ValueError):
                parse_plan_decision(raw, s, 0)
        d = parse_plan_decision('{"action":"withdraw","plan":null,"reason":"parent has nothing to restart"}', s, 0)
        self.assertIsNone(d.plan)
        self.assertEqual(d.reason, "parent has nothing to restart")

    def test_eligibility_rejects_consumed_plan_and_held_balance(self):
        s = state()
        p = mock_plan(s, 0, 1)
        self.assertTrue(check_eligibility(p, s, bid=100).eligible)
        self.assertFalse(check_eligibility(p, s, bid=100, holds=1950).eligible)
        self.assertFalse(check_eligibility(p, s, executed_versions=[p.version_id]).eligible)
        self.assertFalse(check_eligibility(replace(p, agent_id="b"), s).eligible)

    def test_legacy_losing_reuse_revalues_from_new_champion(self):
        records = history()
        first, _ = estimate_value(records, "local_search", "parameter_change", .7, .75)
        second, _ = estimate_value(records, "local_search", "parameter_change", .7, .8)
        self.assertNotEqual(first, second)


class RetrievalMemoryTests(unittest.TestCase):
    def test_free_category_catalog_preserves_distinct_pairs(self):
        s=AgentState('a','parent',.5,history=[
            HistoryRecord('one','a',.5,.6,.5,True,'a/b','c'),
            HistoryRecord('two','a',.5,.6,.5,True,'a','b/c')])
        catalog=build_context(s,{})['evidence_catalog']
        self.assertEqual(catalog['a/b']['c'],['one'])
        self.assertEqual(catalog['a']['b/c'],['two'])

    def test_context_filters_private_board_fields_and_truncation_has_manifest(self):
        s = state(gains=tuple(.01 for _ in range(40)))
        board = {"champion_score": .75, "private_notes": "secret",
                 "results": [{"verified": True, "score": .75, "raw_stdout": "secret2"},
                             {"verified": False, "score": 1, "private_notes": "secret3"}]}
        context = build_context(s, board, token_budget=2200)
        encoded = json.dumps(context, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        self.assertLessEqual(len(encoded), 2200)
        self.assertNotIn("secret", encoded.decode())
        self.assertGreater(context["manifest"]["omitted_record_count"], 0)
        self.assertEqual(len(context["evidence_catalog"]["local_search"]["parameter_change"]), 40)
        value, ids = estimate_value(s.history, "local_search", "parameter_change", .7, .75)
        self.assertEqual(len(ids), 40)
        self.assertEqual(value, Decimal(".250000"))

    def test_foreign_history_is_not_silently_shared(self):
        s = state()
        s.history.append(history("b")[0])
        with self.assertRaises(ValueError):
            build_context(s, {})

    def test_lessons_are_evidence_checked_bounded_revisions(self):
        one = Lesson("l1", "May help", status="hypothesis")
        two = Lesson("l1", "Supported", supporting_ids=("exp-a-2",), status="supported", revision=2)
        self.assertEqual(merge_lessons([one], [two], ["exp-a-2"]), [two])
        with self.assertRaises(ValueError):
            merge_lessons([two], [one], ["exp-a-2"])
        with self.assertRaises(ValueError):
            merge_lessons([], [two], [])
        self.assertEqual(len(merge_lessons([], [Lesson(f"l{i}", "hypothesis") for i in range(8)], [])), 5)
        with self.assertRaises(ValueError):
            strategy_document(profile="v1_cooperative", agent_id="a", revision=1,
                              objective="wealth", strategy="raise bid")


class PlanningCallTests(unittest.TestCase):
    _payload = PlanTests._payload
    def test_exact_one_request_bad_response_consumes_budget(self):
        called = []
        def provider(payload):
            called.append(payload)
            return "not JSON"
        client = PlannerClient(provider)
        with self.assertRaises(ValueError):
            client.plan(state(), 0, {}, request_id="one")
        with self.assertRaises(InferenceBudgetExceeded):
            client.plan(state(), 0, {}, request_id="two")
        self.assertEqual(len(called), 1)

    def test_success_replay_returns_receipt_without_another_request(self):
        called, reserved = [], []
        def provider(payload):
            called.append(payload)
            return json.dumps(self._payload())
        def reserve(*args):
            reserved.append(args)
            return True
        client = PlannerClient(provider, reserve=reserve, episode_id="e")
        first = client.plan(state(), 0, {}, request_id="one")
        second = client.plan(state(), 0, {}, request_id="one")
        self.assertIs(first, second)
        self.assertEqual(len(called), 1)
        self.assertEqual(len(reserved), 1)
        self.assertEqual(first[1].actual_requests, 1)

    def test_live_requires_durable_reservation(self):
        class Live:
            is_live = True
        with self.assertRaises(ValueError):
            PlannerClient(Live())

    def test_parent_source_summary_is_hashed_and_charged(self):
        reservations, payloads = [], []
        def provider(payload):
            payloads.append(payload)
            return json.dumps(self._payload())
        def reserve(*args):
            reservations.append(args)
            return True
        client = PlannerClient(provider, reserve=reserve)
        decision, receipt, context = client.plan(state(), 0, {}, request_id="source",
            harness_context={"parent_source": "def solve(graph): return list(graph)",
                             "direction_brief": {"objective": "improve local search"}})
        self.assertIn("def solve", payloads[0]["messages"][1]["content"])
        self.assertEqual(len(context["manifest"]["harness_context_hash"]), 64)
        self.assertGreater(reservations[0][2], len(payloads[0]["messages"][1]["content"].encode()))
        with self.assertRaises(ValueError):
            PlannerClient(provider).plan(state(), 1, {}, request_id="oversized",
                harness_context={"parent_source": "x" * 10000}, token_budget=2000)


class SettlementTests(unittest.TestCase):
    def test_own_bid_not_cutoff_and_frozen_parent(self):
        result = EvaluationResult("job", "candidate", .78, True)
        settled = settle_agent(wealth=2000, parent=.7, champion=.75,
                               admitted=True, bid=Decimal("2.125"), result=result)
        self.assertEqual(settled.payment, Decimal("2.125000"))
        self.assertEqual(settled.reward, Decimal("4.250000"))
        self.assertEqual(settled.wealth_after, Decimal("2002.125000"))
        self.assertEqual(settled.parent_score_after, .78)

    def test_formal_failure_consumes_payment_precheck_does_not(self):
        failed = EvaluationResult("j", "c", 0, False, "TIMEOUT")
        formal = settle_agent(wealth=2000, parent=.7, champion=.75, admitted=True,
                              bid=3, result=failed)
        precheck = settle_agent(wealth=2000, parent=.7, champion=.75, admitted=False, bid=3)
        self.assertEqual(formal.wealth_after, Decimal("1997.000000"))
        self.assertEqual(formal.parent_score_after, .7)
        self.assertEqual(precheck.wealth_after, Decimal("2000.000000"))

    def test_no_unknown_result_settlement_and_no_duplicate_credit(self):
        result = EvaluationResult("j", "c", .9, True, "UNKNOWN")
        with self.assertRaises(ValueError):
            settle_agent(wealth=2000, parent=.7, champion=.75, admitted=True, result=result)
        result = replace(result, status="COMPLETED")
        settled = settle_agent(wealth=2000, parent=.7, champion=.75, admitted=True,
                               result=result, credited_duplicate=True)
        self.assertEqual(settled.reward, 0)


if __name__ == "__main__":
    unittest.main()
