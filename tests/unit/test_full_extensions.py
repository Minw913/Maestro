from dataclasses import replace
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from maestro.types import HistoryRecord
from maestro.market.reputation import TrackRecord, metrics, update_track_record
from maestro.market.full import FullPlan, allocate
from maestro.market.batches import SiblingOutcome, best_sibling_reward, phi
from maestro.market.audits import audit_slots, sample_audits
from maestro.market.screening import screen_plans, retain_highest_priority_duplicates
from maestro.agents.forecast import reference_forecast
from maestro.agents.memory import Lesson
from maestro.agents.strategy_doc import revise_strategy, persist_strategy
from maestro.sharing.disclosure import DisclosurePackage, verify_disclosure, publish_disclosure
from maestro.sharing.attribution import verify_dependencies, attribute_credit, VerifiedDependency

PROFILE = "full_strategic"


def plan(identity, agent, bid, order=0, q=1):
    return FullPlan(identity, agent, "parent-" + agent, "local_search", "parameter_change",
                    "change-" + identity, Decimal(str(bid)), order=order, q=Decimal(str(q)))


class FullReputationTests(unittest.TestCase):
    def test_discount_tracks_bids_not_forecasts_or_payments(self):
        updated = update_track_record(TrackRecord(10, 20, 2), [(4, 8), (0, 2)],
                                      discount=Decimal(".5"), profile=PROFILE)
        self.assertEqual(updated, TrackRecord(9, 20, 3))
        m = metrics(updated, 3, a0=1, profile=PROFILE)
        self.assertEqual(m.mean_credit, 3)
        self.assertEqual(m.zeta, Decimal(10) / Decimal(21))
        self.assertGreater(m.exploration, 0)

    def test_bid_shading_not_penalized_and_cold_start(self):
        self.assertEqual(metrics(TrackRecord(20, 10, 2), 2, profile=PROFILE).zeta, 1)
        self.assertEqual(metrics(TrackRecord(), 0, profile=PROFILE).zeta, 1)
        self.assertEqual(metrics(TrackRecord(), 0, profile=PROFILE).exploration, float("inf"))
        faded = update_track_record(TrackRecord(4, 8, 2), [], discount=.5, profile=PROFILE)
        self.assertEqual(faded, TrackRecord(2, 4, 1))

    def test_disabled_in_v1(self):
        with self.assertRaises(ValueError):
            update_track_record(TrackRecord(), [], profile="v1_cooperative")


class FullMechanismTests(unittest.TestCase):
    def test_multi_plan_wallet_and_fp_sum(self):
        plans = [plan("a1", "a", 8), plan("a2", "a", 7), plan("b1", "b", 6)]
        clear = allocate(plans, {}, {"a": 10, "b": 10}, slots=2, rule="M_0", profile=PROFILE)
        self.assertEqual([p.plan_id for p in clear.assignments], ["a1", "b1"])
        self.assertEqual(sum(p.payment for p in clear.assignments), 14)
        self.assertEqual(clear.virtual_counts, {"a": Decimal(1), "b": Decimal(1)})

    def test_uniform_price_critical_rejected_bid(self):
        plans = [plan("a", "a", 10), plan("b", "b", 8), plan("c", "c", 3)]
        clear = allocate(plans, {}, {"a": 100, "b": 100, "c": 100}, slots=2,
                         payment_rule="uniform_price", profile=PROFILE)
        self.assertEqual(clear.cutoff, 3)
        self.assertEqual([p.payment for p in clear.assignments], [3, 3])
        with self.assertRaises(ValueError):
            allocate(plans, {}, {"a": 100, "b": 100, "c": 100}, slots=2,
                     payment_rule="uniform_price", epsilon=.5, profile=PROFILE)

    def test_batched_ucb_uses_virtual_counts_but_never_mutates_truth(self):
        records = {"a": TrackRecord(), "b": TrackRecord()}
        plans = [plan("a1", "a", 0, 0), plan("a2", "a", 0, 1), plan("b1", "b", 0)]
        clear = allocate(plans, records, {"a": 0, "b": 0}, slots=2, rule="M_H",
                         payment_rule="zero", profile=PROFILE)
        self.assertEqual({p.agent_id for p in clear.assignments}, {"a", "b"})
        self.assertEqual(records, {"a": TrackRecord(), "b": TrackRecord()})

    def test_learning_penalizes_overbidding_q_weights_and_nesting(self):
        plans = [plan("a", "a", 100), plan("b", "b", 2)]
        records = {"a": TrackRecord(0, 100, 1), "b": TrackRecord(2, 2, 1)}
        learning = allocate(plans, records, {"a": 100, "b": 100}, slots=1,
                            rule="M_L", gamma=0, profile=PROFILE)
        self.assertEqual(learning.assignments[0].agent_id, "b")
        screened = allocate([replace(plans[0], q=Decimal(1)), replace(plans[1], q=Decimal(0))],
                            records, {"a": 100, "b": 100}, slots=1,
                            rule="M_C", gamma=0, profile=PROFILE)
        self.assertEqual(screened.assignments[0].agent_id, "a")
        fixed = allocate(plans, {}, {"a": 100, "b": 100}, slots=1, rule="M_0", profile=PROFILE)
        nested = allocate(plans, {}, {"a": 100, "b": 100}, slots=1, rule="M_L", gamma=0, profile=PROFILE)
        self.assertEqual(fixed.assignments[0].plan_id, nested.assignments[0].plan_id)

    def test_audits_use_unselected_plan_pay_zero_keep_original_bid(self):
        clear = allocate([plan("a", "a", 1), plan("b", "b", 100)], {},
                         {"a": 1, "b": 0}, slots=2, epsilon=.5, seed=10, profile=PROFILE)
        audited = [a for a in clear.assignments if a.audit]
        self.assertEqual(len(audited), 1)
        self.assertEqual(audited[0].agent_id, "b")
        self.assertEqual(audited[0].payment, 0)
        self.assertEqual(audited[0].bid, 100)
        self.assertEqual(audit_slots(3, .1, profile=PROFILE), 1)
        self.assertEqual(sample_audits(["a", "b", "c"], ["a"], 1, 9, profile=PROFILE),
                         sample_audits(["c", "a", "b"], ["a"], 1, 9, profile=PROFILE))

    def test_uniform_history_rules_follow_each_agents_plan_order(self):
        plans = [plan("a2", "a", 100, 1), plan("a1", "a", 1, 0), plan("b1", "b", 1)]
        for rule in ("M_U", "M_H"):
            clear = allocate(plans, {}, {"a": 1000, "b": 1000}, slots=3, rule=rule,
                             payment_rule="zero", profile=PROFILE)
            self.assertEqual([a.plan_id for a in clear.assignments if a.agent_id == "a"], ["a1", "a2"])


class FullBatchForecastTests(unittest.TestCase):
    def test_wallet_best_sibling_not_sum_realized_plan_credits(self):
        outcomes = [SiblingOutcome("p1", "parent", "c1", .78, True),
                    SiblingOutcome("p2", "parent", "c2", .80, True)]
        r = best_sibling_reward(outcomes, parent_hash="parent", parent_score=.7,
                                champion_score=.75, profile=PROFILE)
        self.assertEqual(r.wallet_reward, Decimal("6.25"))
        self.assertEqual(sum(r.credit_per_plan.values()), Decimal("10.50"))
        self.assertEqual(r.best_candidate_hash, "c2")
        with self.assertRaises(ValueError):
            best_sibling_reward([replace(outcomes[0], parent_hash="other")], parent_hash="parent",
                                parent_score=.7, champion_score=.75, profile=PROFILE)

    def test_credited_duplicates_receive_no_reward(self):
        r = best_sibling_reward([SiblingOutcome("p", "parent", "old", .9, True)],
                                parent_hash="parent", parent_score=.7, champion_score=.75,
                                credited_hashes=["old"], profile=PROFILE)
        self.assertEqual(r.wallet_reward, 0)
        self.assertEqual(r.credit_per_plan["p"], 0)
        self.assertEqual(phi(.78, .7, .75, alpha=100, beta=25, profile=PROFILE), Decimal("4.25"))

    def test_reference_mixture_counts_failures_and_no_history_diagnostic(self):
        records = [HistoryRecord("success", "a", .5, .6, .58, True, "x", "x"),
                   HistoryRecord("invalid", "a", .5, .6, 0, False, "x", "x")]
        f = reference_forecast(records, parent=.7, champion=.75, diagnostic_probability=.8,
                               diagnostic_gain=.04, kappa=2, profile=PROFILE)
        self.assertEqual(f.probability, Decimal(".65"))
        self.assertEqual(f.conditional_gain, Decimal(".16") / Decimal(3))
        self.assertEqual(f.sample_count, 2)
        empty = reference_forecast([], parent=.7, champion=.75, diagnostic_probability=.8,
                                   diagnostic_gain=.04, kappa=2, profile=PROFILE)
        self.assertEqual(empty.probability, Decimal(".8"))
        self.assertEqual(empty.conditional_gain, Decimal(".04"))


class FullScreenTests(unittest.TestCase):
    def test_fixed_prompt_no_allocation_and_high_priority_duplicate_copy(self):
        calls = []
        def provider(payload):
            calls.append(payload)
            return json.dumps({"decisions": [{"plan_id": "a", "q": .4, "duplicate_of": "b"},
                                              {"plan_id": "b", "q": .9}]})
        run = screen_plans([{"plan_id": "a"}, {"plan_id": "b"}], [], provider=provider, profile=PROFILE)
        self.assertEqual(calls[0]["temperature"], 0)
        self.assertEqual(len(run.prompt_hash), 64)
        self.assertEqual(retain_highest_priority_duplicates(["b", "a"], run.decisions, profile=PROFILE), ["b"])
        self.assertIn('"decisions"', run.raw_response)

    def test_duplicate_of_past_is_ineligible_and_bad_screen_fails_closed(self):
        def provider(payload):
            return '{"decisions":[{"plan_id":"a","q":1,"duplicate_of":"past"}]}'
        run = screen_plans([{"plan_id": "a"}], [], provider=provider, profile=PROFILE)
        self.assertEqual(retain_highest_priority_duplicates(["a"], run.decisions,
                         past_ids=["past"], profile=PROFILE), [])
        with self.assertRaises(ValueError):
            screen_plans([{"plan_id": "a"}], [], provider=lambda _: '{"decisions":[]}', profile=PROFILE)


def public_module(identity="module", owner="source"):
    artifact = b"verified module bytes"
    package = DisclosurePackage(identity, owner, "module", hashlib.sha256(artifact).hexdigest(),
                                ("exp-" + owner,), "Reusable verified component")
    ledger = {"exp-" + owner: {"verified": True, "agent_id": owner, "valid": True}}
    receipt = verify_disclosure(package, artifact_bytes=artifact, verified_experiments=ledger,
                                verifier=lambda *_: True, verifier_version="toy-v1", profile=PROFILE)
    return package, receipt, publish_disclosure(package, receipt, profile=PROFILE)


class FullSharingStrategyTests(unittest.TestCase):
    def test_disclosure_requires_hash_evidence_and_actual_verification(self):
        package, receipt, item = public_module()
        with self.assertRaises(ValueError):
            publish_disclosure(package, replace(receipt, verified=False), profile=PROFILE)
        with self.assertRaises(ValueError):
            verify_disclosure(package, artifact_bytes=b"changed", verified_experiments={},
                              verifier=lambda *_: True, verifier_version="v", profile=PROFILE)
        with self.assertRaises(ValueError):
            verify_disclosure(package, artifact_bytes=b"verified module bytes", verified_experiments={},
                              verifier=lambda *_: True, verifier_version="v", profile=PROFILE)

    def test_lineage_verified_attribution_is_conserved_not_minted(self):
        _, _, item = public_module()
        dependencies = verify_dependencies(receiver_id="receiver", candidate_hash="candidate",
            depends_on=["module"], public_items={"module": item},
            lineage_checker=lambda *_: "matching module tree", profile=PROFILE)
        attributed = attribute_credit({"receiver": 10, "source": 2}, {"receiver": dependencies},
            eta=.2, credited_candidates={"receiver": "candidate"}, profile=PROFILE)
        self.assertEqual(attributed.rewards_after, {"receiver": Decimal("8"), "source": Decimal("4")})
        self.assertEqual(sum(attributed.rewards_after.values()), 12)
        with self.assertRaises(ValueError):
            verify_dependencies(receiver_id="receiver", candidate_hash="candidate", depends_on=["module"],
                public_items={"module": item}, lineage_checker=lambda *_: None, profile=PROFILE)
        with self.assertRaises(ValueError):
            attribute_credit({"receiver": 10, "source": 2}, {"receiver": dependencies}, eta=.2,
                credited_candidates={"receiver": "different-candidate"}, profile=PROFILE)

    def test_multiple_sources_share_one_eta_pool_and_decimal_remainder(self):
        deps = tuple(VerifiedDependency(f"module-{i}", f"source-{i}", "candidate", "receipt", "lineage")
                     for i in range(5))
        rewards = {"receiver": Decimal(".000010"), **{f"source-{i}": Decimal(0) for i in range(5)}}
        result = attribute_credit(rewards, {"receiver": deps}, eta=.3,
                                  credited_candidates={"receiver": "candidate"}, profile=PROFILE)
        self.assertEqual(sum(result.rewards_after.values()), Decimal(".000010"))
        self.assertEqual(result.rewards_after["receiver"], Decimal(".000007"))
        self.assertTrue(all(value >= 0 for value in result.rewards_after.values()))

    def test_strategy_is_private_revision_chain_with_five_lessons(self):
        facts = [{"agent_id": "a", "verified": True, "episode_id": "e", "round_index": 0,
                  "bid": 2, "credit": 3, "payment": 2, "raw_private_other": "excluded"}]
        doc = revise_strategy(agent_id="a", objective="utility", strategy="Use calibrated bids",
            ledger_observations=facts, lessons=[Lesson(f"l{i}", "hypothesis") for i in range(8)], profile=PROFILE)
        self.assertEqual(len(doc.standing_lessons), 5)
        self.assertNotIn("raw_private_other", doc.observations[0])
        second = revise_strategy(agent_id="a", objective="utility", strategy="Refined bids",
                                  ledger_observations=facts, previous=doc, profile=PROFILE)
        self.assertEqual(second.previous_hash, doc.content_hash)
        temporary_root = Path(__file__).resolve().parents[2] / ".tmp"
        temporary_root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temporary_root) as folder:
            receipt = persist_strategy(folder, doc, profile=PROFILE)
            self.assertEqual(receipt["visibility"], "private")
            persist_strategy(folder, second, profile=PROFILE)
            with self.assertRaises(ValueError):
                persist_strategy(folder, replace(doc, strategy="Overwrite"), profile=PROFILE)


if __name__ == "__main__":
    unittest.main()
