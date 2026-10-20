from collections import Counter
from decimal import Decimal
import unittest

from maestro.types import AgentState, HistoryRecord, BootstrapIncomplete
from maestro.bootstrap.director import make_direction_briefs
from maestro.bootstrap.warm_start import balanced_schedule, validate_warm_completion
from maestro.bootstrap.snapshot import (make_snapshot, verify_snapshot, clone_arms,
                                        serialize_state, deserialize_state)


def states():
    return {a: AgentState(a, "parent-" + a, .7, wealth=Decimal("2010"),
               history=[HistoryRecord("warm-" + a, a, .6, .7, .7, True,
                                      "local_search", "parameter_change", Decimal("2.5"))],
               lessons=[{"claim": "keep real evidence"}],
               ucb_n=1, ucb_total=Decimal("2.5")) for a in "abcd"}


class BootstrapTests(unittest.TestCase):
    def test_ten_opportunities_each_needs_twenty_waves(self):
        schedule = balanced_schedule(list("abcd"), 2, 10, 42)
        self.assertEqual(len(schedule), 20)
        self.assertEqual(Counter(a for wave in schedule for a in wave), {a: 10 for a in "abcd"})
        self.assertEqual(schedule, balanced_schedule(list("dcba"), 2, 10, 42))
        self.assertTrue(all(len(set(w)) == len(w) <= 2 for w in schedule))

    def test_non_divisible_wave_counts_and_invalid_inputs(self):
        schedule = balanced_schedule(list("abc"), 2, 3, 5)
        self.assertEqual(Counter(a for wave in schedule for a in wave), {a: 3 for a in "abc"})
        self.assertEqual(len(schedule[-1]), 1)
        with self.assertRaises(ValueError):
            balanced_schedule(list("abc"), 4, 3, 5)

    def test_director_is_deterministic_diverse_and_not_a_market_bidder(self):
        briefs = make_direction_briefs(list("abcd"), 19, context={"seed_artifact": "seed"})
        self.assertEqual(briefs, make_direction_briefs(list("abcd"), 19, context={"seed_artifact": "seed"}))
        self.assertEqual(len({b["objective"] for b in briefs}), 4)
        self.assertEqual(len({b["direction_id"] for b in briefs}), 4)
        self.assertTrue(all(b["seed_artifact"] == "seed" and "bid" not in b for b in briefs))

    def test_live_director_calls_once_and_fails_without_fallback(self):
        calls = []
        def provider(context):
            calls.append(context)
            return []
        with self.assertRaises(ValueError):
            make_direction_briefs(list("abcd"), 0, provider=provider)
        self.assertEqual(len(calls), 1)

    def test_warm_requires_equal_opportunities_and_real_outcomes(self):
        self.assertTrue(validate_warm_completion(states(), {a: 10 for a in "abcd"}, 10))
        s = states()
        s["a"].history = []
        with self.assertRaises(BootstrapIncomplete):
            validate_warm_completion(s, {a: 10 for a in "abcd"}, 10)

    def test_clone_preserves_history_and_ucb_resets_wallet_independently(self):
        snapshot = make_snapshot(states(), {"champion_score": .7}, {"contract": "frozen"})
        self.assertTrue(verify_snapshot(snapshot))
        arms = clone_arms(snapshot)
        for arm in arms.values():
            self.assertEqual(arm["a"].wealth, Decimal("2000.000000"))
            self.assertEqual(arm["a"].ucb_n, 1)
            self.assertEqual(arm["a"].ucb_total, Decimal("2.500000"))
            self.assertEqual(arm["a"].history[0].experiment_id, "warm-a")
        arms["uniform"]["a"].history.clear()
        arms["uniform"]["a"].lessons[0]["claim"] = "changed"
        self.assertEqual(len(arms["ucb"]["a"].history), 1)
        self.assertEqual(arms["ucb"]["a"].lessons[0]["claim"], "keep real evidence")

    def test_tampered_or_unsettled_snapshot_rejected(self):
        snapshot = make_snapshot(states())
        snapshot["states"]["a"]["parent_score"] = .99
        with self.assertRaises(ValueError):
            clone_arms(snapshot)
        with self.assertRaises(ValueError):
            make_snapshot(states(), settled=False)

    def test_state_roundtrip_keeps_decimal_values(self):
        s = states()["a"]
        roundtrip = deserialize_state(serialize_state(s))
        self.assertEqual(roundtrip, s)
        self.assertIsInstance(roundtrip.history[0].credit, Decimal)


if __name__ == "__main__":
    unittest.main()
