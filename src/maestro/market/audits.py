"""Seeded full-profile audit reserve; audits pay zero and retain reported bids."""
from decimal import Decimal, ROUND_CEILING
import hashlib
from maestro.market.credit import decimal
from maestro.market.reputation import require_full_profile


def audit_slots(slots, epsilon, *, profile):
    require_full_profile(profile)
    fraction = decimal(epsilon)
    if not isinstance(slots, int) or slots < 0 or not 0 <= fraction <= 1:
        raise ValueError("nonnegative slots and epsilon in [0,1] required")
    return int((fraction * slots).to_integral_value(rounding=ROUND_CEILING))


def sample_audits(eligible_plan_ids, selected_plan_ids, count, seed, *, profile):
    require_full_profile(profile)
    ids, selected = list(eligible_plan_ids), set(selected_plan_ids)
    if len(set(ids)) != len(ids) or count < 0 or not selected <= set(ids):
        raise ValueError("invalid audit population/selection/count")
    remaining = [identity for identity in ids if identity not in selected]
    # Independent seeded hashes give an order-independent uniform permutation.
    remaining.sort(key=lambda identity: hashlib.sha256(
        repr((seed, "audit", identity)).encode()).hexdigest())
    return remaining[:count]
