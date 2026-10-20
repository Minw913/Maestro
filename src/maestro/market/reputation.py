"""Full-spec A/B/N statistics, separate from V1 warm UCB statistics."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import math
from maestro.market.credit import decimal, money

FULL_PROFILES = {"full", "full_cooperative", "full_strategic", "full_adversarial"}


def require_full_profile(profile):
    if profile not in FULL_PROFILES:
        raise ValueError("extension requires an explicitly enabled full profile")


@dataclass(frozen=True)
class TrackRecord:
    A: Decimal = Decimal(0)
    B: Decimal = Decimal(0)
    N: Decimal = Decimal(0)

    def __post_init__(self):
        for name in ("A", "B", "N"):
            value = decimal(getattr(self, name))
            if value < 0:
                raise ValueError("track record totals must be nonnegative")
            object.__setattr__(self, name, value)


@dataclass(frozen=True)
class ReputationMetrics:
    mean_credit: Decimal
    exploration: float
    zeta: Decimal


def update_track_record(current, executed, *, discount=Decimal(1), profile):
    """Fade once per round, then add ALL executed (credit,bid) pairs.

    Include audited bids even though audited payment is zero. Forecasts and
    payments are not B. An empty executed list still discounts old totals.
    """
    require_full_profile(profile)
    fading = decimal(discount)
    if not 0 < fading <= 1:
        raise ValueError("discount must be in (0,1]")
    pairs = [(decimal(credit), decimal(bid)) for credit, bid in executed]
    if any(credit < 0 or bid < 0 for credit, bid in pairs):
        raise ValueError("negative realized credit/bid")
    return TrackRecord(money(fading * current.A + sum((x[0] for x in pairs), Decimal(0))),
                       money(fading * current.B + sum((x[1] for x in pairs), Decimal(0))),
                       fading * current.N + len(pairs))


def metrics(record, total_count, *, a0=Decimal(1), virtual_count=None, profile):
    require_full_profile(profile)
    prior = decimal(a0)
    total = decimal(total_count)
    count = record.N if virtual_count is None else decimal(virtual_count)
    if prior <= 0 or count < 0 or total < 0:
        raise ValueError("positive a0 and nonnegative counts required")
    mean = record.A / record.N if record.N else Decimal(0)
    exploration = math.inf if not count else math.sqrt(math.log1p(float(total)) / float(count))
    zeta = min(Decimal(1), (record.A + prior) / (record.B + prior))
    return ReputationMetrics(mean, exploration, zeta)
