"""Full-spec diagnostic/history mixture. V1 history-only value stays separate."""
from dataclasses import dataclass
from decimal import Decimal
from maestro.market.credit import decimal, money, score
from maestro.market.batches import phi
from maestro.market.reputation import require_full_profile


@dataclass(frozen=True)
class ReferenceForecast:
    probability: Decimal
    conditional_gain: Decimal
    value: Decimal
    sample_count: int
    success_count: int
    evidence_ids: tuple[str, ...]


def reference_forecast(records, *, parent, champion, diagnostic_probability,
                       diagnostic_gain, kappa, alpha=Decimal(100), beta=Decimal(25), profile):
    """Caller prefilters same task/protocol/labels before passing records."""
    require_full_profile(profile)
    p, c = score(parent), score(champion)
    pd, gd, weight = decimal(diagnostic_probability), decimal(diagnostic_gain), decimal(kappa)
    if p == 1:
        raise ValueError("no positive conditional gain is possible at score one")
    if not 0 <= pd <= 1 or not 0 < gd <= 1 - p or weight <= 0:
        raise ValueError("invalid diagnostic forecast/kappa")
    rows = list(records)
    gains = [max(Decimal(0), score(r.score) - score(r.parent_score))
             if r.valid else Decimal(0) for r in rows]
    positive = [gain for gain in gains if gain > 0]
    probability = (len(positive) + weight * pd) / (len(rows) + weight)
    conditional = (sum(positive, Decimal(0)) + weight * gd) / (len(positive) + weight)
    # The declared two-point outcome must remain inside [parent,1]. Historical
    # gains are rebased onto today's parent, requiring this explicit clipping.
    conditional = min(1 - p, conditional)
    value = money(probability * phi(p + conditional, p, c, alpha=alpha, beta=beta, profile=profile))
    return ReferenceForecast(probability, conditional, value, len(rows), len(positive),
                              tuple(r.experiment_id for r in rows))
