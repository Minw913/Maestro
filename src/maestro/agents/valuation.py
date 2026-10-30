"""Discounted actual-credit valuation, with frozen legacy replay support."""
from __future__ import annotations

from decimal import Decimal, localcontext
from maestro.market.credit import money, reward, score
from maestro.types import BootstrapIncomplete


def estimate_legacy_value(history, target, mod_type, parent, champion,
                   scale=Decimal("100"), catchup=Decimal("0.25")):
    records = list(history)
    if not records:
        raise BootstrapIncomplete("history-only valuation requires a formal evaluation record")
    selected = [r for r in records if r.target == target and r.mod_type == mod_type]
    if not selected:
        selected = records
    p, c = score(parent), score(champion)
    credits = []
    for record in selected:
        # Recompute gain from the frozen historical baseline, including zeros.
        # Do not reuse old credit: the CURRENT parent/champion determine value.
        gain = (max(Decimal(0), score(record.score) - score(record.parent_score))
                if record.valid else Decimal(0))
        credits.append(reward(min(Decimal(1), p + gain), p, c, scale, catchup))
    with localcontext() as ctx:
        ctx.prec = 40
        value = money(sum(credits, Decimal(0)) / len(credits))
    return value, [r.experiment_id for r in selected]


def validate_discount(gamma):
    """Discount is per own settled evaluation, never per global round."""
    if isinstance(gamma, bool) or not isinstance(gamma, (int, float, Decimal)):
        raise ValueError('valuation_discount_factor must be numeric in (0, 1]')
    try:
        value = Decimal(str(gamma))
    except Exception as error:
        raise ValueError('valuation_discount_factor must be numeric in (0, 1]') from error
    if not value.is_finite() or not 0 < value <= 1:
        raise ValueError('valuation_discount_factor must be numeric in (0, 1]')
    return value


def estimate_value(history, *, gamma=0.8):
    """Normalized exponentially weighted ACTUAL credits, in settlement order.

    All own categories participate. Zero-credit terminal failures age history;
    losing bids and pre-admission failures do not. Warm history stays a prefix.
    No cached derived state: checkpoints retain their original credit records.
    """
    discount = validate_discount(gamma)
    records = list(history)
    if not records:
        raise BootstrapIncomplete('history-only valuation requires a formal evaluation record')
    total = weight = Decimal(0)
    with localcontext() as ctx:
        ctx.prec = 40
        for record in records:
            if record.status in {'UNKNOWN', 'RUNNING', 'CLAIMED', 'ADMITTED'}:
                raise ValueError('Valuation requires settled terminal history')
            credit = Decimal(str(record.credit))
            if not credit.is_finite() or credit < 0:
                raise ValueError('Historical credit must be finite and nonnegative')
            total = discount * total + credit
            weight = discount * weight + 1
        value = money(total / weight)
    return value, [r.experiment_id for r in records]
