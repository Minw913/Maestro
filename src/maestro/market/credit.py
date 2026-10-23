"""Versioned V1 credit arithmetic with decimal accounting boundaries."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext

QUANTUM = Decimal("0.000001")
_ALPHA = Decimal(100)


@dataclass(frozen=True)
class CreditSettings:
    """Default phi scale alpha, catch-up fraction beta/alpha and bid cap; RunConfig and every function default read these."""
    reward_scale: Decimal = _ALPHA
    catchup_fraction: Decimal = Decimal("0.25")
    bid_cap: Decimal = _ALPHA  # spec b_max = alpha

    @property
    def beta(self) -> Decimal:
        return self.reward_scale * self.catchup_fraction


DEFAULT_CREDIT = CreditSettings()


def decimal(value: Decimal | int | float | str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("numeric value required") from exc
    if not result.is_finite():
        raise ValueError("non-finite numeric value")
    return result


def money(value: Decimal | int | float | str) -> Decimal:
    """Quantize once at an accounting boundary; never use binary float money."""
    with localcontext() as ctx:
        ctx.prec = 40
        return decimal(value).quantize(QUANTUM, rounding=ROUND_HALF_EVEN)


def score(value: Decimal | int | float | str) -> Decimal:
    result = decimal(value)
    if not 0 <= result <= 1:
        raise ValueError("score must be in [0, 1]")
    return result


def reward(score_value=None, parent=None, champion=None,
           scale=DEFAULT_CREDIT.reward_scale, catchup=DEFAULT_CREDIT.catchup_fraction, valid=True,
           **kwargs) -> Decimal:
    """scale * ([Y-C]+ + catchup * [min(Y,C)-P]+), or zero if invalid.

    ``score=`` is accepted as a keyword to match the external contract. Invalid
    formal outcomes have no usable score and receive exactly zero credit.
    """
    if "score" in kwargs:
        if score_value is not None:
            raise TypeError("score specified twice")
        score_value = kwargs.pop("score")
    if kwargs:
        raise TypeError(f"unexpected reward arguments: {', '.join(kwargs)}")
    if not valid:
        return money(0)
    y, p, c = score(score_value), score(parent), score(champion)
    a, b = decimal(scale), decimal(catchup)
    if a < 0 or not 0 <= b <= 1:
        raise ValueError("nonnegative scale and catchup in [0, 1] required")
    return money(a * (max(Decimal(0), y - c)
                      + b * max(Decimal(0), min(y, c) - p)))
