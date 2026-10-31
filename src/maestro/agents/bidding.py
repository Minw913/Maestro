"""V1 cooperative first-price bids. No learned strategic override."""
from decimal import Decimal
from maestro.market.credit import decimal, money


def cooperative_bid(value, *, available_wealth=None) -> Decimal:
    value = decimal(value)
    if value < 0:
        raise ValueError("bid value must be nonnegative")
    if available_wealth is not None:
        available = decimal(available_wealth)
        if available < 0:
            raise ValueError('available wealth must be nonnegative')
        value = min(value, available)
    return money(value)
