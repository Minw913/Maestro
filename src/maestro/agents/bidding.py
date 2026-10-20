"""V1 cooperative first-price bids. No learned strategic override."""
from decimal import Decimal
from maestro.market.credit import decimal, money


def cooperative_bid(value, bid_cap=Decimal("100")) -> Decimal:
    value, cap = decimal(value), decimal(bid_cap)
    if value < 0 or cap < 0:
        raise ValueError("bid value and cap must be nonnegative")
    return money(min(cap, value))
