"""History-only valuation; prompt truncation never changes numeric samples."""
from __future__ import annotations

from decimal import Decimal, localcontext
from maestro.market.credit import DEFAULT_CREDIT, money, reward, score
from maestro.types import BootstrapIncomplete


def estimate_value(history, target, mod_type, parent, champion,
                   scale=DEFAULT_CREDIT.reward_scale, catchup=DEFAULT_CREDIT.catchup_fraction):
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
