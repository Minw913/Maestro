"""Same-parent sibling accounting: per-plan reputation versus best-only wallet."""
from dataclasses import dataclass
from decimal import Decimal
from maestro.market.credit import DEFAULT_CREDIT, decimal, money, score
from maestro.market.reputation import require_full_profile


def phi(y, parent, champion, *, alpha=DEFAULT_CREDIT.reward_scale, beta=DEFAULT_CREDIT.beta, valid=True, profile):
    require_full_profile(profile)
    a, b = decimal(alpha), decimal(beta)
    if not 0 <= b <= a:
        raise ValueError("full credit requires alpha >= beta >= 0")
    if not valid:
        return money(0)
    y, p, c = score(y), score(parent), score(champion)
    if p > c:
        raise ValueError("frozen champion cannot be below an agent parent")
    return money(a * max(Decimal(0), y - c) + b * max(Decimal(0), min(y, c) - p))


@dataclass(frozen=True)
class SiblingOutcome:
    plan_id: str
    parent_hash: str
    candidate_hash: str
    score: float
    valid: bool
    status: str = "COMPLETED"


@dataclass(frozen=True)
class BatchReward:
    wallet_reward: Decimal
    credit_per_plan: dict[str, Decimal]
    best_score: float
    best_candidate_hash: str


def best_sibling_reward(outcomes, *, parent_hash, parent_score, champion_score,
                        credited_hashes=(), alpha=DEFAULT_CREDIT.reward_scale, beta=DEFAULT_CREDIT.beta, profile):
    require_full_profile(profile)
    p = score(parent_score)
    scores, credits = [], {}
    seen = set(credited_hashes)
    actual_best = (p, parent_hash)
    for outcome in outcomes:
        if outcome.plan_id in credits:
            raise ValueError("duplicate sibling plan ID")
        if outcome.parent_hash != parent_hash:
            raise ValueError("all siblings must bind the same pre-round parent")
        if outcome.status in {"UNKNOWN", "RUNNING", "CLAIMED", "ADMITTED"}:
            raise ValueError("cannot credit unresolved sibling outcomes")
        y = score(outcome.score) if outcome.valid else Decimal(0)
        if outcome.valid and y > actual_best[0]:
            actual_best = (y, outcome.candidate_hash)
        eligible = outcome.valid and outcome.candidate_hash not in seen
        credited = phi(y, p, champion_score, alpha=alpha, beta=beta, valid=eligible, profile=profile)
        credits[outcome.plan_id] = credited
        if eligible:
            scores.append(y)
        seen.add(outcome.candidate_hash)
    # Per-plan R values inform A, but wallet credit is phi(best), NEVER sum(R).
    best_new = max([p, *scores])
    wallet = phi(best_new, p, champion_score, alpha=alpha, beta=beta, profile=profile)
    return BatchReward(wallet, credits, float(actual_best[0]), actual_best[1])
