"""Versioned private full-profile market strategy, grounded in ledger facts."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from maestro.agents.memory import Lesson, merge_lessons
from maestro.market.credit import money
from maestro.market.reputation import require_full_profile
from maestro.types import canonical_json


@dataclass(frozen=True)
class StrategyRevision:
    agent_id: str
    revision: int
    objective: str
    strategy: str
    observations: tuple[dict, ...]
    standing_lessons: tuple[Lesson, ...]
    previous_hash: str | None

    @property
    def content_hash(self):
        return hashlib.sha256(canonical_json(self).encode()).hexdigest()


def revise_strategy(*, agent_id, objective, strategy, ledger_observations,
                     lessons=(), own_experiment_ids=(), previous=None, profile):
    require_full_profile(profile)
    if not agent_id or not objective.strip() or not strategy.strip():
        raise ValueError("strategy identity/objective/text required")
    if previous is not None and previous.agent_id != agent_id:
        raise ValueError("cannot revise another agent's private strategy")
    # Only trusted caller-provided ledger facts enter observations; model prose
    # belongs to strategy/lessons and cannot overwrite a bid, outcome or payment.
    observations = []
    allowed = {"episode_id", "round_index", "plan_id", "bid", "won", "audit",
               "clearing_price", "payment", "credit", "forecast_probability",
               "forecast_gain", "brier_score", "conditional_gain_error"}
    for record in ledger_observations:
        if record.get("verified") is not True or record.get("agent_id") != agent_id:
            raise ValueError("strategy observations must be own verified ledger facts")
        if "round_index" not in record or "episode_id" not in record:
            raise ValueError("strategy observation needs episode/round provenance")
        row = {key: record[key] for key in allowed if key in record}
        for key in ("bid", "payment", "credit", "clearing_price"):
            if key in row:
                row[key] = money(row[key])
                if row[key] < 0:
                    raise ValueError("negative strategy market fact")
        observations.append(row)
    standing = merge_lessons(previous.standing_lessons if previous else (),
                              lessons, own_experiment_ids, max_standing=5)
    return StrategyRevision(agent_id, 1 if previous is None else previous.revision + 1,
        objective, strategy, tuple(observations), tuple(standing),
        previous.content_hash if previous else None)


def persist_strategy(private_root, document, *, profile):
    """Write immutable revisions under the owner's PRIVATE mounted memory root.

    Creating directories does not establish an OS security boundary; the
    supervisor must restrict the supplied root to this owner before this call.
    """
    require_full_profile(profile)
    root = Path(private_root)
    if root.is_symlink():
        raise ValueError("private strategy root cannot be a shared symlink")
    directory = root / "strategy"
    if directory.is_symlink():
        raise ValueError("strategy store cannot alias a public path")
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"revision-{document.revision:06d}.json"
    if document.revision > 1:
        prior = directory / f"revision-{document.revision - 1:06d}.json"
        if (not prior.is_file() or hashlib.sha256(prior.read_bytes()).hexdigest()
                != document.previous_hash):
            raise ValueError("strategy revision must extend the persisted previous hash")
    payload = canonical_json(document)
    # Exclusive create ensures a previously persisted revision is never changed.
    try:
        with destination.open("x", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush()
            __import__("os").fsync(stream.fileno())
    except FileExistsError:
        if destination.read_text(encoding="utf-8") != payload:
            raise ValueError("immutable strategy revision collision")
    return {"path": str(destination), "content_hash": document.content_hash,
            "revision": document.revision, "visibility": "private"}
