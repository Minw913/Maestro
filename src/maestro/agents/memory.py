"""Private semantic memory; immutable outcomes remain authoritative elsewhere."""
from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
import hashlib
import json
from pathlib import Path
import os
import tempfile

STORES = ("candidates", "experiments", "notes", "skills", "strategy")
LESSON_STATUSES = {"hypothesis", "supported", "refuted"}


@dataclass(frozen=True)
class Lesson:
    lesson_id: str
    claim: str
    supporting_ids: tuple[str, ...] = ()
    opposing_ids: tuple[str, ...] = ()
    status: str = "hypothesis"
    revision: int = 1


def validate_lesson(lesson, own_experiment_ids):
    if not isinstance(lesson, Lesson):
        raise TypeError("Lesson required")
    if not lesson.lesson_id or not lesson.claim.strip() or len(lesson.claim) > 4000:
        raise ValueError("invalid lesson")
    if lesson.status not in LESSON_STATUSES or lesson.revision < 1:
        raise ValueError("invalid lesson status/revision")
    cited = set(lesson.supporting_ids) | set(lesson.opposing_ids)
    if not cited <= set(own_experiment_ids):
        raise ValueError("lesson references inaccessible evidence")
    if lesson.status == "supported" and not lesson.supporting_ids:
        raise ValueError("supported lesson requires evidence")
    if lesson.status == "refuted" and not lesson.opposing_ids:
        raise ValueError("refuted lesson requires opposing evidence")
    return lesson


def merge_lessons(existing, incoming, own_experiment_ids, max_standing=5):
    """Explicit revisions replace a claim; different history stays immutable."""
    if max_standing < 1:
        raise ValueError("positive standing lesson limit required")
    merged = {}
    order = []
    for item in [*existing, *incoming]:
        if isinstance(item, dict):
            data = dict(item)
            for key in ("supporting_ids", "opposing_ids"):
                if key in data:
                    data[key] = tuple(data[key])
            item = Lesson(**data)
        validate_lesson(item, own_experiment_ids)
        old = merged.get(item.lesson_id)
        if old is not None and item != old and item.revision <= old.revision:
            raise ValueError("lesson updates require a strictly newer revision")
        if item.lesson_id in order:
            order.remove(item.lesson_id)
        order.append(item.lesson_id)
        merged[item.lesson_id] = item
    return [merged[key] for key in order[-max_standing:]]


def ensure_private_stores(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise ValueError("private memory root cannot be a symlink")
    for store in STORES:
        path = root / store
        if path.is_symlink():
            raise ValueError("private stores cannot alias shared paths")
        path.mkdir(exist_ok=True)
    # This layout does not claim to be an OS access-control boundary. The
    # supervisor mounts only the owner root, and enforces containment separately.
    return {store: root / store for store in STORES}


def write_semantic_memory(root, lessons, *, own_experiment_ids):
    """Atomic derived view; never a replacement for the canonical ledger."""
    stores = ensure_private_stores(root)
    validated = merge_lessons([], lessons, own_experiment_ids)
    payload = json.dumps([asdict(x) for x in validated], sort_keys=True,
                         separators=(",", ":"), ensure_ascii=False).encode()
    path = stores["notes"] / "lessons.json"
    handle, temporary = tempfile.mkstemp(prefix=".lessons-", dir=path.parent)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return {"path": str(path), "sha256": hashlib.sha256(payload).hexdigest(),
            "lesson_count": len(validated)}


@dataclass(frozen=True)
class BiddingStrategyDoc:
    agent_id: str
    revision: int
    objective: str
    strategy: str
    evidence_ids: tuple[str, ...] = ()


def strategy_document(*, profile, agent_id, revision, objective, strategy,
                      evidence_ids=(), own_experiment_ids=()):
    """Full-profile artifact seam. Never active in deterministic V1 bidding."""
    if profile != "full_strategic":
        raise ValueError("bidding strategy documents require full_strategic profile")
    if revision < 1 or not objective.strip() or not strategy.strip():
        raise ValueError("invalid strategy document")
    if not set(evidence_ids) <= set(own_experiment_ids):
        raise ValueError("strategy references inaccessible evidence")
    return BiddingStrategyDoc(agent_id, revision, objective, strategy, tuple(evidence_ids))
