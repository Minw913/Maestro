"""Import verified immutable Warm candidates for an isolated Market root.

This copies only candidates named in the frozen Warm checkpoint. It does not
change the checkpoint, source ledger, or historical request authority.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sqlite3

from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.storage.ledger import Ledger
from maestro.types import stable_hash


def import_candidates(checkpoint: Path, target_root: Path) -> dict:
    checkpoint = checkpoint.resolve(strict=True)
    target_root = target_root.resolve()
    envelope = json.loads(checkpoint.read_text(encoding="utf-8"))
    payload = envelope["payload"]
    if stable_hash(payload) != envelope["artifact_hash"]:
        raise ValueError("Warm checkpoint integrity failure")
    source_root = checkpoint.parent.parent.parent
    source_cas = source_root / "coral-cas"
    source_db = source_root / "authority" / "ledger.sqlite"
    source = sqlite3.connect(source_db.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        candidates = {row[0]: json.loads(row[1]) for row in source.execute(
            "SELECT hash,payload FROM candidates")}
    finally:
        source.close()
    # Search the checkpoint JSON for complete content hashes only. This
    # includes histories and board candidates, not later Market candidates.
    frozen = json.dumps(payload, sort_keys=True)
    hashes = sorted(key for key in candidates if key in frozen)
    parents = {agent: state["parent_hash"] for agent, state in payload["states"].items()}
    if not set(parents.values()) <= set(hashes):
        raise ValueError("Warm parent missing from source ledger")
    if not hashes:
        raise ValueError("No Warm candidates found")
    target_cas = target_root / "coral-cas"
    target_cas.mkdir(parents=True, exist_ok=True)
    ledger = Ledger(target_root / "authority" / "ledger.sqlite")
    try:
        for key in hashes:
            row = candidates[key]
            source_path = Path(row["candidate_path"]).resolve(strict=True)
            if source_path != source_cas / key or candidate_tree_hash(source_path) != key:
                raise ValueError(f"Source candidate identity mismatch: {key}")
            target = target_cas / key
            if not target.exists():
                shutil.copytree(source_path, target, symlinks=False)
            if candidate_tree_hash(target) != key:
                raise ValueError(f"Copied candidate identity mismatch: {key}")
            ledger.record_candidate(key, {"candidate_hash": key, "candidate_path": str(target)})
    finally:
        ledger.close()
    result = {
        "status": "PASSED", "utc": datetime.now(timezone.utc).isoformat(),
        "checkpoint": str(checkpoint), "checkpoint_hash": envelope["artifact_hash"],
        "source_root": str(source_root), "target_root": str(target_root),
        "candidate_count": len(hashes), "candidate_hashes": hashes,
        "parent_hashes": parents,
    }
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--target-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(import_candidates(args.checkpoint, args.target_root), indent=2))
