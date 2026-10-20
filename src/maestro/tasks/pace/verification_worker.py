"""Trusted verifier entrypoint, run in a separate CPU/wall limited container.

The worker is separate from every candidate container. It is mounted from the
trusted package, never from a candidate directory. Its only output is a bounded
JSON assessment; candidate stdout is data, never executable code.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

if __name__ == "__main__":
    # ``python3 -I /input/source/.../verification_worker.py`` deliberately ignores
    # user site packages and environment path injection. This one trusted source
    # root is explicitly added; no candidate tree is mounted in this container.
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from maestro.tasks.pace.parser import parse_graph
from maestro.tasks.pace.scoring import instance_score
from maestro.tasks.pace.verifier import verify_solution


def assess_instance(graph_text: str, item: dict) -> dict:
    graph = parse_graph(graph_text)
    if graph.n != item["n"]:
        raise ValueError("graph vertex count differs from the frozen manifest")
    verified = verify_solution(graph, item["stdout"])
    score = 0.0
    if item["timed_out"] and not verified.valid:
        status, valid, reason = "TIMEOUT", None, "timeout without a valid result"
    elif item["returncode"] != 0 and not item["timed_out"]:
        status, valid, reason = "FAILED", False, f"solver exit code {item['returncode']}"
    elif not verified.valid:
        status, valid, reason = "INVALID", False, verified.reason
    else:
        status, valid, reason = "COMPLETED", True, ""
        score = instance_score(verified.size, graph.n, item["reference_size"])
    return {
        "instance_id": item["instance_id"], "result_size": verified.size if valid else None,
        "score": score, "valid": valid, "status": status, "reason": reason,
        "better_than_reference": bool(valid and verified.size < item["reference_size"]),
    }


def assess_payload(payload: dict, *, read_graph=None) -> dict:
    if payload.get("schema_version") != 1:
        raise ValueError("unsupported verifier worker schema")
    results = []
    for item in payload["instances"]:
        if read_graph:
            graph_bytes = read_graph(item)
        else:
            path = Path(item["graph_path"])
            if not path.is_absolute() or not str(path).startswith("/input/graphs/") or ".." in path.parts:
                raise ValueError("verifier graph path is outside its read-only input mounts")
            graph_bytes = path.read_bytes()
        if hashlib.sha256(graph_bytes).hexdigest() != item["sha256"]:
            raise ValueError("graph checksum differs from frozen manifest")
        results.append(assess_instance(graph_bytes.decode("utf-8"), item))
    return {"schema_version": 1, "instances": results}


def main() -> int:
    try:
        payload = json.load(sys.stdin)
        result = assess_payload(payload)
        print(json.dumps(result, separators=(",", ":"), allow_nan=False))
        return 0
    except Exception as exc:
        print(f"trusted verifier failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
