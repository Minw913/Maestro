"""Projection to Coral ScoreBundle; canonical outcomes remain in the ledger."""
from __future__ import annotations

from maestro.backends.base import BackendUnavailable


def to_score_bundle(result):
    try:
        from coral.types import Score, ScoreBundle
    except ImportError as error:
        raise BackendUnavailable("Coral must be installed to construct its real ScoreBundle") from error
    return ScoreBundle(
        scores={"maestro": Score(value=result.score if result.valid else 0.0,
                                 name="maestro", metadata={"valid": result.valid})},
        aggregated=result.score if result.valid else 0.0, is_public=False,
        feedback="Verified MAESTRO result; public release occurs at the settlement barrier.",
        metadata={"job_id": result.job_id, "candidate_hash": result.candidate_hash,
                  "status": result.status, "cpu_seconds": result.cpu_seconds,
                  "wall_seconds": result.wall_seconds})


def public_result(result, *, settled: bool) -> dict:
    if not settled:
        raise PermissionError("Same-round results remain private until settlement")
    return {"job_id": result.job_id, "candidate_hash": result.candidate_hash,
            "score": result.score if result.valid else 0.0, "valid": result.valid,
            "status": result.status, "evaluator_verified": True}
