"""Verified voluntary disclosure; raw submissions never enter the public board."""
from dataclasses import asdict, dataclass
import hashlib
import json
from maestro.market.reputation import require_full_profile


@dataclass(frozen=True)
class DisclosurePackage:
    disclosure_id: str
    owner_id: str
    kind: str
    payload_hash: str
    evidence_ids: tuple[str, ...]
    summary: str


@dataclass(frozen=True)
class VerificationReceipt:
    disclosure_id: str
    owner_id: str
    kind: str
    payload_hash: str
    evidence_ids: tuple[str, ...]
    verified: bool
    verifier_version: str
    reason: str

    @property
    def receipt_hash(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True,
                              separators=(",", ":")).encode()).hexdigest()


def verify_disclosure(package, *, artifact_bytes, verified_experiments,
                      verifier, verifier_version, profile):
    """Use a trusted checker. Existence of code/evidence alone is not verification.

    ``verifier(package, artifact_bytes, evidence_rows)`` must validate the module
    or finding under its declared test. No arbitrary agent code executes here.
    Callers provide a separately bounded runner when verification needs execution.
    """
    require_full_profile(profile)
    if package.kind not in {"module", "finding"} or not package.disclosure_id or not package.owner_id:
        raise ValueError("invalid disclosure identity/kind")
    if not isinstance(artifact_bytes, bytes) or hashlib.sha256(artifact_bytes).hexdigest() != package.payload_hash:
        raise ValueError("disclosure artifact hash mismatch")
    if not package.summary.strip() or not verifier_version or not callable(verifier):
        raise ValueError("disclosure requires summary and a versioned trusted verifier")
    if not package.evidence_ids or len(set(package.evidence_ids)) != len(package.evidence_ids):
        raise ValueError("disclosure requires distinct real evidence IDs")
    evidence = []
    for identity in package.evidence_ids:
        row = verified_experiments.get(identity)
        if not isinstance(row, dict) or row.get("verified") is not True or row.get("agent_id") != package.owner_id:
            raise ValueError("disclosure cites missing, foreign, or unverified evidence")
        evidence.append(row)
    accepted = verifier(package, artifact_bytes, tuple(evidence))
    if not isinstance(accepted, bool):
        raise ValueError("disclosure verifier must explicitly return bool")
    return VerificationReceipt(package.disclosure_id, package.owner_id, package.kind,
        package.payload_hash, tuple(package.evidence_ids), accepted, verifier_version,
        "VERIFIED" if accepted else "VERIFICATION_FAILED")


def publish_disclosure(package, receipt, *, profile):
    require_full_profile(profile)
    if not receipt.verified or (package.disclosure_id, package.owner_id, package.kind,
         package.payload_hash, tuple(package.evidence_ids)) != (
         receipt.disclosure_id, receipt.owner_id, receipt.kind, receipt.payload_hash, receipt.evidence_ids):
        raise ValueError("only a matching verified disclosure may enter the public board")
    return {"disclosure_id": package.disclosure_id, "owner_id": package.owner_id,
            "kind": package.kind, "payload_hash": package.payload_hash,
            "evidence_ids": list(package.evidence_ids), "summary": package.summary,
            "verification_receipt": asdict(receipt), "receipt_hash": receipt.receipt_hash,
            "verified": True}
