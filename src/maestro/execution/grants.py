"""Short-lived edit capability. Formal evaluation is authorized only by Ledger tickets."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import hmac
import json
import secrets
import time


class GrantError(PermissionError):
    pass


@dataclass(frozen=True)
class EditGrant:
    grant_id: str
    episode_id: str
    round_index: int
    agent_id: str
    plan_version_id: str
    parent_hash: str
    epoch: int
    expires_at: float
    signature: str = ""
    purpose: str = "legacy"
    source_candidate_hash: str = ""


class GrantAuthority:
    """Trusted, injectable capability signer; never place its key in agent mounts.

    Restart revocation uses the ledger round epoch. Signatures alone do not make
    permissions: a containment provider must enforce the returned grant bindings.
    """
    def __init__(self, key: bytes | None = None, clock=time.time):
        self._key = key or secrets.token_bytes(32)
        if len(self._key) < 32:
            raise ValueError("Grant keys require at least 256 bits")
        self.clock = clock
        self._revoked: set[str] = set()

    def _signature(self, grant: EditGrant) -> str:
        payload = asdict(grant)
        payload.pop("signature")
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hmac.new(self._key, raw, hashlib.sha256).hexdigest()

    def issue(self, *, episode_id: str, round_index: int, agent_id: str,
              plan_version_id: str, parent_hash: str, epoch: int,
              expires_at: float, purpose: str = 'legacy', source_candidate_hash: str = '') -> EditGrant:
        if expires_at <= self.clock():
            raise GrantError("Cannot issue an expired grant")
        grant = EditGrant(secrets.token_hex(16), episode_id, round_index, agent_id,
                          plan_version_id, parent_hash, epoch, expires_at,
                          purpose=purpose, source_candidate_hash=source_candidate_hash)
        return EditGrant(**{**asdict(grant), "signature": self._signature(grant)})

    def verify(self, grant: EditGrant, *, agent_id: str, plan_version_id: str,
               parent_hash: str, epoch: int) -> None:
        if not hmac.compare_digest(grant.signature, self._signature(grant)):
            raise GrantError("Invalid grant signature")
        if grant.grant_id in self._revoked or self.clock() >= grant.expires_at:
            raise GrantError("Expired or revoked grant")
        if (grant.agent_id, grant.plan_version_id, grant.parent_hash, grant.epoch) != (
                agent_id, plan_version_id, parent_hash, epoch):
            raise GrantError("Grant is not bound to this agent/plan/parent/epoch")

    def revoke(self, grant: EditGrant) -> None:
        self._revoked.add(grant.grant_id)
