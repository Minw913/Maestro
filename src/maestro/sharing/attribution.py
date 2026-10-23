"""Verified dependency attribution. Transfers do not mint additional reward."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
import hashlib
import json
from maestro.market.credit import QUANTUM, decimal, money
from maestro.market.reputation import require_full_profile
from maestro.sharing.disclosure import VerificationReceipt


@dataclass(frozen=True)
class VerifiedDependency:
    disclosure_id: str
    owner_id: str
    candidate_hash: str
    receipt_hash: str
    lineage_proof_hash: str


@dataclass(frozen=True)
class CreditTransfer:
    receiver_id: str
    source_id: str
    amount: Decimal
    disclosure_ids: tuple[str, ...]


@dataclass(frozen=True)
class AttributionResult:
    rewards_after: dict[str, Decimal]
    transfers: tuple[CreditTransfer, ...]
    policy: str = "eta_total_pool_equal_distinct_sources_v1"


def verify_dependencies(*, receiver_id, candidate_hash, depends_on, public_items,
                         lineage_checker, profile):
    """Authenticate against trusted public receipts and explicit code lineage.

    ``lineage_checker(candidate_hash, public_item)`` returns a nonempty proof
    string only when the declared module is actually reused. A declared ID alone
    or an agent assertion cannot authorize a credit transfer.
    """
    require_full_profile(profile)
    if not callable(lineage_checker) or not receiver_id or not candidate_hash:
        raise ValueError("identity and trusted lineage checker required")
    claims = list(depends_on)
    if len(set(claims)) != len(claims):
        raise ValueError("duplicate dependency claims")
    verified = []
    for identity in claims:
        item = public_items.get(identity)
        if not isinstance(item, dict) or item.get("verified") is not True or item.get("kind") != "module":
            raise ValueError("dependency must name a publicly verified module")
        receipt_data = dict(item.get("verification_receipt", {}))
        receipt_data["evidence_ids"] = tuple(receipt_data.get("evidence_ids", ()))
        try:
            receipt = VerificationReceipt(**receipt_data)
        except TypeError as exc:
            raise ValueError("dependency verification receipt missing") from exc
        if (not receipt.verified or receipt.receipt_hash != item.get("receipt_hash")
                or receipt.disclosure_id != identity or receipt.owner_id != item.get("owner_id")
                or receipt.payload_hash != item.get("payload_hash")):
            raise ValueError("dependency receipt binding mismatch")
        proof = lineage_checker(candidate_hash, item)
        if not isinstance(proof, str) or not proof.strip():
            raise ValueError("declared dependency is not supported by verified code lineage")
        verified.append(VerifiedDependency(identity, receipt.owner_id, candidate_hash,
            receipt.receipt_hash, hashlib.sha256(proof.encode()).hexdigest()))
    return tuple(verified)


def attribute_credit(base_rewards, dependencies, *, eta, credited_candidates=None, profile):
    """Transfer one eta share per improvement, split across distinct sources.

    The full text does not specify the multi-source split. This explicit policy
    keeps a TOTAL eta pool, divides it equally among distinct other owners, and
    assigns any decimal remainder to the final sorted owner. Multiple modules
    from one source do not multiply its share. No secondary royalty cascade.
    Inputs must be trusted verification outputs, never raw agent submissions.
    """
    require_full_profile(profile)
    fraction = decimal(eta)
    if not 0 <= fraction < 1:
        raise ValueError("eta must be in [0,1)")
    original = {agent: money(value) for agent, value in base_rewards.items()}
    if any(value < 0 for value in original.values()):
        raise ValueError("base rewards cannot be negative")
    after, transfers = dict(original), []
    for receiver in sorted(dependencies):
        if receiver not in original:
            raise ValueError("attribution receiver missing from reward ledger")
        source_modules = {}
        seen = set()
        for dependency in dependencies[receiver]:
            if not isinstance(dependency, VerifiedDependency) or not dependency.lineage_proof_hash:
                raise ValueError("only verified lineage dependencies can transfer credit")
            if (credited_candidates is None
                    or dependency.candidate_hash != credited_candidates.get(receiver)):
                raise ValueError("dependency does not bind the receiver's credited improvement")
            if dependency.disclosure_id in seen:
                raise ValueError("duplicate verified dependency")
            seen.add(dependency.disclosure_id)
            if dependency.owner_id != receiver:
                if dependency.owner_id not in original:
                    raise ValueError("attribution source missing from reward ledger")
                source_modules.setdefault(dependency.owner_id, []).append(dependency.disclosure_id)
        if not source_modules or original[receiver] == 0:
            continue
        owners = sorted(source_modules)
        pool = money(original[receiver] * fraction)
        allocated = Decimal(0)
        after[receiver] = money(after[receiver] - pool)
        for index, owner in enumerate(owners):
            amount = (money(pool - allocated) if index == len(owners) - 1
                      else (pool / len(owners)).quantize(QUANTUM, rounding=ROUND_DOWN))
            allocated += amount
            after[owner] = money(after[owner] + amount)
            transfers.append(CreditTransfer(receiver, owner, amount, tuple(sorted(source_modules[owner]))))
    if sum(after.values(), Decimal(0)) != sum(original.values(), Decimal(0)) or any(v < 0 for v in after.values()):
        raise ArithmeticError("attribution violated conservation/nonnegative rewards")
    return AttributionResult(after, tuple(transfers))
