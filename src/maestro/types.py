"""Shared, explicit wire types. Monetary values always use Decimal."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any
import hashlib
import json


class MaestroError(RuntimeError):
    """A domain or execution contract was violated."""


class BootstrapIncomplete(MaestroError):
    pass


class AuthorizationError(MaestroError):
    pass


class IntegrityError(MaestroError):
    pass


@dataclass(frozen=True)
class Plan:
    plan_id: str
    version_id: str
    agent_id: str
    parent_hash: str
    target: str
    mod_type: str
    diagnosis: str
    change: str
    expected_effect: str
    evidence_run_ids: tuple[str, ...] = ()
    category_detail: str = ""

    @property
    def content_hash(self) -> str:
        return stable_hash(self)


@dataclass(frozen=True)
class HistoryRecord:
    experiment_id: str
    agent_id: str
    parent_score: float
    champion_score: float
    score: float
    valid: bool
    target: str
    mod_type: str
    credit: Decimal = Decimal("0")
    status: str = "COMPLETED"
    candidate_hash: str = ""

    @property
    def gain(self) -> float:
        return float(max(Decimal(0), Decimal(str(self.score)) - Decimal(str(self.parent_score)))) if self.valid else 0.0


@dataclass(frozen=True)
class Submission:
    agent_id: str
    plan_version_id: str
    value: Decimal
    bid: Decimal
    eligible: bool = True


@dataclass
class AgentState:
    agent_id: str
    parent_hash: str
    parent_score: float
    wealth: Decimal = Decimal("2000")
    history: list[HistoryRecord] = field(default_factory=list)
    lessons: list[dict] = field(default_factory=list)
    pending_plan: Plan | None = None
    last_planning_round: int = -999
    needs_refresh: bool = False
    ucb_n: int = 0
    ucb_total: Decimal = Decimal("0")


@dataclass(frozen=True)
class EvaluationResult:
    job_id: str
    candidate_hash: str
    score: float
    valid: bool
    status: str = "COMPLETED"
    cpu_seconds: float = 0.0
    wall_seconds: float = 0.0
    diagnostics: dict = field(default_factory=dict)
    instance_results: tuple[dict, ...] = ()


def to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        # A dataclass may own its serialized form (RunConfig omits empty optional sections).
        return value.to_dict() if hasattr(value, 'to_dict') else to_jsonable(asdict(value))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (Path, Enum)):
        return str(value.value if isinstance(value, Enum) else value)
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, float) and not __import__('math').isfinite(value):
        raise ValueError("Nonfinite numeric value")
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(to_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def state_from_dict(value: dict) -> AgentState:
    item = dict(value)
    item['wealth'] = Decimal(str(item.get('wealth', '2000')))
    item['ucb_total'] = Decimal(str(item.get('ucb_total', '0')))
    item['history'] = [HistoryRecord(**{**r, 'credit': Decimal(str(r.get('credit', '0')))}) for r in item.get('history', [])]
    if item.get('pending_plan'):
        p = dict(item['pending_plan'])
        p['evidence_run_ids'] = tuple(p.get('evidence_run_ids', ()))
        item['pending_plan'] = Plan(**p)
    return AgentState(**item)


def result_from_dict(value: dict) -> EvaluationResult:
    return EvaluationResult(**{**value, 'instance_results': tuple(value.get('instance_results', ()))})
