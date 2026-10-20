"""Frozen, validated configuration. Real prerequisites are checked explicitly."""
from __future__ import annotations

from dataclasses import dataclass, field, fields
from decimal import Decimal
from pathlib import Path
import json
import math

from maestro.types import to_jsonable, stable_hash, MaestroError


CORAL_REVISION = '0123dfb939b35228cf2c1fde224cd0561e727408'
TARGETS = ('reduction','initialization','local_search','tie_break','restart','postprocess','data_structure','time_management','other')
MOD_TYPES = ('parameter_change','rule_replacement','new_component','removal','reimplementation','other')


@dataclass(frozen=True)
class RunConfig:
    n_agents: int = 4
    slots: int = 2
    rounds: int = 20
    warm_opportunities_per_agent: int = 5
    warm_concurrency: int = 4
    cpu_workers: int = 4
    instance_workers: int = 1
    refresh_interval: int = 3
    reward_scale: Decimal = Decimal('100')
    catchup_fraction: Decimal = Decimal('0.25')
    bid_cap: Decimal = Decimal('100')
    initial_wealth: Decimal = Decimal('2000')
    seed: int = 0
    mode: str = 'mock'
    profile: str = 'v1_cooperative'
    generation_calls: int = 3
    planning_calls: int = 1
    planning_window_seconds: float = 120
    generation_window_seconds: float = 300
    preparation_cpu_seconds: float = 5
    generation_cpu_seconds: float = 30
    memory_bytes: int = 1073741824
    pids_max: int = 32
    output_bytes: int = 1048576
    panel_size: int = 20
    instance_cpu_seconds: float = 30
    verification_cpu_seconds: float = 20
    max_candidate_revisions: int = 3
    failures: dict = field(default_factory=dict)
    coral: dict = field(default_factory=dict)
    pace: dict = field(default_factory=dict)
    initialization_mode: str = 'common_seed_directed_warm'

    def __post_init__(self):
        for name in ('reward_scale','catchup_fraction','bid_cap','initial_wealth'):
            value=Decimal(str(getattr(self,name)))
            if not value.is_finite():
                raise ValueError(f'{name} must be finite')
            object.__setattr__(self, name,value)
        for name in ('n_agents','slots','rounds','warm_opportunities_per_agent','warm_concurrency','cpu_workers','instance_workers','refresh_interval','generation_calls','planning_calls','max_candidate_revisions'):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(f'{name} must be a positive integer')
        if self.slots > self.n_agents:
            raise ValueError('slots exceeds agent population')
        if self.mode not in ('mock', 'pace'):
            raise ValueError('mode must be explicitly mock or pace')
        if type(self.seed) is not int:
            raise ValueError('seed must be an integer')
        if self.initialization_mode!='common_seed_directed_warm':
            raise ValueError('Only common_seed_directed_warm initialization is implemented')
        for name in ('failures','coral','pace'):
            if not isinstance(getattr(self,name),dict):
                raise ValueError(f'{name} must be a mapping')
        if self.profile not in ('v1_cooperative', 'codex_session_pilot'):
            raise ValueError('full profiles require explicit separate configuration; V1 is the executable profile')
        if self.profile == 'codex_session_pilot' and self.mode != 'pace':
            raise ValueError('codex_session_pilot requires real pace mode; smoke stays explicitly simulated')
        if self.generation_calls > 3 or self.planning_calls > 1:
            raise ValueError('V1 inference call caps exceeded')
        if self.reward_scale <= 0 or not 0 <= self.catchup_fraction <= 1 or self.bid_cap < 0:
            raise ValueError('invalid reward/bid units')
        if self.initial_wealth < self.rounds * self.bid_cap:
            raise ValueError('V1 requires ample wealth >= rounds * bid_cap')
        for name in ('planning_window_seconds','generation_window_seconds','preparation_cpu_seconds','generation_cpu_seconds','memory_bytes','pids_max','output_bytes','panel_size','instance_cpu_seconds','verification_cpu_seconds'):
            value=getattr(self,name)
            if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or value<=0:
                raise ValueError(f'{name} must be positive')
        for name in ('memory_bytes','pids_max','output_bytes','panel_size'):
            if type(getattr(self,name)) is not int:
                raise ValueError(f'{name} must be an integer')

    @property
    def reserved_cpu_seconds(self):
        return self.panel_size * self.instance_cpu_seconds + self.verification_cpu_seconds

    @property
    def warm_batch_width(self):
        """Independent warm concurrency, bounded by the explicit CPU pool."""
        return min(self.warm_concurrency, self.n_agents, self.cpu_workers)

    def evaluation_batch_width(self, candidate_count):
        """Queue excess candidates rather than changing their market admission."""
        if type(candidate_count) is not int or candidate_count < 1:
            raise ValueError('candidate_count must be a positive integer')
        return min(candidate_count, self.cpu_workers)

    def effective_instance_workers(self, candidate_batch_width, panel_size=None):
        """Divide the same frozen CPU pool between candidate evaluators."""
        if type(candidate_batch_width) is not int or candidate_batch_width < 1:
            raise ValueError('candidate_batch_width must be a positive integer')
        if candidate_batch_width > self.cpu_workers:
            raise ValueError('candidate_batch_width exceeds cpu_workers')
        count = self.panel_size if panel_size is None else panel_size
        if type(count) is not int or count < 1:
            raise ValueError('panel_size must be a positive integer')
        # The global executor pool enforces the total. A ceiling rather than
        # floor allows leftover cores to serve some candidates immediately.
        fair_ceiling = (self.cpu_workers + candidate_batch_width - 1) // candidate_batch_width
        return min(self.instance_workers, count, fair_ceiling)

    @property
    def hash(self):
        return stable_hash(self.to_dict())

    def to_dict(self):
        return to_jsonable(self)

    @classmethod
    def from_dict(cls, value):
        known = {f.name for f in fields(cls)}
        unknown = set(value) - known
        if unknown:
            raise ValueError(f'Unknown config keys: {sorted(unknown)}')
        return cls(**value)


def load_config(path: str | Path) -> RunConfig:
    path = Path(path)
    if path.suffix.lower() == '.json':
        value = json.loads(path.read_text(encoding='utf-8'))
    else:
        try:
            import yaml
        except ImportError as exc:
            raise MaestroError('YAML configuration requires PyYAML; JSON is supported without it') from exc
        value = yaml.safe_load(path.read_text(encoding='utf-8'))
    return RunConfig.from_dict(value or {})
