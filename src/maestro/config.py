"""Frozen, validated configuration. Real prerequisites are checked explicitly."""
from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from decimal import Decimal
from pathlib import Path
import json
import math
import hashlib
from typing import Any

from maestro.market.credit import DEFAULT_CREDIT
from maestro.tasks.base import BENCHMARKS, benchmark_for
from maestro.tasks.taxonomy import taxonomy_identity
from maestro.types import MaestroError, stable_hash, to_jsonable

CORAL_REVISION = '0123dfb939b35228cf2c1fde224cd0561e727408'



PHASE_BUDGET_FIELDS = frozenset({
    'preparation_calls','preparation_window_seconds','planning_calls','planning_window_seconds',
    'generation_calls','generation_window_seconds','generation_cpu_seconds','memory_bytes',
    'pids_max','output_bytes','max_candidate_revisions','runtime_repair_calls','valuation_calls','valuation_window_seconds'})
WARM_FIELDS = frozenset({'warm_opportunities_per_agent','warm_concurrency'})
MARKET_FIELDS = frozenset({'slots','rounds','refresh_interval','reward_scale',
                           'catchup_fraction','initial_wealth'})
STAGE_FIELDS = PHASE_BUDGET_FIELDS | WARM_FIELDS | MARKET_FIELDS
EXPERIMENT_STAGES = frozenset({'warm_start','market'})


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
    # New runs default to greedy; value 1 retains the historical wire identity.
    ucb_exploration_coefficient: float = field(default=0.0, metadata={'omit_value': 1.0})
    # None is reserved for reading pre-discount historical wire records.
    valuation_discount_factor: float | None = field(default=0.8, metadata={'omit_value': None})
    reward_scale: Decimal = DEFAULT_CREDIT.reward_scale
    catchup_fraction: Decimal = DEFAULT_CREDIT.catchup_fraction
    initial_wealth: Decimal | None = Decimal('2000')
    seed: int = 0
    execution_mode: str = 'mock'
    benchmark: str = 'pace'
    profile: str = 'v1_cooperative'
    # Independent read-only preparation budget for the Coral native backend.
    preparation_calls: int = 4
    preparation_window_seconds: float = 300
    valuation_calls: int = 4
    valuation_window_seconds: float = 300
    generation_calls: int = 1
    planning_calls: int = 1
    planning_window_seconds: float = 120
    generation_window_seconds: float = 300
    generation_cpu_seconds: float = 30
    memory_bytes: int = 1073741824
    pids_max: int = 32
    output_bytes: int = 1048576
    evaluation_instance_count: int = 20
    cpu_seconds_per_instance: float = 30
    verification_cpu_seconds: float = 20
    # Historical tool-loop path only; not the runtime-repair attempt count.
    max_candidate_revisions: int = 3
    runtime_repair_calls: int = 3
    failures: dict = field(default_factory=dict)
    coral: dict[str, Any] = field(default_factory=dict)
    pace: dict[str, Any] = field(default_factory=dict)
    task: dict[str, Any] = field(default_factory=dict, metadata={'omit_value': {}})  # settings of a non-PACE benchmark
    plan_taxonomy: dict = field(default_factory=taxonomy_identity)
    experiment_stage: str | None = None
    stage_configs: dict = field(default_factory=dict)
    config_sources: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.plan_taxonomy != taxonomy_identity():
            raise ValueError("Plan taxonomy differs from the supported frozen PACE vocabulary")
        if self.experiment_stage=='warm_start':
            object.__setattr__(self,'initial_wealth',None)
        for name in ('reward_scale','catchup_fraction'):
            value=Decimal(str(getattr(self,name)))
            if not value.is_finite():
                raise ValueError(f'{name} must be finite')
            object.__setattr__(self, name,value)
        if self.initial_wealth is not None:
            value=Decimal(str(self.initial_wealth))
            if not value.is_finite():raise ValueError('initial_wealth must be finite')
            object.__setattr__(self,'initial_wealth',value)
        if type(self.preparation_calls) is not int or self.preparation_calls < 0:
            raise ValueError('preparation_calls must be a nonnegative integer (0 = no call-count cap)')
        for name in ('n_agents','slots','rounds','warm_opportunities_per_agent','warm_concurrency','cpu_workers','instance_workers','refresh_interval','generation_calls','planning_calls','max_candidate_revisions','runtime_repair_calls','valuation_calls'):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(f'{name} must be a positive integer')
        if self.slots > self.n_agents:
            raise ValueError('slots exceeds agent population')
        if self.execution_mode not in ('mock', 'real'):
            raise ValueError('execution_mode must be explicitly mock or real')
        if self.benchmark not in BENCHMARKS:
            raise ValueError(f'benchmark must be one of {sorted(BENCHMARKS)}')
        if type(self.seed) is not int:
            raise ValueError('seed must be an integer')
        if self.valuation_discount_factor is not None:
            from maestro.agents.valuation import validate_discount
            object.__setattr__(self,'valuation_discount_factor',float(validate_discount(self.valuation_discount_factor)))
        coefficient=self.ucb_exploration_coefficient
        if (isinstance(coefficient,bool) or not isinstance(coefficient,(int,float))
                or not math.isfinite(coefficient) or coefficient < 0):
            raise ValueError('ucb_exploration_coefficient must be finite and nonnegative')
        object.__setattr__(self,'ucb_exploration_coefficient',float(coefficient))
        for name in ('failures','coral','pace','task'):
            if not isinstance(getattr(self,name),dict):
                raise ValueError(f'{name} must be a mapping')
        # Direct constructors create new configurations. Frozen readers supply
        # an explicit legacy version below, so absence never upgrades an archive.
        if 'candidate_pipeline' not in self.coral:
            object.__setattr__(self,'coral',{'candidate_pipeline':'prebid_v2',**self.coral})
        if self.coral.get('candidate_pipeline','postbid_v1') not in ('postbid_v1','prebid_v2'):
            raise ValueError('Unknown candidate_pipeline version')
        if self.profile not in ('v1_cooperative', 'codex_session_pilot'):
            raise ValueError('full profiles require explicit separate configuration; V1 is the executable profile')
        if self.profile == 'codex_session_pilot' and self.execution_mode != 'real':
            raise ValueError('codex_session_pilot requires execution_mode=real and benchmark=pace; smoke stays explicitly simulated')
        if self.valuation_calls>4:
            raise ValueError('Valuation is bounded to at most four actual requests')
        if self.runtime_repair_calls > 3:
            raise ValueError("Runtime repair cap must not exceed three")
        if self.planning_calls > 1:
            raise ValueError('V1 inference call caps exceeded')
        if self.reward_scale <= 0 or not 0 <= self.catchup_fraction <= 1:
            raise ValueError('invalid reward units')
        if self.initial_wealth is not None and self.initial_wealth < 0:
            raise ValueError('initial_wealth must be nonnegative')
        if self.experiment_stage=='market' and self.initial_wealth is None:
            raise ValueError('Select Market credits before running')
        for name in ('valuation_window_seconds','preparation_window_seconds','planning_window_seconds','generation_window_seconds','generation_cpu_seconds','memory_bytes','pids_max','output_bytes','evaluation_instance_count','cpu_seconds_per_instance','verification_cpu_seconds'):
            value=getattr(self,name)
            if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or value<=0:
                raise ValueError(f'{name} must be positive')
        for name in ('memory_bytes','pids_max','output_bytes','evaluation_instance_count'):
            if type(getattr(self,name)) is not int:
                raise ValueError(f'{name} must be an integer')


        benchmark_for(self.benchmark).validate(self)
        if self.experiment_stage is not None and self.experiment_stage not in EXPERIMENT_STAGES:
            raise ValueError('experiment_stage must be warm_start or market')
        if not isinstance(self.stage_configs,dict) or not isinstance(self.config_sources,dict):
            raise ValueError('stage_configs and config_sources must be mappings')
        if self.stage_configs:
            if set(self.stage_configs) != EXPERIMENT_STAGES or self.experiment_stage is None:
                raise ValueError('Layered experiments require both frozen stages and an active stage')
            shared={f.name:getattr(self,f.name) for f in fields(self)
                    if f.name not in STAGE_FIELDS | {'stage_configs','config_sources','experiment_stage'}}
            for name,values in self.stage_configs.items():
                keys=set(values) if isinstance(values,dict) else set()
                allowed={STAGE_FIELDS,STAGE_FIELDS-{'initial_wealth'}}
                if not isinstance(values,dict) or keys not in allowed:
                    raise ValueError(f'Incomplete frozen stage configuration: {name}')
                # Check both stages before any execution, not just the selected one.
                type(self)(**shared,**values)
        elif self.config_sources:
            raise ValueError('Experiment sources require frozen stage configurations')
        for role,source in self.config_sources.items():
            if (not isinstance(source,dict) or set(source) != {'path','sha256'}
                    or not isinstance(source['path'],str) or not source['path']
                    or not isinstance(source['sha256'],str) or len(source['sha256']) != 64
                    or any(c not in '0123456789abcdef' for c in source['sha256'])):
                raise ValueError(f'Invalid configuration source identity: {role}')

    def for_stage(self, stage):
        if stage not in EXPERIMENT_STAGES:
            raise ValueError('stage must be warm_start or market')
        if not self.stage_configs:
            if self.experiment_stage=='warm_start' and stage=='market':
                raise ValueError('Select Market credits with --config or --credits at run')
            return self
        if stage=='market' and 'initial_wealth' not in self.stage_configs['market']:
            raise ValueError('Select Market credits with --config or --credits at run')
        return replace(self, experiment_stage=stage, **self.stage_configs[stage])

    def with_market_credits(self, credits):
        """Set the Market wallet without changing the Warm checkpoint."""
        amount=Decimal(str(credits))
        if not self.stage_configs:
            return replace(self,experiment_stage='market',initial_wealth=amount)
        stages={name:dict(values) for name,values in self.stage_configs.items()}
        stages['market']['initial_wealth']=str(amount)
        active={'initial_wealth':amount} if self.experiment_stage=='market' else {}
        return replace(self,stage_configs=stages,**active)

    def with_market_rounds(self, rounds):
        """Select the Market horizon without changing the Warm checkpoint."""
        if type(rounds) is not int or rounds < 1:
            raise ValueError('Market rounds must be a positive integer')
        if self.experiment_stage=='warm_start':
            raise ValueError('Market rounds cannot override Warm Start')
        if not self.stage_configs:
            return replace(self,rounds=rounds)
        stages={name:dict(values) for name,values in self.stage_configs.items()}
        stages['market']['rounds']=rounds
        return replace(self,rounds=rounds,stage_configs=stages)

    @property
    def effective_initial_wealth(self):
        """Zero credits request a finite balance sufficient for every round.

        A formal score lies in [0,1]. Even without assuming parent <= champion,
        reward is at most scale * (1 + catchup). Include Warm's reward units,
        since first-price's Market estimate averages credits earned during Warm. One
        agent buys at most one formal admission per Market round.
        """
        if self.initial_wealth is None:
            raise ValueError('Warm Start has no Market wallet')
        if self.initial_wealth:
            return self.initial_wealth
        from maestro.market.credit import money
        bounds=[money(self.reward_scale*(1+self.catchup_fraction))]
        if self.stage_configs:
            warm=self.stage_configs['warm_start']
            bounds.append(money(Decimal(str(warm['reward_scale']))*
                (1+Decimal(str(warm['catchup_fraction'])))))
        return self.rounds*max(bounds)

    @property
    def shared_config_hash(self):
        value=self.to_dict()
        for key in STAGE_FIELDS | {'experiment_stage','failures'}:
            value.pop(key,None)
        for stage in value.get('stage_configs',{}).values():
            stage.pop('initial_wealth',None)
        value.get('config_sources',{}).pop('market',None)
        return stable_hash(value)

    def validate_warm_source(self, warm):
        """Freeze Warm identity while allowing Market to choose its round horizon."""
        from maestro.types import IntegrityError
        if bool(self.stage_configs) != bool(warm.stage_configs):
            raise IntegrityError('Warm/main configuration layout mismatch')
        if self.stage_configs:
            if warm.experiment_stage != 'warm_start' or self.experiment_stage != 'market':
                raise IntegrityError('Warm/main experiment stage mismatch')
            # The Market round horizon may be chosen when an arm is cloned from
            # an otherwise frozen Warm checkpoint. Keep every Warm setting and
            # every other Market setting bound to that checkpoint.
            aligned_stages={name:dict(values) for name,values in warm.stage_configs.items()}
            aligned_stages['market']['rounds']=self.stage_configs['market']['rounds']
            aligned_warm=replace(warm,stage_configs=aligned_stages)
            if aligned_warm.shared_config_hash != self.shared_config_hash:
                raise IntegrityError('Warm/main shared configuration or frozen stage mismatch')
            if (warm.hash != warm.for_stage('warm_start').hash
                    or self.hash != self.for_stage('market').hash):
                raise IntegrityError('Active settings differ from the frozen stage configuration')
            return
        source,target=warm.to_dict(),self.to_dict()
        # Preserve existing explicitly simulated main fault-injection scenarios.
        source.pop('failures',None);target.pop('failures',None)
        source.pop('initial_wealth',None);target.pop('initial_wealth',None)
        source.pop('experiment_stage',None);target.pop('experiment_stage',None)
        source.pop('rounds',None);target.pop('rounds',None)
        if source != target:
            raise IntegrityError('Warm/main configuration mismatch')

    @property
    def reserved_cpu_seconds(self):
        return self.evaluation_instance_count * self.cpu_seconds_per_instance + self.verification_cpu_seconds

    @property
    def warm_batch_width(self):
        """Independent warm concurrency, bounded by the explicit CPU pool."""
        return min(self.warm_concurrency, self.n_agents, self.cpu_workers)

    def evaluation_batch_width(self, candidate_count):
        """Queue excess candidates rather than changing their market admission."""
        if type(candidate_count) is not int or candidate_count < 1:
            raise ValueError('candidate_count must be a positive integer')
        return min(candidate_count, self.cpu_workers)

    def effective_instance_workers(self, candidate_batch_width, evaluation_instance_count=None):
        """Divide the same frozen CPU pool between candidate evaluators."""
        if type(candidate_batch_width) is not int or candidate_batch_width < 1:
            raise ValueError('candidate_batch_width must be a positive integer')
        if candidate_batch_width > self.cpu_workers:
            raise ValueError('candidate_batch_width exceeds cpu_workers')
        count = self.evaluation_instance_count if evaluation_instance_count is None else evaluation_instance_count
        if type(count) is not int or count < 1:
            raise ValueError('evaluation_instance_count must be a positive integer')
        # The global executor pool enforces the total. A ceiling rather than
        # floor allows leftover cores to serve some candidates immediately.
        fair_ceiling = (self.cpu_workers + candidate_batch_width - 1) // candidate_batch_width
        return min(self.instance_workers, count, fair_ceiling)

    @property
    def hash(self):
        return stable_hash(self.to_dict())

    def to_dict(self):
        value=to_jsonable(self)
        value.get('stage_configs',{}).get('warm_start',{}).pop('initial_wealth',None)
        if self.experiment_stage=='warm_start':
            value.pop('initial_wealth',None)
            value.get('stage_configs',{}).get('market',{}).pop('initial_wealth',None)
            value.get('config_sources',{}).pop('market',None)
        return value

    @classmethod
    def from_dict(cls, value):
        """Deserialize wire records; old records without the coefficient used 1.

        New config documents go through load_config/load_experiment, which insert
        the current default explicitly before using this compatibility reader.
        """
        if not isinstance(value, dict):
            raise ValueError("Configuration must be a mapping")
        known = {f.name for f in fields(cls)}
        unknown = set(value) - known
        if unknown:
            raise ValueError(f'Unknown config keys: {sorted(unknown)}')
        coral={'candidate_pipeline':'postbid_v1',**value.get('coral',{})}
        return cls(**{'ucb_exploration_coefficient':1.0,'valuation_discount_factor':None,**value,'coral':coral})


def _unique_mapping(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'Duplicate config key: {key}')
        result[key] = value
    return result


def read_config_document(path: str | Path):
    path = Path(path)
    raw = path.read_bytes()
    content = raw.decode('utf-8')
    if path.suffix.lower() == '.json':
        value = json.loads(content, object_pairs_hook=_unique_mapping)
    else:
        try:
            import yaml
        except ImportError as exc:
            raise MaestroError('YAML configuration requires PyYAML; JSON is supported without it') from exc
        class UniqueLoader(yaml.SafeLoader):
            pass
        def mapping(loader, node):
            loader.flatten_mapping(node)
            return _unique_mapping(loader.construct_pairs(node))
        UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
        value = yaml.load(content, Loader=UniqueLoader)
    if value is None:
        value = {}
    if not isinstance(value,dict):
        raise ValueError('Configuration must be a mapping')
    return value, hashlib.sha256(raw).hexdigest()


def load_config(path: str | Path, *, stage: str | None = None) -> RunConfig:
    value, digest = read_config_document(path)
    if value.get('kind') == 'experiment':
        from maestro.config_layers import load_experiment
        return load_experiment(Path(path), value, digest, stage=stage or 'warm_start')
    if stage is not None and stage not in EXPERIMENT_STAGES:
        raise ValueError('stage must be warm_start or market')
    if value.get('valuation_discount_factor',0.8) is None:
        raise ValueError('New configs require a numeric valuation_discount_factor')
    from maestro.execution.pipeline import new_pipeline_settings
    return RunConfig.from_dict({'ucb_exploration_coefficient':RunConfig().ucb_exploration_coefficient,
        'valuation_discount_factor':RunConfig().valuation_discount_factor,**value,
        'coral':new_pipeline_settings(value.get('coral',{}))})
