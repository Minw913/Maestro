"""Strict composition of runtime, deployment, benchmark and stage documents.

Documents own disjoint fields. References are relative to the referring file;
filesystem values are relative to the declaring fragment. No recursive includes,
implicit overrides, environment interpolation or list concatenation exist.
"""
from __future__ import annotations
from copy import deepcopy
from pathlib import Path

from maestro.config import (RunConfig, read_config_document, EXPERIMENT_STAGES,
    STAGE_FIELDS, PHASE_BUDGET_FIELDS, WARM_FIELDS, MARKET_FIELDS)

RUNTIME_CORAL_FIELDS = frozenset({
    'backend_factory','revision','model','codex_cli_version','codex_reasoning_effort',
    'director_mode','runtime','runtime_model','provider_model','planning_model',
    'planning_api_base','generation_api_base','planning_api_key_env','generation_api_key_env',
    'planning_seconds','planning_output_tokens','generation_output_tokens','anthropic_beta',
    'session_timeout_seconds','deepseek_api_base','deepseek_api_key_env',
    'deepseek_cli_version','deepseek_cli_sha256','deepseek_reasoning_effort','deepseek_max_output_tokens'})
DEPLOYMENT_CORAL_FIELDS = frozenset({'checkout_path','codex_executable','deepseek_executable','generation_image','smoke_graph'})
DEPLOYMENT_PACE_FIELDS = frozenset({'panel_manifest','seed_solver','heldout_manifest'})
DEPLOYMENT_CONTRACT_FIELDS = frozenset({'image','hardware_id','toolchain_digest'})
BENCHMARK_CONTRACT_FIELDS = frozenset({
    'schema_version','label','instances','cpu_seconds_per_instance','verification_cpu_seconds',
    'verification_wall_seconds','verification_code_hash','wall_seconds_per_instance',
    'memory_mb','pids','output_bytes','cpus','solver_argv'})


def _keys(value, allowed, label):
    if not isinstance(value,dict):
        raise ValueError(f'{label} must be a mapping')
    unknown=set(value)-allowed
    if unknown:
        raise ValueError(f'Unknown or misplaced fields in {label}: {sorted(unknown)}')


def _merge(target, source, label=''):
    for key,value in source.items():
        name=f'{label}.{key}' if label else key
        if key not in target:
            target[key]=deepcopy(value)
        elif isinstance(value,dict) and isinstance(target[key],dict):
            _merge(target[key],value,name)
        else:
            raise ValueError(f'Conflicting duplicate configuration definition: {name}')


def _path(value, base, label, *, executable=False):
    if not isinstance(value,str) or not value.strip():
        raise ValueError(f'{label} needs a nonempty filesystem path')
    if '$' in value or value.startswith('~'):
        raise ValueError(f'{label} does not support environment/home interpolation')
    if executable and '/' not in value and '\\' not in value:
        return value  # An explicit command name is resolved through PATH later.
    path=Path(value)
    return str((base/path).resolve())


def _fragment(path, role, seen, sources):
    path=path.resolve()
    if path in seen:
        raise ValueError(f'Configuration reference cycle or duplicate source: {path}')
    seen.add(path)
    value,digest=read_config_document(path)
    if value.get('kind') != role or type(value.get('schema_version')) is not int or value['schema_version'] != 1:
        raise ValueError(f'Expected kind={role}, schema_version=1 in {path}')
    sources[role]={'path':str(path),'sha256':digest}
    payload={key:deepcopy(item) for key,item in value.items() if key not in {'kind','schema_version'}}
    if role=='runtime':
        _keys(payload,{'profile','coral'},role)
        if 'coral' in payload:
            _keys(payload['coral'],RUNTIME_CORAL_FIELDS,'runtime.coral')
            settings=payload['coral']
            factory=settings.get('backend_factory')
            if factory=='maestro.backends.coral.deepseek_backend:native_factory':
                if settings.get('runtime')!='dsh' or any(k.startswith('codex_') for k in settings):
                    raise ValueError('DeepSeek runtime requires runtime=dsh without Codex settings')
                from maestro.backends.coral.deepseek_gate import provider_endpoint
                provider_endpoint(settings.get('deepseek_api_base','https://api.deepseek.com/anthropic'))
                if settings.get('deepseek_reasoning_effort','low') not in ('off','low','high','max'):
                    raise ValueError('Invalid DeepSeek reasoning effort')
                maximum=settings.get('deepseek_max_output_tokens',32000)
                if type(maximum) is not int or not 1<=maximum<=96000:
                    raise ValueError('Invalid DeepSeek output-token bound')
            elif factory=='maestro.backends.coral.native_backend:native_factory':
                if settings.get('runtime','codex')!='codex' or any(k.startswith('deepseek_') for k in settings):
                    raise ValueError('Codex runtime cannot inherit DeepSeek settings')
    elif role=='deployment':
        _keys(payload,{'cpu_workers','instance_workers','coral','pace'},role)
        if 'coral' in payload:
            _keys(payload['coral'],DEPLOYMENT_CORAL_FIELDS,'deployment.coral')
            for name in ('checkout_path','codex_executable','deepseek_executable','smoke_graph'):
                if name in payload['coral']:
                    payload['coral'][name]=_path(payload['coral'][name],path.parent,
                        'deployment.coral.'+name,executable=name.endswith('_executable'))
        if 'pace' in payload:
            _keys(payload['pace'],DEPLOYMENT_PACE_FIELDS | {'contract'},'deployment.pace')
            for name in DEPLOYMENT_PACE_FIELDS:
                if name in payload['pace']:
                    payload['pace'][name]=_path(payload['pace'][name],path.parent,'deployment.pace.'+name)
            if 'contract' in payload['pace']:
                _keys(payload['pace']['contract'],DEPLOYMENT_CONTRACT_FIELDS,'deployment.pace.contract')
    elif role=='benchmark':
        _keys(payload,{'benchmark','evaluation_instance_count','cpu_seconds_per_instance',
                       'verification_cpu_seconds','pace'},role)
        if payload.get('benchmark') != 'pace':
            raise ValueError('Only benchmark=pace is implemented')
        if 'pace' in payload:
            _keys(payload['pace'],{'contract'},'benchmark.pace')
            if 'contract' in payload['pace']:
                _keys(payload['pace']['contract'],BENCHMARK_CONTRACT_FIELDS,'benchmark.pace.contract')
    else:
        owned=WARM_FIELDS if role=='warm_start' else MARKET_FIELDS
        _keys(payload,owned | PHASE_BUDGET_FIELDS,role)
    return payload


def load_experiment(path, manifest, digest, *, stage):
    path=path.resolve()
    _keys(manifest,{'kind','schema_version','runtime','deployment','benchmark','stages',
                    'execution_mode','n_agents','seed','failures'},'experiment')
    if type(manifest.get('schema_version')) is not int or manifest['schema_version'] != 1:
        raise ValueError('Experiment schema_version must be 1')
    if stage not in EXPERIMENT_STAGES:
        raise ValueError('stage must be warm_start or market')
    refs=manifest.get('stages')
    _keys(refs,EXPERIMENT_STAGES,'experiment.stages')
    if set(refs) != EXPERIMENT_STAGES:
        raise ValueError('Experiment must declare both warm_start and market stage files')
    sources={'experiment':{'path':str(path),'sha256':digest}}
    seen={path}
    def reference(value, role):
        if not isinstance(value,str) or not value.strip():
            raise ValueError(f'Experiment {role} must reference one configuration file')
        return _fragment(path.parent/value,role,seen,sources)
    common={}
    for role in ('runtime','deployment','benchmark'):
        _merge(common,reference(manifest.get(role),role))
    for key in ('execution_mode','n_agents','seed','failures'):
        if key in manifest:
            _merge(common,{key:manifest[key]})
    if 'execution_mode' not in common:
        raise ValueError('Experiment execution_mode must be explicit')
    # Repeated resource values inside an evaluation contract must agree with
    # their top-level meaning, even though they are distinct serialized fields.
    contract=common.get('pace',{}).get('contract',{})
    for outer,inner in [('evaluation_instance_count','instances'),
                        ('cpu_seconds_per_instance','cpu_seconds_per_instance'),
                        ('verification_cpu_seconds','verification_cpu_seconds')]:
        if outer in common and inner in contract and common[outer] != contract[inner]:
            raise ValueError(f'Benchmark contract conflict: {outer} != pace.contract.{inner}')
    defaults=RunConfig().to_dict()
    stage_configs={}
    for name in ('warm_start','market'):
        overrides=reference(refs[name],name)
        values={key:defaults[key] for key in STAGE_FIELDS}
        values.update(overrides)
        checked=RunConfig.from_dict({**common,**values})
        stage_configs[name]={key:checked.to_dict()[key] for key in STAGE_FIELDS}
    return RunConfig.from_dict({**common,**stage_configs[stage],
        'experiment_stage':stage,'stage_configs':stage_configs,'config_sources':sources})
