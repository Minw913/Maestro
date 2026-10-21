from dataclasses import replace
from pathlib import Path
import shutil

import pytest
import yaml

from maestro.config import load_config
from maestro.cli import build_parser,_config
from maestro.types import IntegrityError

ROOT=Path(__file__).resolve().parents[2]


def test_codex_preserved_and_deepseek_is_opt_in():
    codex=load_config(ROOT/'configs/experiments/pace_remote.yaml')
    deepseek=load_config(ROOT/'configs/experiments/pace_remote_deepseek.yaml')
    assert codex.coral['model']=='gpt-6-astra'
    assert codex.coral['backend_factory']=='maestro.backends.coral.native_backend:native_factory'
    assert deepseek.coral['runtime']=='dsh'
    assert deepseek.coral['backend_factory']=='maestro.backends.coral.deepseek_backend:native_factory'
    assert deepseek.stage_configs==codex.stage_configs
    assert deepseek.pace==codex.pace
    assert (deepseek.preparation_calls,deepseek.planning_calls,deepseek.generation_calls)==(codex.preparation_calls,codex.planning_calls,codex.generation_calls)
    assert deepseek.reserved_cpu_seconds==codex.reserved_cpu_seconds==620
    with pytest.raises(IntegrityError):deepseek.for_stage('market').validate_warm_source(codex)
    deepseek.for_stage('market').validate_warm_source(deepseek)
    args=build_parser().parse_args(['bootstrap','--config',str(ROOT/'configs/experiments/pace_remote_deepseek.yaml')])
    assert _config(args)==deepseek


@pytest.mark.parametrize('changes',[{'runtime':'codex'},{'codex_cli_version':'anything'},
    {'deepseek_api_base':'http://insecure.invalid'},{'deepseek_max_output_tokens':True},
    {'deepseek_reasoning_effort':'invalid'},{'deepseek_api_key':'never-store-secrets'}])
def test_invalid_or_mixed_deepseek_config_rejected(tmp_path,changes):
    shutil.copytree(ROOT/'configs',tmp_path/'configs')
    path=tmp_path/'configs/runtimes/coral_deepseek.yaml'
    value=yaml.safe_load(path.read_text());value['coral'].update(changes);path.write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError):load_config(tmp_path/'configs/experiments/pace_remote_deepseek.yaml')


def test_missing_api_key_never_falls_back_to_codex(monkeypatch):
    from maestro.backends.coral.deepseek_backend import native_factory
    from maestro.backends.base import BackendUnavailable
    config=load_config(ROOT/'configs/experiments/pace_remote_deepseek.yaml')
    monkeypatch.delenv('DEEPSEEK_API_KEY',raising=False)
    def forbidden(*args,**kwargs):pytest.fail('DeepSeek must not invoke Codex authentication')
    monkeypatch.setattr('maestro.backends.codex_session.client.CodexSessionClient.require_chatgpt_login',forbidden)
    with pytest.raises(BackendUnavailable,match='controller environment'):native_factory(config)
