"""Offline retirement gates; no model, Docker job or historical run is modified."""
from dataclasses import replace
import json
from pathlib import Path

import pytest

from maestro.cli import main
from maestro.config import RunConfig, load_config
from maestro.execution.pipeline import pipeline_version, require_prebid
from maestro.loop import ExperimentEngine
from maestro.replay import verify_ledger
from maestro.storage import ArtifactStore, Ledger
from maestro.types import MaestroError, canonical_json, stable_hash


def legacy_wire(explicit):
    raw=RunConfig().to_dict()
    raw['coral']={'candidate_pipeline':'postbid_v1'} if explicit else {}
    return raw


def legacy_root(root,raw):
    with Ledger(root/'authority/ledger.sqlite') as ledger:
        ledger.create_episode('old','warm',RunConfig(),{})
        with ledger.transaction() as db:
            db.execute('UPDATE episodes SET config_json=?,config_hash=? WHERE id=?',
                       (canonical_json(raw),stable_hash(raw),'old'))
    return root/'authority/ledger.sqlite'


@pytest.mark.parametrize('explicit',[False,True])
def test_retired_constructor_rejected_before_directory_or_backend(tmp_path,monkeypatch,explicit):
    raw=legacy_wire(explicit)
    assert pipeline_version(raw)=='postbid_v1'
    config=RunConfig.from_dict(raw)
    def forbidden(*args):pytest.fail('retired configuration reached a backend')
    monkeypatch.setattr(ExperimentEngine,'_prepare_live',forbidden)
    root=tmp_path/'new'
    with pytest.raises(MaestroError,match='postbid_v1 execution is retired'):
        ExperimentEngine(root,replace(config,execution_mode='real'))
    assert not root.exists()


@pytest.mark.parametrize('explicit',[False,True])
@pytest.mark.parametrize('command',['bootstrap','run','resume'])
def test_cli_cannot_relabel_or_reuse_archive(tmp_path,monkeypatch,explicit,command):
    root=tmp_path/'old';raw=legacy_wire(explicit);ledger=legacy_root(root,raw)
    before=ledger.read_bytes()
    checkpoint=tmp_path/'old-warm.json'
    checkpoint.write_text(json.dumps({'payload':{'config':raw}}))
    override=tmp_path/'new.yaml';override.write_text('execution_mode: real\ncoral:\n  candidate_pipeline: prebid_v2\n')
    def forbidden(*args):pytest.fail('legacy source reached model/backend initialization')
    monkeypatch.setattr(ExperimentEngine,'_prepare_live',forbidden)
    argv=['--root',str(root),command,'--config',str(override)]
    if command=='run':argv+=['--bootstrap',str(checkpoint)]
    if command=='resume':argv+=['--episode','old']
    assert main(argv)==2
    assert ledger.read_bytes()==before
    assert not (root/'authority/config.resolved.json').exists()
    assert not (root/'authority/cas').exists()


@pytest.mark.parametrize('explicit',[False,True])
def test_direct_run_and_resume_reject_frozen_legacy_identity(tmp_path,explicit):
    raw=legacy_wire(explicit)
    checkpoint=tmp_path/'old.json';checkpoint.write_text(json.dumps({'payload':{'config':raw}}))
    with ExperimentEngine(tmp_path/'fresh',RunConfig()) as engine:
        before=(engine.root/'authority/config.resolved.json').read_bytes()
        with pytest.raises(MaestroError,match='postbid_v1 execution is retired'):engine.run(checkpoint)
        engine.ledger.create_episode('old','first_price_naive',engine.config,{})
        with engine.ledger.transaction() as db:
            db.execute('UPDATE episodes SET config_json=?,config_hash=? WHERE id=?',
                       (canonical_json(raw),stable_hash(raw),'old'))
        with pytest.raises(MaestroError,match='postbid_v1 execution is retired'):engine.resume('old')
        assert not engine.ledger.connection.execute('SELECT * FROM resource_usage').fetchone()
        assert not engine.ledger.connection.execute('SELECT * FROM jobs').fetchone()
        assert (engine.root/'authority/config.resolved.json').read_bytes()==before


def test_new_default_and_documents_only_select_prebid(tmp_path):
    require_prebid(RunConfig())
    path=tmp_path/'new.yaml';path.write_text('execution_mode: mock\n')
    require_prebid(load_config(path))
    path.write_text('coral:\n  candidate_pipeline: postbid_v1\n')
    with pytest.raises(ValueError,match='retired'):load_config(path)
    assert not hasattr(ExperimentEngine,'_generate')


def test_mock_uses_implementation_barrier_and_keeps_losers_unscored(tmp_path):
    with ExperimentEngine(tmp_path,RunConfig(rounds=1,warm_opportunities_per_agent=1)) as engine:
        warm=engine.bootstrap();result=engine.run(warm['checkpoint'],'uniform')
        e=result['episode_id'];db=engine.ledger.connection
        events=[(r['kind'],json.loads(r['payload'])) for r in db.execute(
            'SELECT kind,payload FROM events WHERE episode_id=? ORDER BY id',(e,))]
        frozen=[i for i,(kind,_) in enumerate(events) if kind=='PROPOSAL_FROZEN']
        bids=[i for i,(kind,_) in enumerate(events) if kind=='PLAN_SUBMITTED']
        invited=[i for i,(kind,_) in enumerate(events) if kind=='INVITED']
        assert len(frozen)==len(bids)==4 and len(invited)==2
        assert max(frozen)<min(bids)<min(invited)
        assert db.execute('SELECT count(*) FROM prechecks WHERE episode_id=?',(e,)).fetchone()[0]==2
        assert db.execute('SELECT count(*) FROM eval_tickets WHERE episode_id=?',(e,)).fetchone()[0]==2
        assert db.execute("SELECT count(*) FROM settlements WHERE episode_id=? AND reward='0'",(e,)).fetchone()[0]>=2
        usage=db.execute("SELECT payload FROM resource_usage WHERE episode_id=? AND phase='generation'",(e,)).fetchall()
        assert len(usage)==4 and all(json.loads(r[0])['simulated'] for r in usage)
    before=(tmp_path/'authority/ledger.sqlite').read_bytes()
    assert verify_ledger(tmp_path)['verified']
    assert (tmp_path/'authority/ledger.sqlite').read_bytes()==before


def test_read_only_stores_cannot_create_missing_authority(tmp_path):
    with pytest.raises(Exception):Ledger(tmp_path/'missing/ledger.sqlite',read_only=True)
    assert not (tmp_path/'missing').exists()
    store=ArtifactStore(tmp_path/'absent',read_only=True)
    with pytest.raises(PermissionError):store.put(b'no writes')
    assert not store.root.exists()


@pytest.mark.parametrize('provider',['codex','deepseek'])
@pytest.mark.parametrize('explicit',[False,True])
def test_direct_factory_rejects_retired_config_before_any_prerequisite(tmp_path,provider,explicit):
    from maestro.backends.codex_session.autonomous_backend import strict_factory
    from maestro.backends.coral.deepseek_backend import native_factory
    factory=strict_factory if provider=='codex' else native_factory
    config=replace(RunConfig.from_dict(legacy_wire(explicit)),execution_mode='real')
    root=tmp_path/'forbidden'
    with pytest.raises(MaestroError,match='postbid_v1 execution is retired'):
        factory(config,root)
    assert not root.exists()


@pytest.mark.parametrize('provider',['codex','deepseek'])
def test_direct_factory_cannot_initialize_inside_retired_root(tmp_path,provider):
    from maestro.backends.codex_session.autonomous_backend import strict_factory
    from maestro.backends.coral.deepseek_backend import native_factory
    factory=strict_factory if provider=='codex' else native_factory
    ledger=legacy_root(tmp_path,legacy_wire(False));before=ledger.read_bytes()
    with pytest.raises(MaestroError,match='postbid_v1 execution is retired'):
        factory(RunConfig(execution_mode='real'),tmp_path)
    assert ledger.read_bytes()==before
    assert not (tmp_path/'authority/cas').exists()
