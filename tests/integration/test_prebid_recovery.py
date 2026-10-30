"""A reconciled prebid attempt keeps the original event anchor and quotas."""
import json
import sqlite3

import pytest

from maestro.config import RunConfig
from maestro.storage import Ledger
from maestro.types import AgentState, IntegrityError


def test_reconciled_round_requires_exact_snapshot_and_is_one_shot(tmp_path):
    with Ledger(tmp_path/'ledger.sqlite') as ledger:
        ledger.create_episode('episode','ucb',RunConfig(),{'agent-1':AgentState('agent-1','parent',0.5)})
        snapshot={'states':{},'board':{},'candidate_pipeline':'prebid_v2'}
        ledger.open_round('episode',0,snapshot)
        ledger.set_round_phase('episode',0,'IMPLEMENTATION')
        with pytest.raises(IntegrityError,match='Recovery snapshot/status'):
            ledger.restart_reconciled_round('episode',0,snapshot,'episode-recovery-r0-a1')
        with ledger.transaction() as db:
            db.execute("UPDATE rounds SET status='RESTART_AUTHORIZED' WHERE episode_id='episode' AND round_index=0")
        with pytest.raises(IntegrityError,match='Recovery snapshot/status'):
            ledger.restart_reconciled_round('episode',0,{'states':{'changed':1}},'episode-recovery-r0-a1')
        ledger.restart_reconciled_round('episode',0,snapshot,'episode-recovery-r0-a1')
        with pytest.raises(IntegrityError,match='Recovery snapshot/status'):
            ledger.restart_reconciled_round('episode',0,snapshot,'episode-recovery-r0-a1')
        assert ledger.connection.execute("SELECT count(*) FROM events WHERE kind='ROUND_OPENED'").fetchone()[0]==1
        assert ledger.connection.execute("SELECT count(*) FROM events WHERE kind='ROUND_RESTARTED'").fetchone()[0]==1
        ledger.verify_event_chain()


def test_resume_reaches_only_explicitly_authorized_restart(tmp_path):
    from maestro.loop import ExperimentEngine
    config=RunConfig(rounds=2,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        normal=engine._round
        episode='reconciled-main'
        def interrupt(e,r,states,policy,*args,**kwargs):
            if r==0:
                return normal(e,r,states,policy,*args,**kwargs)
            engine.ledger.open_round(e,r,{'test':'interrupted'})
            engine.ledger.set_round_phase(e,r,'IMPLEMENTATION')
            raise RuntimeError('synthetic interruption before formal evaluation')
        engine._round=interrupt
        with pytest.raises(RuntimeError,match='synthetic interruption'):
            engine.run(warm['checkpoint'],'ucb',episode_id=episode)
        with pytest.raises(Exception,match='containment reconciliation'):
            engine.resume(episode)
        with engine.ledger.transaction() as db:
            db.execute("UPDATE rounds SET status='RESTART_AUTHORIZED' WHERE episode_id=? AND round_index=1",(episode,))
            engine.ledger.event('PREBID_RECOVERY_AUTHORIZED',
                {'request_namespace':f'{episode}-recovery-r1-a1'},episode,1)
        class Backend:
            request_episode_id=episode
            def isolate_recovered_workspaces(self,namespace):self.workspace_namespace=namespace
            def close(self):pass
        engine.backend=Backend()
        called=[]
        engine._round=lambda e,r,*args,**kwargs:called.append((e,r,engine.backend.request_episode_id))
        engine.resume(episode)
        assert called==[(episode,1,f'{episode}-recovery-r1-a1')]
        assert engine.backend.workspace_namespace==f'{episode}-recovery-r1-a1'


def test_workspace_collision_stops_preparation_instead_of_settling_empty_round(tmp_path):
    from maestro.loop import ExperimentEngine
    engine=ExperimentEngine(tmp_path,RunConfig(rounds=1))
    class Backend:
        def prepare_with_receipt(self,*args,**kwargs):raise FileExistsError('old workspace')
        def _usage(self,*args):return {'calls':0,'usage_complete':True}
        def close(self):pass
    engine.backend=Backend()
    state=AgentState('agent-1','parent',0.5)
    engine.ledger.create_episode('episode','warm',engine.config,{'agent-1':state})
    engine.ledger.open_round('episode',0,{'test':'collision'})
    try:
        with pytest.raises(Exception,match='workspace collision'):
            engine._prepare_agents('episode',0,{'agent-1':state},{})
        assert engine.ledger.connection.execute('SELECT status FROM episodes').fetchone()[0]=='INCOMPLETE'
        assert engine.ledger.connection.execute('SELECT status FROM rounds').fetchone()[0]=='PREPARATION_FAILED'
        assert not engine.ledger.connection.execute('SELECT * FROM settlements').fetchall()
        from maestro.reports import report
        assert report(tmp_path)['episodes'][0]['workspace_collision_rounds']==[0]
    finally:engine.close()


def test_recovery_workspace_namespace_preserves_old_files(tmp_path):
    from maestro.backends.coral.backend import CoralBackend
    from maestro.backends.coral.workspace import WorkspaceStore
    base=tmp_path/'workspaces'
    old=base/'episode'/'agent-1'
    old.mkdir(parents=True)
    (old/'evidence.txt').write_text('prior attempt')
    backend=object.__new__(CoralBackend)
    backend._agents={}
    backend._workspace_base=base
    backend.workspaces=WorkspaceStore(base/'episode',tmp_path/'cas')
    backend.isolate_recovered_workspaces('episode-recovery-r12-a1')
    assert backend.workspaces.root==base/'episode-recovery-r12-a1'
    assert (old/'evidence.txt').read_text()=='prior attempt'
    with pytest.raises(FileExistsError,match='already consumed'):
        backend.isolate_recovered_workspaces('episode')


def test_preparation_collision_recovery_checks_funded_snapshot_and_no_dispatch(tmp_path):
    from maestro.execution.prebid_recovery import (
        _collision_has_no_model_dispatch, _frozen_source_matches)
    db=sqlite3.connect(':memory:')
    db.execute('CREATE TABLE events(episode_id TEXT,round_index INTEGER,kind TEXT,payload TEXT)')
    db.execute('INSERT INTO events VALUES(?,?,?,?)',('episode',9,'MARKET_ROUNDS_EXTENDED',
        json.dumps({'checkpoint_round_index':9,'funding_per_agent':'1250.000000'})))
    checkpoint={'board':{'champion_score':.9},'states':{
        'agent-1':{'parent_hash':'parent','wealth':'100.000000'}}}
    snapshot={'board':checkpoint['board'],'states':{
        'agent-1':{'parent_hash':'parent','wealth':'1350.000000'}}}
    assert _frozen_source_matches(db,'episode',checkpoint,snapshot,10)
    changed=json.loads(json.dumps(snapshot));changed['states']['agent-1']['parent_hash']='other'
    assert not _frozen_source_matches(db,'episode',checkpoint,changed,10)
    db.execute('INSERT INTO events VALUES(?,?,?,?)',('episode',10,'PREPARATION_OUTCOME',
        json.dumps({'agent_id':'agent-1','status':'ABSTAIN','error_type':'FileExistsError',
                    'request_count_unknown':False,'usage':{'calls':0,'unresolved_requests':0}})))
    native=tmp_path/'requests.sqlite'
    with sqlite3.connect(native) as connection:
        for table in ('inference_reservations','deepseek_logical_requests','codex_phase_scopes'):
            connection.execute(f'CREATE TABLE {table}(account TEXT)')
    assert _collision_has_no_model_dispatch(db,native,'episode',10,checkpoint['states'])
    with sqlite3.connect(native) as connection:
        connection.execute('INSERT INTO inference_reservations VALUES(?)',('episode:10:agent-1:PREPARATION',))
    assert not _collision_has_no_model_dispatch(db,native,'episode',10,checkpoint['states'])


def test_completed_extension_isolates_old_agent_workspaces(tmp_path):
    from maestro.loop import ExperimentEngine
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        engine.run(warm['checkpoint'],'uniform',episode_id='extension-workspaces')
    with ExperimentEngine(tmp_path,config.with_market_rounds(2)) as engine:
        class Backend:
            def __init__(self):self.namespaces=[]
            def begin_episode(self,*args):pass
            def isolate_recovered_workspaces(self,namespace):self.namespaces.append(namespace)
            def close(self):pass
        backend=Backend()
        engine.backend=backend
        def interrupted(*args,**kwargs):
            assert backend.namespaces==['extension-workspaces-continuation-r1-to2']
            raise RuntimeError('stop after workspace isolation')
        engine._round=interrupted
        with pytest.raises(RuntimeError,match='stop after workspace isolation'):
            engine.resume('extension-workspaces')
