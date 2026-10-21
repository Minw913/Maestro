"""Offline controller scheduling; injected callbacks, no experiment/model run."""
from dataclasses import replace
import json
import threading
import time
from types import SimpleNamespace

import pytest

from maestro.config import RunConfig
from maestro.loop import ExperimentEngine
from maestro.types import AgentState, MaestroError


def setup(tmp_path, backend, window=1):
    engine=ExperimentEngine(tmp_path,RunConfig(rounds=1,preparation_window_seconds=window))
    states={f'a{i}':AgentState(f'a{i}','parent',.5) for i in range(4)}
    engine.ledger.create_episode('e','warm',engine.config,states)
    engine.ledger.open_round('e',0,{'purpose':'WARM'})
    engine.backend=backend
    return engine,states


class Backend:
    def __init__(self):
        self.barrier=threading.Barrier(4)
        self.calls=[]
    def prepare_with_receipt(self,state,round_index,context,*,deadline):
        self.calls.append((state.agent_id,deadline,time.monotonic()))
        self.barrier.wait(timeout=1)
        evidence={'summary':state.agent_id,'accepted_monotonic':time.monotonic()}
        return evidence,{'calls':2,'usage_complete':True}
    def close(self):pass
    def _usage(self,scope):return {'calls':0}


def test_four_agents_share_preparation_deadline_and_sync_before_planning(tmp_path):
    backend=Backend();engine,states=setup(tmp_path,backend)
    try:
        accepted,failed=engine._prepare_agents('e',0,states,{})
        assert set(accepted)==set(states) and not failed
        assert len({item[1] for item in backend.calls})==1
        assert engine.ledger.connection.execute("SELECT status FROM rounds").fetchone()[0]=='PLANNING'
        rows=engine.ledger.connection.execute("SELECT llm_calls FROM resource_usage WHERE phase='preparation'").fetchall()
        assert len(rows)==4 and sum(row[0] for row in rows)==8
    finally:engine.close()


def test_unknown_blocks_next_phase_without_retries(tmp_path):
    class Unknown(RuntimeError):inflight_unknown=True
    class Failed(Backend):
        def prepare_with_receipt(self,*args,**kwargs):raise Unknown('offline unknown')
    backend=Failed();engine,states=setup(tmp_path,backend)
    try:
        with pytest.raises(MaestroError,match='reconcile'):engine._prepare_agents('e',0,states,{})
        assert engine.ledger.connection.execute('SELECT status FROM rounds').fetchone()[0]=='PREPARATION_UNKNOWN'
        assert engine.ledger.connection.execute('SELECT status FROM episodes').fetchone()[0]=='INCOMPLETE'
        assert not engine.ledger.connection.execute('SELECT * FROM edit_grants').fetchall()
    finally:engine.close()


def test_pending_callback_is_unknown_and_authority_is_retained(tmp_path):
    release=threading.Event()
    class Slow(Backend):
        def prepare_with_receipt(self,*args,**kwargs):
            release.wait(2)
            return {'accepted_monotonic':time.monotonic()}, {'calls':1}
    engine,states=setup(tmp_path,Slow(),window=.02)
    try:
        with pytest.raises(MaestroError,match='reconcile'):engine._prepare_agents('e',0,states,{})
        with pytest.raises(MaestroError,match='cleanup unresolved'):engine.close(timeout=0)
        assert engine.ledger.connection.execute('SELECT status FROM rounds').fetchone()[0]=='PREPARATION_UNKNOWN'
    finally:
        release.set()
        engine.close()


def provider_evidence(backend,account,known=True):
    import sqlite3
    db=sqlite3.connect(':memory:')
    db.execute('CREATE TABLE inference_reservations(reservation_id TEXT, account TEXT)')
    db.execute('CREATE TABLE codex_gateway_transport(reservation_id TEXT,http_status INTEGER,error_type TEXT,error_code TEXT,known_rejected INTEGER)')
    db.execute('INSERT INTO inference_reservations VALUES(?,?)',('r',account))
    db.execute('INSERT INTO codex_gateway_transport VALUES(?,?,?,?,?)',('r',429,'rate_limit_error','turn_rate_limit_exceeded',int(known)))
    backend.budget_store=SimpleNamespace(connection=db,_lock=threading.RLock())
    return db


def test_provider_rate_limit_stops_before_planning_or_later_waves(tmp_path):
    class Rejected(Backend):
        def prepare_with_receipt(self,*args,**kwargs):raise RuntimeError('known provider rejection')
    backend=Rejected();engine,states=setup(tmp_path,backend)
    db=provider_evidence(backend,'e:0:a0:PREPARATION')
    try:
        with pytest.raises(MaestroError,match='Provider rejected'):engine._prepare_agents('e',0,states,{})
        assert engine.ledger.connection.execute('SELECT status FROM rounds').fetchone()[0]=='INFRASTRUCTURE_FAILURE'
        assert engine.ledger.connection.execute('SELECT status FROM episodes').fetchone()[0]=='STOPPED_INFRASTRUCTURE'
        assert not engine.ledger.connection.execute('SELECT * FROM edit_grants').fetchall()
    finally:engine.close();db.close()


@pytest.mark.parametrize('account,known',[('historical:0:a0:PREPARATION',True),('e:0:a0:PREPARATION',False)])
def test_old_quarantine_and_unproven_rejection_do_not_trigger_known_rejection_stop(tmp_path,account,known):
    backend=Backend();engine,states=setup(tmp_path,backend);db=provider_evidence(backend,account,known)
    try:
        engine._assert_provider_available('e',0)
        assert engine.ledger.connection.execute('SELECT status FROM episodes').fetchone()[0]!='STOPPED_INFRASTRUCTURE'
    finally:engine.close();db.close()
