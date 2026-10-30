"""Offline scheduler timing/priority controls; no native agents or benchmark run."""
from dataclasses import replace
import json
import threading

import pytest

from maestro.config import RunConfig, load_config
from maestro.loop import ExperimentEngine
from maestro.llm import InferenceRequestUnknown
from maestro.replay import verify_ledger


def priority(engine, episode, round_index):
    with engine.ledger._lock:
        row=engine.ledger.connection.execute(
            'SELECT payload FROM priority_orders WHERE episode_id=? AND round_index=?',
            (episode,round_index)).fetchone()
    return json.loads(row[0])['order']


def test_replacement_starts_before_slow_peer_returns_and_shares_deadline(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        original=engine._check_frozen_proposal
        peer_started=threading.Event()
        replacement_started=threading.Event()
        failed_scope_reclaimed=threading.Event()
        peer_returned=threading.Event()
        lock=threading.Lock()
        deadlines={}
        active=[0]
        peak=[0]
        def generation(e,r,a,state,plan,purpose,deadline,candidate):
            order=priority(engine,e,r)
            with lock:
                deadlines[a]=deadline
                active[0]+=1
                peak[0]=max(peak[0],active[0])
            try:
                if a==order[0]:
                    assert peer_started.wait(timeout=2)
                    # A known terminal callback models reclamation completion;
                    # merely cancelling a Future is not used as proof.
                    failed_scope_reclaimed.set()
                    return None
                if a==order[1]:
                    peer_started.set()
                    assert replacement_started.wait(timeout=2), 'backfill waited for a slow peer'
                    result=original(e,r,a,state,plan,purpose,deadline,candidate)
                    peer_returned.set()
                    return result
                assert failed_scope_reclaimed.is_set()
                assert not peer_returned.is_set()
                replacement_started.set()
                return original(e,r,a,state,plan,purpose,deadline,candidate)
            finally:
                with lock:
                    active[0]-=1
        engine._check_frozen_proposal=generation
        engine.run(warm['checkpoint'],episode_id='event-backfill')
        order=priority(engine,'event-backfill',0)
        assert replacement_started.is_set() and peer_returned.is_set()
        assert peak[0]==config.slots
        assert set(deadlines)==set(order[:3])
        assert len(set(deadlines.values()))==1  # No full new window for replacement.
        rows=engine.ledger.connection.execute(
            "SELECT agent_id FROM eval_tickets WHERE episode_id='event-backfill'").fetchall()
        assert {r[0] for r in rows}==set(order[1:3])
        events=engine.ledger.connection.execute(
            "SELECT kind,payload FROM events WHERE episode_id='event-backfill' ORDER BY id").fetchall()
        released=next(i for i,row in enumerate(events)
            if row[0]=='HOLD_RELEASED' and json.loads(row[1])['agent_id']==order[0])
        invited=next(i for i,row in enumerate(events)
            if row[0]=='INVITED' and json.loads(row[1])['agent_id']==order[2])
        assert released<invited
    assert verify_ledger(tmp_path)['verified']


def test_faster_lower_priority_duplicate_cannot_win_by_completion_speed(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
    duplicate=replace(config,failures={'duplicate_candidate_rounds':[0]})
    with ExperimentEngine(tmp_path,duplicate) as engine:
        original=engine._check_frozen_proposal
        event=engine.ledger.event
        lower_observed=threading.Event()
        def record(kind,payload,*args,**kwargs):
            result=event(kind,payload,*args,**kwargs)
            if kind=='GENERATION_CALLBACK_COMPLETED':
                order=priority(engine,'priority-backfill',0)
                if payload['agent_id']==order[1]:
                    lower_observed.set()
            return result
        engine.ledger.event=record
        def generation(e,r,a,state,plan,purpose,deadline,candidate):
            order=priority(engine,e,r)
            if a==order[0]:
                assert lower_observed.wait(timeout=2), 'controller did not observe the fast peer'
            return original(e,r,a,state,plan,purpose,deadline,candidate)
        engine._check_frozen_proposal=generation
        engine.run(warm['checkpoint'],episode_id='priority-backfill')
        order=priority(engine,'priority-backfill',0)
        rows=engine.ledger.connection.execute(
            "SELECT agent_id FROM eval_tickets WHERE episode_id='priority-backfill'").fetchall()
        assert [r[0] for r in rows]==[order[0]]
    assert verify_ledger(tmp_path)['verified']


@pytest.mark.parametrize('error_kind',['inference','unreaped'])
def test_unknown_stops_immediately_without_waiting_for_or_replacing_peer(tmp_path,error_kind):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        release=threading.Event()
        returned=threading.Event()
        errors=[]
        def generation(e,r,a,*args):
            order=priority(engine,e,r)
            if a==order[0]:
                if error_kind=='inference':
                    raise InferenceRequestUnknown('synthetic unresolved provider execution',
                        request_id='request',scope_id='scope',context={})
                error=RuntimeError('synthetic process not confirmed stopped')
                error.local_quiescent=False
                raise error
            release.wait(timeout=5)
            return None
        engine._check_frozen_proposal=generation
        def run():
            try:
                engine.run(warm['checkpoint'],episode_id='unknown-backfill')
            except BaseException as error:
                errors.append(error)
            finally:
                returned.set()
        controller=threading.Thread(target=run)
        controller.start()
        try:
            assert returned.wait(timeout=2), 'UNKNOWN was hidden behind a slow peer'
            assert len(errors)==1
            if error_kind=='inference':
                assert isinstance(errors[0],InferenceRequestUnknown)
            else:
                assert getattr(errors[0],'local_quiescent',None) is False
            assert engine.ledger.connection.execute(
                "SELECT status FROM rounds WHERE episode_id='unknown-backfill'").fetchone()[0]=='GENERATION_UNKNOWN'
            assert engine.ledger.connection.execute(
                "SELECT count(*) FROM invitations WHERE episode_id='unknown-backfill'").fetchone()[0]==config.slots
            assert not engine.ledger.connection.execute(
                "SELECT 1 FROM holds WHERE episode_id='unknown-backfill' AND status!='HELD'").fetchone()
            assert not engine.ledger.connection.execute(
                "SELECT 1 FROM eval_tickets WHERE episode_id='unknown-backfill'").fetchone()
        finally:
            release.set()
            controller.join(timeout=2)
            assert not controller.is_alive()


def test_layered_windows_are_tunable_and_stage_specific(tmp_path):
    from pathlib import Path
    import shutil
    import yaml
    source=Path(__file__).resolve().parents[2]/'configs'
    target=tmp_path/'configs'
    shutil.copytree(source,target)
    entry=target/'experiments/pace_remote.yaml'
    warm=load_config(entry,stage='warm_start')
    market=load_config(entry,stage='market')
    assert warm.generation_window_seconds==market.generation_window_seconds==360
    old_hash=market.hash
    path=target/'benchmarks/pace/market.yaml'
    value=yaml.safe_load(path.read_text());value['generation_window_seconds']=450
    path.write_text(yaml.safe_dump(value))
    changed=load_config(entry,stage='market')
    assert changed.generation_window_seconds==450 and changed.hash!=old_hash
    assert load_config(entry,stage='warm_start').generation_window_seconds==360
    assert changed.generation_calls==market.generation_calls
    assert changed.runtime_repair_calls==market.runtime_repair_calls
    assert changed.generation_cpu_seconds==market.generation_cpu_seconds
