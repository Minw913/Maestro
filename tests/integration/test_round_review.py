"""Adversarial review cases for synchronized preparation and atomic settlement."""
from copy import deepcopy
from dataclasses import replace
import json
import threading
import time

import pytest

from maestro.config import RunConfig
from maestro.loop import ExperimentEngine
from maestro.replay import verify_ledger
from maestro.types import EvaluationResult, IntegrityError, MaestroError
from maestro.llm import InferenceRequestUnknown


def test_initial_generation_winners_start_together_before_any_formal_execution(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        original=engine._check_frozen_proposal
        rendezvous=threading.Barrier(config.slots)
        completed=[]
        lock=threading.Lock()

        def generation(*args):
            rendezvous.wait(timeout=2)
            candidate=original(*args)
            with lock:
                completed.append(args[2])
            return candidate

        engine._check_frozen_proposal=generation
        evaluate=engine.dispatcher.dispatch

        def dispatch(*args):
            assert len(completed)==config.slots
            return evaluate(*args)

        engine.dispatcher.dispatch=dispatch
        run=engine.run(warm['checkpoint'],'uniform')
        assert len(completed)==2
        assert len(engine.ledger.connection.execute(
            'SELECT * FROM eval_tickets WHERE episode_id=?',(run['episode_id'],)).fetchall())==2
    assert verify_ledger(tmp_path)['verified']


@pytest.mark.parametrize('corruption',['missing','duplicate','foreign','forged','plan'])
def test_settlement_rejects_incomplete_foreign_or_forged_results(tmp_path,corruption):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        settle=engine._settle
        foreign=engine.ledger.existing_result(engine.ledger.connection.execute(
            "SELECT job_id FROM eval_tickets WHERE round_index=-1").fetchone()[0])

        def corrupt(e,r,states,frozen,champion,plans,results,purpose):
            if corruption=='missing':
                results=results[:-1]
            elif corruption=='duplicate':
                results=[results[0],results[0]]
            elif corruption=='foreign':
                results=[foreign,*results[1:]]
            elif corruption=='plan':
                owner=engine.ledger.connection.execute('SELECT agent_id FROM eval_tickets WHERE job_id=?',(results[0].job_id,)).fetchone()[0]
                plans={**plans,owner:replace(plans[owner],change='substituted after admission')}
            else:
                results=[replace(results[0],score=.999999),*results[1:]]
            return settle(e,r,states,frozen,champion,plans,results,purpose)

        engine._settle=corrupt
        with pytest.raises(IntegrityError):
            engine.run(warm['checkpoint'],episode_id='corrupt-main')
        assert engine.ledger.connection.execute(
            "SELECT count(*) FROM settlements WHERE episode_id='corrupt-main'").fetchone()[0]==0
        engine._settle=settle
        engine.resume('corrupt-main')
    assert verify_ledger(tmp_path)['verified']


def test_settlement_rollback_does_not_mutate_caller_history_or_ucb(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        put_json=engine.artifacts.put_json
        observed={}
        settle=engine._settle

        def fail_result_artifact(value):
            if isinstance(value,EvaluationResult):
                raise OSError('injected artifact persistence failure')
            return put_json(value)

        def capture(*args):
            states=args[2]
            observed['states']=states
            observed['before']=deepcopy(states)
            return settle(*args)

        engine.artifacts.put_json=fail_result_artifact
        engine._settle=capture
        with pytest.raises(OSError,match='persistence failure'):
            engine.run(warm['checkpoint'],episode_id='atomic-main')
        assert observed['states']==observed['before']
        assert engine.ledger.connection.execute(
            "SELECT count(*) FROM settlements WHERE episode_id='atomic-main'").fetchone()[0]==0
        engine.artifacts.put_json=put_json
        engine._settle=settle
        engine.resume('atomic-main')
    assert verify_ledger(tmp_path)['verified']


def test_recovery_preserves_failed_precheck_refresh_requirement(tmp_path):
    config=RunConfig(rounds=2,warm_opportunities_per_agent=1,
        failures={'agent-1':'precheck','agent-2':'precheck'})
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        settle=engine._settle
        engine._settle=lambda *args: (_ for _ in ()).throw(RuntimeError('before settlement'))
        with pytest.raises(RuntimeError,match='before settlement'):
            engine.run(warm['checkpoint'],episode_id='refresh-main')
        failed=[row[0] for row in engine.ledger.connection.execute(
            "SELECT agent_id FROM invitations WHERE episode_id='refresh-main' AND status='FAILED'")]
        assert failed
        engine._settle=settle
        engine.resume('refresh-main')
        for agent in failed:
            submission=engine.ledger.connection.execute(
                "SELECT payload FROM round_submissions WHERE episode_id='refresh-main' AND round_index=1 AND agent_id=?",(agent,)).fetchone()
            assert not json.loads(submission[0])['reused']


def test_recovery_restores_last_main_private_memory_before_interrupted_settlement(tmp_path):
    class SemanticMemory:
        def __init__(self):
            self.memory={}
            self.exports=[]
        def begin_episode(self,*args):
            self.memory={}
        def restore_warm_memory(self,manifest):
            self.memory={'phase':'warm'}
        def restore_checkpoint_memory(self,manifest):
            self.memory=deepcopy(manifest)
        def export_checkpoint_memory(self):
            self.exports.append(deepcopy(self.memory))
            return self.memory
        def close(self):
            pass

    config=RunConfig(rounds=2,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        memory=SemanticMemory()
        engine.backend=memory
        settle=engine._settle

        def interrupted(e,r,*args):
            if r==0:
                memory.memory={'phase':'main','own_lesson':'retained after the warm prefix'}
                return settle(e,r,*args)
            raise RuntimeError('interrupted second settlement')

        engine._settle=interrupted
        with pytest.raises(RuntimeError,match='second settlement'):
            engine.run(warm['checkpoint'],episode_id='memory-main')
        engine._settle=settle
        engine.resume('memory-main')
        assert memory.exports[-1]=={'phase':'main','own_lesson':'retained after the warm prefix'}


@pytest.mark.parametrize('corruption',['priority','ucb'])
def test_replay_checks_policy_inputs_and_materialized_credit_statistics(tmp_path,corruption):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        run=engine.run(warm['checkpoint'],'uniform')
        episode=run['episode_id']
        if corruption=='priority':
            row=engine.ledger.connection.execute('SELECT payload FROM priority_orders WHERE episode_id=?',(episode,)).fetchone()
            data=json.loads(row[0])
            data['order'].reverse()
            engine.ledger.connection.execute('UPDATE priority_orders SET payload=? WHERE episode_id=?',(json.dumps(data),episode))
        else:
            row=engine.ledger.connection.execute('SELECT agent_id,state_json FROM agents WHERE episode_id=? LIMIT 1',(episode,)).fetchone()
            state=json.loads(row['state_json'])
            state['ucb_total']='999999'
            engine.ledger.connection.execute('UPDATE agents SET state_json=? WHERE episode_id=? AND agent_id=?',(json.dumps(state),episode,row['agent_id']))
    with pytest.raises(IntegrityError,match='priority|UCB'):
        verify_ledger(tmp_path)


def test_unresolved_generation_never_advances_to_formal_admission(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()
        engine.config=replace(config,generation_window_seconds=.05)
        # Keep warm/main equality except the deliberately tiny runtime window.
        envelope=json.loads(open(warm['checkpoint'],encoding='utf-8').read())
        from maestro.types import stable_hash
        envelope['payload']['config']=engine.config.to_dict()
        envelope['payload']['config_hash']=engine.config.hash
        envelope['artifact_hash']=stable_hash(envelope['payload'])
        checkpoint=tmp_path/'short-window.json'
        checkpoint.write_text(json.dumps(envelope),encoding='utf-8')
        release=threading.Event()
        completed=threading.Event()
        lock=threading.Lock()
        count=[0]

        def unbounded(*args):
            release.wait(timeout=2)
            with lock:
                count[0]+=1
                if count[0]==config.slots:
                    completed.set()
            return None

        engine._check_frozen_proposal=unbounded
        try:
            with pytest.raises(MaestroError,match='containment reconciliation'):
                engine.run(checkpoint,episode_id='unresolved-main')
            assert engine.ledger.connection.execute(
                "SELECT status FROM rounds WHERE episode_id='unresolved-main'").fetchone()[0]=='GENERATION_UNKNOWN'
            assert not engine.ledger.connection.execute(
                "SELECT 1 FROM eval_tickets WHERE episode_id='unresolved-main'").fetchone()
            assert not engine.ledger.connection.execute(
                "SELECT 1 FROM settlements WHERE episode_id='unresolved-main'").fetchone()
        finally:
            release.set()
            assert completed.wait(timeout=2)


def test_unknown_generation_inference_keeps_holds_and_stops_before_backfill(tmp_path):
    config=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,config) as engine:
        warm=engine.bootstrap()

        def unresolved(*args):
            raise InferenceRequestUnknown('provider processing state cannot be established',
                request_id='request',scope_id='scope',context={})

        engine._check_frozen_proposal=unresolved
        with pytest.raises(InferenceRequestUnknown):
            engine.run(warm['checkpoint'],episode_id='unknown-inference-main')
        assert engine.ledger.connection.execute(
            "SELECT status FROM rounds WHERE episode_id='unknown-inference-main'").fetchone()[0]=='GENERATION_UNKNOWN'
        assert engine.ledger.connection.execute(
            "SELECT count(*) FROM invitations WHERE episode_id='unknown-inference-main'").fetchone()[0]==config.slots
        assert not engine.ledger.connection.execute(
            "SELECT 1 FROM holds WHERE episode_id='unknown-inference-main' AND status!='HELD'").fetchone()
        assert not engine.ledger.connection.execute(
            "SELECT 1 FROM eval_tickets WHERE episode_id='unknown-inference-main'").fetchone()


def test_live_generation_unknown_receipt_is_recorded_without_inventing_zero_requests(tmp_path):
    from maestro.types import AgentState
    from maestro.agents.planner import mock_plan

    class UnknownInference:
        supports_runtime_only_repair=True
        def generate(self,*args,**kwargs):
            self.calls+=1
            self.last_generation_usage={'calls':None,'input_tokens':None,'output_tokens':None,
                'cpu_seconds':None,'wall_seconds':None,'usage_complete':False,'session_turns':1}
            raise InferenceRequestUnknown('remote inference is unresolved',
                request_id='request',scope_id='scope',context={})
        def close(self):
            pass

    with ExperimentEngine(tmp_path,RunConfig(rounds=1,warm_opportunities_per_agent=1)) as engine:
        backend=UnknownInference()
        backend.calls=0
        engine.backend=backend
        engine.config=replace(engine.config,execution_mode='real')
        state=AgentState('a','parent',.5)
        plan=mock_plan(state,0,1)
        with pytest.raises(InferenceRequestUnknown):
            engine._implement_proposal('unit',0,'a',state,plan,'MAIN',time.monotonic()+5)
        assert backend.calls==1
        usage=engine.ledger.connection.execute("SELECT * FROM resource_usage WHERE phase='generation'").fetchone()
        details=json.loads(usage['payload'])
        assert usage['llm_calls']==0
        assert details['request_count_unknown'] is True
        assert details['usage']['session_turns']==1
        assert details['usage_complete'] is False


@pytest.mark.parametrize('drain', [True, False])
def test_close_preserves_authority_until_late_generation_cleanup_finishes(tmp_path, drain):
    config = RunConfig(rounds=1, warm_opportunities_per_agent=1)
    engine = ExperimentEngine(tmp_path, config)
    release = threading.Event()
    closed = threading.Event()
    close_errors = []
    closer = None
    try:
        warm = engine.bootstrap()
        engine.config = replace(config, generation_window_seconds=.05)
        envelope = json.loads(open(warm['checkpoint'], encoding='utf-8').read())
        from maestro.types import stable_hash
        envelope['payload']['config'] = engine.config.to_dict()
        envelope['payload']['config_hash'] = engine.config.hash
        envelope['artifact_hash'] = stable_hash(envelope['payload'])
        checkpoint = tmp_path/'cleanup-window.json'
        checkpoint.write_text(json.dumps(envelope), encoding='utf-8')
        generate = engine._check_frozen_proposal

        def late_cleanup(*args):
            assert release.wait(timeout=3)
            # The actual callback still creates its budget and writes the late
            # failure event, even though the round has already been isolated.
            return generate(*args)

        engine._check_frozen_proposal = late_cleanup
        with pytest.raises(MaestroError, match='containment reconciliation'):
            engine.run(checkpoint, episode_id='cleanup-main')
        callbacks = tuple(engine._generation_futures)
        assert len(callbacks) == config.slots
        if drain:
            def close():
                try:
                    engine.close(timeout=2)
                except BaseException as error:
                    close_errors.append(error)
                finally:
                    closed.set()
            closer = threading.Thread(target=close)
            closer.start()
            assert not closed.wait(timeout=.03)
        else:
            with pytest.raises(MaestroError, match='preserve authority connections'):
                engine.close(timeout=0)
            assert engine.ledger.connection.execute('SELECT 1').fetchone()[0] == 1
            assert engine.budgets.connection.execute('SELECT 1').fetchone()[0] == 1
        release.set()
        for callback in callbacks:
            assert callback.result(timeout=2) is None
        if closer:
            closer.join(timeout=2)
            assert closed.is_set() and not close_errors
        else:
            engine.close()
        import sqlite3
        with sqlite3.connect(engine.ledger.path) as db:
            assert db.execute("SELECT status FROM rounds WHERE episode_id='cleanup-main'").fetchone()[0] == 'GENERATION_UNKNOWN'
            assert db.execute("SELECT count(*) FROM eval_tickets WHERE episode_id='cleanup-main'").fetchone()[0] == 0
    finally:
        release.set()
        if closer:
            closer.join(timeout=3)
        engine.close()


def test_grant_expiring_before_initial_verification_never_dispatches(tmp_path):
    from maestro.agents.planner import mock_plan
    from maestro.execution.grants import GrantAuthority, GrantError
    from maestro.types import AgentState

    class SlowAuthority(GrantAuthority):
        entered = False
        issued = None
        def issue(self, **kwargs):
            self.issued = super().issue(**kwargs)
            return self.issued
        def verify(self, *args, **kwargs):
            self.entered = True
            # Deterministically model authority persistence/host scheduling
            # crossing the deadline before the first provider dispatch.
            time.sleep(.3)
            return super().verify(*args, **kwargs)

    with ExperimentEngine(tmp_path, RunConfig()) as engine:
        engine.grants = SlowAuthority()
        state = AgentState('a', 'parent', .5)
        plan = mock_plan(state, 0, 1)
        assert engine._implement_proposal('expired-grant', 0, 'a', state, plan, 'MAIN', time.monotonic()+.2) is None
        assert engine.grants.entered
        assert engine.budgets.snapshot('expired-grant:0:a:generation')['calls'] == 0
        assert not engine.ledger.connection.execute('SELECT 1 FROM candidates').fetchone()
        with pytest.raises(GrantError):
            GrantAuthority.verify(engine.grants, engine.grants.issued, agent_id='a',
                plan_version_id=plan.version_id, parent_hash=plan.parent_hash, epoch=0)
