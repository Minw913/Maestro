from dataclasses import replace
from decimal import Decimal
import pytest
from maestro.config import RunConfig
from maestro.loop import ExperimentEngine
from maestro.types import MaestroError,EvaluationResult,AuthorizationError
from maestro.replay import verify_ledger


def test_result_before_settlement_crash_recovers_without_rerun(tmp_path):
    c=RunConfig(rounds=2,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,c) as engine:
        warm=engine.bootstrap()
        real_settle=engine._settle
        def crash(*args,**kwargs):
            raise RuntimeError('injected after all results before settlement')
        engine._settle=crash
        with pytest.raises(RuntimeError,match='injected'):
            engine.run(warm['checkpoint'],episode_id='crashed-main')
        assert engine.ledger.connection.execute("SELECT status FROM rounds WHERE episode_id='crashed-main'").fetchone()[0]=='EVALUATING'
        previous=len(engine.evaluator.calls)
        engine._settle=real_settle
        engine.resume('crashed-main')
        # Only the second round executes. The first results are durable.
        assert len(engine.evaluator.calls)==previous+2
    assert verify_ledger(tmp_path)['verified']


def test_unknown_job_is_not_relaunched_or_refunded(tmp_path):
    c=RunConfig(rounds=1,warm_opportunities_per_agent=1)
    with ExperimentEngine(tmp_path,c) as engine:
        warm=engine.bootstrap()
        evaluator=engine.evaluator
        def bad(ticket):
            raise RuntimeError('transport died after ambiguous launch')
        engine.dispatcher.evaluator=type('Ambiguous',(),{'evaluate':staticmethod(bad)})()
        with pytest.raises(RuntimeError):
            engine.run(warm['checkpoint'],episode_id='unknown-main')
        before=engine.ledger.connection.execute("SELECT count(*) FROM wallet_entries WHERE episode_id='unknown-main'").fetchone()[0]
        with pytest.raises(MaestroError,match='reconciled'):
            engine.resume('unknown-main')
        assert engine.ledger.connection.execute("SELECT count(*) FROM wallet_entries WHERE episode_id='unknown-main'").fetchone()[0]==before
        assert not engine.ledger.connection.execute("SELECT 1 FROM wallet_entries WHERE episode_id='unknown-main' AND kind='COMPENSATION'").fetchone()
