from dataclasses import replace
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.config import RunConfig
from maestro.execution.dispatcher import Dispatcher
from maestro.execution.sandbox import RunReceipt
from maestro.storage import ArtifactStore, Ledger
from maestro.tasks.pace.heldout import evaluate_final
from maestro.tasks.pace.manifest import EvaluationContract, ManifestError, load_panel
from maestro.tasks.pace.runner import PaceEvaluator
from maestro.types import AgentState, MaestroError, canonical_json


class Runner:
    def __init__(self):
        self.calls = 0

    def run(self, request, limits):
        self.calls += 1
        return RunReceipt(0, "1\n1\n", "", 0.1, 0.2)


def write_panel(root, name, graph, split):
    path = root / f"{name}.gr"
    path.write_bytes(graph.encode())
    manifest = root / f"{name}.json"
    manifest.write_text(json.dumps({"schema_version": 1, "panel_id": name,
        "split": split, "instances": [{"instance_id": name, "path": path.name,
            "sha256": hashlib.sha256(graph.encode()).hexdigest(), "reference_size": 1}]}))
    return manifest


@pytest.fixture
def engine(tmp_path):
    train_path = write_panel(tmp_path, "train", "p ds 3 2\n1 2\n2 3\n", "training")
    panel = load_panel(train_path)
    contract = EvaluationContract(instances=1)
    runner = Runner()
    evaluator = PaceEvaluator(panel, contract, runner, simulate_containment=True)
    candidate_path = tmp_path / "candidate"
    candidate_path.mkdir()
    (candidate_path / "solver.py").write_text("print('1\\n1')\n")
    candidate_hash = candidate_tree_hash(candidate_path)
    config = RunConfig(execution_mode="real", rounds=1, evaluation_instance_count=1)
    artifacts = ArtifactStore(tmp_path / "authority/cas")
    ledger = Ledger(tmp_path / "authority/ledger.sqlite")
    state = AgentState("agent-1", candidate_hash, 0.5)
    ledger.create_episode("source", "first_price_naive", config, {state.agent_id: state})
    ledger.record_candidate(candidate_hash, {"candidate_hash": candidate_hash, "candidate_path": str(candidate_path)})
    ledger.open_round("source", 0, {"test_fixture": True})
    ledger.set_round_phase("source", 0, "SETTLED")
    board = {"champion_hash": candidate_hash, "champion_score": 0.5,
             "panel_hash": panel.panel_hash, "contract_hash": contract.contract_hash}
    checkpoint = {"board": board, "states": {state.agent_id: state}, "round_index": 0}
    digest = artifacts.put_json(checkpoint)
    with ledger.transaction() as db:
        db.execute("INSERT INTO checkpoints VALUES(?,?,?,?)", ("source", 0, digest, canonical_json(checkpoint)))
        db.execute("UPDATE episodes SET status='COMPLETED' WHERE id='source'")
    result = SimpleNamespace(config=config, root=tmp_path, ledger=ledger, artifacts=artifacts,
        _live_panel=panel, _live_contract=contract, backend=SimpleNamespace(candidate_runner=runner),
        dispatcher=Dispatcher(ledger, evaluator))
    yield result
    ledger.close()


def test_final_heldout_has_separate_zero_payment_authority_and_idempotency(engine):
    manifest = write_panel(engine.root, "heldout", "p ds 4 3\n1 2\n1 3\n1 4\n", "heldout")
    before = engine.ledger.connection.execute("SELECT state_json FROM agents WHERE episode_id='source'").fetchone()[0]
    first = evaluate_final(engine, "source", manifest)
    second = evaluate_final(engine, "source", manifest)
    assert first == second
    assert first["score"] == 1 and first["valid"]
    assert first["purpose"] == "HELDOUT"
    assert first["payment"] == first["reward"] == "0"
    assert first["execution_kind"] == "simulated_containment"
    assert engine.backend.candidate_runner.calls == 1
    assert engine.ledger.ticket(first["job_id"] + ":ticket")["purpose"] == "HELDOUT"
    db = engine.ledger.connection
    assert db.execute("SELECT state_json FROM agents WHERE episode_id='source'").fetchone()[0] == before
    assert db.execute("SELECT count(*) FROM settlements WHERE episode_id=?", (first["heldout_episode_id"],)).fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM public_board_versions WHERE episode_id=?", (first["heldout_episode_id"],)).fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM resource_usage WHERE phase='HELDOUT'").fetchone()[0] == 1
    assert not (engine.root / "agents").exists()
    assert (engine.root / "authority/heldout" / (first["heldout_episode_id"] + ".json")).is_file()
    assert engine.ledger.verify_event_chain() > 0


def test_final_heldout_rejects_unfinished_source(engine):
    manifest = write_panel(engine.root, "heldout", "p ds 4 3\n1 2\n1 3\n1 4\n", "heldout")
    engine.ledger.connection.execute("UPDATE episodes SET status='RUNNING' WHERE id='source'")
    with pytest.raises(MaestroError, match="completed"):
        evaluate_final(engine, "source", manifest)
    assert engine.backend.candidate_runner.calls == 0


def test_final_heldout_detaches_layered_market_without_changing_source(engine):
    from maestro.config import STAGE_FIELDS
    manifest = write_panel(engine.root, "heldout", "p ds 4 3\n1 2\n1 3\n1 4\n", "heldout")
    stage = {name: getattr(engine.config, name) for name in STAGE_FIELDS}
    layered = replace(engine.config, experiment_stage="market",
        stage_configs={"warm_start": dict(stage), "market": dict(stage)},
        config_sources={"experiment": {"path": "frozen-experiment.yaml", "sha256": "a" * 64}})
    assert layered.slots == 2
    with engine.ledger.transaction() as db:
        db.execute("UPDATE episodes SET config_json=?,config_hash=? WHERE id='source'",
                       (canonical_json(layered.to_dict()), layered.hash))
    engine.config = layered
    result = evaluate_final(engine, "source", manifest)
    db = engine.ledger.connection
    heldout = json.loads(db.execute("SELECT config_json FROM episodes WHERE id=?",
                                  (result["heldout_episode_id"],)).fetchone()[0])
    assert heldout["n_agents"] == heldout["slots"] == heldout["rounds"] == 1
    assert heldout["experiment_stage"] is None
    assert heldout["stage_configs"] == heldout["config_sources"] == {}
    source = db.execute("SELECT config_json,config_hash FROM episodes WHERE id='source'").fetchone()
    assert source[0] == canonical_json(layered.to_dict()) and source[1] == layered.hash
    assert engine.config == layered and result["valid"]


def test_final_heldout_rejects_training_overlap(engine):
    manifest = write_panel(engine.root, "heldout", "p ds 3 2\n1 2\n2 3\n", "heldout")
    with pytest.raises(ManifestError, match="disjoint"):
        evaluate_final(engine, "source", manifest)
    assert engine.backend.candidate_runner.calls == 0


def test_final_heldout_rejects_simulated_source(engine):
    manifest = write_panel(engine.root, "heldout", "p ds 4 3\n1 2\n1 3\n1 4\n", "heldout")
    engine.ledger.connection.execute("UPDATE episodes SET execution_mode='mock' WHERE id='source'")
    with pytest.raises(MaestroError, match="simulated"):
        evaluate_final(engine, "source", manifest)


def test_final_heldout_rejects_modified_champion(engine):
    manifest = write_panel(engine.root, "heldout", "p ds 4 3\n1 2\n1 3\n1 4\n", "heldout")
    (engine.root / "candidate/solver.py").write_text("modified\n")
    with pytest.raises(Exception, match="changed"):
        evaluate_final(engine, "source", manifest)
    assert engine.backend.candidate_runner.calls == 0


def test_final_heldout_does_not_relaunch_unknown_job(engine):
    manifest = write_panel(engine.root, "heldout", "p ds 4 3\n1 2\n1 3\n1 4\n", "heldout")
    def lost(*_):
        engine.backend.candidate_runner.calls += 1
        raise RuntimeError("simulated lost execution")
    engine.backend.candidate_runner.run = lost
    with pytest.raises(RuntimeError, match="lost"):
        evaluate_final(engine, "source", manifest)
    with pytest.raises(Exception, match="used|revoked|stale"):
        evaluate_final(engine, "source", manifest)
    assert engine.backend.candidate_runner.calls == 1


@pytest.mark.parametrize('explicit',[False,True])
def test_retired_source_cli_final_eval_never_constructs_search_backend(engine,monkeypatch,explicit):
    """Simulated containment fixture only; validates the evaluation-only route."""
    from dataclasses import asdict
    from maestro.cli import main
    from maestro.types import stable_hash
    import maestro.tasks.pace.heldout as heldout_module
    import maestro.execution.sandbox as sandbox
    original=engine.ledger.connection.execute("SELECT config_json FROM episodes WHERE id='source'").fetchone()[0]
    raw=json.loads(original)
    raw.update(bid_cap='100',pace={'panel_manifest':str(engine.root/'train.json'),
                                'contract':asdict(engine._live_contract)})
    raw['coral']={'candidate_pipeline':'postbid_v1'} if explicit else {}
    before=canonical_json(raw);digest=stable_hash(raw)
    with engine.ledger.transaction() as db:
        db.execute("UPDATE episodes SET config_json=?,config_hash=? WHERE id='source'",(before,digest))
    # Existing helper fixture stores CAS one level higher; the CLI uses the
    # production path. Copy the same immutable checkpoint, preserving its hash.
    checkpoint=engine.ledger.connection.execute("SELECT payload FROM checkpoints WHERE episode_id='source'").fetchone()[0]
    ArtifactStore(engine.root/'authority/cas/sha256').put_json(json.loads(checkpoint))
    manifest=write_panel(engine.root,'heldout','p ds 4 3\n1 2\n1 3\n1 4\n','heldout')
    def forbidden(*args,**kwargs):pytest.fail('evaluation-only route initialized a search/backend')
    monkeypatch.setattr('maestro.loop.ExperimentEngine',forbidden)
    monkeypatch.setattr(sandbox,'DockerRunner',lambda:engine.backend.candidate_runner)
    # Allow this one-instance offline fixture; production still requires the
    # unchanged standard 20-instance training contract and physical containment.
    monkeypatch.setattr(EvaluationContract,'standard',classmethod(lambda cls,**kw:cls(**kw)))
    class SimulatedEvaluator(PaceEvaluator):
        def __init__(self,*args,**kwargs):
            kwargs['simulate_containment']=True
            super().__init__(*args,**kwargs)
    monkeypatch.setattr(heldout_module,'PaceEvaluator',SimulatedEvaluator)
    assert main(['--root',str(engine.root),'final-eval','--episode','source','--manifest',str(manifest)])==0
    assert engine.backend.candidate_runner.calls==1
    source=engine.ledger.connection.execute("SELECT config_json,config_hash FROM episodes WHERE id='source'").fetchone()
    assert tuple(source)==(before,digest)
    frozen=json.loads(engine.ledger.connection.execute("SELECT config_json FROM episodes WHERE policy='heldout'").fetchone()[0])
    assert frozen['coral']['candidate_pipeline']=='postbid_v1'
    assert frozen['stage_configs']=={}
    assert not (engine.root/'deepseek-native-authority').exists()
    assert not (engine.root/'codex-autonomous-authority').exists()
