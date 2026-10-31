"""Final-only held-out authority, kept separate from market and agent views."""

from dataclasses import asdict, replace
from decimal import Decimal
import json
from pathlib import Path

from typing import TYPE_CHECKING

from maestro.execution.dispatcher import Dispatcher
from maestro.types import AgentState, IntegrityError, MaestroError, canonical_json, stable_hash

from .manifest import load_panel, assert_smoke_disjoint
from .runner import PaceEvaluator
from .sanity import default_sanity_graph_path

if TYPE_CHECKING:
    from maestro.loop import ExperimentEngine


def evaluate_frozen_final(root,source_episode_id,manifest_path,*,runner=None):
    """Evaluation-only entry, including retired searches; never starts an agent.

    Archive configuration is checked as raw wire data. Only task/runner fields
    are adapted for evaluation, without rewriting the source episode identity.
    ``runner`` injection is for isolated containment tests, never mock mode.
    """
    from types import SimpleNamespace
    from maestro.config import RunConfig
    from maestro.execution.pipeline import read_episode,pipeline_version
    from maestro.execution.sandbox import DockerRunner
    from maestro.storage import Ledger,ArtifactStore
    from .manifest import EvaluationContract
    root=Path(root).resolve()
    source=read_episode(root,source_episode_id)
    raw=json.loads(source['config_json'])
    if (source['status']!='COMPLETED' or source['execution_mode']!='real'
            or source['policy'] in ('warm','heldout')):
        raise MaestroError('held-out evaluation requires a completed real main episode')
    if stable_hash(raw)!=source['config_hash']:
        raise IntegrityError('source episode configuration is corrupted')
    # Do not deserialize retired scheduling/wealth schemas as a new experiment.
    wanted={'cpu_workers','instance_workers','evaluation_instance_count','cpu_seconds_per_instance',
            'verification_cpu_seconds','pace','seed','benchmark'}
    options={k:v for k,v in raw.items() if k in wanted}
    options.update(execution_mode='real',coral={'candidate_pipeline':pipeline_version(raw),**raw.get('coral',{})},
        stage_configs={},config_sources={},experiment_stage=None)
    config=RunConfig(**options)
    training=load_panel(config.pace['panel_manifest'],expected_split='training')
    settings=dict(config.pace.get('contract',{}))
    for key in ('cpu_workers','instance_workers'):
        if key in settings and settings[key]!=getattr(config,key):raise IntegrityError('Frozen worker contract mismatch')
        settings[key]=getattr(config,key)
    contract=EvaluationContract.standard(**settings)
    if len(training.instances)!=contract.instances or config.evaluation_instance_count!=contract.instances:
        raise IntegrityError('Frozen training contract mismatch')
    physical=runner if runner is not None else DockerRunner()
    # Validate manifests before opening a writer or asking Docker for anything.
    heldout=load_panel(manifest_path,expected_split='heldout');training.assert_disjoint(heldout)
    assert_smoke_disjoint(heldout,config.coral.get('smoke_graph') or default_sanity_graph_path())
    with Ledger(root/'authority/ledger.sqlite') as ledger:
        artifacts=ArtifactStore(root/'authority/cas/sha256',ledger.register_artifact)
        evaluator=PaceEvaluator(training,contract,physical,artifact_store=artifacts.put,
            cpu_workers=config.cpu_workers,instance_workers=config.instance_workers)
        evaluator.preflight()
        context=SimpleNamespace(root=root,config=config,ledger=ledger,artifacts=artifacts,
            _live_panel=training,_live_contract=contract,dispatcher=Dispatcher(ledger,evaluator))
        return evaluate_final(context,source_episode_id,manifest_path)


def _summary(source_episode_id, heldout_episode_id, ticket, result, contract):
    return {
        "source_episode_id": source_episode_id,
        "heldout_episode_id": heldout_episode_id,
        "job_id": ticket["job_id"], "purpose": "HELDOUT",
        "candidate_hash": result.candidate_hash, "score": result.score,
        "valid": result.valid, "status": result.status,
        "label": result.diagnostics.get("label", "pace_style_internal"),
        "execution_kind": result.diagnostics.get("execution_kind"),
        "panel_hash": ticket["panel_hash"], "contract_hash": ticket["contract_hash"],
        "cpu_seconds": result.cpu_seconds, "wall_seconds": result.wall_seconds,
        "reserved_cpu_seconds": contract.reserved_cpu_seconds,
        "payment": "0", "reward": "0",
    }


def evaluate_final(engine: 'ExperimentEngine', source_episode_id: str, manifest_path: str | Path) -> dict[str, object]:
    """Evaluate only a completed episode's frozen champion with zero payment.

    This is a trusted operator function, not an agent tool. It creates a separate
    deterministic held-out episode/ticket, uses the ordinary guarded dispatcher,
    and never invokes market settlement or publishes agent context. Identical
    retries return the durable result. A claimed/running/unknown job is never
    automatically relaunched: the operator must reconcile it first.
    """
    ledger = engine.ledger
    db = ledger.connection
    source = db.execute("SELECT * FROM episodes WHERE id=?", (source_episode_id,)).fetchone()
    if source is None or source["status"] != "COMPLETED" or source["policy"] in {"warm", "heldout"}:
        raise MaestroError("held-out evaluation requires a completed main episode")
    if source["execution_mode"] != "real" or engine.config.execution_mode != "real":
        raise MaestroError("real held-out data cannot be evaluated from a simulated episode")
    source_config = json.loads(source["config_json"])
    if stable_hash(source_config) != source["config_hash"]:
        raise IntegrityError("source episode configuration is corrupted")
    latest = db.execute("SELECT * FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1",
                        (source_episode_id,)).fetchone()
    if latest is None or latest["round_index"] != source_config["rounds"] - 1:
        raise MaestroError("source episode has no complete final checkpoint")
    checkpoint = json.loads(latest["payload"])
    if stable_hash(checkpoint) != latest["content_hash"]:
        raise IntegrityError("source final checkpoint was modified")
    if engine.artifacts.get_json(latest["content_hash"]) != checkpoint:
        raise IntegrityError("source checkpoint differs from its immutable artifact")
    unfinished = db.execute("SELECT count(*) FROM rounds WHERE episode_id=? AND status!='SETTLED'",
                            (source_episode_id,)).fetchone()[0]
    if unfinished:
        raise MaestroError("source main episode has unfinished rounds")
    board = checkpoint["board"]
    training = engine._live_panel
    source_contract = engine._live_contract
    if training is None or source_contract is None:
        raise MaestroError("held-out evaluation requires the original PACE panel and contract")
    if (board.get("panel_hash") != training.panel_hash
            or board.get("contract_hash") != source_contract.contract_hash):
        raise IntegrityError("current task contract differs from the source episode")
    panel = load_panel(manifest_path, expected_split="heldout")
    training.assert_disjoint(panel)
    smoke_graph = engine.config.coral.get("smoke_graph") or default_sanity_graph_path()
    assert_smoke_disjoint(panel, smoke_graph)
    contract = replace(source_contract, instances=len(panel.instances))
    candidate_hash = board["champion_hash"]
    candidate_row = db.execute("SELECT payload FROM candidates WHERE hash=?", (candidate_hash,)).fetchone()
    if candidate_row is None:
        raise IntegrityError("final champion has no immutable registered artifact")
    candidate = json.loads(candidate_row[0])
    if not candidate.get("candidate_path"):
        raise IntegrityError("real final champion has no frozen candidate path")
    identity = stable_hash([source_episode_id, candidate_hash, panel.panel_hash, contract.contract_hash])
    heldout_id = "heldout-" + identity[:24]
    job_id = heldout_id + ":0:final-champion:job"
    resource_settings = {"admission_capacity": 1, "cpu_workers": engine.config.cpu_workers,
        "instance_workers": engine.config.instance_workers, "evaluation_batch_width": 1,
        "effective_instance_workers": engine.config.effective_instance_workers(1, len(panel.instances))}
    ticket = {
        "ticket_id": job_id + ":ticket", "job_id": job_id,
        "episode_id": heldout_id, "round_index": 0, "agent_id": "final-champion",
        "candidate_hash": candidate_hash, "candidate_path": candidate["candidate_path"],
        "contract_hash": contract.contract_hash, "panel_hash": panel.panel_hash,
        "plan_version_id": "final-only", "parent_hash": candidate_hash,
        "parent_score": board["champion_score"], "champion_score": board["champion_score"],
        "phase_epoch": 0, "purpose": "HELDOUT", "seed": contract.seed,
        "reserved_cpu_seconds": contract.reserved_cpu_seconds,
        "source_episode_id": source_episode_id,
        **resource_settings,
    }
    existing = ledger.existing_result(job_id)
    if existing is not None:
        # Binding is checked against the persisted trusted ticket on every replay.
        persisted = ledger.ticket(ticket["ticket_id"])
        if (persisted["candidate_hash"] != candidate_hash or persisted["purpose"] != "HELDOUT"
                or persisted["panel_hash"] != panel.panel_hash or persisted["contract_hash"] != contract.contract_hash):
            raise IntegrityError("persisted held-out ticket identity mismatch")
        return _complete(engine, source_episode_id, heldout_id, ticket, existing, contract)

    source_evaluator = engine.dispatcher.evaluator
    if not isinstance(source_evaluator, PaceEvaluator):
        raise MaestroError("held-out evaluation requires the PACE containment adapter")
    evaluator = PaceEvaluator(panel, contract, source_evaluator.runner,
        candidate_resolver=source_evaluator.candidate_resolver,
        artifact_store=engine.artifacts.put,
        simulate_containment=source_evaluator.simulate_containment,
        verification_worker=source_evaluator.verification_worker,
        cpu_workers=source_evaluator.cpu_workers, instance_workers=source_evaluator.instance_workers,
        cpu_pool=source_evaluator.cpu_pool)
    evaluator.preflight()
    # Each frozen source tree is rehashed independently before issuing authority.
    from maestro.backends.coral.workspace import candidate_tree_hash
    if candidate_tree_hash(candidate["candidate_path"]) != candidate_hash:
        raise IntegrityError("frozen final champion source changed before held-out execution")
    heldout_config = replace(engine.config, n_agents=1, slots=1, rounds=1,
        warm_opportunities_per_agent=1,
        # Held-out authority has its own single-candidate lifecycle. Retaining
        # the source's frozen Market stage would revalidate slots=2 against
        # n_agents=1, and would misidentify this as another Market experiment.
        experiment_stage=None, stage_configs={}, config_sources={},
        evaluation_instance_count=len(panel.instances), pace={**engine.config.pace,
            "panel_manifest": str(Path(manifest_path).resolve()), "contract": asdict(contract)})
    with ledger.transaction():
        present = db.execute("SELECT id FROM episodes WHERE id=?", (heldout_id,)).fetchone()
        if present is None:
            state = AgentState("final-champion", candidate_hash, board["champion_score"], Decimal(0))
            ledger.create_episode(heldout_id, "heldout", heldout_config, {state.agent_id: state})
            ledger.open_round(heldout_id, 0, {"purpose": "HELDOUT", "source_episode_id": source_episode_id,
                "candidate_hash": candidate_hash, "panel_hash": panel.panel_hash,
                "contract_hash": contract.contract_hash, "payment": "0", "reward": "0",
                **resource_settings})
            ledger.invite(heldout_id, 0, state.agent_id, Decimal(0), {"purpose": "HELDOUT"})
            ledger.set_round_phase(heldout_id, 0, "ADMISSION")
            ledger.admit(ticket, Decimal(0), 1)
            ledger.set_round_phase(heldout_id, 0, "EVALUATING")
            ledger.event("HELDOUT_AUTHORIZED", {
                "source_episode_id": source_episode_id, "heldout_episode_id": heldout_id,
                "candidate_hash": candidate_hash, "panel_hash": panel.panel_hash,
                "contract_hash": contract.contract_hash, "reserved_cpu_seconds": contract.reserved_cpu_seconds,
                "detail_scope": "authority_private"}, heldout_id, 0)
    result = Dispatcher(ledger, evaluator).dispatch(ticket["ticket_id"], job_id)
    return _complete(engine, source_episode_id, heldout_id, ticket, result, contract)


def _complete(engine, source_id, heldout_id, ticket, result, contract):
    ledger = engine.ledger
    summary = _summary(source_id, heldout_id, ticket, result, contract)
    with ledger.transaction() as db:
        row = db.execute("SELECT status FROM episodes WHERE id=?", (heldout_id,)).fetchone()
        if row is None:
            raise IntegrityError("result has no held-out authority episode")
        if row[0] != "COMPLETED":
            ledger.usage(heldout_id, 0, "final-champion", "HELDOUT",
                         cpu=result.cpu_seconds, wall=result.wall_seconds,
                         payload={"source_episode_id": source_id, "reserved_cpu_seconds": contract.reserved_cpu_seconds})
            db.execute("UPDATE episodes SET status='COMPLETED' WHERE id=?", (heldout_id,))
            db.execute("UPDATE rounds SET status='SETTLED' WHERE episode_id=?", (heldout_id,))
            db.execute("UPDATE eval_tickets SET status='SETTLED' WHERE episode_id=?", (heldout_id,))
            ledger.event("HELDOUT_COMPLETED", summary, heldout_id, 0)
    private = engine.root / "authority" / "heldout"
    private.mkdir(parents=True, exist_ok=True)
    (private / (heldout_id + ".json")).write_text(canonical_json(summary), encoding="utf-8")
    return summary
