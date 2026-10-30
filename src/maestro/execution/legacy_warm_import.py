"""Explicit, provenance-preserving transfer of a retired Warm state.

The archived checkpoint remains schema 2/postbid_v1. Only its immutable agent
states are used as initial conditions for a *new* prebid_v2 Market episode.
"""
from pathlib import Path
import json

from maestro.execution.pipeline import pipeline_version, RETIRED
from maestro.storage import Ledger, ArtifactStore
from maestro.types import IntegrityError, stable_hash


def validate_legacy_warm(checkpoint, envelope, market_config, contract_hash, panel_hash):
    path = Path(checkpoint).resolve(strict=True)
    payload = envelope['payload']
    digest = envelope['artifact_hash']
    if payload.get('schema_version') != 2 or pipeline_version(payload['config']) != RETIRED:
        raise IntegrityError('Explicit legacy import requires a schema 2 postbid_v1 Warm checkpoint')
    if stable_hash(payload) != digest or stable_hash(payload['config']) != payload.get('config_hash'):
        raise IntegrityError('Legacy Warm checkpoint/configuration hash mismatch')
    if payload.get('contract_hash') != contract_hash or payload.get('panel_hash') != panel_hash:
        raise IntegrityError('Legacy Warm evaluation contract/panel mismatch')
    if payload.get('warm_waves') != 5 or payload.get('opportunities') != {f'agent-{i}': 5 for i in range(1, 5)}:
        raise IntegrityError('Legacy Warm is not the completed four-agent, five-wave source')
    old = payload['config']
    new = market_config.to_dict()
    for key in ('execution_mode', 'benchmark', 'n_agents', 'seed', 'evaluation_instance_count',
                'cpu_seconds_per_instance'):
        if old.get(key) != new.get(key):
            raise IntegrityError(f'Legacy Warm {key} differs from Market')
    for key in ('backend_factory', 'model', 'runtime', 'revision'):
        if old.get('coral', {}).get(key) != new.get('coral', {}).get(key):
            raise IntegrityError(f'Legacy Warm Coral {key} differs from Market')
    if set(payload.get('states', {})) != set(payload['opportunities']):
        raise IntegrityError('Legacy Warm agent state set mismatch')
    board = payload.get('board', {})
    if board.get('contract_hash') != contract_hash or board.get('panel_hash') != panel_hash:
        raise IntegrityError('Legacy Warm board identity mismatch')
    for agent, state in payload['states'].items():
        if state.get('agent_id') != agent or len(state.get('history', [])) != 5:
            raise IntegrityError('Legacy Warm agent history is incomplete')
        if not any(row.get('candidate_hash') == state.get('parent_hash') and
                   row.get('score') == state.get('parent_score') and row.get('valid')
                   for row in board.get('results', [])):
            raise IntegrityError('Legacy Warm parent is absent from verified board')
    source_root = path.parent.parent.parent
    with Ledger(source_root / 'authority' / 'ledger.sqlite', read_only=True) as ledger:
        ledger.verify_event_chain()
        row = ledger.connection.execute(
            'SELECT status FROM episodes WHERE id=? AND policy=?', (payload['warm_id'], 'warm')).fetchone()
        record = ledger.connection.execute(
            'SELECT content_hash,payload FROM warm_checkpoints WHERE id=?', (payload['warm_id'],)).fetchone()
        if not row or row['status'] != 'COMPLETED' or not record or record['content_hash'] != digest or json.loads(record['payload']) != payload:
            raise IntegrityError('Legacy Warm source ledger does not attest the completed checkpoint')
    source_cas = ArtifactStore(source_root / 'authority' / 'cas' / 'sha256', read_only=True)
    if source_cas.get_json(digest) != payload:
        raise IntegrityError('Legacy Warm source CAS differs from checkpoint')
    return {'source_checkpoint': str(path), 'source_root': str(source_root),
            'source_hash': digest, 'source_schema': 2, 'source_pipeline': RETIRED,
            'market_pipeline': 'prebid_v2', 'comparison_scope': 'legacy_warm_state_transfer'}
