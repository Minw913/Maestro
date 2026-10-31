"""Offline checks for the explicit old-Warm-to-current-Market transfer."""
import json
from pathlib import Path

import pytest

from maestro.config import RunConfig
from maestro.execution.legacy_warm_import import validate_legacy_warm
from maestro.loop import ExperimentEngine
from maestro.storage import ArtifactStore, Ledger
from maestro.types import IntegrityError, canonical_json, stable_hash


def test_explicit_transfer_preserves_archived_identity_and_rejects_contract_change(tmp_path):
    config = RunConfig(rounds=1, warm_opportunities_per_agent=5)
    source = tmp_path / 'source'
    with ExperimentEngine(source, config) as engine:
        warm = engine.bootstrap()
    checkpoint = Path(warm['checkpoint'])
    envelope = json.loads(checkpoint.read_text())
    payload = envelope['payload']
    payload['schema_version'] = 2
    payload['config']['coral']['candidate_pipeline'] = 'postbid_v1'
    payload['config_hash'] = stable_hash(payload['config'])
    envelope['artifact_hash'] = stable_hash(payload)
    checkpoint.write_text(canonical_json(envelope))
    with Ledger(source / 'authority/ledger.sqlite') as ledger:
        with ledger.transaction() as db:
            db.execute('UPDATE warm_checkpoints SET payload=?,content_hash=? WHERE id=?',
                       (canonical_json(payload), envelope['artifact_hash'], payload['warm_id']))
        assert ArtifactStore(source / 'authority/cas/sha256', ledger.register_artifact).put_json(payload) == envelope['artifact_hash']
    with pytest.raises(IntegrityError, match='contract/panel mismatch'):
        validate_legacy_warm(checkpoint, envelope, config, 'wrong', payload['panel_hash'])
    with ExperimentEngine(tmp_path / 'target', config) as engine:
        result = engine.run(checkpoint, 'uniform', legacy_warm_import=True)
        assert result['status'] == 'COMPLETED'
        event = engine.ledger.connection.execute(
            "SELECT payload FROM events WHERE episode_id=? AND kind='LEGACY_WARM_SOURCE_IMPORTED'",
            (result['episode_id'],)).fetchone()
        assert json.loads(event[0])['source_hash'] == envelope['artifact_hash']
    assert json.loads(checkpoint.read_text()) == envelope
