import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from maestro.storage.export import export_outputs


def test_export_includes_committed_wal_and_excludes_runtime(tmp_path):
    root=tmp_path/'artifacts/runs/demo';(root/'authority').mkdir(parents=True)
    db=sqlite3.connect(root/'authority/ledger.sqlite')
    db.execute('PRAGMA journal_mode=WAL');db.execute('CREATE TABLE outcomes(status TEXT)')
    db.execute("INSERT INTO outcomes VALUES('UNKNOWN')");db.commit()
    (root/'native').mkdir();(root/'native/scope.json').write_text('local secret launch scope')
    (root/'controller.log').write_text('local stderr')
    (root/'coral-workspaces').mkdir();(root/'coral-workspaces/scratch').write_text('mutable')
    (root/'authority/config.resolved.json').write_text('{}')
    (root/'coral-cas/hash').mkdir(parents=True);(root/'coral-cas/hash/run').write_text('solver')
    target=tmp_path/'artifacts/exports/demo'
    try:result=export_outputs(root,target)
    finally:db.close()
    with sqlite3.connect(target/'authority/ledger.sqlite') as snapshot:
        assert snapshot.execute('SELECT status FROM outcomes').fetchall()==[('UNKNOWN',)]
    assert not (target/'native').exists() and not (target/'controller.log').exists()
    assert not (target/'coral-workspaces').exists()
    assert not list(target.rglob('*-wal'))
    manifest=json.loads((target/'EXPORT_MANIFEST.json').read_text())
    assert result['replay_verified'] is False
    for item in manifest['files']:
        assert hashlib.sha256((target/item['path']).read_bytes()).hexdigest()==item['sha256']
    with pytest.raises(ValueError,match='already exists'):export_outputs(root,target)


def test_export_refuses_recursive_destination_and_sensitive_payload(tmp_path):
    root=tmp_path/'run';(root/'authority/cas').mkdir(parents=True)
    with pytest.raises(ValueError,match='outside'):export_outputs(root,root/'copy')
    (root/'authority/cas/secret').write_text('http://127.0.0.1:5555/'+'x'*43)
    with pytest.raises(ValueError,match='credential/capability'):export_outputs(root,tmp_path/'export')

    assert not (tmp_path/'export').exists()
