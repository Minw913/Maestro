"""Offline checks for the external, read-only Market pause monitor."""
import json
import sqlite3

from maestro.storage.paths import run_paths
from scripts.pause_after_market_round import monitor


def fixture(root, *, episode='RUNNING', round_index=9, round_status='EVALUATING',
            ticket_status='RUNNING', request_status='COMPLETED'):
    (root/'authority').mkdir(parents=True)
    (root/'deepseek-native-authority').mkdir()
    db=sqlite3.connect(root/'authority/ledger.sqlite')
    db.executescript('''CREATE TABLE episodes(id TEXT,status TEXT);
        CREATE TABLE rounds(episode_id TEXT,round_index INTEGER,status TEXT);
        CREATE TABLE eval_tickets(episode_id TEXT,round_index INTEGER,status TEXT);''')
    db.execute('INSERT INTO episodes VALUES(?,?)',('e',episode))
    db.execute('INSERT INTO rounds VALUES(?,?,?)',('e',round_index,round_status))
    db.execute('INSERT INTO eval_tickets VALUES(?,?,?)',('e',round_index,ticket_status))
    db.commit();db.close()
    requests=sqlite3.connect(root/'deepseek-native-authority/requests.sqlite')
    requests.execute('CREATE TABLE inference_reservations(status TEXT)')
    requests.execute('INSERT INTO inference_reservations VALUES(?)',(request_status,))
    requests.commit();requests.close()


def test_pause_after_target_evaluations_launched(tmp_path):
    root=tmp_path/'run';fixture(root)
    result=monitor(root,'e',9)
    local=run_paths(root).local
    assert result['status']=='PAUSE_REQUESTED'
    assert (local/'authority/pause-request.json').is_file()
    assert json.loads((local/'authority/pause-after-round-10.json').read_text())['target_round_index']==9


def test_unknown_stops_at_current_safe_round_and_preserves_diagnostic(tmp_path):
    root=tmp_path/'run';fixture(root,round_index=8,request_status='UNKNOWN')
    result=monitor(root,'e',9)
    assert result['status']=='PAUSE_REQUESTED'
    assert result['effective_target_round_index']==8
    assert result['unknown_requests_observed']==1
    assert (run_paths(root).local/'authority/pause-request.json').is_file()


def test_missed_boundary_writes_diagnostic_without_pause_marker(tmp_path):
    root=tmp_path/'run';fixture(root,round_index=10)
    result=monitor(root,'e',9)
    local=run_paths(root).local
    assert result['status']=='MISSED_SAFE_BOUNDARY'
    assert (local/'authority/pause-after-round-10.json').is_file()
    assert not (local/'authority/pause-request.json').exists()
