from pathlib import Path
import subprocess

import pytest

from maestro.storage.paths import run_paths, diagnostic_root, require_separated_runtime
from maestro.execution.pause import PauseController


def test_standard_outputs_and_local_runtime_are_separate(tmp_path):
    output=tmp_path/'artifacts/runs/experiment'
    paths=run_paths(output)
    assert paths.output==output
    assert paths.local==tmp_path/'runtime/runs/experiment'
    assert not output.exists() and not paths.local.exists()


def test_nested_diagnostic_runtime_never_stays_in_artifacts(tmp_path):
    root=tmp_path/'artifacts/qualification/runtime'
    assert run_paths(root).local==tmp_path/'runtime/qualification/runtime'
    local=tmp_path/'runtime/calibration'
    assert run_paths(local).local==local
    assert diagnostic_root(local)==local


def test_diagnostics_do_not_bypass_existing_historical_root(tmp_path):
    old=tmp_path/'artifacts/qualification';old.mkdir(parents=True)
    (old/'requests.sqlite').write_bytes(b'untouched evidence')
    with pytest.raises(ValueError,match='Legacy diagnostic root'):
        diagnostic_root(old)
    assert not (tmp_path/'runtime/qualification').exists()
    assert (old/'requests.sqlite').read_bytes()==b'untouched evidence'


def test_custom_roots_do_not_collide_on_basename(tmp_path):
    left=run_paths(tmp_path/'left'/'run').local
    right=run_paths(tmp_path/'right'/'run').local
    assert left!=right
    assert left.name!=right.name


def test_legacy_native_execution_requires_explicit_migration(tmp_path):
    root=tmp_path/'artifacts/old'
    (root/'deepseek-native-authority/native').mkdir(parents=True)
    with pytest.raises(ValueError,match='explicit offline migration'):
        require_separated_runtime(root)
    assert (root/'deepseek-native-authority/native').exists()


def test_pause_state_is_local_and_legacy_marker_still_honored(tmp_path):
    root=tmp_path/'artifacts/runs/new'
    pause=PauseController(root);pause.request()
    assert pause.path==tmp_path/'runtime/runs/new/authority/pause-request.json'
    assert not root.exists()
    old=tmp_path/'artifacts/old';(old/'authority').mkdir(parents=True)
    (old/'authority/pause-request.json').write_text('{}')
    assert PauseController(old).requested


def test_git_policy_allows_standard_outputs_and_ignores_local_runtime():
    repo=Path(__file__).resolve().parents[2]
    def ignored(path):
        return subprocess.run(['git','check-ignore','--no-index','-q',path],cwd=repo).returncode==0
    assert ignored('runtime/runs/example/native/scope.json')
    assert ignored('artifacts/legacy-mixed/controller.log')
    assert not ignored('artifacts/runs/example/reports/summary.json')
    assert not ignored('artifacts/README.md')


def test_controller_logs_and_exit_are_local_only(tmp_path,capsys):
    import json,sys
    from maestro.execution.local_logging import controller_logs
    output=tmp_path/'artifacts/runs/logged'
    with controller_logs(output,'bootstrap') as result:
        print('controller message')
        print('diagnostic',file=sys.stderr)
        result['exit_code']=2
    local=run_paths(output).local
    assert 'controller message' in (local/'controller.stdout.log').read_text()
    assert 'diagnostic' in (local/'controller.stderr.log').read_text()
    assert json.loads((local/'controller-exits.jsonl').read_text())['exit_code']==2
    assert not output.exists()
    captured=capsys.readouterr()
    assert 'controller message' in captured.out and 'diagnostic' in captured.err


def test_unknown_in_local_qualification_cannot_be_bypassed_by_new_artifact_root(tmp_path):
    import time
    from maestro.execution.budgets import BudgetStore
    from maestro.backends.coral.deepseek_lifecycle import check_restart_ledgers
    path=tmp_path/'runtime/qualification/runtime/deepseek-native-authority/requests.sqlite'
    path.parent.mkdir(parents=True)
    store=BudgetStore(path)
    store.create('unknown-phase',max_calls=1,max_request_bytes=100,max_output_tokens=10,deadline=time.time()+60)
    reservation=store.reserve('unknown-phase','request',request_bytes=1,output_tokens=10)
    store.finish(reservation,failed=True)
    store.close()
    with pytest.raises(RuntimeError,match='unresolved evidence'):
        check_restart_ledgers(tmp_path/'artifacts/runs/new',artifacts_root=tmp_path/'artifacts')
