from maestro.cli import main


def test_qualification_cli_requires_explicit_live_flag_before_dispatch(tmp_path, capsys, monkeypatch):
    from maestro.backends.codex_session import qualification
    calls = []
    monkeypatch.setattr(qualification, 'run_qualification', lambda *a, **kw: calls.append((a, kw)))
    assert main(['--root', str(tmp_path/'run'), 'codex-qualify', '--model', 'test-model']) == 2
    assert calls == []
    assert '--live' in capsys.readouterr().err


def test_qualification_cli_forwards_frozen_settings_and_failure_exit(tmp_path, capsys, monkeypatch):
    from maestro.backends.codex_session import qualification
    calls = []
    def qualify(root, **kwargs):
        calls.append((root, kwargs))
        return {'status': 'FAILED', 'warm_start_started': False}
    monkeypatch.setattr(qualification, 'run_qualification', qualify)
    assert main(['--root', str(tmp_path/'run'), 'codex-qualify', '--live',
        '--model', 'test-model', '--phase-seconds', '60']) == 2
    assert calls[0][0] == str(tmp_path/'run')
    assert calls[0][1]['model'] == 'test-model'
    assert calls[0][1]['phase_seconds'] == 60
    assert calls[0][1]['reasoning_effort'] == 'low'
    assert '"warm_start_started":false' in capsys.readouterr().out
