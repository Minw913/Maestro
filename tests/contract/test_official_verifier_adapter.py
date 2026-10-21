"""Official CLI semantic receipts must not be inferred from exit code alone."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location(
    'official_adapter', Path(__file__).resolve().parents[2] / 'scripts/pace_official_verifier.py')
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)


@pytest.mark.parametrize('code,stdout,stderr,status', [
    (0, 'Solution of size 2 is valid!\n', '', 'VALID'),
    (0, '', 'At least one vertex is not dominated by the given solution!\n', 'INVALID'),
    (0, '', '', 'ERROR'),
    (1, 'Solution of size 2 is valid!', '', 'ERROR'),
    (0, 'Solution of size 2 is valid!', 'java failure', 'ERROR'),
    (0, 'prefix Solution of size 2 is valid!', '', 'ERROR'),
    (0, 'Solution of size 2 is valid!\nSolution of size 1 is valid!', '', 'ERROR'),
])
def test_official_exit_zero_is_insufficient(monkeypatch, code, stdout, stderr, status):
    monkeypatch.setattr(adapter.subprocess, 'run', lambda *a, **kw:
        SimpleNamespace(returncode=code, stdout=stdout, stderr=stderr))
    result = adapter.verify('verifier.jar', 'graph.gr', 'solution.sol')
    assert result['status'] == status
    if status == 'VALID':
        assert result['size'] == 2


def test_official_timeout_is_error(monkeypatch):
    def timeout(*a, **kw):
        raise adapter.subprocess.TimeoutExpired('java', 30)
    monkeypatch.setattr(adapter.subprocess, 'run', timeout)
    assert adapter.verify('jar', 'graph', 'solution')['status'] == 'ERROR'
