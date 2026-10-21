"""Offline acceptance of the exact edit required by the live fault test."""
import importlib.util
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location('qualify_native_policy',
    Path(__file__).resolve().parents[2] / 'scripts/qualify_native_policy.py')
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)
FAULTY = 'def main():\n    __maestro_qualification_missing__\n'


def test_qualification_accepts_only_the_requested_entry_edit(tmp_path):
    (tmp_path / 'run').write_text('# initial-qualified\n' + FAULTY)
    qualification.check_initial_candidate(tmp_path, FAULTY)


@pytest.mark.parametrize('case', ['missing_comment', 'nested_copy', 'fixed_fault',
    'other_change', 'duplicate_comment'])
def test_qualification_rejects_incomplete_or_unrequested_edits(tmp_path, case):
    content = '# initial-qualified\n' + FAULTY
    if case == 'missing_comment':
        content = FAULTY
    elif case == 'nested_copy':
        (tmp_path / 'solver').mkdir()
        (tmp_path / 'solver/run').write_text(content)
        content = FAULTY
    elif case == 'fixed_fault':
        content = content.replace('__maestro_qualification_missing__', 'pass')
    elif case == 'other_change':
        content += 'print("unrequested")\n'
    elif case == 'duplicate_comment':
        content += '# initial-qualified\n'
    (tmp_path / 'run').write_text(content)
    with pytest.raises(ValueError):
        qualification.check_initial_candidate(tmp_path, FAULTY)
