"""Offline acceptance of the exact edit required by the live fault test."""
import importlib.util
import json
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


def test_qualification_accepts_explicit_runtime_context_but_rejects_private_or_missing_input():
    context={'files':[{'path':'run'}],'parent_files':[{'path':'run'}],
        'parent_hash':'parent','source_index':[],'parent_source_index':[],
        'solver_argv':['/candidate/run'],'runtime_error':{'test_context':{
            'kind':'fixed_sanity_only','stdin':'p ds 1 0\n','receipt':{'returncode':1}}}}
    prompt=lambda:'ISOLATED RUNTIME REPAIR.\n'+json.dumps(context)
    assert qualification.check_repair_context(prompt(),'private-nonce')==context
    context['memory']='private-nonce'
    with pytest.raises(ValueError):qualification.check_repair_context(prompt(),'private-nonce')
    del context['memory'];context['runtime_error']['test_context'].pop('stdin')
    with pytest.raises(ValueError):qualification.check_repair_context(prompt(),'private-nonce')
