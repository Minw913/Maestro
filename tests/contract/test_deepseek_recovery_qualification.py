"""Qualification envelopes are enforced before any external dispatch."""
import importlib.util
from pathlib import Path
import time
import pytest
from maestro.backends.coral.deepseek_retry import RetryPolicy
from maestro.execution.budgets import BudgetExceeded

spec=importlib.util.spec_from_file_location('recovery_qualification',Path(__file__).resolve().parents[2]/'scripts/qualify_deepseek_recovery.py')
qualification=importlib.util.module_from_spec(spec);spec.loader.exec_module(qualification)


def test_qualification_rejects_foreign_phases_and_has_global_physical_cap(tmp_path):
    store=qualification.QualificationStore(tmp_path/'requests.sqlite','q',RetryPolicy())
    try:
        with pytest.raises(BudgetExceeded):store.register_phase('q:0:agent-1:PREPARATION',3,100,100,time.time()+30)
        with pytest.raises(BudgetExceeded):store.register_phase('q:0:foreign:PREPARATION',2,100,100,time.time()+30)
        store.register_phase('q:0:agent-1:PREPARATION',2,100,100,time.time()+30)
        for i in range(31):
            account=f'q:0:agent-1:PREPARATION:TRANSPORT_ATTEMPT:fixture-{i}:0'
            store.create(account,max_calls=1,max_request_bytes=100,max_output_tokens=20,deadline=time.time()+30)
            if i<30:store.reserve(account,str(i),request_bytes=1,output_tokens=20)
            else:
                with pytest.raises(BudgetExceeded):store.reserve(account,str(i),request_bytes=1,output_tokens=20)
        assert store.connection.execute('SELECT count(*) FROM inference_reservations').fetchone()[0]==30
    finally:store.close()


def test_initial_qualification_checks_exact_entry_and_preserved_fault(tmp_path):
    original='#!/usr/bin/env python3\nraise NameError("injected")\n'
    (tmp_path/'run').write_text(original+'# initial-qualified\n')
    qualification.check_initial(tmp_path,original)
    (tmp_path/'run').write_text('# initial-qualified\npass\n')
    with pytest.raises(ValueError):qualification.check_initial(tmp_path,original)
