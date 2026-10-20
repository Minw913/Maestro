"""Concurrent generation infrastructure, using explicit fake services."""
from concurrent.futures import ThreadPoolExecutor
import errno
import os
from pathlib import Path
import shutil
import threading
import time
import subprocess

import pytest

from maestro.backends.coral.provider import DockerAgentSandboxProvider
from maestro.backends.coral.workspace import WorkspaceStore, candidate_tree_hash
from maestro.execution.budgets import BudgetStore, BudgetExceeded
from maestro.execution.gateway import BudgetGateway


@pytest.mark.skipif(os.name!='nt',reason='Windows sharing/access errors only')
def test_atomic_freeze_retries_a_transient_windows_scanner_handle(tmp_path,monkeypatch):
    import maestro.backends.coral.workspace as module
    seed=tmp_path/'seed'
    seed.mkdir()
    (seed/'solver.py').write_text('print(1)\n')
    store=WorkspaceStore(tmp_path/'work',tmp_path/'cas')
    original=Path.rename
    attempts=[]
    def rename(path,target):
        if path.name.startswith('freeze-'):
            attempts.append(path)
            if len(attempts)==1:
                error=PermissionError('Synthetic scanner holds new directory')
                error.winerror=32
                raise error
        return original(path,target)
    monkeypatch.setattr(Path,'rename',rename)
    monkeypatch.setattr(module.time,'sleep',lambda seconds:None)
    frozen=store.freeze(seed)
    assert len(attempts)==2
    assert candidate_tree_hash(frozen['candidate_path'])==candidate_tree_hash(seed)


def test_first_generation_wave_starts_one_gateway(tmp_path,monkeypatch):
    import maestro.backends.coral.provider as module
    started=[]
    class Server:
        def __init__(self,*args,**kwargs):
            pass
        def start(self):
            started.append(self)
            time.sleep(.03)  # A competing initializer observes server=None.
            return self
        def close(self):
            pass
    monkeypatch.setattr(module,'GatewayHTTPServer',Server)
    store=BudgetStore(tmp_path/'budgets.sqlite')
    provider=DockerAgentSandboxProvider(tmp_path/'authority',store,
        image='test@sha256:'+'a'*64,upstream=lambda payload: {})
    gate=threading.Barrier(4)
    def initialize(_):
        gate.wait(timeout=2)
        provider._start_gateway()
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(initialize,range(4)))
        assert len(started)==1 and provider.server is started[0]
        provider.close()
        with pytest.raises(RuntimeError,match='cannot reopen'):
            provider._start_gateway()
    finally:
        provider.close()
        store.close()


def test_revoked_credential_cannot_pass_a_delayed_reservation(tmp_path):
    store=BudgetStore(tmp_path/'budgets.sqlite')
    store.create('a',max_calls=1,max_request_bytes=4096,max_output_tokens=10,
                 deadline=time.time()+30)
    forwarded=[]
    gateway=BudgetGateway(store,lambda payload: forwarded.append(payload),allowed_model='fixed')
    token=gateway.credential('a','agent')
    class RevokedDuringValidation(dict):
        def get(self,key,default=None):
            if key=='model':
                gateway.revoke(token)
            return super().get(key,default)
    try:
        with pytest.raises(BudgetExceeded,match='Revoked'):
            gateway.complete(token,'request',RevokedDuringValidation(model='fixed',max_tokens=1))
        assert not forwarded and store.usage('a')['calls']==0
    finally:
        store.close()


@pytest.mark.parametrize('diagnostic,absent',[
    ('Error: No such object: scope',True),
    ('error during connect: docker daemon pipe is unavailable',False)])
def test_executor_disconnection_is_not_proof_a_scope_stopped(tmp_path,diagnostic,absent,monkeypatch):
    store=BudgetStore(tmp_path/'budgets.sqlite')
    provider=DockerAgentSandboxProvider(tmp_path/'authority',store,
        image='test@sha256:'+'a'*64,upstream=lambda payload: {})
    provider._scopes[('a',0)]=('scope',)
    monkeypatch.setattr(provider.runner,'_docker',lambda args:subprocess.CompletedProcess(args,1,'',diagnostic))
    try:
        assert provider.quiescent('a',0) is absent
    finally:
        provider._scopes.clear()
        provider.close()
        store.close()


@pytest.mark.parametrize('same_content',[True,False])
def test_cas_accepts_only_a_verified_concurrent_directory_winner(tmp_path,monkeypatch,same_content):
    seed=tmp_path/'seed'
    seed.mkdir()
    (seed/'solver.py').write_text('verified content')
    store=WorkspaceStore(tmp_path/'agents',tmp_path/'cas')
    original=Path.rename
    def racing_publish(staging,target):
        if staging.name.startswith('freeze-'):
            shutil.copytree(staging,target)
            if not same_content:
                (target/'solver.py').write_text('wrong content')
            raise OSError(errno.ENOTEMPTY,'another publisher already installed the target')
        return original(staging,target)
    monkeypatch.setattr(Path,'rename',racing_publish)
    if same_content:
        result=store.freeze(seed)
        assert result['candidate_hash']==candidate_tree_hash(seed)
    else:
        with pytest.raises(OSError):
            store.freeze(seed)
    assert not list(store.artifact_root.glob('freeze-*'))
