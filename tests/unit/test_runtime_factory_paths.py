"""Factory path acceptance with all model/Docker boundaries injected offline."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from maestro.config import RunConfig


@pytest.mark.parametrize('provider',['codex','deepseek'])
def test_factories_keep_native_and_workspaces_out_of_outputs(tmp_path,monkeypatch,provider):
    import maestro.backends.coral.runtime as runtime
    import maestro.execution.sandbox as sandbox
    import maestro.tasks.pace.manifest as manifest
    monkeypatch.setattr(runtime,'verify_checkout',lambda *args:{})
    monkeypatch.setattr(manifest,'load_panel',lambda *args,**kwargs:object())
    monkeypatch.setattr(manifest,'assert_smoke_disjoint',lambda *args:None)
    class Runner:
        def __init__(self,*args):pass
        def preflight(self,**kwargs):return {}
    monkeypatch.setattr(sandbox,'DockerRunner',Runner)
    class Client:
        def __init__(self,**kwargs):self.settings=kwargs
        def preflight(self,*args,**kwargs):return {}
        def require_chatgpt_login(self):return {'logged_in':True}
    class Backend:
        def __init__(self,client,store,workspaces,**kwargs):
            self.client,self.store,self.workspaces=client,store,workspaces
        def preflight(self,*args):return {}
        def close(self):self.store.close()
    root=tmp_path/'artifacts/runs/example';local=tmp_path/'runtime/runs/example'
    (tmp_path/'artifacts').mkdir()
    settings={'model':'offline','director_mode':'predeclared',**({'runtime':'dsh'} if provider=='deepseek' else {})}
    config=RunConfig(execution_mode='real',coral=settings,pace={'panel_manifest':'unused'})
    if provider=='codex':
        import maestro.backends.codex_session.autonomous_backend as module
        monkeypatch.setattr(module,'CodexSessionClient',Client)
        backend=module.strict_factory(config,root,client_factory=Client,backend_class=Backend)
        directory='codex-autonomous-authority'
    else:
        import maestro.backends.coral.deepseek_backend as module
        monkeypatch.setattr(module,'verify_checkout',lambda *args:{})
        monkeypatch.setattr(module,'check_restart_ledgers',lambda *args,**kwargs:None)
        monkeypatch.setattr(module,'load_panel',lambda *args,**kwargs:object())
        monkeypatch.setattr(module,'assert_smoke_disjoint',lambda *args:None)
        monkeypatch.setattr(module,'DockerRunner',Runner)
        monkeypatch.setattr(module,'CoralDeepSeekClient',Client)
        monkeypatch.setattr(module,'NativeDeepSeekBackend',Backend)
        monkeypatch.setenv('DEEPSEEK_API_KEY','synthetic-test-only')
        settings['runtime']='dsh'
        backend=module.native_factory(config,root)
        directory='deepseek-native-authority'
    try:
        assert Path(backend.client.settings['work_root'])==local/directory/'native'
        assert backend.workspaces.root==local/'coral-workspaces'
        assert backend.workspaces.artifact_root==root/'coral-cas'
        assert (root/directory/'requests.sqlite').exists()
        assert not (root/directory/'native').exists()
        assert not (root/'coral-workspaces').exists()
    finally:backend.close()
