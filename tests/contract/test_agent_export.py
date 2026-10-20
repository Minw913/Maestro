import io
import json
from pathlib import Path
import tarfile

import pytest

from maestro.execution.agent_container import sync_export


def scope(tmp_path):
    authority = tmp_path / "authority"; authority.mkdir()
    (authority / "receipt.json").write_text(json.dumps({"status": "COMPLETED"}))
    target = tmp_path / "agent" / "solver"; target.mkdir(parents=True)
    (target / "old.py").write_text("old")
    return {"authority_dir": str(authority), "workspace_bytes": 1024,
            "workspace_files": 8, "writable": [["/workspace/solver", "/seed/solver"]],
            "export_names": {"/workspace/solver": "solver"},
            "host_export_targets": {"/workspace/solver": str(target)}}


def add_file(archive, path, content=b"new"):
    entry = tarfile.TarInfo(path); entry.size = len(content); entry.mode = 0o644
    archive.addfile(entry, io.BytesIO(content))


def test_bounded_scope_export_updates_only_private_solver(tmp_path):
    settings = scope(tmp_path)
    with tarfile.open(Path(settings["authority_dir"]) / "export.tar", "w") as archive:
        add_file(archive, "solver/main.py", b"print('safe')")
    sync_export(settings)
    target = Path(settings["host_export_targets"]["/workspace/solver"])
    assert not (target / "old.py").exists()
    assert (target / "main.py").read_text() == "print('safe')"
    assert not (Path(settings["authority_dir"]) / "export.tar").exists()


@pytest.mark.parametrize("attack", ["traversal", "symlink", "duplicate", "overflow"])
def test_scope_export_rejects_unsafe_paths_links_duplicates_and_size(tmp_path, attack):
    settings = scope(tmp_path)
    with tarfile.open(Path(settings["authority_dir"]) / "export.tar", "w") as archive:
        if attack == "traversal":
            add_file(archive, "solver/../../outside")
        elif attack == "symlink":
            entry = tarfile.TarInfo("solver/link"); entry.type = tarfile.SYMTYPE; entry.linkname = "/authority/scope.json"
            archive.addfile(entry)
        elif attack == "duplicate":
            add_file(archive, "solver/a"); add_file(archive, "solver/a")
        else:
            add_file(archive, "solver/a", b"x"*1025)
    with pytest.raises((PermissionError, RuntimeError)):
        sync_export(settings)
    assert (Path(settings["host_export_targets"]["/workspace/solver"]) / "old.py").read_text() == "old"


def test_monitor_sources_compile_without_executing_them():
    from maestro.execution.agent_container import _MONITOR
    from maestro.execution.sandbox import _CONTAINER_LAUNCHER
    compile(_MONITOR, "agent-container-monitor", "exec")
    compile(_CONTAINER_LAUNCHER, "candidate-container-monitor", "exec")


def test_phase_bound_outbox_writes_request_without_execution(tmp_path, monkeypatch):
    from maestro.backends.coral.submission import write_bound_eval_request
    solver = tmp_path / "solver"; solver.mkdir(); (solver / "run").write_text("bounded")
    values = {"MAESTRO_EVAL_OUTBOX": str(tmp_path / "outbox"), "MAESTRO_AGENT_ID": "a",
              "MAESTRO_PLAN_VERSION_ID": "pv", "MAESTRO_PARENT_HASH": "parent",
              "MAESTRO_ROUND_INDEX": "2", "MAESTRO_SOLVER_PATH": str(solver)}
    for key,value in values.items():
        monkeypatch.setenv(key,value)
    request = json.loads(write_bound_eval_request().read_text())
    assert request["plan_version_id"] == "pv" and request["round_index"] == 2
    assert "ticket_id" not in request
