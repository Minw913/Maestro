"""Offline dynamic-tool isolation checks. No container or model is started."""
from contextlib import contextmanager
from pathlib import Path
import time

import pytest

from maestro.backends.codex_session.tools import (
    MAESTRO_TOOL_SPEC, StageToolBroker, ToolExecutionUnknown, ToolScopeExpired,
)
from maestro.backends.coral.workspace import AgentWorkspace
from maestro.execution.sandbox import RunReceipt, SandboxUnavailable


IMAGE = "candidate@sha256:" + "a" * 64


def workspace(tmp_path):
    root = tmp_path / "agent-a"
    root.mkdir()
    for name in ("solver", "private", "public", "outbox"):
        (root / name).mkdir()
    (root / "solver" / "run").write_text("#!/bin/sh\necho hello\n", newline="\n")
    (root / "private" / "notes.md").write_text("local memory\n", newline="\n")
    (root / "public" / "board.md").write_text("published observation\n", newline="\n")
    return AgentWorkspace("a", root, *(root / name for name in ("solver", "private", "public", "outbox")))


class Runner:
    def __init__(self, receipts=()):
        self.receipts, self.calls = list(receipts), []

    def run(self, request, limits):
        self.calls.append((request, limits))
        assert {mount.target for mount in request.mounts} == {
            "/candidate", "/input/private", "/input/public"}
        assert all(mount.read_only for mount in request.mounts)
        for mount in request.mounts:
            assert mount.source.is_dir()
            assert not (mount.source / ".git").exists()
            assert not (mount.source / "auth.json").exists()
        if self.receipts:
            receipt = self.receipts.pop(0)
            if isinstance(receipt, BaseException):
                raise receipt
            return receipt
        return metered()


def metered(cpu=.25, wall=.5, **kwargs):
    return RunReceipt(0, "ok", "", cpu, wall,
                      diagnostics={"aggregate_cgroup_cpu": True}, **kwargs)


def broker(tmp_path, **kwargs):
    ws = workspace(tmp_path)
    options = {"phase": "GENERATION", "deadline": time.monotonic() + 30,
               "runner": Runner(), "image": IMAGE, "grant_checker": lambda: None}
    options.update(kwargs)
    return ws, StageToolBroker(ws, **options)


def test_generation_reads_edits_searches_and_persists_private_memory(tmp_path):
    ws, tools = broker(tmp_path)
    assert tools({"action": "read", "path": "private/notes.md"})["content"] == "local memory\n"
    tools.call("write", {"path": "private/strategy.md", "content": "next strategy"})
    tools.call("write", {"path": "solver/src/solver.py", "content": "answer = 42"})
    matches = tools.call("search", {"path": "solver", "query": "42"})["matches"]
    assert matches == [{"path": "solver/src/solver.py", "line": 1, "text": "answer = 42"}]
    assert (ws.private / "strategy.md").read_text() == "next strategy"
    entries = tools.call("list", {"path": "solver/src"})["entries"]
    assert entries == [{"name": "solver.py", "directory": False}]
    tools.call("remove", {"path": "solver/src/solver.py"})
    tools.call("remove", {"path": "solver/src"})
    assert not (ws.solver / "src").exists()


def test_one_trailing_directory_slash_is_accepted_without_normalizing_traversal(tmp_path):
    _, tools = broker(tmp_path)
    for root in ("solver", "private", "public"):
        assert tools.call("list", {"path": root + "/"}) == tools.call("list", {"path": root})
    tools.call("mkdir", {"path": "solver/new/"})
    assert tools.call("list", {"path": "solver/new/"}) == {"entries": []}
    for path in ("solver//", "solver/../", "solver//new/"):
        with pytest.raises(PermissionError):
            tools.call("list", {"path": path})
    with pytest.raises(NotADirectoryError):
        tools.call("read", {"path": "solver/run/"})


def test_public_memory_is_controller_published_readonly(tmp_path):
    ws, tools = broker(tmp_path)
    assert "published" in tools.call("read", {"path": "public/board.md"})["content"]
    for action, args in (("write", {"content": "leak"}), ("remove", {}), ("mkdir", {})):
        with pytest.raises(PermissionError):
            tools.call(action, {"path": "public/board.md", **args})
    assert (ws.public / "board.md").read_text() == "published observation\n"


@pytest.mark.parametrize("path", [
    "../private/notes.md", "solver/../private/notes.md", "/etc/passwd",
    "C:/Users/auth.json", "solver\\run", "solver/./run", "solver//run",
    "solver/.git/config", "solver/.codex/auth.json", "private/auth.json",
    "private/.env", "solver/NUL.txt", "solver/name:stream", "solver/trailing.",
    "peer-b/private/notes.md", "outbox/plan.json", "solver/x\x00y",
])
def test_virtual_paths_never_expose_host_credentials_or_peers(tmp_path, path):
    _, tools = broker(tmp_path)
    with pytest.raises((PermissionError, ValueError)):
        tools.call("read", {"path": path})


def test_plan_tools_do_not_modify_solver_or_private_memory(tmp_path):
    ws, tools = broker(tmp_path, phase="PLAN_RO", grant_checker=None)
    assert "hello" in tools.call("read", {"path": "solver/run"})["content"]
    for root in ("solver", "private"):
        for action, args in (("write", {"content": "bad"}), ("mkdir", {}), ("remove", {})):
            with pytest.raises(PermissionError):
                tools.call(action, {"path": f"{root}/notes.md", **args})
    assert (ws.private / "notes.md").read_text() == "local memory\n"


def test_generation_requires_trusted_edit_grant_callback(tmp_path):
    with pytest.raises(PermissionError, match="EditGrant"):
        broker(tmp_path, grant_checker=None)


def test_revoked_grant_blocks_every_action_including_reads(tmp_path):
    state = {"valid": True}
    def check():
        if not state["valid"]:
            raise PermissionError("revoked")
    _, tools = broker(tmp_path, grant_checker=check)
    tools.call("read", {"path": "solver/run"})
    state["valid"] = False
    for action, args in (("read", {"path": "solver/run"}),
                         ("shell", {"command": "echo bad"}),
                         ("write", {"path": "solver/run", "content": "bad"})):
        with pytest.raises(PermissionError, match="revoked"):
            tools.call(action, args)


def test_expired_scope_and_closed_scope_cannot_resume(tmp_path):
    _, tools = broker(tmp_path)
    tools.deadline = time.monotonic() - 1
    with pytest.raises(ToolScopeExpired):
        tools.call("list", {"path": "solver"})
    tools.deadline = time.monotonic() + 10
    tools.close()
    with pytest.raises(ToolScopeExpired):
        tools.call("list", {"path": "solver"})


def test_shell_uses_detached_snapshots_without_git_or_credentials(tmp_path):
    runner = Runner()
    ws, tools = broker(tmp_path, runner=runner)
    (ws.solver / ".git").mkdir()
    (ws.solver / ".git" / "config").write_text("private repository data")
    (ws.private / "auth.json").write_text("MUST NOT REACH MODEL")
    original = (ws.solver / "run").read_text()
    tools.call("shell", {"command": "echo changed > run; cat /input/private/notes.md"})
    request, limits = runner.calls[0]
    assert request.argv[:2] == ("sh", "-c")
    assert "cd /scratch/work" in request.argv[2]
    assert all(not mount.source.is_relative_to(live_root)
               for mount in request.mounts for live_root in (ws.solver, ws.private, ws.public))
    assert not request.workdir.exists()  # detached snapshot cleaned after receipt
    assert (ws.solver / "run").read_text() == original
    assert limits.cpus == 1 and limits.cpu_seconds == 30
    assert 0 < limits.wall_seconds <= 30
    assert tools.usage()["cpu_seconds"] == .25


def test_shell_cpu_budget_is_cumulative_across_all_calls(tmp_path):
    runner = Runner([metered(cpu=2), metered(cpu=3)])
    _, tools = broker(tmp_path, runner=runner, cpu_budget=5)
    first = tools.call("shell", {"command": "true"})
    second = tools.call("shell", {"command": "true"})
    assert first["remaining_cpu_seconds"] == 3
    assert second["remaining_cpu_seconds"] == 0
    assert [limits.cpu_seconds for _, limits in runner.calls] == [5, 3]
    assert tools.usage()["cpu_seconds"] == 5
    with pytest.raises(ToolScopeExpired, match="CPU"):
        tools.call("shell", {"command": "true"})
    assert len(runner.calls) == 2


@pytest.mark.parametrize("receipt", [
    RunReceipt(0, "", "", 0, .1),
    RunReceipt(0, "", "", float("nan"), .1, diagnostics={"aggregate_cgroup_cpu": True}),
    RunReceipt(0, "", "", -1, .1, diagnostics={"aggregate_cgroup_cpu": True}),
    RunReceipt(0, "", "", 0, .1, diagnostics={"aggregate_cgroup_cpu": True,
                                               "cpu_measurement_incomplete": True}),
])
def test_incomplete_metering_blocks_further_tools_and_poison_pool(tmp_path, receipt):
    class Pool:
        def __init__(self):
            self.poisoned = False
        @contextmanager
        def slot(self):
            yield
        def poison(self, reason):
            self.poisoned = True
    pool = Pool()
    _, tools = broker(tmp_path, runner=Runner([receipt]), cpu_pool=pool)
    with pytest.raises(ToolExecutionUnknown):
        tools.call("shell", {"command": "true"})
    assert pool.poisoned and tools.usage()["execution_unknown"]
    with pytest.raises(ToolScopeExpired):
        tools.call("read", {"path": "solver/run"})


def test_runner_failure_is_not_retried_as_free_cpu(tmp_path):
    runner = Runner([SandboxUnavailable("missing receipt")])
    _, tools = broker(tmp_path, runner=runner)
    with pytest.raises(ToolExecutionUnknown) as error:
        tools.call("shell", {"command": "true"})
    assert error.value.execution_unknown and error.value.inflight_unknown
    with pytest.raises(ToolScopeExpired):
        tools.call("shell", {"command": "true"})
    assert len(runner.calls) == 1


def test_grant_rechecked_after_waiting_for_cpu(tmp_path):
    state = {"valid": True}
    def check():
        if not state["valid"]:
            raise PermissionError("grant revoked during queue wait")
    class Pool:
        @contextmanager
        def slot(self):
            state["valid"] = False
            yield
    runner = Runner()
    _, tools = broker(tmp_path, runner=runner, grant_checker=check, cpu_pool=Pool())
    with pytest.raises(PermissionError, match="queue wait"):
        tools.call("shell", {"command": "true"})
    assert not runner.calls


def test_snapshot_rejects_hardlinks_and_symlinks(tmp_path, monkeypatch):
    ws, tools = broker(tmp_path)
    external = tmp_path / "host-secret"
    external.write_text("secret")
    linked = ws.solver / "hardlink"
    # Windows app containers can deny creating links even in writable roots.
    # Exercise the same inode metadata without requiring that OS privilege.
    linked.write_bytes(external.read_bytes())
    original_lstat = Path.lstat
    def lstat(path, *args, **kwargs):
        info = original_lstat(path, *args, **kwargs)
        if path == linked:
            from types import SimpleNamespace
            return SimpleNamespace(st_mode=info.st_mode, st_nlink=2,
                                   st_file_attributes=getattr(info, "st_file_attributes", 0))
        return info
    monkeypatch.setattr(Path, "lstat", lstat)
    with pytest.raises(PermissionError, match="link"):
        tools.call("read", {"path": "solver/hardlink"})
    with pytest.raises(PermissionError, match="link"):
        tools.call("shell", {"command": "true"})
    assert not tools.runner.calls


def test_schema_exposes_no_host_execution_or_credential_fields():
    properties = MAESTRO_TOOL_SPEC["inputSchema"]["properties"]
    assert "shell" in properties["action"]["enum"]
    assert not {"cwd", "env", "token", "mounts", "host_path"} & set(properties)


def test_snapshot_and_file_boundaries_are_enforced_before_execution(tmp_path):
    runner = Runner()
    ws, tools = broker(tmp_path, runner=runner, max_file_bytes=32,
                       max_snapshot_bytes=64)
    with pytest.raises(ValueError, match="Write"):
        tools.call("write", {"path": "solver/large", "content": "x" * 33})
    (ws.solver / "large").write_text("x" * 100)
    with pytest.raises(ValueError, match="read"):
        tools.call("read", {"path": "solver/large"})
    with pytest.raises(ValueError, match="snapshot"):
        tools.call("shell", {"command": "true"})
    assert not runner.calls


def test_no_shell_fallback_without_verified_candidate_image(tmp_path):
    runner = Runner()
    _, tools = broker(tmp_path, runner=runner, image="candidate:latest")
    with pytest.raises(SandboxUnavailable, match="digest-pinned"):
        tools.call("shell", {"command": "true"})
    assert not runner.calls


def test_call_limit_blocks_unbounded_tool_output_loops(tmp_path):
    _, tools = broker(tmp_path, max_calls=1)
    tools.call("list", {"path": "solver"})
    with pytest.raises(ToolScopeExpired, match="allowance"):
        tools.call("list", {"path": "solver"})


def test_repair_grant_binding_preserves_cpu_and_call_usage(tmp_path):
    runner = Runner([metered(cpu=2), metered(cpu=3)])
    _, tools = broker(tmp_path, runner=runner, cpu_budget=5)
    tools.call("shell", {"command": "true"})
    before = tools.usage()
    verified = []
    tools.bind_grant(lambda: verified.append("new"))
    assert verified == ["new"]
    assert tools.usage() == before
    tools.call("shell", {"command": "true"})
    assert [limits.cpu_seconds for _, limits in runner.calls] == [5, 3]
    assert tools.usage()["calls"] == 2 and tools.usage()["cpu_seconds"] == 5
    with pytest.raises(ValueError, match="common"):
        tools.bind_grant(lambda: None, deadline=tools.deadline + 10)


def test_unknown_or_closed_stage_cannot_rebind_a_fresh_grant(tmp_path):
    _, tools = broker(tmp_path, runner=Runner([RuntimeError("missing receipt")]))
    with pytest.raises(ToolExecutionUnknown):
        tools.call("shell", {"command": "true"})
    with pytest.raises(ToolScopeExpired):
        tools.bind_grant(lambda: None)
    assert tools.usage()["execution_unknown"]


def test_snapshot_cleanup_failure_cannot_hide_unknown_execution(tmp_path, monkeypatch):
    _, tools = broker(tmp_path, runner=Runner([RuntimeError("transport disappeared")]))
    def denied(path):
        raise PermissionError("cleanup unavailable")
    monkeypatch.setattr("maestro.backends.codex_session.tools.shutil.rmtree", denied)
    with pytest.raises(ToolExecutionUnknown) as error:
        tools.call("shell", {"command": "true"})
    assert error.value.execution_unknown and error.value.inflight_unknown
    assert tools.usage()["cleanup_failures"] == 1


def test_writing_existing_executable_preserves_mode_unless_explicitly_changed(tmp_path, monkeypatch):
    import os
    ws, tools = broker(tmp_path)
    run = ws.solver / "run"
    original_stat, original_chmod = Path.stat, Path.chmod
    modes = []
    def stat(path, *args, **kwargs):
        info = original_stat(path, *args, **kwargs)
        if path == run and kwargs.get("follow_symlinks", True):
            return os.stat_result((info.st_mode | 0o111, *info[1:]))
        return info
    def chmod(path, mode, *args, **kwargs):
        if path == run:
            modes.append(mode)
        return original_chmod(path, mode, *args, **kwargs)
    monkeypatch.setattr(Path, "stat", stat)
    monkeypatch.setattr(Path, "chmod", chmod)
    tools.call("write", {"path": "solver/run", "content": "#!/bin/sh\ntrue\n"})
    assert modes[-1] == 0o700
    tools.call("write", {"path": "solver/run", "content": "data", "executable": False})
    assert modes[-1] == 0o600
