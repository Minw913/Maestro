"""Trusted tools for a native Codex session with no native host tool access.

The model sees virtual ``solver/``, ``private/`` and ``public/`` paths. Only
this broker may mutate the private workspace. Shell commands execute in a
networkless container against detached, read-only snapshots; writable scratch
never becomes a host mount. No credential, authority database, or peer workspace
is exposed. One broker is shared by all calls in a stage, including repairs.
"""
from __future__ import annotations

from contextlib import nullcontext
import json
import math
from pathlib import Path, PurePosixPath
import shutil
import stat
import sys
import tempfile
import threading
import time

from maestro.backends.coral.workspace import AgentWorkspace
from maestro.execution.sandbox import (
    ContainmentRunner, FailClosedRunner, Mount, ResourceLimits, RunRequest,
    SandboxUnavailable,
)


_FORBIDDEN = {".git", ".codex", ".claude", ".opencode", ".ssh", ".aws",
              ".azure", ".gnupg", ".env", "auth.json", "credentials.json",
              "credentials", "id_rsa", "id_ed25519", "token", "tokens.json",
              ".coral_agent_id", "__pycache__", ".pytest_cache"}
_DEVICES = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)),
            *(f"lpt{i}" for i in range(1, 10))}
_FILE_ACTION_FIELDS = {
    "read": {"path"}, "list": {"path"},
    "search": {"path", "query", "max_results"},
    "write": {"path", "content", "executable"},
    "mkdir": {"path"}, "remove": {"path"},
}
_MAX_BATCH_OPERATIONS = 8
_MAX_TOOL_OUTPUT_BYTES = 256 * 1024


class ToolScopeExpired(PermissionError):
    """A closed, expired or revoked stage cannot issue another tool call."""


class ToolExecutionUnknown(SandboxUnavailable):
    """No trustworthy aggregate CPU receipt exists; reconcile before continuing."""

    execution_unknown = True
    inflight_unknown = True


def _unsafe(part: str) -> bool:
    folded = part.casefold()
    return (part in {"", ".", ".."} or folded in _FORBIDDEN
            or folded.split(".", 1)[0] in _DEVICES
            or part.endswith((" ", ".")) or any(ord(c) < 32 for c in part)
            or any(c in part for c in "\\:*?\"<>|"))


def _linked(path: Path) -> bool:
    """Reject Windows junctions/reparse points as well as POSIX symlinks."""
    info = path.lstat()
    return (stat.S_ISLNK(info.st_mode)
            or bool(getattr(info, "st_file_attributes", 0) & 0x400)
            or (stat.S_ISREG(info.st_mode) and info.st_nlink > 1))


class StageToolBroker:
    """A bounded callable for the single ``maestro_tool`` dynamic tool.

    ``deadline`` uses ``time.monotonic``. A GENERATION broker requires an
    unforgeable controller-owned ``grant_checker``; no arguments supplied by the
    model can substitute for that callback. Public memory is controller-published
    and always read-only. Private files persist through Coral WorkspaceStore.

    File-only ``batch`` calls run sequentially and are not transactional. All
    operation schemas and path permissions are checked before the first action;
    runtime failures stop the batch without undoing completed actions. The batch
    wrapper and every attempted operation each consume one tool-call allowance.
    """

    def __init__(self, workspace: AgentWorkspace, *, phase: str, deadline: float,
                 runner: ContainmentRunner | None = None, image: str | None = None,
                 cpu_budget: float = 30, grant_checker=None, cpu_pool=None,
                 memory_mb: int = 512, pids: int = 32,
                 max_file_bytes: int = 128 * 1024,
                 max_snapshot_bytes: int = 8 * 1024 * 1024,
                 max_snapshot_files: int = 512, max_calls: int = 128):
        if phase not in {"PLAN_RO", "GENERATION"}:
            raise ValueError("Only PLAN_RO and GENERATION tool scopes are supported")
        for name, value in (("deadline", deadline), ("cpu_budget", cpu_budget)):
            if type(value) not in {int, float} or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        for value in (memory_mb, pids, max_file_bytes, max_snapshot_bytes,
                      max_snapshot_files, max_calls):
            if type(value) is not int or value <= 0:
                raise ValueError("Tool file, call and containment limits must be positive integers")
        if phase == "GENERATION" and not callable(grant_checker):
            raise PermissionError("GENERATION tools require the controller's EditGrant verifier")
        self.workspace, self.phase, self.deadline = workspace, phase, float(deadline)
        self.runner, self.image = runner or FailClosedRunner(), image
        self.cpu_budget, self.grant_checker, self.cpu_pool = float(cpu_budget), grant_checker, cpu_pool
        self.memory_mb, self.pids = memory_mb, pids
        self.max_file_bytes, self.max_snapshot_bytes = max_file_bytes, max_snapshot_bytes
        self.max_snapshot_files, self.max_calls = max_snapshot_files, max_calls
        self._roots = {name: Path(getattr(workspace, name)).absolute()
                       for name in ("solver", "private", "public")}
        for root in self._roots.values():
            if not root.is_dir() or _linked(root):
                raise PermissionError("Tool workspace roots must be existing private directories")
        # Broker-only mutations plus this lock exclude a model-controlled
        # symlink substitution between validation and filesystem operations.
        self._lock = threading.RLock()
        self._closed, self._unknown, self._calls = False, False, 0
        self._cpu_seconds, self._wall_seconds, self._receipts = 0.0, 0.0, []
        self._cleanup_failures = 0

    def _check(self):
        if self._closed or self._unknown or time.monotonic() >= self.deadline:
            raise ToolScopeExpired("Tool stage expired, closed, or needs executor reconciliation")
        if self.phase == "GENERATION":
            self.grant_checker()

    def close(self):
        with self._lock:
            self._closed = True

    def bind_grant(self, checker, *, deadline=None):
        """Rotate an admitted repair capability without resetting any budget.

        The common invitation deadline can be shortened, never extended. A
        revoked old grant need not verify; the new controller callback must.
        Closed or unresolved stages cannot be reopened by rebinding a grant.
        """
        with self._lock:
            if self.phase != "GENERATION" or not callable(checker):
                raise PermissionError("Only GENERATION can bind a controller grant")
            if self._closed or self._unknown or time.monotonic() >= self.deadline:
                raise ToolScopeExpired("A closed or unresolved stage cannot be reopened")
            if deadline is not None:
                if (type(deadline) not in {int, float} or not math.isfinite(deadline)
                        or deadline > self.deadline or deadline <= time.monotonic()):
                    raise ValueError("Repair deadline must remain within the common stage deadline")
            checker()
            self.grant_checker = checker
            if deadline is not None:
                self.deadline = float(deadline)

    def usage(self) -> dict:
        with self._lock:
            return {"cpu_seconds": self._cpu_seconds, "wall_seconds": self._wall_seconds,
                    "cpu_budget": self.cpu_budget, "calls": self._calls,
                    "execution_unknown": self._unknown,
                    "cleanup_failures": self._cleanup_failures,
                    "receipts": [dict(item) for item in self._receipts]}

    def __call__(self, arguments: dict) -> dict:
        if not isinstance(arguments, dict):
            raise ValueError("maestro_tool arguments must be a JSON object")
        arguments = dict(arguments)
        action = arguments.pop("action", None)
        # Accept an explicit nested args object for app-server adapters too.
        if set(arguments) == {"args"} and isinstance(arguments["args"], dict):
            arguments = arguments["args"]
        return self.call(action, arguments)

    def call(self, action: str, args: dict | None = None) -> dict:
        if not isinstance(args, (dict, type(None))):
            raise ValueError("Tool arguments must be an object")
        args = args or {}
        with self._lock:
            self._check()
            if self._calls >= self.max_calls:
                raise ToolScopeExpired("Stage tool call allowance exhausted")
            self._calls += 1
            handlers = {"read": self._read, "list": self._list, "search": self._search,
                        "write": self._write, "remove": self._remove,
                        "mkdir": self._mkdir, "shell": self._shell, "batch": self._batch}
            if action not in handlers:
                raise ValueError("Unknown tool action")
            return handlers[action](args)

    def _batch(self, args):
        operations = args.get("operations")
        if (set(args) != {"operations"} or not isinstance(operations, list)
                or not 1 <= len(operations) <= _MAX_BATCH_OPERATIONS):
            raise ValueError("batch needs only an operations array with 1 to 8 file actions")
        validated = []
        # Reject malformed or unauthorized later operations before an earlier
        # write can commit. File existence is deliberately checked at execution
        # time so a batch may write a file and then read it.
        for operation in operations:
            if not isinstance(operation, dict):
                raise ValueError("Each batch operation must be a flat JSON object")
            operation = dict(operation)
            action = operation.pop("action", None)
            if not isinstance(action, str) or action not in _FILE_ACTION_FIELDS:
                raise ValueError("batch accepts file actions only; shell and nested batch are forbidden")
            if not set(operation) <= _FILE_ACTION_FIELDS[action] or "path" not in operation:
                raise ValueError("Unknown or missing batch operation fields")
            writing = action in {"write", "mkdir", "remove"}
            scope, path = self._path(operation["path"], write=writing)
            if action in {"write", "remove"} and path == self._roots[scope]:
                raise PermissionError("A virtual root cannot be replaced or removed")
            if action == "write":
                if operation["path"].endswith("/"):
                    raise PermissionError("write needs a file path, not a directory path")
                content = operation.get("content")
                if not isinstance(content, str) or ("executable" in operation
                        and type(operation["executable"]) is not bool):
                    raise ValueError("write needs text content and an optional boolean executable")
                if len(content.encode("utf-8")) > self.max_file_bytes:
                    raise ValueError("Write exceeds the stage file allowance")
            if action == "search":
                query, cap = operation.get("query"), operation.get("max_results", 64)
                if not isinstance(query, str) or not query or len(query) > 256:
                    raise ValueError("Search needs a nonempty bounded literal query")
                if type(cap) is not int or not 1 <= cap <= 128:
                    raise ValueError("max_results must be an integer between 1 and 128")
            validated.append((action, operation))
        if self.max_calls - self._calls < len(validated):
            raise ToolScopeExpired("Stage tool call allowance cannot cover this batch")
        results = []
        for action, operation in validated:
            # call() rechecks deadline, grant and allowance immediately before
            # each action. Never catch a revoked grant or UNKNOWN as file feedback.
            result = self.call(action, operation)
            results.append({"action": action, "result": result})
            response = {"results": results, "completed": len(results)}
            if len(json.dumps(response, ensure_ascii=False, allow_nan=False).encode("utf-8")) > _MAX_TOOL_OUTPUT_BYTES:
                raise ValueError("Batch response exceeds 256 KiB; completed actions are not rolled back")
        return response

    def _path(self, name: str, *, write: bool = False) -> tuple[str, Path]:
        if not isinstance(name, str) or len(name) > 1024:
            raise ValueError("Use a bounded virtual path")
        trailing = name.endswith("/")
        if trailing:
            name = name[:-1]
        relative = PurePosixPath(name)
        if (not name or name != relative.as_posix() or relative.is_absolute()
                or any(_unsafe(part) for part in name.split("/"))):
            raise PermissionError("Unsafe virtual path")
        scope = relative.parts[0]
        if scope not in self._roots:
            raise PermissionError("Only solver/, private/ and public/ are exposed")
        if write and (self.phase != "GENERATION" or scope == "public"):
            raise PermissionError("The current phase cannot modify this virtual root")
        root = self._roots[scope]
        target = root.joinpath(*relative.parts[1:])
        # No root is a writable deletion target. Checking each component avoids
        # resolving through a link before we have decided it is safe to access.
        current = root
        for part in relative.parts[1:]:
            current = current / part
            if current.exists() or current.is_symlink():
                if _linked(current):
                    raise PermissionError("Links, junctions and hardlinks are not exposed")
                if current != target and not current.is_dir():
                    raise PermissionError("A path ancestor is not a directory")
        if not target.resolve().is_relative_to(root.resolve()):
            raise PermissionError("Virtual path escaped its workspace")
        if trailing and target.exists() and not target.is_dir():
            raise NotADirectoryError("A trailing slash requires a virtual directory")
        return scope, target

    def _read(self, args):
        _, path = self._path(args.get("path"))
        if not path.is_file():
            raise FileNotFoundError("Virtual file does not exist")
        if path.stat().st_size > self.max_file_bytes:
            raise ValueError("File exceeds tool read allowance")
        data = path.read_bytes()
        if len(data) > self.max_file_bytes:
            raise ValueError("File exceeds tool read allowance")
        return {"content": data.decode("utf-8", "strict"), "bytes": len(data)}

    def _entries(self, scope: str, root: Path):
        """Bounded traversal, never descending through a forbidden component."""
        pending, visited, files, total = [root], 0, 0, 0
        while pending:
            self._check()
            directory = pending.pop()
            if _linked(directory):
                raise PermissionError("A workspace contains an unsafe link")
            with __import__("os").scandir(directory) as entries:
                children = []
                for entry in entries:
                    visited += 1
                    if visited > self.max_snapshot_files * 4:
                        raise ValueError("Directory traversal exceeds the stage allowance")
                    if _unsafe(entry.name):
                        continue
                    path = Path(entry.path)
                    if _linked(path):
                        raise PermissionError("A workspace contains an unsafe link")
                    children.append(path)
            for path in sorted(children, reverse=True):
                if path.is_dir():
                    pending.append(path)
                elif path.is_file():
                    files += 1
                    total += path.stat().st_size
                    if files > self.max_snapshot_files or total > self.max_snapshot_bytes:
                        raise ValueError("Workspace exceeds tool snapshot allowance")
                    yield path
                else:
                    raise PermissionError("Only regular files and directories are exposed")

    def _list(self, args):
        _, path = self._path(args.get("path"))
        if not path.is_dir():
            raise NotADirectoryError("Virtual directory does not exist")
        result, visited = [], 0
        with __import__("os").scandir(path) as entries:
            for entry in entries:
                visited += 1
                if visited > self.max_snapshot_files:
                    raise ValueError("Directory exceeds tool listing allowance")
                if _unsafe(entry.name):
                    continue
                child = Path(entry.path)
                if _linked(child):
                    raise PermissionError("A workspace contains an unsafe link")
                result.append({"name": entry.name, "directory": child.is_dir()})
        return {"entries": sorted(result, key=lambda item: item["name"])}

    def _search(self, args):
        scope, root = self._path(args.get("path"))
        if not root.is_dir() and not root.is_file():
            raise PermissionError("Search requires a regular file or directory")
        query, cap = args.get("query"), args.get("max_results", 64)
        if not isinstance(query, str) or not query or len(query) > 256:
            raise ValueError("Search needs a nonempty bounded literal query")
        if type(cap) is not int or not 1 <= cap <= 128:
            raise ValueError("max_results must be an integer between 1 and 128")
        matches, truncated = [], False
        paths = self._entries(scope, root) if root.is_dir() else [root]
        for path in paths:
            self._check()
            if path.stat().st_size > self.max_file_bytes:
                continue
            try:
                content = path.read_bytes().decode("utf-8", "strict")
            except UnicodeError:
                continue
            for index, line in enumerate(content.splitlines(), 1):
                if query in line:
                    if len(matches) == cap:
                        truncated = True
                        break
                    matches.append({"path": scope + "/" + path.relative_to(self._roots[scope]).as_posix(),
                                    "line": index, "text": line[:2048]})
            if truncated:
                break
        return {"matches": matches, "truncated": truncated}

    def _write(self, args):
        if isinstance(args.get("path"), str) and args["path"].endswith("/"):
            raise PermissionError("write needs a file path, not a directory path")
        scope, path = self._path(args.get("path"), write=True)
        if path == self._roots[scope]:
            raise PermissionError("A virtual root cannot be replaced")
        content = args.get("content")
        executable = args.get("executable", bool(path.exists() and path.stat().st_mode & 0o111))
        if not isinstance(content, str) or type(executable) is not bool:
            raise ValueError("write needs text content and an optional boolean executable")
        data = content.encode("utf-8")
        if len(data) > self.max_file_bytes:
            raise ValueError("Write exceeds the stage file allowance")
        path.parent.mkdir(parents=True, exist_ok=True)
        self._check()
        if path.exists():
            path.chmod(0o600)
        path.write_bytes(data)
        path.chmod(0o700 if executable else 0o600)
        return {"written": len(data)}

    def _mkdir(self, args):
        _, path = self._path(args.get("path"), write=True)
        path.mkdir(parents=True, exist_ok=True)
        return {"created": True}

    def _remove(self, args):
        scope, path = self._path(args.get("path"), write=True)
        if path == self._roots[scope]:
            raise PermissionError("A virtual root cannot be removed")
        # Recursive deletion is unnecessary authority for a coding tool.
        if path.is_dir():
            path.rmdir()
        else:
            path.unlink()
        return {"removed": True}

    def _snapshot(self, destination: Path):
        count = total = 0
        for scope, root in self._roots.items():
            output = destination / scope
            output.mkdir()
            for source in self._entries(scope, root):
                target = output / source.relative_to(root)
                target.parent.mkdir(parents=True, exist_ok=True)
                data = source.read_bytes()
                count += 1
                total += len(data)
                if count > self.max_snapshot_files or total > self.max_snapshot_bytes:
                    raise ValueError("Combined workspace exceeds tool snapshot allowance")
                target.write_bytes(data)
                target.chmod(0o555 if source.stat().st_mode & 0o111 else 0o444)
            for item in output.rglob("*"):
                if item.is_dir():
                    item.chmod(0o555)
            output.chmod(0o555)

    def _shell(self, args):
        command = args.get("command")
        if not isinstance(command, str) or not command.strip() or len(command.encode()) > 16 * 1024:
            raise ValueError("shell needs a bounded command string")
        remaining_cpu = self.cpu_budget - self._cpu_seconds
        if remaining_cpu <= 0:
            raise ToolScopeExpired("Stage aggregate tool CPU allowance exhausted")
        if not isinstance(self.image, str) or "@sha256:" not in self.image:
            raise SandboxUnavailable("Tool execution needs the digest-pinned candidate image")
        # The temporary directory owns only detached copies of allowed roots.
        # It is never populated with host credentials or the ledger.
        snapshot = Path(tempfile.mkdtemp(prefix=".maestro-tools-", dir=self.workspace.root))
        try:
            self._snapshot(snapshot)
            with self.cpu_pool.slot() if self.cpu_pool is not None else nullcontext():
                self._check()
                remaining_wall = self.deadline - time.monotonic()
                if remaining_wall <= 0:
                    raise ToolScopeExpired("Tool stage elapsed while waiting for CPU")
                limits = ResourceLimits(cpu_seconds=remaining_cpu, wall_seconds=remaining_wall,
                    memory_mb=self.memory_mb, pids=self.pids, output_bytes=self.max_file_bytes, cpus=1)
                request = RunRequest(
                    argv=("sh", "-c", "mkdir -p /scratch/work && cp -R /candidate/. /scratch/work/ "
                          "&& chmod -R u+w /scratch/work && cd /scratch/work && " + command),
                    workdir=snapshot / "solver", image=self.image,
                    mounts=(Mount(snapshot / "solver", "/candidate"),
                            Mount(snapshot / "private", "/input/private"),
                            Mount(snapshot / "public", "/input/public")))
                try:
                    receipt = self.runner.run(request, limits)
                    if (type(receipt.cpu_seconds) not in {int, float}
                            or type(receipt.wall_seconds) not in {int, float}
                            or not math.isfinite(receipt.cpu_seconds)
                            or not math.isfinite(receipt.wall_seconds)
                            or min(receipt.cpu_seconds, receipt.wall_seconds) < 0
                            or receipt.diagnostics.get("cpu_measurement_incomplete")
                            or receipt.diagnostics.get("aggregate_cgroup_cpu") is not True):
                        raise ToolExecutionUnknown("Tool executor omitted a trustworthy aggregate CPU receipt")
                except BaseException as error:
                    # An executor exception cannot establish how much CPU
                    # was consumed. No new tool may retry under a fresh cap.
                    self._unknown = True
                    if self.cpu_pool is not None:
                        self.cpu_pool.poison("Unresolved stage tool execution")
                    if isinstance(error, Exception):
                        if getattr(error, "execution_unknown", False):
                            raise
                        raise ToolExecutionUnknown(
                            "Tool executor stopped without a trustworthy final receipt") from error
                    # Preserve interrupts while making the outer phase
                    # owner retain its unresolved execution barrier.
                    error.execution_unknown = True
                    error.inflight_unknown = True
                    raise
                self._cpu_seconds += receipt.cpu_seconds
                self._wall_seconds += receipt.wall_seconds
                self._receipts.append({"returncode": receipt.returncode,
                    "cpu_seconds": receipt.cpu_seconds, "wall_seconds": receipt.wall_seconds,
                    "timed_out": receipt.timed_out})
                return {"returncode": receipt.returncode,
                        "stdout": receipt.stdout[:self.max_file_bytes],
                        "stderr": receipt.stderr[:self.max_file_bytes],
                        "cpu_seconds": receipt.cpu_seconds, "wall_seconds": receipt.wall_seconds,
                        "timed_out": receipt.timed_out,
                        "remaining_cpu_seconds": max(0.0, self.cpu_budget - self._cpu_seconds)}
        finally:
            primary = sys.exc_info()[1]
            try:
                # A cleanup failure must not hide an unknown executor state.
                # Paths here are detached files owned by this broker only.
                for item in snapshot.rglob("*"):
                    item.chmod(0o700 if item.is_dir() else 0o600)
                resolved, allowed = snapshot.resolve(strict=True), Path(self.workspace.root).resolve(strict=True)
                if resolved == allowed or not resolved.is_relative_to(allowed):
                    raise PermissionError("Detached tool snapshot escaped its controller workspace")
                shutil.rmtree(snapshot)
            except BaseException:
                self._cleanup_failures += 1
                if primary is None:
                    raise


MAESTRO_TOOL_SPEC = {
    "name": "maestro_tool",
    "description": (
        "Read/search/list solver/, private/ memory and public/ shared memory. "
        "During GENERATION write/mkdir/remove solver/ or private/ files; public/ stays read-only. "
        "shell executes a networkless CPU-bounded scratch copy of solver/ with private memory at "
        "/input/private and public memory at /input/public. Shell edits do not persist; use write "
        "for lasting changes. batch runs 1 to 8 related file operations sequentially, e.g. read "
        "private and public memory together, or write solver and private notes together. It forbids "
        "shell/nested batches, is not transactional, stops at the first error, and returns at most "
        "256 KiB. The wrapper and each operation consume one tool-call allowance. "
        "Never request host paths or credentials."),
    "inputSchema": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["read", "list", "search", "write", "mkdir", "remove", "shell", "batch"]},
            "path": {"type": "string"}, "content": {"type": "string"},
            "query": {"type": "string"}, "command": {"type": "string"},
            "max_results": {"type": "integer"}, "executable": {"type": "boolean"},
            "operations": {"type": "array", "minItems": 1, "maxItems": _MAX_BATCH_OPERATIONS,
                "items": {"type": "object", "properties": {
                    "action": {"type": "string", "enum": list(_FILE_ACTION_FIELDS)},
                    "path": {"type": "string"}, "content": {"type": "string"},
                    "query": {"type": "string"}, "max_results": {"type": "integer"},
                    "executable": {"type": "boolean"},
                }, "required": ["action", "path"], "additionalProperties": False}},
        },
        "required": ["action"], "additionalProperties": False,
    },
}
