"""Default generation boundary: rootless Docker + Unix-socket inference gateway.

No agent network interface exists. Its localhost relay reaches only the mounted
Unix quota socket. This avoids granting an internal-network route to arbitrary
host ports. Real use requires an audited digest-pinned image with the runtime.
"""
from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import shutil
import sys
import time
import threading
import uuid

from maestro.backends.base import BackendUnavailable
from maestro.execution.budgets import BudgetStore, BudgetExceeded
from maestro.execution.gateway import BudgetGateway, GatewayHTTPServer, HTTPUpstream
from maestro.execution.sandbox import DockerRunner, ResourceLimits, RunRequest, SandboxUnavailable


class DockerAgentSandboxProvider:
    def __init__(self, authority_root, store: BudgetStore, *, image: str,
                 upstream: HTTPUpstream, runtime_command: str = "claude",
                 limits: ResourceLimits = ResourceLimits(), max_output_tokens: int = 4096,
                 allowed_model: str | None = None):
        self.root = Path(authority_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.store, self.image, self.runtime_command = store, image, runtime_command
        self.limits, self.max_output_tokens = limits, max_output_tokens
        self.runner = DockerRunner(image)
        self.gateway = BudgetGateway(store, upstream, allow_streaming=True, allowed_model=allowed_model)
        self.server = None
        self._scopes = {}
        self._checked = False
        self._closed = False
        self._lifecycle_lock = threading.RLock()
        self.store.connection.executescript("""
        CREATE TABLE IF NOT EXISTS compute_budgets(account TEXT PRIMARY KEY,max_cpu REAL NOT NULL,
          consumed_cpu REAL NOT NULL DEFAULT 0,deadline REAL NOT NULL,blocked INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS compute_scopes(id TEXT PRIMARY KEY,account TEXT NOT NULL,
          cpu_reserved REAL NOT NULL,status TEXT NOT NULL,receipt TEXT);
        """)

    def preflight(self):
        with self._lifecycle_lock:
            return self._preflight_locked()

    def _preflight_locked(self):
        if self._closed:
            raise BackendUnavailable("This Coral boundary has been closed; construct a fresh episode owner")
        if os.name != "posix" or not hasattr(__import__("socket"), "AF_UNIX"):
            raise BackendUnavailable("Default Coral generation requires Linux/rootless Docker and Unix sockets; Windows execution is denied")
        if self.runtime_command != "claude":
            raise BackendUnavailable("Default network gateway currently qualifies only the claude_code runtime; use an audited custom provider for another CLI")
        if not self._checked:
            self.runner.preflight()
            checked = self.runner.run(RunRequest(("claude", "--version"), self.root, image=self.image),
                ResourceLimits(cpu_seconds=5,wall_seconds=10,memory_mb=256,pids=16,output_bytes=65536))
            if checked.returncode:
                raise BackendUnavailable("Generation image lacks the required runtime, Python 3, or cgroup metering")
            if len(str(self.root / "gateway.sock").encode()) >= 100:
                raise BackendUnavailable("Coral authority root is too long for a portable Unix socket path; choose a short run_root")
            self._checked = True
        return {key: True for key in ("filesystem_isolation", "process_tree_limits",
                "monotonic_phase_deadline", "gateway_only_inference", "persistent_inference_quota",
                "private_git_objects")} | {"simulation": False, "image": self.image}

    def _start_gateway(self):
        with self._lifecycle_lock:
            if self._closed:
                raise BackendUnavailable("Closed Coral boundary cannot reopen its gateway")
            if self.server is None:
                self.server = GatewayHTTPServer(self.gateway, unix_socket=self.root / "gateway.sock").start()

    @staticmethod
    def _writable_tree(path: Path):
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(0o777)
        for item in path.rglob("*"):
            if item.is_symlink():
                raise PermissionError("Agent filesystem contains a symlink outside the immutable artifact contract")
            item.chmod(0o777 if item.is_dir() else (0o777 if item.stat().st_mode & 0o111 else 0o666))

    def prepare_phase(self, context):
        self.preflight()
        self._start_gateway()
        if context.phase != "GENERATE" or not context.budget_scope:
            raise PermissionError("Default autonomous runtime accepts only ticket-bound generation scopes")
        remaining_wall = context.deadline - time.monotonic()
        if remaining_wall <= 0:
            raise TimeoutError("Generation deadline elapsed")
        account = context.budget_scope
        now_deadline = time.time() + remaining_wall
        try:
            self.store.snapshot(account)
        except BudgetExceeded:
            self.store.create(account, max_calls=context.max_calls,
                max_request_bytes=24_000_000, max_output_tokens=context.max_calls*self.max_output_tokens,
                deadline=now_deadline)
        scope_id = "maestro-" + uuid.uuid4().hex
        with self.store._transaction():
            db = self.store.connection
            row = db.execute("SELECT * FROM compute_budgets WHERE account=?", (account,)).fetchone()
            if row is None:
                db.execute("INSERT INTO compute_budgets VALUES(?,?,0,?,0)", (account,self.limits.cpu_seconds,now_deadline))
                remaining_cpu = self.limits.cpu_seconds
            else:
                if row["blocked"] or time.time() >= row["deadline"]:
                    raise BudgetExceeded("Generation compute account is blocked or expired")
                remaining_cpu = row["max_cpu"] - row["consumed_cpu"]
            if remaining_cpu <= 0 or db.execute("SELECT 1 FROM compute_scopes WHERE account=? AND status='ACTIVE'",(account,)).fetchone():
                raise BudgetExceeded("No unused generation CPU remains, or an older scope is unresolved")
            db.execute("INSERT INTO compute_scopes VALUES(?,?,?,'ACTIVE',NULL)", (scope_id,account,remaining_cpu))
        phase = self.root / scope_id
        phase.mkdir(mode=0o700)
        token = self.gateway.credential(account, context.agent_id)
        workspace = context.workspace
        workspace.root.chmod(0o755)
        home = workspace.root / "runtime-home"
        for directory in (workspace.solver, workspace.private, workspace.outbox, home):
            self._writable_tree(directory)
        workspace.public.mkdir(exist_ok=True)
        workspace.public.chmod(0o755)
        mounts = [[str(workspace.root), str(workspace.root), True],
                  [str(workspace.root), "/seed/workspace", True],
                  [str(self.root / "gateway.sock"), "/gateway/socket", True],
                  [str(phase), "/authority", False]]
        writable = [[str(workspace.solver), "/seed/workspace/solver"],
                    [str(workspace.private), "/seed/workspace/private"],
                    [str(workspace.outbox), "/seed/workspace/outbox"],
                    ["/home/maestro", "/seed/workspace/runtime-home"]]
        names = {target: name for (target, _), name in zip(writable, ("solver", "private", "outbox", "runtime-home"))}
        limits = {**asdict(self.limits), "cpu_seconds": remaining_cpu,
                  "wall_seconds": min(remaining_wall, self.limits.wall_seconds)}
        gateway_port = 37261
        settings = {"docker": self.runner.executable, "image": self.image,
                    "runtime_command": self.runtime_command, "container_name": scope_id,
                    "limits": limits, "mounts": mounts, "workdir": str(workspace.solver),
                    "writable": writable, "export_names": names,
                    "host_export_targets": {target: str(workspace.root/name) for target,name in names.items()},
                    "authority_dir": str(phase), "workspace_bytes": 64*1024*1024, "workspace_files": 8192,
                    "gateway_port": gateway_port, "environment": {
                        "HOME": "/home/maestro", "IS_SANDBOX": "1",
                        "ANTHROPIC_BASE_URL": f"http://127.0.0.1:{gateway_port}",
                        "ANTHROPIC_API_KEY": token, "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                        "CLAUDE_CODE_MAX_OUTPUT_TOKENS": str(self.max_output_tokens),
                        "MAESTRO_EVAL_OUTBOX": str(workspace.outbox), "MAESTRO_AGENT_ID": context.agent_id,
                        "MAESTRO_PLAN_VERSION_ID": context.plan_version_id or "",
                        "MAESTRO_PARENT_HASH": context.parent_hash or "",
                        "MAESTRO_ROUND_INDEX": str(context.round_index) if context.round_index is not None else "",
                        "MAESTRO_SOLVER_PATH": str(workspace.solver)}}
        scope_path = phase / "scope.json"
        scope_path.write_text(json.dumps(settings), encoding="utf-8")
        scope_path.chmod(0o600)
        self._scopes[(context.agent_id, context.epoch)] = (scope_id, account, token, phase)
        try:
            from coral.sandbox.protocol import AgentSandboxSpec
        except ImportError as error:
            raise BackendUnavailable("Pinned Coral must be installed for its AgentSandboxSpec") from error
        return AgentSandboxSpec(command_prefix=[sys.executable,
            str(Path(__file__).parents[2] / "execution" / "agent_container.py"), "--scope", str(scope_path), "--"],
            env={"MAESTRO_GATEWAY_URL": f"http://127.0.0.1:{gateway_port}", "MAESTRO_GATEWAY_TOKEN": token})

    def stop_scope(self, agent_id: str, epoch: int):
        scope = self._scopes.get((agent_id, epoch))
        if scope is None:
            return
        scope_id, account, token, directory = scope
        self.gateway.revoke(token)
        self.runner._docker(["rm", "--force", scope_id])
        receipt_path = directory / "receipt.json"
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        with self.store._transaction():
            db = self.store.connection
            row = db.execute("SELECT * FROM compute_scopes WHERE id=?", (scope_id,)).fetchone()
            if row["status"] != "ACTIVE":
                return
            trustworthy = (isinstance(receipt, dict) and receipt.get("status") in ("COMPLETED", "LIMIT")
                           and isinstance(receipt.get("cpu_seconds"), (int, float))
                           and 0 <= receipt["cpu_seconds"] <= row["cpu_reserved"] + 0.2)
            used = min(row["cpu_reserved"], receipt["cpu_seconds"]) if trustworthy else row["cpu_reserved"]
            db.execute("UPDATE compute_budgets SET consumed_cpu=consumed_cpu+?,blocked=max(blocked,?) WHERE account=?",(used,0 if trustworthy else 1,account))
            db.execute("UPDATE compute_scopes SET status=?,receipt=? WHERE id=?",("COMPLETED" if trustworthy else "UNKNOWN",json.dumps(receipt),scope_id))

    def quiescent(self, agent_id: str, epoch: int) -> bool:
        scope = self._scopes.get((agent_id, epoch))
        if scope is None:
            return True
        probe = self.runner._docker(["inspect", scope[0]])
        # A disconnected/denied daemon is not evidence of an absent container.
        return probe.returncode != 0 and any(message in probe.stderr.lower()
            for message in ("no such object:", "no such container:"))

    def close(self):
        with self._lifecycle_lock:
            self._close_locked()

    def _close_locked(self):
        if self._closed:
            return
        for agent_id, epoch in list(self._scopes):
            self.stop_scope(agent_id, epoch)
        if self.server:
            self.server.close()
            self.server = None
        self._closed = True

    def usage(self, account):
        try:
            measured = self.store.usage(account)
        except BudgetExceeded:
            return {}
        row = self.store.connection.execute("SELECT consumed_cpu FROM compute_budgets WHERE account=?", (account,)).fetchone()
        scopes = self.store.connection.execute("SELECT receipt FROM compute_scopes WHERE account=? AND receipt IS NOT NULL", (account,)).fetchall()
        wall = sum((json.loads(item[0]) or {}).get("wall_seconds", 0) for item in scopes)
        return {**measured, "cpu_seconds": row[0] if row else 0, "wall_seconds": wall}
