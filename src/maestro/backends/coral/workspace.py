"""Private agent repositories and immutable solver artifacts.

Independent repositories, not linked Git worktrees: private historical blobs
must not be discoverable via another agent's shared Git object database.
Filesystem privacy additionally requires an enforcing sandbox provider.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


_IGNORED = {".git", ".claude", ".codex", ".opencode", "__pycache__", ".pytest_cache", ".coral_agent_id"}


def remove_private_path(path: Path, allowed_root: Path):
    """Delete only a verified private workspace path; handle readonly Windows copies."""
    path, allowed_root = Path(path), Path(allowed_root).resolve()
    if path.is_symlink():
        path.unlink()
        return
    if not path.resolve().is_relative_to(allowed_root) or path.resolve() == allowed_root:
        raise PermissionError("Refusing to remove a path outside its private workspace")
    if path.is_dir():
        for item in path.rglob("*"):
            if not item.is_symlink():
                item.chmod(0o777 if item.is_dir() else 0o666)
        path.chmod(0o777)
        shutil.rmtree(path)
    else:
        path.chmod(0o666)
        path.unlink()


def solver_files(path: Path):
    base = Path(path).resolve(strict=True)
    for item in sorted(base.rglob("*"), key=lambda p: p.relative_to(base).as_posix()):
        relative = item.relative_to(base)
        if any(part in _IGNORED for part in relative.parts):
            continue
        if item.is_symlink():
            raise PermissionError(f"Solver artifacts may not contain symlinks: {relative}")
        if item.is_file():
            yield item, relative


def candidate_tree_hash(path: str | Path) -> str:
    digest = hashlib.sha256()
    for item, relative in solver_files(Path(path)):
        name = relative.as_posix().encode("utf-8")
        content = item.read_bytes()
        digest.update(len(name).to_bytes(8, "big")); digest.update(name)
        digest.update(len(content).to_bytes(8, "big")); digest.update(content)
        # Executability changes candidate behavior and belongs in its identity.
        digest.update(b"x" if item.stat().st_mode & 0o111 else b"-")
    return digest.hexdigest()


@dataclass(frozen=True)
class AgentWorkspace:
    agent_id: str
    root: Path
    solver: Path
    private: Path
    public: Path
    outbox: Path


class WorkspaceStore:
    def __init__(self, root: str | Path, artifact_root: str | Path):
        self.root, self.artifact_root = Path(root).resolve(), Path(artifact_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.artifact_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        self.artifact_root.chmod(0o700)

    def create(self, agent_id: str, seed: str | Path) -> AgentWorkspace:
        if not agent_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in agent_id):
            raise ValueError("Unsafe agent ID")
        root = self.root / agent_id
        if root.exists():
            raise FileExistsError(f"Agent workspace already exists: {root}")
        root.mkdir(mode=0o700)
        solver = root / "solver"
        solver.mkdir(mode=0o700)
        for source, relative in solver_files(Path(seed)):
            destination = solver / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        # Fresh independent Git object database, with no alternates or remotes.
        for args in (["init", "--quiet"], ["config", "user.name", "MAESTRO"],
                     ["config", "user.email", "maestro@local.invalid"], ["add", "."],
                     ["commit", "--allow-empty", "--quiet", "-m", "Verified seed snapshot"]):
            subprocess.run(["git", "-C", str(solver), *args], check=True,
                           capture_output=True, text=True)
        for name in ("private", "public", "outbox"):
            (root / name).mkdir(mode=0o700)
        return AgentWorkspace(agent_id, root, solver, root / "private", root / "public", root / "outbox")

    def freeze(self, solver: str | Path) -> dict[str, str]:
        """Call only after the supervisor proves the complete old scope stopped."""
        solver = Path(solver)
        key = candidate_tree_hash(solver)
        target = self.artifact_root / key
        if not target.exists():
            staging = Path(tempfile.mkdtemp(prefix="freeze-", dir=self.artifact_root))
            try:
                for source, relative in solver_files(solver):
                    destination = staging / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, destination)
                if candidate_tree_hash(staging) != key:
                    raise RuntimeError("Solver changed while being frozen")
                for attempt in range(4):
                    try:
                        staging.rename(target)
                        break
                    except OSError as error:
                        # Another publisher may have won. Accept only the
                        # exact immutable content, never a different winner.
                        if target.is_dir() and candidate_tree_hash(target) == key:
                            break
                        # Windows scanners can briefly hold our newly copied
                        # directory open. Retry only those sharing/access errors
                        # on this same verified target, with a finite deadline.
                        if (os.name == 'nt' and getattr(error,'winerror',None) in {5,32}
                                and attempt < 3 and not target.exists()):
                            time.sleep(.025 * 2**attempt)
                            continue
                        raise
            finally:
                if staging.exists():
                    shutil.rmtree(staging)
        if candidate_tree_hash(target) != key:
            raise PermissionError("Immutable artifact content was modified")
        # Namespace-mapped UID 65534 must be able to traverse read-only mounts.
        # Hash semantics include only executability, so this normalization does
        # not alter the captured candidate identity.
        for item in target.rglob("*"):
            item.chmod(0o555 if item.is_dir() or item.stat().st_mode & 0o111 else 0o444)
        target.chmod(0o555)
        return {"candidate_hash": key, "candidate_path": str(target)}

    def restore(self, workspace: AgentWorkspace, artifact_path: str | Path) -> None:
        """Restore a frozen verified parent while no agent process is running."""
        for child in workspace.solver.iterdir():
            if child.name == ".git":
                continue
            remove_private_path(child, workspace.solver)
        for source, relative in solver_files(Path(artifact_path)):
            target = workspace.solver / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
