"""Separate portable experiment outputs from machine-local runtime material."""
from dataclasses import dataclass
import hashlib
from pathlib import Path


@dataclass(frozen=True)
class RunPaths:
    output: Path
    local: Path


def run_paths(root):
    root=Path(root).resolve()
    ancestors=(root,*root.parents)
    artifact=next((base for base in ancestors if base.name=='artifacts'),None)
    if artifact is not None:
        return RunPaths(root,artifact.parent/'runtime'/root.relative_to(artifact))
    if any(base.name=='runtime' for base in ancestors):
        # A qualification/calibration root is already local-only.
        return RunPaths(root,root)
    # Custom output roots must not collide merely because their basename matches.
    identity=hashlib.sha256(str(root).encode()).hexdigest()[:12]
    return RunPaths(root,root.parent/'runtime'/(root.name+'-'+identity))


def diagnostic_root(root):
    """Redirect new diagnostics out of artifacts without reusing old quotas."""
    root=Path(root).resolve()
    if any(p.name=='artifacts' for p in (root,*root.parents)):
        local=run_paths(root).local
        if root.exists():
            raise ValueError('Legacy diagnostic root already exists; inspect its ledger, do not remap or retry: '+str(root))
        return local
    return root


def require_separated_runtime(root):
    """Never silently resume a legacy mixed-layout native execution."""
    root=Path(root).resolve()
    if run_paths(root).local==root:return
    for relative in ('coral-workspaces','codex-autonomous-authority/native',
                     'codex-autonomous-authority/auth-check','deepseek-native-authority/native',
                     'codex-session-authority/broker'):
        if (root/relative).exists():
            raise ValueError('Legacy mixed output/runtime layout requires explicit offline migration before model execution: '+str(root))


def diagnostic_native_root(root):
    """Read-only diagnostics may inspect immutable legacy phase evidence."""
    root=Path(root).resolve()
    current=run_paths(root).local/'codex-autonomous-authority'/'native'
    if current.exists():return current
    for old in (root/'runtime/codex-autonomous-authority/native',root/'codex-autonomous-authority/native'):
        if old.exists():return old
    return current
