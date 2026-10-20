"""Lazy Coral runtime resolution with honest provisioning errors."""
from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path
import subprocess
from . import CORAL_REVISION
from maestro.backends.base import BackendUnavailable


def verify_checkout(checkout: str | Path | None = None) -> dict:
    """Check both Git provenance and the code Python would actually import."""
    checkout = Path(checkout or Path(__file__).resolve().parents[4] / "vendor/coral").resolve()
    try:
        result = subprocess.run(["git", "-C", str(checkout), "rev-parse", "HEAD"],
                                capture_output=True, text=True, timeout=10, check=False)
        dirty = subprocess.run(["git", "-C", str(checkout), "status", "--porcelain", "--untracked-files=no"],
                               capture_output=True, text=True, timeout=10, check=False)
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired) as error:
        raise BackendUnavailable("Cannot verify the pinned Coral checkout; install Git and initialize vendor/coral") from error
    if result.returncode or result.stdout.strip() != CORAL_REVISION:
        raise BackendUnavailable(f"Coral checkout must be pinned to {CORAL_REVISION}: {checkout}")
    if dirty.returncode or dirty.stdout.strip():
        raise BackendUnavailable("Pinned Coral tracked source is modified; restore it or declare a separately audited backend revision")
    spec = importlib.util.find_spec("coral")
    if spec is None or not spec.origin:
        raise BackendUnavailable(f"Install the cloned backend with: python -m pip install -e {checkout}")
    expected = checkout / "coral"
    if not Path(spec.origin).resolve().is_relative_to(expected):
        raise BackendUnavailable("Python resolves another coral package; install the pinned vendor/coral checkout in this environment")
    try:
        import coral
    except ImportError as error:
        raise BackendUnavailable(f"Coral installation/dependencies incomplete; run python -m pip install -e {checkout}") from error
    return {"coral_revision": CORAL_REVISION, "coral_path": str(Path(coral.__file__).resolve())}


def resolve_runtime(name: str):
    try:
        from coral.agent.registry import get_runtime
    except ImportError as error:
        raise BackendUnavailable(f"Install the pinned Coral backend ({CORAL_REVISION}) before real execution") from error
    return get_runtime(name)


def load_factory(entrypoint: str):
    """Trusted operator config only; never accept module names from agent output."""
    if not entrypoint or ":" not in entrypoint:
        raise BackendUnavailable("A trusted module:factory entrypoint is required")
    module, name = entrypoint.rsplit(":", 1)
    try:
        value = getattr(importlib.import_module(module), name)
    except (ImportError, AttributeError) as error:
        raise BackendUnavailable(f"Cannot load configured backend factory: {entrypoint}") from error
    if not callable(value):
        raise BackendUnavailable("Configured factory is not callable")
    return value
