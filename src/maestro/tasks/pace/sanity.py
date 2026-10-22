"""Packaged PACE graph used for bounded sanity checks, never formal scoring."""

from pathlib import Path


def default_sanity_graph_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "star5.gr"
