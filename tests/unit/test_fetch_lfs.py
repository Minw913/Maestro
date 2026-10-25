import hashlib
import subprocess
from pathlib import Path

import fetch_lfs
import pytest


def pointer(tmp_path: Path, content: bytes) -> tuple[Path, Path, str]:
    source = tmp_path / 'object.bin'
    _ = source.write_bytes(content)
    target = tmp_path / 'data.parquet'
    oid = hashlib.sha256(content).hexdigest()
    _ = target.write_text(f'version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {len(content)}\n')
    return source, target, oid


def leftovers(tmp_path: Path) -> list[str]:
    return [p.name for p in tmp_path.iterdir() if p.name.startswith('.')]


def test_verified_content_replaces_the_pointer(tmp_path: Path) -> None:
    source, target, oid = pointer(tmp_path, b'parquet bytes')
    fetch_lfs.fetch_one(source.as_uri(), target, oid, len(b'parquet bytes'), 10.0)
    assert target.read_bytes() == b'parquet bytes' and leftovers(tmp_path) == []


def test_mismatched_content_leaves_the_pointer_for_the_next_run(tmp_path: Path) -> None:
    source, target, oid = pointer(tmp_path, b'expected')
    _ = source.write_bytes(b'tampered')
    before = target.read_text()
    with pytest.raises(SystemExit, match='does not match'):
        fetch_lfs.fetch_one(source.as_uri(), target, oid, len(b'expected'), 10.0)
    assert target.read_text() == before and leftovers(tmp_path) == []


def test_an_unreadable_pointer_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / 'repo' / 'sub').mkdir(parents=True)
    _ = (tmp_path / 'repo' / 'sub' / 'x').write_text('version https://git-lfs.github.com/spec/v1\next-0-foo sha256:ab\n')

    def git(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        return subprocess.CompletedProcess([], 0, stdout='rev\n')

    monkeypatch.setattr(subprocess, 'run', git)  # fetch_lfs calls subprocess.run for the revision
    with pytest.raises(SystemExit, match='cannot read'):
        _ = fetch_lfs.fetch(tmp_path / 'repo', 'o/r', tmp_path / 'repo' / 'sub', 10.0)
