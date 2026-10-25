"""Replaces Git LFS pointer files under a path with their verified content from GitHub's LFS media endpoint.

Each object streams to a temporary file beside its pointer, is checked against the sha256 and size in the pointer,
and only then atomically replaces it; an interrupted run leaves the pointer, so the next run fetches it again.
git-lfs itself is not required.
"""
import argparse
import hashlib
import http.client
import os
import re
import shutil
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path
from typing import cast

from maestro.tasks import fields

POINTER = re.compile(r'^version https://git-lfs\.github\.com/spec/v1\noid sha256:([0-9a-f]{64})\nsize (\d+)\n$')
LFS_HEADER = 'version https://git-lfs.github.com/spec/v1'


def fetch_one(url: str, target: Path, oid: str, size: int, timeout: float) -> None:
    """Stream `url` into `target` atomically, refusing content that differs from its pointer."""
    digest, received = hashlib.sha256(), 0
    handle, partial = tempfile.mkstemp(prefix=f'.{target.name}.', dir=target.parent)
    try:
        with os.fdopen(handle, 'wb') as out, \
                cast(http.client.HTTPResponse, urllib.request.urlopen(url, timeout=timeout)) as response:
            while chunk := response.read(1 << 20):
                digest.update(chunk)
                received += len(chunk)
                _ = out.write(chunk)
        if received != size or digest.hexdigest() != oid:
            raise SystemExit(f'{target}: downloaded content does not match its LFS pointer')
        shutil.copymode(target, partial)
        os.replace(partial, target)
    finally:
        if os.path.exists(partial):
            os.unlink(partial)


def fetch(repo: Path, slug: str, root: Path, revision: str, timeout: float) -> int:
    """Number of pointer files under `root` replaced with verified content from upstream `revision`."""
    replaced = 0
    for path in sorted(p for p in root.rglob('*') if p.is_file() and p.stat().st_size < 1024):
        text = path.read_text(encoding='utf-8', errors='replace')
        if not text.startswith(LFS_HEADER):
            continue
        match = POINTER.match(text)
        if not match:
            raise SystemExit(f'{path}: an LFS pointer this fetcher cannot read; use git-lfs for it')
        relative = path.relative_to(repo).as_posix()
        url = f'https://media.githubusercontent.com/media/{slug}/{revision}/{urllib.parse.quote(relative)}'
        fetch_one(url, path, match[1], int(match[2]), timeout)
        replaced += 1
    return replaced


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument('--repo', required=True, help='local clone')
    _ = parser.add_argument('--slug', required=True, help='GitHub owner/name of that clone')
    _ = parser.add_argument('--path', required=True, help='directory inside the clone whose pointers to resolve')
    _ = parser.add_argument('--revision', required=True, help='upstream commit the pointers come from')
    _ = parser.add_argument('--timeout', type=float, required=True, help='seconds a stalled connection may idle')
    options = fields.mapping(vars(parser.parse_args()), 'arguments')
    repo = Path(fields.text(options, 'repo')).absolute()
    print(fetch(repo, fields.text(options, 'slug'), repo / fields.text(options, 'path'), fields.text(options, 'revision'),
                fields.number(options, 'timeout')), 'LFS objects fetched and verified')
