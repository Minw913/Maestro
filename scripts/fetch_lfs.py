"""Replaces Git LFS pointer files under a path with their verified content from GitHub's LFS media endpoint.

Each download is checked against the sha256 and size in its pointer, so the result is exactly the committed
object; git-lfs itself is not required.
"""
import argparse
import hashlib
import http.client
import re
import subprocess
import urllib.parse
import urllib.request
from pathlib import Path
from typing import cast

from maestro.tasks import fields

POINTER = re.compile(r'^version https://git-lfs\.github\.com/spec/v1\noid sha256:([0-9a-f]{64})\nsize (\d+)\n$')


def fetch(repo: Path, slug: str, root: Path) -> int:
    """Number of pointer files under `root` replaced with verified content."""
    revision = subprocess.run(['git', '-C', str(repo), 'rev-parse', 'HEAD'], capture_output=True, text=True,
                              check=True).stdout.strip()
    replaced = 0
    for path in sorted(p for p in root.rglob('*') if p.is_file() and p.stat().st_size < 1024):
        match = POINTER.match(path.read_text(encoding='utf-8', errors='replace'))
        if not match:
            continue
        oid, size = match[1], int(match[2])
        relative = path.relative_to(repo).as_posix()
        url = f'https://media.githubusercontent.com/media/{slug}/{revision}/{urllib.parse.quote(relative)}'
        with cast(http.client.HTTPResponse, urllib.request.urlopen(url)) as response:
            data = response.read()
        if len(data) != size or hashlib.sha256(data).hexdigest() != oid:
            raise SystemExit(f'{relative}: downloaded content does not match its LFS pointer')
        _ = path.write_bytes(data)
        replaced += 1
    return replaced


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument('--repo', required=True, help='local clone')
    _ = parser.add_argument('--slug', required=True, help='GitHub owner/name of that clone')
    _ = parser.add_argument('--path', required=True, help='directory inside the clone whose pointers to resolve')
    options = fields.mapping(vars(parser.parse_args()), 'arguments')
    repo = Path(fields.text(options, 'repo')).absolute()
    print(fetch(repo, fields.text(options, 'slug'), repo / fields.text(options, 'path')), 'LFS objects fetched and verified')
