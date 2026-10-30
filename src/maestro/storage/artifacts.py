"""Immutable, verified content-addressed objects; no agent-visible global CAS."""
from __future__ import annotations

from pathlib import Path
import hashlib
import os
import tempfile

from maestro.types import canonical_json, IntegrityError


class ArtifactStore:
    def __init__(self, root, register=None, *, read_only=False):
        self.root = Path(root).resolve()
        self.read_only=read_only
        if not read_only:self.root.mkdir(parents=True, exist_ok=True)
        self.register = register

    def path(self, digest):
        if len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
            raise IntegrityError('Invalid artifact digest')
        return self.root / digest[:2] / digest

    def put(self, data: bytes) -> str:
        if self.read_only:raise PermissionError('Read-only artifact store')
        digest = hashlib.sha256(data).hexdigest()
        target = self.path(digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            self.get(digest)
            if self.register:
                self.register(digest,len(data))
            return digest
        descriptor, name = tempfile.mkstemp(dir=target.parent, prefix='.tmp-')
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, target)
        finally:
            Path(name).unlink(missing_ok=True)
        if self.register:
            self.register(digest,len(data))
        return digest

    def put_json(self, value):
        return self.put(canonical_json(value).encode())

    def get(self, digest):
        try:
            data = self.path(digest).read_bytes()
        except OSError as exc:
            raise IntegrityError(f'Missing artifact {digest}') from exc
        if hashlib.sha256(data).hexdigest() != digest:
            raise IntegrityError(f'Artifact checksum mismatch {digest}')
        return data

    def get_json(self, digest):
        import json
        return json.loads(self.get(digest))

    def export(self, digest, destination):
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.get(digest))
        return target
