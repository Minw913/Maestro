"""Enforcing command prefix for the pinned upstream Coral CodexRuntime.

Coral owns the CLI lifecycle. This prefix removes its full-access flag and
applies the controller's provider/tool policy before exec; it never implements
an agent loop. The scope lives in the private controller artifact directory.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sys


def command(scope, original):
    if not original or Path(original[0]).name not in {'codex', 'codex.exe'}:
        raise PermissionError('Only the Coral Codex command is supported')
    # Accept only the exact shape emitted by the pinned Coral runtime. Unknown
    # future flags must be audited rather than inherited into the agent process.
    expected = ['codex', 'exec', scope['prompt'], '--dangerously-bypass-approvals-and-sandbox',
                '--model', scope['model'], '--json']
    if original != expected:
        raise PermissionError('Pinned Coral runtime command shape changed')
    argv = [scope['executable'], 'exec', scope['prompt'], '--model', scope['model'],
            '--json', '--sandbox', 'read-only', '--ignore-user-config', '--ignore-rules',
            '--skip-git-repo-check', '--ephemeral', '--output-last-message', scope['result_path']]
    if scope.get('schema_path'):
        argv += ['--output-schema', scope['schema_path']]
    for key, value in scope['settings'].items():
        from maestro.backends.codex_session.autonomous_client import _toml_literal
        argv += ['-c', f'{key}={_toml_literal(value)}']
    return argv


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scope', required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    scope = json.loads(Path(args.scope).read_text())
    original = args.command[1:] if args.command[:1] == ['--'] else args.command
    argv = command(scope, original)
    # No external provider credentials or base-url overrides are inherited.
    from maestro.backends.codex_session.client import _session_environment
    env = _session_environment()
    permitted = {'HOME','USER','LOGNAME','PATH','CODEX_HOME','PYTHONPATH','LANG','LC_ALL',
                 'TMPDIR','SSL_CERT_FILE','SSL_CERT_DIR','HTTPS_PROXY','HTTP_PROXY','ALL_PROXY','NO_PROXY'}
    env = {key:value for key,value in env.items() if key in permitted}
    # The prompt is in argv; an inherited open stdin makes `codex exec` wait for more input forever.
    os.dup2(os.open(os.devnull, os.O_RDONLY), 0)
    os.execvpe(argv[0], argv, env)


if __name__ == '__main__':
    main()
