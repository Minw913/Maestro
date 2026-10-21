"""Enforcing prefix for upstream Coral DeepSeekHarnessRuntime (no agent loop)."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path

# Audited against deepseek-harness 687f282e8625c6b8d11ff58211a90524be2e4317.
# The gate independently removes every non-broker tool and rejects its output.
DISABLED = ('tool-bash','tool-pwsh','tool-jobs','tool-fs','tool-fs-search',
    'tool-subagent-control','tool-subagent-list-agents','tool-subagent','tool-subagent-fork',
    'tool-workflow','tool-todo','tool-goal','tool-web','tool-plugin-manager',
    'plugin-manager','hmr','settings','config-editor','credentials','authorization',
    'deepseek-account','llm-deepseek-account','llm-pi-ai','llm-retry','session-title-llm',
    'compaction-basic','command-compact','session-telemetry-otel','web-search-deepseek',
    'web-deepseek-api','web-fetch-http','agent-instructions','skill',
    # Installed 0.2.1-rc.2 adds provider metadata and native tools; neither
    # belongs to the MAESTRO Messages/broker contract.
    'session-log-deepseek','plugin-package-inventory-deepseek',
    'tool-working-directory','plan-mode','tool-skill','skill-filesystem','mcp-resources')


def command(scope,original):
    expected=[scope['executable'],'--profile','headless','--patch',scope['model_patch'],scope['prompt']]
    if original!=expected:raise PermissionError('Pinned Coral DeepSeek command shape changed')
    return original[:-1]+['--patch',scope['policy_patch'],original[-1]]


def environment(scope):
    # No upstream credential, inherited .env, provider config, HTTP proxy or
    # NODE_OPTIONS reaches the native runtime. Home and cwd are ephemeral.
    return {**scope['environment'],'HOME':scope['home'],'XDG_CONFIG_HOME':scope['home'],
        'DSH_HOME':scope['dsh_home'],'DEEPSEEK_BASE_URL':scope['base_url'],
        'DEEPSEEK_API_KEY':'maestro-phase-gateway-only','DSH_PERMISSION_MODE':'read-only',
        'DSH_TOOLS_MODE':'native','DSH_TELEMETRY_DISABLED':'1'}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--scope',required=True)
    parser.add_argument('command',nargs=argparse.REMAINDER);args=parser.parse_args()
    scope=json.loads(Path(args.scope).read_text())
    original=args.command[1:] if args.command[:1]==['--'] else args.command
    argv=command(scope,original)
    os.execvpe(argv[0],argv,environment(scope))


if __name__=='__main__':main()
