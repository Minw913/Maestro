"""Read-only export of standard experiment outputs, never native runtime logs."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile

from maestro.storage.paths import run_paths


PUBLIC_TREES={'coral-cas','bootstrap','arms','reports','exports'}
PROVIDER_DATABASES={'codex-autonomous-authority/requests.sqlite',
    'deepseek-native-authority/requests.sqlite','codex-session-authority/session-turns.sqlite',
    'coral-authority/budgets.sqlite'}
SECRETS=re.compile(rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\bsk-(?:proj-|ant-)?[A-Za-z0-9_-]{24,}|\bgh[pousr]_[A-Za-z0-9]{30,}|eyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}|https?://(?:127\.0\.0\.1|localhost):[0-9]+/[A-Za-z0-9_-]{24,}')


def _build_export(root,output):
    root,output=Path(root).resolve(),Path(output).resolve()
    if not root.is_dir():raise ValueError('Experiment root does not exist')
    if output==root or output.is_relative_to(root):raise ValueError('Export destination must be outside the original experiment root')
    if output.exists():raise ValueError('Export destination already exists; original/export records are never overwritten')
    output.mkdir(parents=True)
    manifest={'source_root':str(root),'started_utc':datetime.now(timezone.utc).isoformat(),
        'scope':'standard experiment outputs only; no native/controller logs, credentials or mutable workspaces',
        'consistency':'independent SQLite online backups and file reads, not a globally atomic snapshot',
        'status_note':'Statuses and UNKNOWN are preserved; this export does not assert completion or replay success',
        'files':[]}
    for source in sorted(root.rglob('*')):
        if not source.is_file() or source.is_symlink():continue
        relative=source.relative_to(root);parts=relative.parts;name=relative.as_posix()
        if any(p in {'.git','__pycache__','.ssh','.codex','.dsh'} for p in parts):continue
        if source.name.endswith(('-wal','-shm')):continue
        if source.name in {'.env','auth.json','credentials.json'} or source.name.startswith('.env.'):continue
        allowed=(parts[0] in PUBLIC_TREES or name in PROVIDER_DATABASES
            or (parts[0]=='authority' and (name=='authority/ledger.sqlite'
                or (len(parts)==2 and source.name.startswith('config.') and source.name.endswith('.resolved.json'))
                or name=='authority/deepseek-historical-exceptions.json'
                or (len(parts)>=3 and parts[1]=='cas'))))
        if not allowed:continue
        target=output/relative;target.parent.mkdir(parents=True,exist_ok=True)
        record={'path':name}
        if source.suffix=='.sqlite':
            db=sqlite3.connect(source.as_uri()+'?mode=ro',uri=True,timeout=10)
            copy=sqlite3.connect(target)
            try:
                db.backup(copy,pages=256,sleep=.01)
                copy.execute('PRAGMA journal_mode=DELETE')
                copy.execute('VACUUM')
                if copy.execute('PRAGMA integrity_check').fetchall()!=[('ok',)]:
                    raise ValueError('Export SQLite integrity check failed: '+name)
                record['method']='sqlite_online_backup'
            finally:copy.close();db.close()
        else:
            data=source.read_bytes()
            if SECRETS.search(data):raise ValueError('Export blocked by credential/capability pattern: '+name)
            target.write_bytes(data);shutil.copymode(source,target)
            record['method']='byte_copy'
        data=target.read_bytes()
        if SECRETS.search(data):raise ValueError('Export blocked by credential/capability pattern: '+name)
        record.update(sha256=hashlib.sha256(data).hexdigest(),bytes=len(data))
        manifest['files'].append(record)
    if not (output/'authority/ledger.sqlite').exists():raise ValueError('No experiment ledger found; diagnostic roots are not experiment outputs')
    manifest['finished_utc']=datetime.now(timezone.utc).isoformat()
    (output/'EXPORT_MANIFEST.json').write_text(json.dumps(manifest,indent=2)+'\n')
    (output/'SHA256SUMS').write_text(''.join(f"{r['sha256']}  {r['path']}\n" for r in manifest['files']))
    return {'output':str(output),'files':len(manifest['files']),'snapshot':True,'replay_verified':False}


def export_outputs(root,output):
    root,output=Path(root).resolve(),Path(output).resolve()
    if not root.is_dir():raise ValueError('Experiment root does not exist')
    if output==root or output.is_relative_to(root):raise ValueError('Export destination must be outside the original experiment root')
    if output.exists():raise ValueError('Export destination already exists; original/export records are never overwritten')
    local=run_paths(root).local;local.mkdir(parents=True,exist_ok=True,mode=0o700)
    # Screen private staging before creating anything in a Git-shareable tree.
    with tempfile.TemporaryDirectory(prefix='export-staging-',dir=local) as temporary:
        staging=Path(temporary)/'outputs'
        result=_build_export(root,staging)
        output.parent.mkdir(parents=True,exist_ok=True)
        shutil.copytree(staging,output)
        result['output']=str(output)
        return result
