"""Freezes one FrontierCS-2 task as a MAESTRO panel: pristine repo, dataset selectors, contract, images and seed.

The contract (allowed paths, limits, timeout, cores, memory) is read from the task's own judge/config.yaml; the
seed is the pristine repo with the task's baseline_naive.patch applied.
"""
import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import cast

import yaml

from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.tasks import fields
from maestro.tasks.frontiercs2.benchmark import FrontierCSManifest

TASKS = Path('tasks/frontier-cs-2.0-demo/problems')


def problem_brief(task: Path, contract: fields.Fields, allow_paths: list[str]) -> str:
    """PROBLEM.md for the seed: the task's research statement plus the MAESTRO submission contract."""
    statement = (task / 'agent-base' / 'frontier' / 'task' / 'repo' / 'README.md').read_text(encoding='utf-8')
    return (f"# FrontierCS-2 task {task.parent.name}/{task.name}\n\n"
            f"Your `solver/` directory is the task repository. Change only: {', '.join(allow_paths)}; "
            f"every other change is rejected before evaluation. A formal evaluation turns your change into a patch "
            f"against the pristine repository and runs the task's own verifier on its training workloads "
            f"(up to {fields.number(contract, 'timeout_seconds')} s, {fields.number(contract, 'cpus')} cores, "
            f"{fields.integer(contract, 'memory_mb')} MiB). Its score is 0-100; the research statement below "
            f"defines it and what beating the published method means.\n\n{statement}\n")


def main(options: fields.Fields) -> None:
    repo, out = Path(fields.text(options, 'repo')).absolute(), Path(fields.text(options, 'out'))
    task_name = fields.text(options, 'task')
    task = repo / TASKS / task_name
    config = fields.mapping(cast(object, yaml.safe_load((task / 'judge' / 'config.yaml').read_text(encoding='utf-8'))),
                            'judge/config.yaml')
    runtime, environment = fields.section(config, 'runtime'), fields.section(config, 'environment')
    submission = fields.section(config, 'submission')
    allow = submission.get('allow_paths')
    if not isinstance(allow, list) or not allow:
        raise SystemExit('judge/config.yaml has no submission.allow_paths')
    allow_paths = [str(p) for p in cast(list[object], allow)]
    contract: dict[str, object] = {'timeout_seconds': fields.number(runtime, 'timeout_seconds'),
                                   'cpus': fields.number(environment, 'cpus'),
                                   'memory_mb': fields.integer(environment, 'memory_mb')}
    out.mkdir(parents=True, exist_ok=False)
    _ = shutil.copytree(task / 'agent-base' / 'frontier' / 'task' / 'repo', out / 'base')
    with tempfile.TemporaryDirectory(prefix='fcs2-seed-') as scratch:
        # Inside another work tree git apply resolves paths against that repo and silently skips every hunk.
        seed = Path(scratch) / 'seed'
        _ = shutil.copytree(out / 'base', seed)
        _ = subprocess.run(['git', 'apply', str(task / 'judge' / 'baseline_naive.patch')], cwd=seed, check=True)
        _ = shutil.copytree(seed, out / 'seed_solver')
    if candidate_tree_hash(out / 'seed_solver') == candidate_tree_hash(out / 'base'):
        raise SystemExit('baseline_naive.patch changed nothing; refusing a seed identical to the pristine repo')
    _ = (out / 'seed_solver' / 'PROBLEM.md').write_text(problem_brief(task, contract, allow_paths), encoding='utf-8')
    (out / 'datasets').mkdir()
    digests: dict[str, str] = {}
    for mode in ('train', 'eval'):
        _ = shutil.copyfile(task / 'datasets' / f'{mode}.json', out / 'datasets' / f'{mode}.json')
        digests[mode] = hashlib.sha256((out / 'datasets' / f'{mode}.json').read_bytes()).hexdigest()
    revision = subprocess.run(['git', '-C', str(repo), 'rev-parse', 'HEAD'], capture_output=True, text=True,
                              check=True).stdout.strip()
    manifest = {'schema_version': 1, 'task': task_name, 'problem_id': task.name, 'frontiercs_revision': revision,
                'verifier_image': fields.text(options, 'verifier_image'),
                'agent_image': fields.text(options, 'agent_image'), 'allow_paths': allow_paths,
                'allow_empty': fields.flag(submission, 'allow_empty') if 'allow_empty' in submission else False,
                'max_changed_files': fields.integer(submission, 'max_changed_files'),
                'max_patch_bytes': fields.integer(submission, 'max_patch_bytes'), **contract,
                'datasets': digests, 'base_hash': candidate_tree_hash(out / 'base')}
    _ = (out / 'manifest.json').write_text(json.dumps(manifest, indent=1) + '\n', encoding='utf-8')
    frozen = FrontierCSManifest.load(out / 'manifest.json')
    print(json.dumps({'panel_hash': frozen.panel_hash, 'task': frozen.task, 'allow_paths': list(frozen.allow_paths),
                      'timeout_seconds': frozen.timeout_seconds, 'cpus': frozen.cpus}, indent=1))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument('--repo', required=True, help='FrontierCS-2 clone with this task\'s LFS objects fetched')
    _ = parser.add_argument('--task', required=True, help='task path under problems/, e.g. Database/join_tree_rewriting')
    _ = parser.add_argument('--verifier-image', required=True, help='pinned ID of the task\'s verifier image')
    _ = parser.add_argument('--agent-image', required=True, help='pinned ID of the task\'s agent-base image')
    _ = parser.add_argument('--out', required=True)
    main(fields.mapping(vars(parser.parse_args()), 'arguments'))
