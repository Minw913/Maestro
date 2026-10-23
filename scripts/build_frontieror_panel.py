"""Freezes one FrontierOR task as a MAESTRO panel: dev instance(s), held-out test instances, smoke instance,
trusted checker, Gurobi reference objectives and the common seed, all taken from FrontierOR itself."""
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from maestro.tasks.frontieror.benchmark import ENTRY, FrontierManifest

# Runs inside FrontierOR's venv so its own dev-set rule, reference table and metadata decide the panel.
_QUERY = """
import json, sys
import pandas as pd
from test_time_self_evolution.scoring import building_blocks as bb
paper = sys.argv[1]
meta = json.load(open(bb._data_root() + '/metadata/paper_meta_info.json'))
entry = meta[paper] if isinstance(meta, dict) else next(m for m in meta if m.get('paper_id') == paper)
refs = pd.read_parquet(bb._data_root() + '/metadata/gurobi_references.parquet')
refs = refs[refs.task_id == paper]
print(json.dumps({'direction': entry['direction'], 'dev': bb.pick_median_tau_g_instance(paper),
                  'large': bb.list_large_instances(paper),
                  'references': {r.instance: r.objective_value for r in refs.itertuples() if r.feasible}}))
"""


def frontieror(repo: Path, data: Path, *args: str) -> str:
    env = {**os.environ, 'FRONTIER_OR_DATA_DIR': str(data.resolve())}
    return subprocess.run([str(repo / '.venv' / 'bin' / 'python'), *args], cwd=repo, env=env,
                          capture_output=True, text=True, check=True).stdout


def environment_packages(repo: Path) -> str:
    """Packages FrontierOR's candidate image installs, read from its Dockerfile."""
    text = (repo / 'Dockerfile').read_text(encoding='utf-8')
    block = re.search(r'pip install --no-cache-dir((?:\s*\\\n)?(?:[^\n]*\\\n)*[^\n]*)', text)
    return ' '.join(token for token in block[1].replace('\\', ' ').split()) if block else ''


def problem_brief(task: Path, info: dict[str, Any], time_limit: int, packages: str) -> str:
    """PROBLEM.md for the seed: FrontierOR's problem text, formulation, schemas and the run contract."""
    sense = 'minimize' if info['direction'] == 'min' else 'maximize'
    parts = [f"# FrontierOR task {task.name}\n",
             (f"Write `{ENTRY}`. The judge runs `python3 {ENTRY} --instance_path IN.json --solution_path OUT.json "
             f"--time_limit {time_limit}` once per instance, single core, with no grace period past the time limit.\n"
             f"Write a solution matching the solution schema, including `objective_value`; the checker recomputes it. "
             f"Objective: {sense}. Score per instance: your objective relative to Gurobi's best within one hour.\n"
             f"Python 3.13 with: {packages}. No Gurobi license is mounted, so gurobipy fails on large models.\n")]
    for name, title in (('problem_description.txt', 'Problem'), ('mathematical_formulation.md', 'Formulation'),
                        ('instance_schema.json', 'Instance schema'), ('solution_schema.json', 'Solution schema')):
        if (task / name).is_file():
            parts.append(f"## {title}\n\n{(task / name).read_text(encoding='utf-8')}\n")
    return '\n'.join(parts)


def freeze(src: Path, dest: Path, name: str) -> dict[str, str]:
    shutil.copyfile(src, dest / name)
    return {'file': name, 'input_sha256': hashlib.sha256((dest / name).read_bytes()).hexdigest()}


def main(args):
    repo, data, out = Path(args.frontieror_repo).absolute(), Path(args.frontieror_data).absolute(), Path(args.out)
    task = data / 'tasks' / args.paper
    info = json.loads(frontieror(repo, data, '-c', _QUERY, args.paper))
    out.mkdir(parents=True, exist_ok=False)
    (out / 'instances').mkdir()
    (out / 'checker').mkdir()
    shutil.copyfile(task / 'feasibility_check.py', out / 'checker' / 'feasibility_check.py')
    panel_names = args.panel or [info['dev']]
    heldout_names = [name for name in info['large'] if name not in panel_names]

    def entries(names):
        return [{'instance': name, 'reference_objective': info['references'][name],
                 **freeze(task / 'instance' / f'{name.replace("large_", "large_instance_")}.json', out / 'instances',
                          f'{name}.json')} for name in names]
    smoke = {'instance': 'tiny', **freeze(task / 'instance' / 'tiny_instance.json', out / 'instances', 'tiny.json')}
    shutil.copytree(args.seed_solver, out / 'seed_solver')
    if not (out / 'seed_solver' / ENTRY).is_file():
        raise SystemExit(f'seed solver has no {ENTRY}')
    (out / 'seed_solver' / 'PROBLEM.md').write_text(
        problem_brief(task, info, args.time_limit, environment_packages(repo)), encoding='utf-8')
    revision = subprocess.run(['git', '-C', str(repo), 'rev-parse', 'HEAD'], capture_output=True, text=True,
                              check=True).stdout.strip()
    manifest = {'schema_version': 1, 'paper_id': args.paper, 'direction': info['direction'],
                'frontieror_revision': revision, 'time_limit': args.time_limit,
                'smoke_time_limit': args.smoke_time_limit, 'panel': entries(panel_names),
                'heldout': entries(heldout_names), 'smoke': smoke,
                'checker_sha256': hashlib.sha256((out / 'checker' / 'feasibility_check.py').read_bytes()).hexdigest(),
                'reference_source': 'frontier-or/metadata/gurobi_references.parquet'}
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=1) + '\n', encoding='utf-8')
    frozen = FrontierManifest.load(out / 'manifest.json')
    print(json.dumps({'panel_hash': frozen.panel_hash, 'direction': frozen.direction,
                      'panel': [e['instance'] for e in frozen.panel], 'heldout': [e['instance'] for e in frozen.heldout],
                      'time_limit': frozen.time_limit}, indent=1))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--paper', required=True, help='FrontierOR task id, e.g. bierwirth2017')
    parser.add_argument('--frontieror-repo', required=True, help='FrontierOR clone with its own .venv')
    parser.add_argument('--frontieror-data', required=True, help='downloaded frontier-or dataset directory')
    parser.add_argument('--seed-solver', required=True, help=f'directory holding the common seed {ENTRY}')
    parser.add_argument('--out', required=True)
    parser.add_argument('--time-limit', type=int, required=True, help='seconds per instance (FrontierOR uses 3600)')
    parser.add_argument('--smoke-time-limit', type=int, required=True,
                        help='seconds for the tiny precheck (FrontierOR stage1 uses 300)')
    parser.add_argument('--panel', nargs='*', help="training instances; default: FrontierOR's median-τ_g dev pick")
    main(parser.parse_args())
