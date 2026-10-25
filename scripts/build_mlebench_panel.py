"""Freezes one MLE-bench competition as a MAESTRO panel: validation split, private test, leaderboard and seed.

The validation split is the competition's own prepare() run on MLE-bench's public train split, so formal
evaluations never touch the private test; the seed submits the sample submission unchanged.
"""
import argparse
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from maestro.tasks import fields
from maestro.tasks.mlebench.benchmark import HASHED, MleBenchManifest, content_hash

SEED = '''"""Seed: submit the sample submission unchanged. Usage: python3 solution.py INPUT_DIR SUBMISSION_CSV"""
import shutil
import sys
from pathlib import Path

shutil.copyfile(Path(sys.argv[1]) / {sample!r}, sys.argv[2])
'''


def problem_brief(competition: str, description: str, sample: str) -> str:
    """PROBLEM.md for the seed: the competition description plus the MAESTRO submission contract."""
    return (f"# MLE-bench competition {competition}\n\n"
            f"`solution.py INPUT_DIR SUBMISSION_CSV` must train on the files in INPUT_DIR and write a submission "
            f"shaped like INPUT_DIR/{sample} to SUBMISSION_CSV. A formal evaluation runs it with no network on a "
            f"held-back validation split and scores it by the fraction of the real Kaggle leaderboard it beats. "
            f"`data/` holds the training inputs for local experiments; only the libraries in this image exist.\n\n"
            f"{description}")


def build(competition: str, cache: Path, mlebench_python: str, leaderboard: Path, revision: str, image: str,
          timeout: float, cpus: float, memory_mb: int, out: Path) -> MleBenchManifest:
    tool = Path(__file__).resolve().parent / 'mlebench_tool.py'
    with tempfile.TemporaryDirectory(prefix='mlebench-split-') as scratch:  # MLE-bench caches in its cwd
        done = subprocess.run([os.path.abspath(mlebench_python), '-I', str(tool), 'split', '--competition', competition,
                               '--data-dir', str(cache.resolve()), '--public-out', str((out / 'train' / 'inputs').resolve()),
                               '--private-out', str((out / 'train' / 'answers').resolve())],
                              capture_output=True, text=True, check=False, cwd=scratch)
    if done.returncode != 0:
        raise SystemExit('validation split failed: ' + done.stderr[-800:])
    names = fields.parse_object(done.stdout.strip().splitlines()[-1], 'split result')
    answers, sample = fields.text(names, 'answers'), fields.text(names, 'sample_submission')
    prepared = cache / competition / 'prepared'
    _ = shutil.copytree(prepared / 'public', out / 'final' / 'inputs', ignore=shutil.ignore_patterns('description.md'))
    (out / 'final' / 'answers').mkdir(parents=True)
    _ = shutil.copyfile(prepared / 'private' / answers, out / 'final' / 'answers' / answers)
    _ = shutil.copyfile(leaderboard, out / 'leaderboard.csv')
    seed = out / 'seed_solver'
    # shortcut: every candidate snapshot carries this copy of the training inputs; replace it with a read-only
    # /input mount in the agent container before running GB-scale competitions.
    _ = shutil.copytree(out / 'train' / 'inputs', seed / 'data')
    _ = (seed / 'solution.py').write_text(SEED.format(sample=sample), encoding='utf-8')
    description = (prepared / 'public' / 'description.md').read_text(encoding='utf-8')
    _ = (seed / 'PROBLEM.md').write_text(problem_brief(competition, description, sample), encoding='utf-8')
    data: dict[str, object] = {
        'schema_version': 1, 'competition': competition, 'mlebench_revision': revision, 'image': image,
        'timeout_seconds': timeout, 'cpus': cpus, 'memory_mb': memory_mb, 'answers': answers,
        'hashes': {key: content_hash(out / key) for key in HASHED}}
    _ = (out / 'manifest.json').write_text(json.dumps(data, indent=2) + '\n', encoding='utf-8')
    return MleBenchManifest.load(out / 'manifest.json')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('--competition', '--cache', '--mlebench-python', '--leaderboard', '--revision', '--image', '--out'):
        _ = parser.add_argument(name, required=True)
    _ = parser.add_argument('--timeout', type=float, required=True, help='seconds one evaluation may train')
    _ = parser.add_argument('--cpus', type=float, required=True, help='cores one evaluation may use')
    _ = parser.add_argument('--memory-mb', type=int, required=True, help='memory one evaluation may use')
    options = fields.mapping(vars(parser.parse_args()), 'arguments')
    manifest = build(fields.text(options, 'competition'), Path(fields.text(options, 'cache')),
                     fields.text(options, 'mlebench_python'), Path(fields.text(options, 'leaderboard')),
                     fields.text(options, 'revision'), fields.text(options, 'image'), fields.number(options, 'timeout'),
                     fields.number(options, 'cpus'), fields.integer(options, 'memory_mb'),
                     Path(fields.text(options, 'out')))
    print('panel', manifest.panel_hash[:12], 'for', manifest.competition)
