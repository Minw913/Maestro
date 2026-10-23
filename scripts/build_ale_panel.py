"""Freezes one ALE-Bench problem as a MAESTRO panel: manifest, per-case reference scores, smoke input, seed.

The reference score of each case is the median over --repeats official public evaluations, because
time-bounded heuristics score differently run to run.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import statistics

from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.tasks.ale.benchmark import AleManifest, ale_python, ale_revision, case_score, worker_call


def main(args):
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    python = ale_python(args.ale_checkout)
    base = {'problem_id': args.problem, 'lite_version': args.lite, 'code_language': args.language,
            'judge_version': args.judge_version, 'num_workers': args.workers}
    info = worker_call(python, {'operation': 'describe', **base}, timeout=None)
    reference_code = (Path(args.reference) / info['submission_file']).read_text(encoding='utf-8')
    runs = [worker_call(python, {'operation': 'public_eval', 'code': reference_code, **base}, timeout=None)
            for _ in range(args.repeats)]
    for run in runs:
        rejected = [c['judge_result'] for c in run['cases'] if c['judge_result'] != 'ACCEPTED']
        if rejected:
            raise SystemExit(f'reference is not accepted on every public case: {sorted(set(rejected))}')
    inputs = [c['input_sha256'] for c in runs[0]['cases']]
    if any([c['input_sha256'] for c in run['cases']] != inputs for run in runs):
        raise SystemExit('public cases changed between reference runs')
    cases = [{'input_sha256': digest, 'reference_score': int(statistics.median(run['cases'][i]['absolute_score']
                                                                             for run in runs))}
             for i, digest in enumerate(inputs)]
    smoke = worker_call(python, {'operation': 'case_gen', 'seed': args.smoke_seed, **base}, timeout=None)['input']
    if hashlib.sha256(smoke.encode()).hexdigest() in set(inputs):
        raise SystemExit('smoke seed reproduces a public case; choose another --smoke-seed')
    (out / 'smoke_input.txt').write_text(smoke, encoding='utf-8')
    for name, source in (('reference', args.reference), ('seed_solver', args.seed_solver)):
        shutil.copytree(source, out / name)
    manifest = {'schema_version': 1, 'problem_id': args.problem, 'lite_version': args.lite,
                'judge_version': args.judge_version, 'code_language': args.language,
                'score_type': info['score_type'], 'time_limit': info['time_limit'],
                'memory_limit': info['memory_limit'], 'compile_timeout': info['compile_timeout'],
                'compile_command': info['compile_command'], 'run_command': info['run_command'],
                'submission_file': info['submission_file'],
                'ale_revision': ale_revision(args.ale_checkout),
                'reference_candidate_hash': candidate_tree_hash(out / 'reference'),
                'cases': cases, 'smoke_input': 'smoke_input.txt'}
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=1) + '\n', encoding='utf-8')
    frozen = AleManifest.load(out / 'manifest.json')
    seed_code = (out / 'seed_solver' / info['submission_file']).read_text(encoding='utf-8')
    seed = worker_call(python, {'operation': 'public_eval', 'code': seed_code, **base}, timeout=None)
    seed_scores = [case_score(c['absolute_score'], f['reference_score'], frozen.score_type,
                              c['judge_result'] == 'ACCEPTED') for c, f in zip(seed['cases'], frozen.cases)]
    print(json.dumps({'panel_hash': frozen.panel_hash, 'cases': len(cases), 'score_type': frozen.score_type,
                      'reference_repeats': args.repeats,
                      'seed_accepted': sum(c['judge_result'] == 'ACCEPTED' for c in seed['cases']),
                      'seed_F': sum(seed_scores) / len(seed_scores)}, indent=1))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--problem', required=True)
    parser.add_argument('--reference', required=True, help='directory holding the reference submission')
    parser.add_argument('--seed-solver', required=True, help='directory holding the common seed submission')
    parser.add_argument('--out', required=True)
    parser.add_argument('--ale-checkout', required=True, help='ALE-Bench clone with its own .venv')
    parser.add_argument('--language', required=True)
    parser.add_argument('--judge-version', required=True)
    parser.add_argument('--workers', type=int, required=True)
    parser.add_argument('--repeats', type=int, required=True)
    parser.add_argument('--smoke-seed', type=int, required=True, help='generator seed outside the public panel')
    parser.add_argument('--lite', action='store_true')
    main(parser.parse_args())
