"""Freezes one ALE-Bench problem as a MAESTRO panel: manifest, per-case reference scores, smoke input, seed.

The reference score of each case is the median over --repeats official public evaluations, because
time-bounded heuristics score differently run to run.
"""
import argparse
import hashlib
import json
import shutil
import statistics
from dataclasses import dataclass
from pathlib import Path

from maestro.backends.coral.workspace import candidate_tree_hash
from maestro.tasks import fields
from maestro.tasks.ale.benchmark import (
    AleManifest,
    JudgedCase,
    ale_python,
    ale_revision,
    case_score,
    worker_call,
)


@dataclass(frozen=True)
class ProblemInfo:
    """The judge contract ALE reports for one problem."""
    title: str
    statement: str
    score_type: str
    time_limit: float
    memory_limit: int
    compile_timeout: float
    compile_command: str
    run_command: str
    submission_file: str

    @classmethod
    def parse(cls, raw: fields.Fields) -> 'ProblemInfo':
        return cls(fields.text(raw, 'title'), fields.text(raw, 'statement'), fields.text(raw, 'score_type'),
                   fields.number(raw, 'time_limit'), fields.integer(raw, 'memory_limit'),
                   fields.number(raw, 'compile_timeout'), fields.text(raw, 'compile_command'),
                   fields.text(raw, 'run_command'), fields.text(raw, 'submission_file'))


def problem_brief(info: ProblemInfo, language: str) -> str:
    """PROBLEM.md for the seed: the official statement plus the submission contract agents must meet."""
    direction = 'higher is better' if info.score_type == 'maximize' else 'lower is better'
    return (f"# {info.title}\n\n"
            f"Submission: `{info.submission_file}` ({language}), compiled by the judge with\n\n"
            f"    {info.compile_command}\n\n"
            f"and run as `{info.run_command}`: the case arrives on stdin, the answer goes to stdout.\n"
            f"Time limit {info.time_limit} s and memory limit {info.memory_limit // 2**20} MiB per case; "
            f"score is {info.score_type}d ({direction}).\n\n"
            f"{info.statement}\n")


def judged(reply: fields.Fields) -> list[JudgedCase]:
    return [JudgedCase.parse(raw) for raw in fields.objects(reply, 'cases')]


def main(options: fields.Fields) -> None:
    out = Path(fields.text(options, 'out'))
    out.mkdir(parents=True, exist_ok=False)
    checkout, language = fields.text(options, 'ale_checkout'), fields.text(options, 'language')
    reference, seed_solver = Path(fields.text(options, 'reference')), Path(fields.text(options, 'seed_solver'))
    repeats = fields.integer(options, 'repeats')
    python = ale_python(checkout)
    base: dict[str, object] = {'problem_id': fields.text(options, 'problem'), 'lite_version': fields.flag(options, 'lite'),
                               'code_language': language, 'judge_version': fields.text(options, 'judge_version'),
                               'num_workers': fields.integer(options, 'workers')}
    info = ProblemInfo.parse(worker_call(python, {'operation': 'describe', **base}, timeout=None))
    reference_code = (reference / info.submission_file).read_text(encoding='utf-8')
    runs = [judged(worker_call(python, {'operation': 'public_eval', 'code': reference_code, **base}, timeout=None))
            for _ in range(repeats)]
    for run in runs:
        rejected = {c.judge_result for c in run if c.judge_result != 'ACCEPTED'}
        if rejected:
            raise SystemExit(f'reference is not accepted on every public case: {sorted(rejected)}')
    inputs = [c.input_sha256 for c in runs[0]]
    if any([c.input_sha256 for c in run] != inputs for run in runs):
        raise SystemExit('public cases changed between reference runs')
    cases = [{'input_sha256': digest, 'reference_score': int(statistics.median(run[i].absolute_score for run in runs))}
             for i, digest in enumerate(inputs)]
    smoke = fields.text(worker_call(python, {'operation': 'case_gen', 'seed': fields.integer(options, 'smoke_seed'),
                                             **base}, timeout=None), 'input')
    if hashlib.sha256(smoke.encode()).hexdigest() in set(inputs):
        raise SystemExit('smoke seed reproduces a public case; choose another --smoke-seed')
    _ = (out / 'smoke_input.txt').write_text(smoke, encoding='utf-8')
    for name, source in (('reference', reference), ('seed_solver', seed_solver)):
        _ = shutil.copytree(source, out / name)
    _ = (out / 'seed_solver' / 'PROBLEM.md').write_text(problem_brief(info, language), encoding='utf-8')
    manifest = {'schema_version': 1, 'problem_id': base['problem_id'], 'lite_version': base['lite_version'],
                'judge_version': base['judge_version'], 'code_language': language,
                'score_type': info.score_type, 'time_limit': info.time_limit,
                'memory_limit': info.memory_limit, 'compile_timeout': info.compile_timeout,
                'compile_command': info.compile_command, 'run_command': info.run_command,
                'submission_file': info.submission_file, 'ale_revision': ale_revision(checkout),
                'reference_candidate_hash': candidate_tree_hash(out / 'reference'),
                'cases': cases, 'smoke_input': 'smoke_input.txt'}
    _ = (out / 'manifest.json').write_text(json.dumps(manifest, indent=1) + '\n', encoding='utf-8')
    frozen = AleManifest.load(out / 'manifest.json')
    seed_code = (out / 'seed_solver' / info.submission_file).read_text(encoding='utf-8')
    seed = judged(worker_call(python, {'operation': 'public_eval', 'code': seed_code, **base}, timeout=None))
    seed_scores = [case_score(c.absolute_score, f.reference_score, frozen.score_type, c.judge_result == 'ACCEPTED')
                   for c, f in zip(seed, frozen.cases, strict=True)]
    print(json.dumps({'panel_hash': frozen.panel_hash, 'cases': len(cases), 'score_type': frozen.score_type,
                      'reference_repeats': repeats, 'seed_accepted': sum(c.judge_result == 'ACCEPTED' for c in seed),
                      'seed_F': sum(seed_scores) / len(seed_scores)}, indent=1))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument('--problem', required=True)
    _ = parser.add_argument('--reference', required=True, help='directory holding the reference submission')
    _ = parser.add_argument('--seed-solver', required=True, help='directory holding the common seed submission')
    _ = parser.add_argument('--out', required=True)
    _ = parser.add_argument('--ale-checkout', required=True, help='ALE-Bench clone with its own .venv')
    _ = parser.add_argument('--language', required=True)
    _ = parser.add_argument('--judge-version', required=True)
    _ = parser.add_argument('--workers', type=int, required=True)
    _ = parser.add_argument('--repeats', type=int, required=True)
    _ = parser.add_argument('--smoke-seed', type=int, required=True, help='generator seed outside the public panel')
    _ = parser.add_argument('--lite', action='store_true')
    main(fields.mapping(vars(parser.parse_args()), 'arguments'))
