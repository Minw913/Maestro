"""ALE-Bench bridge run by ALE's own interpreter: one JSON request on stdin, one JSON reply on stdout.

It imports only ale_bench and the standard library; MAESTRO's environment never loads ALE's dependencies.
"""
import contextlib
import hashlib
import json
import sys


def _session(request):
    import ale_bench  # pyright: ignore[reportMissingImports]  # resolved only in ALE's own venv
    return ale_bench.start(problem_id=request['problem_id'], lite_version=request['lite_version'],
                           num_workers=request.get('num_workers', 1), run_visualization_server=False)


def _cases(result):
    return [{'absolute_score': case.absolute_score, 'judge_result': str(case.judge_result.value),
             'execution_time': case.execution_time, 'memory_usage': case.memory_usage,
             'message': case.message,
             'input_sha256': hashlib.sha256((case.input_str or '').encode()).hexdigest()}
            for case in result.case_results]


def describe(request):
    import ale_bench.constants  # pyright: ignore[reportMissingImports]
    from ale_bench.code_language import (  # pyright: ignore[reportMissingImports]
        CodeLanguage,
        JudgeVersion,
        get_compile_command,
        get_run_command,
        get_submission_file_path,
    )
    session = _session(request)
    try:
        language, judge = CodeLanguage(request['code_language']), JudgeVersion(request['judge_version'])
        problem = session.problem
        return {'score_type': str(problem.metadata.score_type.value), 'title': problem.metadata.title,
                'statement': problem.statement, 'time_limit': problem.constraints.time_limit,
                'memory_limit': problem.constraints.memory_limit,
                'compile_timeout': ale_bench.constants.COMPILE_TIMEOUT,
                'num_public_cases': session.num_public_cases,
                'compile_command': get_compile_command(language, judge),
                'run_command': get_run_command(language, judge),
                'submission_file': get_submission_file_path(language, judge)}
    finally:
        session.close()


def public_eval(request):
    session = _session(request)
    try:
        result = session.public_eval(request['code'], code_language=request['code_language'],
                                     judge_version=request['judge_version'])
        return {'overall_absolute_score': result.overall_absolute_score, 'cases': _cases(result)}
    finally:
        session.close()


def private_eval(request):
    session = _session(request)
    try:
        result, rank, performance = session.private_eval(request['code'], code_language=request['code_language'],
                                                         judge_version=request['judge_version'])
        return {'overall_absolute_score': result.overall_absolute_score, 'rank': rank,
                'performance': performance, 'cases': _cases(result)}
    finally:
        session.close()


def case_gen(request):
    session = _session(request)
    try:
        return {'input': session.case_gen(request['seed'])}
    finally:
        session.close()


OPERATIONS = {'describe': describe, 'public_eval': public_eval, 'private_eval': private_eval, 'case_gen': case_gen}

if __name__ == '__main__':
    request = json.load(sys.stdin)
    # ALE logs to stdout; keep stdout for the single JSON reply.
    with contextlib.redirect_stdout(sys.stderr):
        reply = OPERATIONS[request['operation']](request)
    json.dump(reply, sys.stdout)
