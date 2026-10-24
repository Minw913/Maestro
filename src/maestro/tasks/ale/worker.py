"""ALE-Bench bridge run by ALE's own interpreter: one JSON request on stdin, one JSON reply on stdout.

It imports only ale_bench and the standard library; MAESTRO's environment never loads ALE's dependencies.
"""
from __future__ import annotations

import contextlib
import hashlib
import importlib
import json
import sys
from collections.abc import Callable, Sequence
from typing import Protocol, cast


# The slice of ALE-Bench's API this worker uses; ale_bench is importable only inside ALE's own venv.
class _Enum(Protocol):
    @property
    def value(self) -> str: ...


class _Case(Protocol):
    absolute_score: int
    judge_result: _Enum
    execution_time: float
    memory_usage: int
    message: str
    input_str: str | None


class _Result(Protocol):
    overall_absolute_score: int
    case_results: Sequence[_Case]


class _Metadata(Protocol):
    score_type: _Enum
    title: str


class _Constraints(Protocol):
    time_limit: float
    memory_limit: int


class _Problem(Protocol):
    metadata: _Metadata
    constraints: _Constraints
    statement: str


class _Session(Protocol):
    problem: _Problem
    num_public_cases: int

    def public_eval(self, code: str, *, code_language: str, judge_version: str) -> _Result: ...
    def private_eval(self, code: str, *, code_language: str, judge_version: str) -> tuple[_Result, int, int]: ...
    def case_gen(self, seed: int) -> str: ...
    def close(self) -> None: ...


Request = dict[str, object]


def _text(request: Request, key: str) -> str:
    return str(request[key])


def _session(request: Request) -> _Session:
    start = cast(Callable[..., _Session], importlib.import_module('ale_bench').start)
    workers = request.get('num_workers', 1)
    return start(problem_id=_text(request, 'problem_id'), lite_version=bool(request['lite_version']),
                 num_workers=workers if isinstance(workers, int) else 1, run_visualization_server=False)


def _cases(result: _Result) -> list[dict[str, object]]:
    return [{'absolute_score': case.absolute_score, 'judge_result': str(case.judge_result.value),
             'execution_time': case.execution_time, 'memory_usage': case.memory_usage,
             'message': case.message,
             'input_sha256': hashlib.sha256((case.input_str or '').encode()).hexdigest()}
            for case in result.case_results]


def describe(request: Request) -> dict[str, object]:
    language_module = importlib.import_module('ale_bench.code_language')
    constants = importlib.import_module('ale_bench.constants')
    language = cast(Callable[[str], object], language_module.CodeLanguage)(_text(request, 'code_language'))
    judge = cast(Callable[[str], object], language_module.JudgeVersion)(_text(request, 'judge_version'))

    def command(name: str) -> str:
        return cast(Callable[[object, object], str], getattr(language_module, name))(language, judge)

    session = _session(request)
    try:
        problem = session.problem
        return {'score_type': str(problem.metadata.score_type.value), 'title': problem.metadata.title,
                'statement': problem.statement, 'time_limit': problem.constraints.time_limit,
                'memory_limit': problem.constraints.memory_limit,
                'compile_timeout': cast(float, constants.COMPILE_TIMEOUT),
                'num_public_cases': session.num_public_cases,
                'compile_command': command('get_compile_command'), 'run_command': command('get_run_command'),
                'submission_file': command('get_submission_file_path')}
    finally:
        session.close()


def public_eval(request: Request) -> dict[str, object]:
    session = _session(request)
    try:
        result = session.public_eval(_text(request, 'code'), code_language=_text(request, 'code_language'),
                                     judge_version=_text(request, 'judge_version'))
        return {'overall_absolute_score': result.overall_absolute_score, 'cases': _cases(result)}
    finally:
        session.close()


def private_eval(request: Request) -> dict[str, object]:
    session = _session(request)
    try:
        result, rank, performance = session.private_eval(_text(request, 'code'),
                                                         code_language=_text(request, 'code_language'),
                                                         judge_version=_text(request, 'judge_version'))
        return {'overall_absolute_score': result.overall_absolute_score, 'rank': rank,
                'performance': performance, 'cases': _cases(result)}
    finally:
        session.close()


def case_gen(request: Request) -> dict[str, object]:
    session = _session(request)
    try:
        seed = request['seed']
        return {'input': session.case_gen(seed if isinstance(seed, int) else int(str(seed)))}
    finally:
        session.close()


OPERATIONS: dict[str, Callable[[Request], dict[str, object]]] = {
    'describe': describe, 'public_eval': public_eval, 'private_eval': private_eval, 'case_gen': case_gen}

if __name__ == '__main__':
    request = cast(Request, json.load(sys.stdin))
    # ALE logs to stdout; keep stdout for the single JSON reply.
    with contextlib.redirect_stdout(sys.stderr):
        reply = OPERATIONS[_text(request, 'operation')](request)
    json.dump(reply, sys.stdout)
