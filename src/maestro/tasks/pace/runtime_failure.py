"""Conservative pre-admission runtime evidence; never authorizes a re-evaluation."""
from dataclasses import dataclass, field
import re


@dataclass(frozen=True)
class RuntimeFailure:
    candidate_hash: str
    exception_type: str
    diagnostic: str
    locations: tuple[tuple[str, int], ...]
    # Controller-supplied fixed sanity input, never training/reference data.
    test_context: dict = field(default_factory=dict)


class RuntimePrecheckFailure(tuple):
    def __new__(cls, reason, evidence):
        obj = super().__new__(cls, (False, reason))
        obj.evidence = evidence
        return obj


_RUNTIME_EXCEPTIONS = frozenset({'NameError','UnboundLocalError','TypeError','ValueError',
    'IndexError','KeyError','AttributeError','ZeroDivisionError','RuntimeError',
    'OverflowError','AssertionError','StopIteration'})


def classify_runtime_failure(candidate_hash, receipt, *, test_context=None):
    # Missing commands/dependencies, syntax/format/solution errors, OOM/signals,
    # output/CPU/wall limits and executor failures all fail closed. This is a
    # diagnostic classifier, not a proof that untrusted stderr is honest.
    if (receipt.returncode != 1 or receipt.timed_out
            or receipt.diagnostics.get('reason') != 'COMPLETED'
            or receipt.diagnostics.get('aggregate_cgroup_cpu') is not True
            or receipt.diagnostics.get('cpu_measurement_incomplete')):
        return None
    error = receipt.stderr[-8192:]
    if 'Traceback (most recent call last):' not in error:
        return None
    tail = error.rstrip().splitlines()[-1] if error.strip() else ''
    kind = tail.split(':', 1)[0]
    if kind not in _RUNTIME_EXCEPTIONS:
        return None
    locations = tuple((p, int(n)) for p,n in re.findall(r'File "/candidate/([^"\n]+)", line ([0-9]+)',error))
    if not locations:
        return None
    return RuntimeFailure(candidate_hash, kind, error, locations, test_context or {})
