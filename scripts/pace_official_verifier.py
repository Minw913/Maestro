"""Run unmodified official PACE Java verifier with a fail-closed JSON/exit adapter.

This is an operator cross-check tool, not the MAESTRO containment runner. Java
zero exit alone is never accepted. Keep benchmark paths away from agent tools.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess


def verify(jar, graph, solution, *, timeout=30):
    try:
        completed = subprocess.run(
            ["java", "-Xmx384m", "-jar", str(jar), str(graph), str(solution)],
            capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"status": "ERROR", "reason": type(error).__name__}
    stdout, stderr = completed.stdout.strip(), completed.stderr.strip()
    match = re.fullmatch(r"Solution of size ([0-9]+) is valid!", stdout)
    if completed.returncode == 0 and match and not stderr:
        return {"status": "VALID", "size": int(match[1])}
    if (completed.returncode == 0 and not stdout and
            stderr == "At least one vertex is not dominated by the given solution!"):
        return {"status": "INVALID", "reason": "not a dominating set"}
    # Exceptions, unexpected messages and resource failures cannot become passes.
    return {"status": "ERROR", "reason": "official verifier rejected input or failed",
            "java_exit_code": completed.returncode}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jar", type=Path)
    parser.add_argument("graph", type=Path)
    parser.add_argument("solution", type=Path)
    args = parser.parse_args()
    result = verify(args.jar, args.graph, args.solution)
    print(json.dumps(result, sort_keys=True))
    return {"VALID": 0, "INVALID": 1, "ERROR": 2}[result["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
