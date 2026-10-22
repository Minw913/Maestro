"""Toy feasible baseline. This file is executed only inside containment."""
import sys

for raw in sys.stdin:
    fields = raw.split()
    if fields[:2] == ["p", "ds"]:
        n = int(fields[2])
        print(n)
        print("\n".join(map(str, range(1, n + 1))))
        break
else:
    raise SystemExit("missing graph header")
