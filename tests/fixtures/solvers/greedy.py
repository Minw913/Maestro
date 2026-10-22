"""Deterministic toy baseline; no claim of competitive PACE performance."""
import sys

n = None
edges = []
for raw in sys.stdin:
    fields = raw.split()
    if not fields or fields[0] in {"c", "#"}:
        continue
    if fields[:2] == ["p", "ds"]:
        n = int(fields[2])
    else:
        edges.append(tuple(map(int, fields)))
if n is None:
    raise SystemExit("missing graph header")
adj = {v: {v} for v in range(1, n + 1)}
for a, b in edges:
    adj[a].add(b)
    adj[b].add(a)
remaining = set(adj)
solution = []
while remaining:
    chosen = min(adj, key=lambda v: (-len(adj[v] & remaining), v))
    solution.append(chosen)
    remaining.difference_update(adj[chosen])
print(len(solution))
print("\n".join(map(str, solution)))
