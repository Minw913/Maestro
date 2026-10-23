#!/usr/bin/env bash
# One MAESTRO experiment: Warm Start, then each market policy from the same frozen checkpoint.
# usage: scripts/run_experiment.sh EXPERIMENT.yaml OUTPUT_ROOT [POLICY ...]   (default: uniform ucb first_price)
set -euo pipefail
cd "$(dirname "$0")/.."
config=$1 root=$2; shift 2
policies=("$@"); [ ${#policies[@]} -gt 0 ] || policies=(uniform ucb first_price)
maestro() { uv run --no-sync python -m maestro --root "$root" "$@" < /dev/null; }
mkdir -p "$root"
maestro bootstrap --config "$config" > "$root/bootstrap.json"
checkpoint=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["checkpoint"])' "$root/bootstrap.json")
for policy in "${policies[@]}"; do
  maestro run --bootstrap "$checkpoint" --config "$config" --policy "$policy" > "$root/market-$policy.json"
done
maestro report > "$root/report.json"
echo "done: $root/report.json"
