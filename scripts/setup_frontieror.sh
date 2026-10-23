#!/usr/bin/env bash
# Prepares one FrontierOR task for MAESTRO on this machine and prints its one-line run command.
# usage: scripts/setup_frontieror.sh PAPER TIME_LIMIT SMOKE_TIME_LIMIT CPU_WORKERS [RUNTIME_YAML]
#   PAPER             FrontierOR task id with a seed in configs/benchmarks/frontieror/seeds/ (e.g. bierwirth2017)
#   TIME_LIMIT        seconds per instance; FrontierOR's protocol uses 3600
#   SMOKE_TIME_LIMIT  seconds for the tiny precheck; must fit the 360 s generation window with the checker
#   CPU_WORKERS       cores MAESTRO may use for formal evaluation (one per instance)
set -euo pipefail
cd "$(dirname "$0")/.."
paper=$1 limit=$2 smoke=$3 workers=$4 runtime=${5:-configs/runtimes/coral_codex.yaml}
revision=f15471c413f894964f5c5c3b5ca93461cfd8a95b  # FrontierOR commit this adapter was verified against
# Per-instance container limits (settings): RAM, largest solution JSON, trusted checker budget.
memory_mb=4096 solution_bytes=16777216 checker_seconds=60
repo=repos/FrontierOR data=repos/FrontierOR/frontier-or out=data/frontieror/$paper-t$limit
seed=configs/benchmarks/frontieror/seeds/$paper
[ -f "$seed/code.py" ] || { echo "no seed for $paper: add $seed/code.py (feasible by construction) first" >&2; exit 2; }
[ -d "$repo/.git" ] || git clone -q https://github.com/Minw913/FrontierOR.git "$repo"
git -C "$repo" checkout -q "$revision" 2>/dev/null || { git -C "$repo" fetch -q --depth 1 origin "$revision"; git -C "$repo" checkout -q "$revision"; }
[ -x "$repo/.venv/bin/python" ] || uv --no-config venv -q --python 3.13 "$repo/.venv"
uv --no-config pip install -q --python "$repo/.venv/bin/python" -r "$repo/requirements.txt" "huggingface_hub[cli]"
"$repo/.venv/bin/hf" download frontieror/FrontierOR --repo-type dataset --local-dir "$data" \
  --include "metadata/**" --include "tasks/$paper/**" > /dev/null
docker build -q -t "frontieror-candidate:${revision:0:7}" "$repo" > /dev/null
image=$(docker image inspect "frontieror-candidate:${revision:0:7}" --format '{{.Id}}')
rm -rf "$out"
uv run --no-sync python scripts/build_frontieror_panel.py --paper "$paper" --frontieror-repo "$repo" \
  --frontieror-data "$data" --seed-solver "$seed" --out "$out" --time-limit "$limit" --smoke-time-limit "$smoke"
panel=$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["panel"]))' "$out/manifest.json")
cat > "$out/benchmark.yaml" <<YAML
kind: benchmark
schema_version: 1
benchmark: frontieror
evaluation_instance_count: $panel
cpu_seconds_per_instance: $limit
verification_cpu_seconds: $((checker_seconds * panel))  # one checker run per panel instance
task:
  contract:
    image: $image
    memory_mb: $memory_mb
    solution_bytes: $solution_bytes
    checker_seconds: $checker_seconds
YAML
cat > "$out/deployment.yaml" <<YAML
kind: deployment
schema_version: 1
cpu_workers: $workers
instance_workers: 1
coral:
  checkout_path: ../../../vendor/coral
  codex_executable: codex
task:
  paths:
    manifest: manifest.json
    seed_solver: seed_solver
YAML
cat > "$out/experiment.yaml" <<YAML
kind: experiment
schema_version: 1
runtime: ../../../$runtime
deployment: deployment.yaml
benchmark: benchmark.yaml
stages:
  warm_start: ../../../configs/benchmarks/frontieror/warm_start.yaml
  market: ../../../configs/benchmarks/frontieror/market.yaml
execution_mode: real
n_agents: 4
seed: 0
YAML
uv run --no-sync python -c 'import sys; from maestro.config import load_config; from maestro.tasks.base import benchmark_for; c = load_config(sys.argv[1], stage="market"); benchmark_for(c.benchmark).live(c); print("config ok:", c.hash[:12])' "$out/experiment.yaml"
echo "run with: scripts/run_experiment.sh $out/experiment.yaml artifacts/runs/frontieror-$paper-t$limit"
