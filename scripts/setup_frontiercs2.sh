#!/usr/bin/env bash
# Prepares one FrontierCS-2 task for MAESTRO on this machine and prints its one-line run command.
# usage: scripts/setup_frontiercs2.sh TASK CPU_WORKERS [RUNTIME_YAML]
#   TASK         path under tasks/frontier-cs-2.0-demo/problems/ with a baseline_naive.patch (e.g. Database/join_tree_rewriting)
#   CPU_WORKERS  cores MAESTRO may use for formal evaluation
set -euo pipefail
cd "$(dirname "$0")/.."
task=$1 workers=$2 runtime=${3:-configs/runtimes/coral_codex_local.yaml}
revision=e989e8c6826585a336fb999db1b87fa327073c2d  # FrontierCS-2 commit this adapter was verified against
# FrontierCS-2 sets no process limit; a generous cap still stops a fork bomb in the verifier.
verifier_pids=1024
# Seconds an LFS download may stall before setup fails instead of hanging.
lfs_timeout=300
repo=repos/FrontierCS-2 problem=${task##*/} out=data/frontiercs2/${task##*/}
source=$repo/tasks/frontier-cs-2.0-demo/problems/$task
[ -d "$repo/.git" ] || git clone -q --depth 1 https://github.com/FrontierCS/FrontierCS-2.git "$repo"
git -C "$repo" checkout -q "$revision" 2>/dev/null || { git -C "$repo" fetch -q --depth 1 origin "$revision"; git -C "$repo" checkout -q "$revision"; }
[ -f "$source/judge/baseline_naive.patch" ] || { echo "$task ships no baseline_naive.patch to seed from" >&2; exit 2; }
uv run --no-sync python scripts/fetch_lfs.py --repo "$repo" --slug FrontierCS/FrontierCS-2 --path "${source#"$repo"/}" --revision "$revision" --timeout "$lfs_timeout"
docker build -q -f "$source/verifier/Dockerfile" -t "fcs2-$problem:${revision:0:8}" "$source" > /dev/null
docker build -q -f "$source/agent-base/Dockerfile" -t "fcs2-$problem-agent:${revision:0:8}" "$source" > /dev/null
verifier=$(docker image inspect "fcs2-$problem:${revision:0:8}" --format '{{.Id}}')
agent=$(docker image inspect "fcs2-$problem-agent:${revision:0:8}" --format '{{.Id}}')
rm -rf "$out"
uv run --no-sync python scripts/build_frontiercs2_panel.py --repo "$repo" --task "$task" \
  --verifier-image "$verifier" --agent-image "$agent" --out "$out"
budget=$(python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print(m["cpus"] * m["timeout_seconds"])' "$out/manifest.json")
cat > "$out/benchmark.yaml" <<YAML
kind: benchmark
schema_version: 1
benchmark: frontiercs2
evaluation_instance_count: 1  # one verifier run per evaluation
cpu_seconds_per_instance: $budget  # the task's cores x timeout_seconds
verification_cpu_seconds: 1  # RunConfig needs a positive reserve; verification happens inside the verifier run
task:
  contract:
    pids: $verifier_pids
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
  warm_start: ../../../configs/benchmarks/frontiercs2/warm_start.yaml
  market: ../../../configs/benchmarks/frontiercs2/market.yaml
execution_mode: real
n_agents: 4
seed: 0
YAML
uv run --no-sync python -c 'import sys; from maestro.config import load_config; from maestro.tasks.base import benchmark_for; c = load_config(sys.argv[1], stage="market"); benchmark_for(c.benchmark).live(c); print("config ok:", c.hash[:12])' "$out/experiment.yaml"
echo "check with: uv run --no-sync python scripts/check_benchmark.py $out/experiment.yaml <fresh-dir>"
echo "run with:   scripts/run_experiment.sh $out/experiment.yaml artifacts/runs/frontiercs2-$problem"
