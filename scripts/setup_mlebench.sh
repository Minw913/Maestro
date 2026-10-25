#!/usr/bin/env bash
# Prepares one MLE-bench competition for MAESTRO on this machine and prints its one-line run command.
# usage: scripts/setup_mlebench.sh COMPETITION CPU_WORKERS TIMEOUT_SECONDS CPUS MEMORY_MB [RUNTIME_YAML]
#   COMPETITION      an MLE-bench competition id with a validation view in scripts/mlebench_tool.py RAW_VIEWS
#   CPU_WORKERS      cores MAESTRO may use for formal evaluation
#   TIMEOUT_SECONDS, CPUS, MEMORY_MB   what one evaluation may spend training
# Needs Kaggle credentials (~/.kaggle) and the competition's rules accepted on kaggle.com.
set -euo pipefail
cd "$(dirname "$0")/.."
competition=$1 workers=$2 timeout=$3 cpus=$4 memory=$5 runtime=${6:-configs/runtimes/coral_codex.yaml}
revision=507f92e1138bb6e40dac5c6ee7a6758e6424bf97  # MLE-bench commit this adapter was verified against
kaggle_version=2.2.5  # first client verified with Kaggle's access_token credentials
# A training run may use many worker processes; a generous cap still stops a fork bomb.
candidate_pids=1024
# Seconds an LFS download may stall before setup fails instead of hanging.
lfs_timeout=300
repo=repos/mle-bench cache=data/mlebench/cache out=data/mlebench/$competition
[ -d "$repo/.git" ] || git clone -q https://github.com/openai/mle-bench.git "$repo"
git -C "$repo" checkout -q "$revision" -- . 2>/dev/null || { git -C "$repo" fetch -q origin "$revision"; git -C "$repo" checkout -q "$revision" -- .; }
# kaggle>=1.8 removed kaggle.rest; its HTTP errors are requests.HTTPError.
sed -i 's/^\(\s*\)from kaggle.rest import ApiException$/\1from requests import HTTPError as ApiException/' "$repo/mlebench/data.py"
[ -x "$repo/.venv/bin/python" ] || uv --no-config venv -q --python 3.11 "$repo/.venv"
uv --no-config pip install -q --python "$repo/.venv/bin/python" -e "$repo"
# A separate install, because mlebench pins kaggle<1.7, which cannot read access_token credentials.
uv --no-config pip install -q --python "$repo/.venv/bin/python" "kaggle==$kaggle_version"
uv run --no-sync python scripts/fetch_lfs.py --repo "$repo" --slug openai/mle-bench \
  --path "mlebench/competitions/$competition" --timeout "$lfs_timeout"
# MLE-bench opens a disk cache in its working directory, so run it from its own clone.
(cd "$repo" && .venv/bin/mlebench prepare -c "$competition" --data-dir "$OLDPWD/$cache")
# Rebuilding gives a new image ID even from cache, so the panel would change; build once per Dockerfile.
tag=maestro-mlebench:$(sha256sum docker/Dockerfile.mlebench | cut -c1-12)
docker image inspect "$tag" > /dev/null 2>&1 || docker build -q -f docker/Dockerfile.mlebench -t "$tag" docker > /dev/null
image=$(docker image inspect "$tag" --format '{{.Id}}')
rm -rf "$out"
uv run --no-sync python scripts/build_mlebench_panel.py --competition "$competition" --cache "$cache" \
  --mlebench-python "$repo/.venv/bin/python" --leaderboard "$repo/mlebench/competitions/$competition/leaderboard.csv" \
  --revision "$revision" --image "$image" --timeout "$timeout" --cpus "$cpus" --memory-mb "$memory" --out "$out"
budget=$(python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print(m["cpus"] * m["timeout_seconds"])' "$out/manifest.json")
cat > "$out/benchmark.yaml" <<YAML
kind: benchmark
schema_version: 1
benchmark: mlebench
evaluation_instance_count: 1  # one graded training run per evaluation
cpu_seconds_per_instance: $budget  # the evaluation's cores x timeout_seconds
verification_cpu_seconds: 1  # RunConfig needs a positive reserve; grading happens after the run
task:
  contract:
    pids: $candidate_pids
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
    mlebench_venv: ../../../$repo/.venv
YAML
cat > "$out/experiment.yaml" <<YAML
kind: experiment
schema_version: 1
runtime: ../../../$runtime
deployment: deployment.yaml
benchmark: benchmark.yaml
stages:
  warm_start: ../../../configs/benchmarks/mlebench/warm_start.yaml
  market: ../../../configs/benchmarks/mlebench/market.yaml
execution_mode: real
n_agents: 4
seed: 0
YAML
uv run --no-sync python -c 'import sys; from maestro.config import load_config; from maestro.tasks.base import benchmark_for; c = load_config(sys.argv[1], stage="market"); benchmark_for(c.benchmark).live(c); print("config ok:", c.hash[:12])' "$out/experiment.yaml"
echo "check with: uv run --no-sync python scripts/check_benchmark.py $out/experiment.yaml <fresh-dir>"
echo "run with:   scripts/run_experiment.sh $out/experiment.yaml artifacts/runs/mlebench-$competition"
