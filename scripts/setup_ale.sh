#!/usr/bin/env bash
# Prepares ALE-Bench AHC001 for MAESTRO on this machine and prints its one-line run command.
# usage: scripts/setup_ale.sh CPU_WORKERS INSTANCE_WORKERS [RUNTIME_YAML]
#   CPU_WORKERS       cores MAESTRO may use for formal evaluation
#   INSTANCE_WORKERS  judge cases one evaluation runs in parallel
set -euo pipefail
cd "$(dirname "$0")/.."
workers=$1 instance_workers=$2 runtime=${3:-configs/runtimes/coral_codex_local.yaml}
revision=8ba16e6baf743aa8736516c45d1c52afdbd4de99  # ALE-Bench commit the rootless patch applies to
patched=fc9e010759d31f3d04019fdfca26b21b9dc4f5d1  # that patch as a commit; fixed identity and dates reproduce it
problem=ahc001 language=cpp20 judge=202301
repeats=3  # reference score per case is the median of this many public runs; measured CV is 0.02%
smoke_seed=1000003  # generator seed outside the 50-case public panel
repo=repos/ALE-Bench out=data/ale/$problem
[ -d "$repo/.git" ] || git clone -q https://github.com/SakanaAI/ALE-Bench.git "$repo"
if [ "$(git -C "$repo" rev-parse HEAD)" != "$patched" ]; then
  git -C "$repo" checkout -q "$revision"
  GIT_COMMITTER_NAME="Javi Alfaro" GIT_COMMITTER_EMAIL=javi.alfaro94@gmail.com GIT_COMMITTER_DATE="Fri, 23 Oct 2026 14:47:39 -0600" \
    git -C "$repo" -c user.name=maestro -c user.email=maestro@localhost am -q "$PWD/patches/ale-bench-rootless.patch"
  [ "$(git -C "$repo" rev-parse HEAD)" = "$patched" ] || { echo "patched ALE-Bench is not $patched" >&2; exit 2; }
fi
(cd "$repo" && uv --no-config sync -q --frozen)  # its lockfile; --no-config skips its uv version pin
# Judge containers run as the host user, which is root inside the container under rootless Docker.
if docker info --format '{{.SecurityOptions}}' | grep -q rootless; then uid=0 gid=0; else uid=$(id -u) gid=$(id -g); fi
# The candidate image the benchmark config pins is also the judge image's base, which names it by tag.
image=$(uv run --no-sync python -c 'import sys, yaml; print(yaml.safe_load(open(sys.argv[1]))["task"]["contract"]["image"])' configs/benchmarks/ale/benchmark.yaml)
docker pull -q "$image" > /dev/null
docker tag "$image" "yimjk/ale-bench:$language-$judge"
docker build -q "$repo/dockerfiles" -t "ale-bench:$language-$judge" -f "$repo/dockerfiles/Dockerfile_${language}_$judge" \
  --build-arg UID=$uid --build-arg GID=$gid > /dev/null
reference=$(mktemp -d)  # ALE's own accepted solution, under the submission name the builder reads
trap 'rm -rf "$reference"' EXIT
cp "$repo/tests/judge/codes/ac_${language}_$problem.cpp" "$reference/Main.cpp"
rm -rf "$out"
uv run --no-sync python scripts/build_ale_panel.py --problem "$problem" --reference "$reference" \
  --seed-solver configs/benchmarks/ale/seeds/$problem --out "$out" --ale-checkout "$repo" --language "$language" \
  --judge-version "$judge" --workers "$workers" --repeats "$repeats" --smoke-seed "$smoke_seed" < /dev/null
cat > "$out/deployment.yaml" <<YAML
kind: deployment
schema_version: 1
cpu_workers: $workers
instance_workers: $instance_workers
coral:
  checkout_path: ../../../vendor/coral
  codex_executable: codex
task:
  paths:
    manifest: manifest.json
    seed_solver: seed_solver
    ale_checkout: ../../../$repo
YAML
cat > "$out/experiment.yaml" <<YAML
kind: experiment
schema_version: 1
runtime: ../../../$runtime
deployment: deployment.yaml
benchmark: ../../../configs/benchmarks/ale/benchmark.yaml
stages:
  warm_start: ../../../configs/benchmarks/ale/warm_start.yaml
  market: ../../../configs/benchmarks/ale/market.yaml
execution_mode: real
n_agents: 4
seed: 0
YAML
uv run --no-sync python -c 'import sys; from maestro.config import load_config; from maestro.tasks.base import benchmark_for; c = load_config(sys.argv[1], stage="market"); benchmark_for(c.benchmark).live(c); print("config ok:", c.hash[:12])' "$out/experiment.yaml"
echo "check with: uv run --no-sync python scripts/check_benchmark.py $out/experiment.yaml <fresh-dir>"
echo "run with:   scripts/run_experiment.sh $out/experiment.yaml artifacts/runs/ale-$problem"
