# Benchmarks

Setup once per benchmark, then one line per experiment (Warm Start, then `uniform`, `ucb`, `first_price` from the same checkpoint).

## Prerequisites

Linux, Docker with cgroup v2, `uv`, `git`, and Codex CLI logged in (`codex login`).

```sh
git clone --recurse-submodules https://github.com/Minw913/Maestro.git && cd Maestro
git checkout feat/benchmark-adapters
uv sync
```

## ALE-Bench (AHC001)

```sh
scripts/setup_ale.sh 8 4        # CPU_WORKERS INSTANCE_WORKERS
uv run --no-sync python scripts/check_benchmark.py data/ale/ahc001/experiment.yaml /tmp/check-ale
scripts/run_experiment.sh data/ale/ahc001/experiment.yaml artifacts/runs/ale-ahc001
```

## FrontierCS-2

```sh
scripts/setup_frontiercs2.sh Database/join_tree_rewriting 8     # TASK CPU_WORKERS
uv run --no-sync python scripts/check_benchmark.py data/frontiercs2/join_tree_rewriting/experiment.yaml /tmp/check-fcs2
scripts/run_experiment.sh data/frontiercs2/join_tree_rewriting/experiment.yaml artifacts/runs/fcs2-join_tree_rewriting
```

Any task with a `baseline_naive.patch` works. CPU tasks only for now.

## MLE-bench

Needs Kaggle credentials (`~/.kaggle/access_token` or `kaggle.json`) and each competition's rules accepted on kaggle.com.

```sh
scripts/setup_mlebench.sh spooky-author-identification 8 600 2 4096   # COMPETITION CPU_WORKERS TIMEOUT_S CPUS MEMORY_MB
uv run --no-sync python scripts/check_benchmark.py data/mlebench/spooky-author-identification/experiment.yaml /tmp/check-mle
scripts/run_experiment.sh data/mlebench/spooky-author-identification/experiment.yaml artifacts/runs/mle-spooky
```

Spooky verifies the setup. Other competitions need a validation view in `scripts/mlebench_tool.py` (`RAW_VIEWS`).

## What a passing check looks like

| Benchmark | `formal_F` of the seed |
|---|---|
| ALE AHC001 | ~0.00002 |
| FrontierCS-2 join_tree_rewriting | ~0.025 (timing-based; ±20%) |
| MLE-bench spooky | 0.086 |

## Not ready yet

GPU tasks (FrontierCS-2 GPU, MLE-bench High split), KernelBench.
