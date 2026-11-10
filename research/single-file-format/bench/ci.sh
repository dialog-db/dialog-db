#!/usr/bin/env bash
# Runs the team-of-six bench for main's `fs` and this branch's file store on
# one machine, for the "File archive against main" workflow, and writes
# Markdown tables to <out>/summary.md.
#
#   research/single-file-format/bench/ci.sh <out dir>
#
# - tonk-shaped 1× with the week and 20 warm commands of each kind, ROUNDS
#   rounds (default 3) alternating main and the branch, on the runner's disk
#   and again on /dev/shm (no disk), so a difference the disk makes shows
#   apart from one the code makes;
# - tonk-shaped 10×, setup and the views only (no week), one round.
#
# Datasets are generated from the committed schema fixture, so both builds
# replay the same lines. Nothing here gates: it reports.
set -euo pipefail
here="$(git rev-parse --show-toplevel)"
out="$(mkdir -p "${1:?an output directory}" && cd "$1" && pwd)"
rounds="${ROUNDS:-3}"
work="${TMPDIR:-/tmp}/file-archive-bench"
bench="$here/research/single-file-format/bench"
mkdir -p "$work" "$out/bin" "$out/datasets"

# Builds: this branch, then main with the bench copied in.
(cd "$here" && cargo build --release -p dialog-baseline --example tonk_bench --example tonk_dataset)
cp "$here/target/release/examples/tonk_bench" "$out/bin/branch"
cp "$here/target/release/examples/tonk_dataset" "$out/bin/dataset"
main="$("$bench/main-baseline.sh" "$work/main" | tail -1)"
cp "$main" "$out/bin/main"

fixture="$here/rust/dialog-baseline/fixtures/tonk-team-schema.jsonl"
DIALOG_DATASET_STEADY=20 "$out/bin/dataset" generate "$fixture" 1 "$out/datasets/tonk-1x.jsonl"
"$out/bin/dataset" generate "$fixture" 10 "$out/datasets/tonk-10x.jsonl"

# One run: <build> <store> <dataset> <runs dir> <round> <where to work>.
run() {
  local build="$1" store="$2" dataset="$3" runs="$4" round="$5" scratch="$6"
  rm -rf "$scratch" && mkdir -p "$scratch" "$runs"
  "$out/bin/$build" "$out/datasets/$dataset" "$store" "$scratch" \
    > /dev/null 2> "$runs/$build-$round.err"
  cp "$scratch/$store/report.json" "$runs/$build-$round.json"
  rm -rf "$scratch"
}

for round in $(seq "$rounds"); do
  run main fs tonk-1x.jsonl "$out/runs/1x-disk" "$round" "$work/run"
  run branch file tonk-1x.jsonl "$out/runs/1x-disk" "$round" "$work/run"
  run main fs tonk-1x.jsonl "$out/runs/1x-shm" "$round" /dev/shm/file-archive-bench
  run branch file tonk-1x.jsonl "$out/runs/1x-shm" "$round" /dev/shm/file-archive-bench
done
export DIALOG_BENCH_WEEK=0
run main fs tonk-10x.jsonl "$out/runs/10x-disk" 1 "$work/run"
run branch file tonk-10x.jsonl "$out/runs/10x-disk" 1 "$work/run"
unset DIALOG_BENCH_WEEK

{
  echo "## File archive against main"
  echo
  echo "Runner: $(nproc) cores, $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)."
  echo "Branch $(git -C "$here" rev-parse --short HEAD), main $(git -C "$work/main" rev-parse --short HEAD)."
  echo
  python3 "$bench/ci_summary.py" "$out/runs"
} > "$out/summary.md"
