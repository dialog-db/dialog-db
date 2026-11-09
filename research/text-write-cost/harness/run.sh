#!/usr/bin/env bash
# E1: replay a concurrent editing trace into Dialog under each layout and
# flush policy, one JSON line per run appended to $OUT.
#
#   [EXTRA="--limit N"] research/text-write-cost/harness/run.sh <trace.json.gz> <out.jsonl> <variant> <flush>... [--sync]
#
# The harness is the dialog-soak example `text_write_cost`; build it with
#   cargo build --release -p dialog-soak --example text_write_cost
set -euo pipefail
trace=$1 out=$2 variant=$3
shift 3
sync=()
flushes=()
for arg in "$@"; do
  if [ "$arg" = --sync ]; then sync=(--sync); else flushes+=("$arg"); fi
done
root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
bin=${CARGO_TARGET_DIR:-$root/target}/release/examples/text_write_cost
vault=${VAULT_DIR:-$(mktemp -d)}
for flush in "${flushes[@]}"; do
  "$bin" --trace "$trace" --variant "$variant" --flush "$flush" --vault-dir "$vault" ${EXTRA:-} "${sync[@]}" >> "$out"
  tail -n 1 "$out" | cut -c1-200
done
