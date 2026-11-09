#!/usr/bin/env bash
# The E1 matrix: every variant and flush policy on both concurrent traces.
#
#   research/text-write-cost/harness/matrix.sh <concurrent_traces dir> <results dir>
#
# Facts at one commit per transaction would hold several GB of blocks in
# the in-memory store, so they run on a prefix of each trace (PREFIX
# transactions), next to the coarser policies on the same prefix.
set -euo pipefail
traces=$1 results=$2
here=$(dirname "$0")
run=$here/run.sh
prefix=${PREFIX:-5000}
mkdir -p "$results"
export VAULT_DIR=${VAULT_DIR:-$(mktemp -d)}

for name in friendsforever clownschool; do
  trace=$traces/$name.json.gz
  out=$results/$name.jsonl
  time_policies=()
  if [ "$name" = clownschool ]; then time_policies=(second pause:2 pause:5); fi
  "$run" "$trace" "$out" raw all
  for variant in region-canonical region-buffered; do
    "$run" "$trace" "$out" "$variant" all 1000 100 10 1 "${time_policies[@]}"
  done
  # Doc per transaction fits in 7 GB on friendsforever only.
  doc_one=()
  if [ "$name" = friendsforever ]; then doc_one=(1); fi
  "$run" "$trace" "$out" doc all 1000 100 10 "${doc_one[@]}" "${time_policies[@]}"
  # Per-second facts on clownschool (4 923 commits) does not fit: prefix.
  facts_time=()
  if [ "$name" = clownschool ]; then facts_time=(pause:2 pause:5); fi
  "$run" "$trace" "$out" facts all 1000 100 10 "${facts_time[@]}"
  prefix_time=()
  if [ "$name" = clownschool ]; then prefix_time=(second); fi
  for variant in facts region-canonical doc; do
    EXTRA="--limit $prefix" "$run" "$trace" "$results/$name-prefix.jsonl" "$variant" all 100 10 1 "${prefix_time[@]}"
  done
done

# Two or three replicas (one per agent) syncing through a shared remote.
for name in friendsforever clownschool; do
  trace=$traces/$name.json.gz
  time_policies=()
  if [ "$name" = clownschool ]; then time_policies=(pause:2 pause:5); fi
  "$run" "$trace" "$results/$name-sync.jsonl" facts all 1000 100 "${time_policies[@]}" --sync
  prefix_time=()
  if [ "$name" = clownschool ]; then prefix_time=(second); fi
  EXTRA="--limit $prefix" "$run" "$trace" "$results/$name-sync-prefix.jsonl" facts all 100 10 1 "${prefix_time[@]}" --sync
done
