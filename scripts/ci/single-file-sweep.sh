#!/usr/bin/env bash
# Explicit experiment: preserve failures and attribution evidence, never rewrite
# existing perf baselines or change the ordinary PR gates.
set -euo pipefail
out=target/single-file-comparison
mkdir -p "$out"
git rev-parse HEAD > "$out/commit.txt"
rustc --version > "$out/toolchain.txt"
cargo build --locked --release -p dialog-perf --features single-file-archive
for archive in fs single-file; do
  DIALOG_PERF_ARCHIVE="$archive" target/release/perf sweep --out-dir "$out/$archive" \
    > "$out/$archive.stdout" 2> "$out/$archive.stderr"
done
# Stable uninlined codec boundaries remain visible under release LTO. Print
# every positive self cost plus callees: inclusive sorting can hit its cumulative
# threshold at main and omit the codec. Test writes and a cold first query.
for scenario in commit-batch first-query; do
  callgrind_annotate --inclusive=no --tree=calling --threshold=100 --auto=no \
    "$out/single-file/$scenario.callgrind" > "$out/$scenario.annotated.txt"
done
for check in 'commit-batch encode' 'commit-batch decode' 'first-query decode'; do
  read -r scenario operation <<< "$check"
  printf '%s %s\n' "$scenario" "$operation" >> "$out/codec-attribution.txt"
  if ! awk -v operation="$operation" '
    /ProfiledCodec/ && index($0, operation) {
      cost = $1
      gsub(/,/, "", cost)
      if (cost + 0 > 0) { print; found = 1 }
    }
    END { exit !found }
  ' "$out/$scenario.annotated.txt" >> "$out/codec-attribution.txt"; then
    echo "No measured nonzero codec $operation cost in $scenario; instruction comparison is invalid" >&2
    exit 1
  fi
done
# A negative result is a valid exploration outcome, explicitly recorded below.
# The existing comparison's 10% exploration threshold is unchanged from the idea.
set +e
target/release/perf compare "$out/fs" "$out/single-file" --base --threshold 10 \
  --markdown "$out/comparison.md" > "$out/comparison.txt" 2>&1
comparison_status=$?
set -e
printf '%s\n' "$comparison_status" > "$out/comparison.exit-code"
{
  echo '# Single-file archive experiment'
  echo
  echo "Candidate: $(cat "$out/commit.txt")"
  echo
  if test "$comparison_status" -eq 0; then
    echo 'Paired filesystem comparison: PASS within the configured exploration tolerance.'
  else
    echo "Paired filesystem comparison: FAIL (exit $comparison_status). This provider has not met the performance adoption bar."
  fi
  echo
  echo 'Codec encode/decode attribution was checked for nonzero measured instructions. This synchronous profiling adapter excludes worker scheduling; production wall-clock measurements use the async provider.'
  echo
  cat "$out/comparison.txt"
} > "$out/summary.md"
cat "$out/summary.md"
