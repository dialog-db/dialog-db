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
# Stable uninlined codec boundaries ensure inclusive instruction attribution is
# visible even under release LTO. Test both writes and a cold first query.
for scenario in commit-batch first-query; do
  callgrind_annotate --inclusive=yes --threshold=100 --auto=no \
    "$out/single-file/$scenario.callgrind" > "$out/$scenario.annotated.txt"
done
python3 - "$out" <<'PY'
import pathlib, re, sys
root = pathlib.Path(sys.argv[1])
checks = [("commit-batch", "encode"), ("commit-batch", "decode"), ("first-query", "decode")]
evidence = []
for scenario, operation in checks:
    matches = []
    for line in (root / f"{scenario}.annotated.txt").read_text().splitlines():
        if "ProfiledCodec" in line and operation in line:
            count = re.match(r"\s*([0-9,]+)", line)
            if count and int(count[1].replace(",", "")) > 0:
                matches.append(line)
    if not matches:
        raise SystemExit(f"No measured nonzero codec {operation} cost in {scenario}; instruction comparison is invalid")
    evidence.extend([f"{scenario} {operation}", *matches])
(root / "codec-attribution.txt").write_text("\n".join(evidence) + "\n")
PY
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
