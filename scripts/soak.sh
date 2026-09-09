#!/usr/bin/env bash
# Sync/join soak sweep: runs the dialog-soak join scenario across network
# profiles (and optionally across tree branching factors), writing one JSON
# report per configuration plus a combined summary table.
#
# Usage:
#   scripts/soak.sh                 # sweep networks at the default fanout
#   SWEEP_FANOUT=1 scripts/soak.sh  # also sweep fanout_n 5 (32) vs 8 (256)
#   OUT_DIR=soak-results scripts/soak.sh
#
# Each configuration runs in its own process because the tree manifest and
# the fs-network environment default are read once per process
# (DIALOG_TREE_FANOUT_N, DIALOG_FS_NETWORK).
#
# Compare two runs with scripts/soak-compare.py:
#   scripts/soak-compare.py baseline-dir new-dir

set -euo pipefail

cd "$(dirname "$0")/.."

OUT_DIR="${OUT_DIR:-target/soak}"
ENTITIES="${ENTITIES:-4000}"
COMMITS="${COMMITS:-32}"
# `none` runs unshaped: its request counts are deterministic (no latency,
# so no duplicate in-flight fetches) and the regression gate holds them
# tight; the shaped profiles measure modeled time and are gated loosely.
NETWORKS="${NETWORKS:-none localhost broadband mobile intercontinental}"
if [[ "${SWEEP_FANOUT:-0}" == "1" ]]; then
  FANOUTS="${FANOUTS:-5 8}"
else
  FANOUTS="${FANOUTS:-}"
fi

mkdir -p "$OUT_DIR"
cargo build -p dialog-soak --release

# Each configuration runs REPEATS times and keeps the run with the median
# lazy-join modeled time: the harness randomizes identities and commit
# timestamps shift leaf boundaries, so single runs wobble by a block or
# two; the median run is what the regression gate compares.
REPEATS="${REPEATS:-3}"

run() {
  local network="$1" fanout="$2"
  local name="join-${network}"
  local env_prefix=()
  if [[ -n "$fanout" ]]; then
    name="${name}-fanout${fanout}"
    env_prefix=(env "DIALOG_TREE_FANOUT_N=${fanout}")
  fi
  echo "== ${name} (entities=${ENTITIES} commits=${COMMITS}, ${REPEATS} repeats)" >&2
  local tmp
  tmp="$(mktemp -d)"
  for repeat in $(seq 1 "$REPEATS"); do
    "${env_prefix[@]}" target/release/soak \
      --network "$network" \
      --entities "$ENTITIES" \
      --commits "$COMMITS" \
      --json-only \
      >"$tmp/run-${repeat}.json"
  done
  python3 - "$tmp" "$OUT_DIR/${name}.json" <<'EOF'
import json, sys, glob
tmp, out = sys.argv[1], sys.argv[2]
runs = []
for path in sorted(glob.glob(tmp + "/run-*.json")):
    with open(path) as f:
        report = json.load(f)
    lazy = sum(p["virtual_ms"] for p in report["phases"] if p["name"] != "download")
    runs.append((lazy, report))
runs.sort(key=lambda pair: pair[0])
with open(out, "w") as f:
    json.dump(runs[len(runs) // 2][1], f, indent=2)
EOF
  rm -rf "$tmp"
}

for network in $NETWORKS; do
  if [[ -n "$FANOUTS" ]]; then
    for fanout in $FANOUTS; do
      run "$network" "$fanout"
    done
  else
    run "$network" ""
  fi
done

echo "reports written to $OUT_DIR" >&2
python3 - "$OUT_DIR" <<'EOF'
import json, sys, glob, os
out = sys.argv[1]
print(f"| {'config':<34} | {'phase':<10} | {'virt ms':>8} | {'reqs':>6} | {'KiB':>8} |")
print(f"|{'-'*36}|{'-'*12}|{'-'*10}|{'-'*8}|{'-'*10}|")
for path in sorted(glob.glob(os.path.join(out, '*.json'))):
    with open(path) as f:
        report = json.load(f)
    config = os.path.basename(path)[:-5]
    for phase in report['phases']:
        print(f"| {config:<34} | {phase['name']:<10} | {phase['virtual_ms']:>8} "
              f"| {phase['traffic']['requests']:>6} | {phase['traffic']['bytes']//1024:>8} |")
EOF
