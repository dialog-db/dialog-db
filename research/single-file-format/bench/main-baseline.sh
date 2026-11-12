#!/usr/bin/env bash
# Builds `tonk_bench` against origin/main, for the `main/fs` baseline.
#
# The bench uses only API main and this branch share. Main's
# `dialog-baseline` lacks the bench's sources and dev-dependencies, so they
# are copied into a worktree of origin/main (never committed there), and it
# is built with `--cfg dialog_main`, which leaves out the stores main does
# not have.
#
#   research/single-file-format/bench/main-baseline.sh <worktree dir> [<ref>]
#
# <ref> is what to build as main, origin/main by default.
#
# The binary lands in <worktree dir>/target/release/examples/tonk_bench.
set -euo pipefail
here="$(git rev-parse --show-toplevel)"
worktree="${1:?the directory to check origin/main out into}"
ref="${2:-origin/main}"
git -C "$here" fetch -q origin main
if [ ! -d "$worktree" ]; then
  git -C "$here" worktree add -q --detach "$worktree" "$ref"
fi
git -C "$worktree" checkout -q --detach "$ref"
baseline="$worktree/rust/dialog-baseline"
mkdir -p "$baseline/examples/tonk"
cp "$here/rust/dialog-baseline/examples/tonk_bench.rs" "$baseline/examples/"
cp "$here/rust/dialog-baseline/examples/tonk/dataset.rs" "$baseline/examples/tonk/"
# The bench's dev-dependencies, added to main's manifest if it lacks them.
for dep in dialog-credentials dialog-query dialog-remote-fs dialog-varsig serde_json; do
  if ! grep -q "^$dep = " "$baseline/Cargo.toml"; then
    sed -i "/^\[target.'cfg(not(target_arch = \"wasm32\"))'.dev-dependencies\]/a $dep = { workspace = true }" "$baseline/Cargo.toml"
  fi
done
if ! grep -q '^serde = ' "$baseline/Cargo.toml"; then
  sed -i "/^\[target.'cfg(not(target_arch = \"wasm32\"))'.dev-dependencies\]/a serde = { workspace = true, features = [\"derive\"] }" "$baseline/Cargo.toml"
fi
(cd "$worktree" && RUSTFLAGS="--cfg dialog_main" cargo build --release -p dialog-baseline --example tonk_bench)
echo "$worktree/target/release/examples/tonk_bench"
