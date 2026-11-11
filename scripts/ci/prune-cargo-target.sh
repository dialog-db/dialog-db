#!/usr/bin/env bash
# Cut a cargo target directory down to what is worth keeping between CI
# runs: the dependencies.
#
#   scripts/ci/prune-cargo-target.sh <target-dir>
#   scripts/ci/prune-cargo-target.sh /nix/cargo/target
#
# The Tests job (.github/workflows/test.yml) saves its target directory so
# the next run does not compile every dependency again. What it built of
# the workspace's own crates is no use to that run: a fresh checkout gives
# every source file a new modification time, and cargo rebuilds a path
# dependency whose sources are newer than its artifacts. Those artifacts
# are also most of the directory (every test binary links the whole graph),
# so they are removed here before the directory is saved:
#
#   - everything cargo built for a workspace member, in every profile and
#     target: its fingerprints, build script output, libraries and test
#     binaries;
#   - what cargo copies beside `deps` for the workspace's own targets
#     (binaries, libraries, examples);
#   - incremental state, and nextest's and cargo's own reports.
#
# Removing too much costs the next run a rebuild of what went; removing too
# little costs space. Neither breaks a build.
#
# Run from the repository root: the members are read from rust/*/Cargo.toml.
set -euo pipefail

target=${1:?usage: prune-cargo-target.sh <target-dir>}

if [ ! -d "$target" ]; then
  echo "No target directory at $target, nothing to prune."
  exit 0
fi

members=()
for manifest in rust/*/Cargo.toml; do
  # The first `name` of the manifest's [package] table.
  name=$(sed -n '/^\[package\]/,/^\[/{s/^name *= *"\(.*\)".*/\1/p}' "$manifest" | head -n 1)
  if [ -n "$name" ]; then
    members+=("$name")
  fi
done

if [ "${#members[@]}" -eq 0 ]; then
  echo "Found no workspace members under rust/, refusing to guess." >&2
  exit 1
fi

before=$(du -sm "$target" | cut -f1)

# A profile directory is one that holds `deps`: target/debug,
# target/release, target/wasm32-unknown-unknown/debug and so on.
while IFS= read -r deps; do
  profile=$(dirname "$deps")

  find "$profile" -maxdepth 1 -type f ! -name '.cargo-lock' -delete
  rm -rf "$profile/examples" "$profile/incremental"

  for name in "${members[@]}"; do
    crate=${name//-/_}
    rm -rf \
      "$profile/.fingerprint/$name-"* \
      "$profile/build/$name-"* \
      "$profile/deps/$crate-"* \
      "$profile/deps/lib$crate-"*
  done
done < <(find "$target" -type d -name deps -not -path '*/deps/*')

rm -rf "$target/nextest" "$target/cargo-timings" "$target/tmp"

after=$(du -sm "$target" | cut -f1)
echo "Pruned $target from ${before} MB to ${after} MB (${#members[@]} workspace members)."
