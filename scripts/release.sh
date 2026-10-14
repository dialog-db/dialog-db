#!/usr/bin/env bash
# The workspace is released as one version: every crate takes
# `[workspace.package] version`, and every crate another one depends on is
# listed under `[workspace.dependencies.<crate>]` with that same version, so
# the crates can be published together once they are. A release is the
# commit whose version and changelog section this script writes, tagged
# `v<version>`.
#
#   scripts/release.sh prepare <version>   set the version, date the changelog
#   scripts/release.sh check <version>     verify a commit is that release
#   scripts/release.sh notes <version>     print that release's changelog
#
# A pre-release (`0.3.0-rc.1`) sets the version and leaves the changelog's
# `## Unreleased` section where it is; its notes are that section.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$root/Cargo.toml"
changelog="$root/CHANGELOG.md"

usage() {
  echo "usage: $0 prepare|check|notes <version>" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
command="$1"
version="${2#v}"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || {
  echo "not a semver version: $2" >&2
  exit 2
}

# Non-empty for a pre-release version.
prerelease="${version#*-}"
[[ "$prerelease" == "$version" ]] && prerelease=""

# The version `[workspace.package]` declares.
workspace_version() {
  awk '
    /^\[/ { section = $0 }
    section == "[workspace.package]" && /^version = "/ {
      gsub(/^version = "|"$/, ""); print; exit
    }
  ' "$manifest"
}

# Each `<crate> <version>` under `[workspace.dependencies.<crate>]`, with an
# empty version where the table names none.
internal_versions() {
  awk '
    function flush() { if (crate != "") print crate, found }
    /^\[/ {
      flush(); crate = ""; found = ""
      if ($0 ~ /^\[workspace\.dependencies\./) {
        crate = $0; gsub(/^\[workspace\.dependencies\.|\]$/, "", crate)
      }
      next
    }
    crate != "" && /^version = "/ { found = $0; gsub(/^version = "|"$/, "", found) }
    END { flush() }
  ' "$manifest"
}

# The changelog section headed `## <version>`, up to the next `## `.
section() {
  awk -v heading="## $1" '
    index($0, heading) == 1 && (length($0) == length(heading) || substr($0, length(heading) + 1, 1) == " ") { inside = 1; next }
    inside && /^## / { exit }
    inside { print }
  ' "$changelog"
}

prepare() {
  local tmp
  tmp="$(mktemp)"
  awk -v version="$version" '
    function close_table() {
      if (crate && !written) print "version = \"" version "\""
      crate = 0; written = 0
    }
    /^\[/ {
      close_table()
      section = $0
      crate = (section ~ /^\[workspace\.dependencies\./)
      print; next
    }
    section == "[workspace.package]" && /^version = "/ {
      print "version = \"" version "\""; next
    }
    crate && /^version = "/ { print "version = \"" version "\""; written = 1; next }
    crate && /^$/ { close_table(); print; next }
    { print }
    END { close_table() }
  ' "$manifest" >"$tmp"
  mv "$tmp" "$manifest"
  (cd "$root" && cargo update --workspace --quiet)

  if [[ -n "$prerelease" ]]; then
    echo "prepared $version: review Cargo.toml and Cargo.lock, then commit and tag v$version"
    return
  fi
  if ! grep -q '^## Unreleased$' "$changelog"; then
    echo "CHANGELOG.md has no '## Unreleased' section to release" >&2
    exit 1
  fi
  tmp="$(mktemp)"
  awk -v version="$version" -v date="$(date -u +%Y-%m-%d)" '
    !done && $0 == "## Unreleased" {
      print "## Unreleased"; print ""; print "## " version " (" date ")"
      done = 1; next
    }
    { print }
  ' "$changelog" >"$tmp"
  mv "$tmp" "$changelog"
  echo "prepared $version: review Cargo.toml, Cargo.lock and CHANGELOG.md, then open the release PR"
}

check() {
  local failed=0 declared
  declared="$(workspace_version)"
  if [[ "$declared" != "$version" ]]; then
    echo "[workspace.package] version is $declared, not $version" >&2
    failed=1
  fi
  while read -r crate dependency; do
    if [[ "$dependency" != "$version" ]]; then
      echo "[workspace.dependencies.$crate] version is '${dependency}', not $version" >&2
      failed=1
    fi
  done < <(internal_versions)
  if [[ -z "$prerelease" && -z "$(section "$version" | tr -d '[:space:]')" ]]; then
    echo "CHANGELOG.md has no '## $version' section" >&2
    failed=1
  fi
  exit "$failed"
}

case "$command" in
  prepare) prepare ;;
  check) check ;;
  notes) if [[ -n "$prerelease" ]]; then section Unreleased; else section "$version"; fi ;;
  *) usage ;;
esac
