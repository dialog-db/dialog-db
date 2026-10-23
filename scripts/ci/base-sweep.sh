#!/usr/bin/env bash
# Fetch the sweep CI recorded for the main commit a pull request lands on,
# so a gate can compare the pull request's sweep against it (perf:check,
# soak:check) as well as against the checked-in baseline files.
#
#   scripts/ci/base-sweep.sh <artifact> <job> <baseline-dir> <out-dir> <delta-dir>
#   scripts/ci/base-sweep.sh perf-current 'Perf gate' perf/baseline target/perf-base target/perf-delta
#
# On a `pull_request` event the checkout is the merge commit CI built:
# the pull request's head merged onto main. Its first parent is the main
# commit the measured code lands on, which is the base to compare against.
# The merge base would attribute every main commit since the branch point
# to the pull request; when the branch is up to date the two coincide.
#
# If that commit has no usable sweep (its run still going past the wait
# below, cancelled before its gate finished, or expired), the newest
# earlier main commit with one stands in, and the provenance line says so.
# With none in reach, nothing is fetched: the gate then compares against
# the baseline files only, and the provenance line says that too.
#
# Writes:
#   <out-dir>/            the base sweep's reports, when one is found
#   <delta-dir>/source.md one line naming the base and how it was chosen
#   <delta-dir>/rebaselined  baseline files this pull request re-records,
#                            one stem per line: costs it accepted
#
# Needs `gh` with GH_TOKEN (actions: read) and GITHUB_REPOSITORY,
# GITHUB_SERVER_URL as GitHub Actions sets them, and a checkout with the
# merge commit's parents (fetch-depth: 2).
set -euo pipefail

artifact=$1
job=$2
baseline=$3
out=$4
delta=$5

repo=$GITHUB_REPOSITORY
server=${GITHUB_SERVER_URL:-https://github.com}
# How far back main to look, and how long to wait for the landed-on
# commit's own gate when its run is still going.
depth=20
wait_polls=20
wait_seconds=30

mkdir -p "$delta"
rm -rf "$out"

parent=$(git rev-parse HEAD^1)

# The baseline files this pull request changes. Re-recording a file is
# how a change accepts a scenario's new cost, so growth against the base
# in that scenario is reported as accepted rather than failed.
{ git diff --name-only HEAD^1 HEAD -- "$baseline" | grep '\.json$' || true; } |
  xargs -r -n1 basename | sed 's/\.json$//' >"$delta/rebaselined"

short() { echo "${1:0:10}"; }
commit_link() { echo "[\`$(short "$1")\`]($server/$repo/commit/$1)"; }

# The gate job's state in `run`: its conclusion once finished, else its
# status (queued, in_progress), else nothing.
gate_state() {
  gh api "repos/$repo/actions/runs/$1/jobs?per_page=100" \
    --jq ".jobs[] | select(.name == \"$job\") | (.conclusion // .status)" | head -n1
}

# Download the artifact from a run of the push event on `sha` whose gate
# job finished (passed or failed: either way the sweep is whole; a
# cancelled job may have uploaded half of one). Prints the run id.
fetch() {
  local sha=$1 wait=$2 run state polls
  for run in $(gh run list -R "$repo" --workflow test.yml --commit "$sha" \
    --json databaseId,event --jq '.[] | select(.event == "push") | .databaseId'); do
    state=$(gate_state "$run")
    polls=0
    while [ "$wait" = yes ] && { [ "$state" = queued ] || [ "$state" = in_progress ] || [ "$state" = waiting ]; } &&
      [ "$polls" -lt "$wait_polls" ]; do
      echo "waiting for $job on $(short "$sha") ($state)" >&2
      sleep "$wait_seconds"
      polls=$((polls + 1))
      state=$(gate_state "$run")
    done
    case "$state" in
    success | failure) ;;
    *) continue ;;
    esac
    if gh run download "$run" -R "$repo" -n "$artifact" -D "$out" >&2; then
      echo "$run"
      return 0
    fi
    rm -rf "$out"
  done
  return 1
}

skipped=0
for sha in $(gh api "repos/$repo/commits?sha=$parent&per_page=$depth" --jq '.[].sha'); do
  wait=no
  [ "$sha" = "$parent" ] && wait=yes
  if run=$(fetch "$sha" "$wait"); then
    run_link="[run]($server/$repo/actions/runs/$run)"
    if [ "$skipped" -eq 0 ]; then
      echo "Against $(commit_link "$sha"), the main commit this pull request's merge commit is built on ($run_link)." >"$delta/source.md"
    else
      echo "Against $(commit_link "$sha"), $skipped commit(s) behind the main commit this pull request is built on, $(commit_link "$parent"), which has no usable sweep (its run is still going, was cancelled before the gate finished, or expired). Deltas include those $skipped main commit(s) ($run_link)." >"$delta/source.md"
    fi
    cat "$delta/source.md"
    exit 0
  fi
  skipped=$((skipped + 1))
done

echo "No sweep found on the $depth main commits up to $(commit_link "$parent"); compared against the baseline files only." >"$delta/source.md"
cat "$delta/source.md"
