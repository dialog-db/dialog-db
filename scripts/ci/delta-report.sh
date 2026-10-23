#!/usr/bin/env bash
# Assemble the pull request's delta report from the gates' delta
# artifacts, and post it as one comment, edited in place on every push.
#
#   scripts/ci/delta-report.sh <perf-delta-dir> <soak-delta-dir> <body-file> [<pr-number>]
#
# Each delta dir holds what scripts/ci/base-sweep.sh and perf:check /
# soak:check wrote: source.md (the base and how it was chosen) and
# table.md (every row against it). A missing dir means the gate did not
# get that far, and the report says so.
#
# The body always goes to the job summary. With a pull request number it
# is also posted: the comment carrying the marker below is edited if there
# is one, else created. Needs `gh` with GH_TOKEN (pull-requests: write)
# and GITHUB_REPOSITORY.
set -euo pipefail

perf=$1
soak=$2
body=$3
pr=${4:-}

marker='<!-- dialog-delta-report -->'
# GitHub refuses a comment body past 65536 characters.
limit=65000

section() {
  local title=$1 dir=$2
  echo "### $title"
  echo
  if [ -f "$dir/source.md" ]; then
    cat "$dir/source.md"
    echo
  fi
  if [ -f "$dir/table.md" ]; then
    cat "$dir/table.md"
  elif [ -f "$dir/source.md" ]; then
    echo "No comparison against the base: see the job log."
  else
    echo "No report: the gate did not get as far as comparing. See the job log."
  fi
  echo
}

{
  echo "$marker"
  echo "## Cost against the base"
  echo
  echo "Each gate's sweep on this pull request against the sweep CI recorded for the main commit it lands on. ❌ rows fail the gate unless the section says it only reports; ⚠️ rows grew past tolerance in a scenario whose baseline file this pull request re-records, which accepts them. The gates also compare against the baseline files, as before."
  echo
  section "Perf gate" "$perf"
  section "Replication gate" "$soak"
} >"$body"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  cat "$body" >>"$GITHUB_STEP_SUMMARY"
fi

if [ "$(wc -c <"$body")" -gt "$limit" ]; then
  # Drop the folded unchanged rows; the job summary keeps them.
  awk '/^<details>/ { skip = 1 } !skip { print } /^<\/details>/ { skip = 0; print "_Unchanged rows omitted for length; the job summary has them._" }' \
    "$body" >"$body.short"
  mv "$body.short" "$body"
fi

if [ -z "$pr" ]; then
  exit 0
fi

repo=$GITHUB_REPOSITORY
existing=$(gh api --paginate "repos/$repo/issues/$pr/comments" \
  --jq ".[] | select(.body | startswith(\"$marker\")) | .id" | head -n1)
if [ -n "$existing" ]; then
  gh api -X PATCH "repos/$repo/issues/comments/$existing" -F "body=@$body" --jq .html_url
else
  gh api -X POST "repos/$repo/issues/$pr/comments" -F "body=@$body" --jq .html_url
fi
