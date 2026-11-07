#!/usr/bin/env bash
# Streams the first records of MusicBrainz's JSON dumps into JSON lines,
# one file per entity type, without downloading or unpacking whole dumps.
#
#     musicbrainz-fetch.sh <dump> <records per type> <dir> [types...]
#
# <dump> is a directory under the JSON dumps, such as 20261104-001002 (see
# https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/LATEST).
# Types default to area, place, label, event and artist. The dumps are
# CC0. The output is data: keep it out of the repository.
set -euo pipefail

dump=$1
count=$2
dir=$3
shift 3
types=("$@")
if [ ${#types[@]} -eq 0 ]; then
  types=(area place label event artist)
fi

base=https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/$dump
mkdir -p "$dir"
for type in "${types[@]}"; do
  # `head` closing the pipe ends the download early; the errors that
  # leaves upstream are expected.
  curl -sS "$base/$type.tar.xz" 2>/dev/null \
    | xz -dc 2>/dev/null \
    | tar -xOf - "mbdump/$type" 2>/dev/null \
    | head -n "$count" > "$dir/$type.jsonl" || true
  echo "$type: $(wc -l < "$dir/$type.jsonl") records"
done
