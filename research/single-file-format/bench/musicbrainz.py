#!/usr/bin/env python3
"""Turns MusicBrainz records into a fact log the Stack Exchange replay reads.

    musicbrainz.py <out.csv> <per commit> <type>=<records.jsonl>...

Each input is JSON lines of one entity type, as `musicbrainz-fetch.sh`
streams them. The output is `txn,at,the,of,as,is` (see `dialog-baseline`'s
`se.rs`): a commit per `<per commit>` entities, the entity types taken in
turn so commits land across the key space, as a replica that imports a
catalogue does.

An entity `mb:<id>` gets, under `mb.<type>/...`:

- its name, sort name, type, disambiguation, country, gender, address,
  coordinates, life span (begin, end, ended), time and whether it was
  cancelled, as text or booleans;
- its area, begin area and end area, as entities;
- every alias, tag, genre, ISNI and IPI, one fact each;
- every relation, as `mb.<type>/<relation>-<target type>` naming the
  target entity, so a catalogue's references cross between types.

Every `mb.` attribute may hold many values; the replay asserts them all
(see `is_multi_valued`). Nothing is superseded: each entity is written
once. Prints the log's hash, which names the dataset.
"""

import csv
import hashlib
import io
import json
import re
import sys

TEXT = ("name", "sort-name", "type", "disambiguation", "country", "gender",
        "time", "label-code")
AREAS = ("area", "begin-area", "end-area")
MANY = (("aliases", "alias"), ("tags", "tag"), ("genres", "genre"))
CODES = (("isnis", "isni"), ("ipis", "ipi"))


def slug(text):
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "related"


def facts(kind, record):
    """The (attribute, as, value) facts of one record."""
    the = lambda name: f"mb.{kind}/{name}"
    out = []
    for field in TEXT:
        value = record.get(field)
        if value not in (None, ""):
            out.append((the(field), "text", str(value)))
    span = record.get("life-span") or {}
    for field in ("begin", "end"):
        if span.get(field):
            out.append((the(field), "text", span[field]))
    if span.get("ended") is not None:
        out.append((the("ended"), "boolean", str(bool(span["ended"])).lower()))
    if record.get("cancelled") is not None:
        out.append((the("cancelled"), "boolean", str(bool(record["cancelled"])).lower()))
    if record.get("address"):
        out.append((the("address"), "text", record["address"]))
    coordinates = record.get("coordinates") or {}
    if coordinates.get("latitude") is not None:
        out.append((the("coordinates"), "text",
                    f"{coordinates['latitude']},{coordinates['longitude']}"))
    for field in AREAS:
        area = record.get(field) or {}
        if area.get("id"):
            out.append((the(field), "entity", f"mb:{area['id']}"))
    for field, name in MANY:
        for item in record.get(field) or []:
            if item.get("name"):
                out.append((the(name), "text", item["name"]))
    for field, name in CODES:
        for code in record.get(field) or []:
            out.append((the(name), "text", code))
    for relation in record.get("relations") or []:
        target_type = relation.get("target-type")
        target = (relation.get(target_type) or {}) if target_type else {}
        if target.get("id"):
            name = f"{slug(relation.get('type') or 'related')}-{slug(target_type)}"
            out.append((the(name), "entity", f"mb:{target['id']}"))
    return out


def main():
    if len(sys.argv) < 4:
        sys.exit(__doc__)
    out, per_commit = sys.argv[1], int(sys.argv[2])
    sources = []
    for argument in sys.argv[3:]:
        kind, path = argument.split("=", 1)
        sources.append((kind, open(path, encoding="utf-8")))

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["txn", "at", "the", "of", "as", "is"])
    txn = entities = rows = 0
    in_commit = 0
    live = list(sources)
    while live:
        for source in list(live):
            kind, lines = source
            line = lines.readline()
            if not line:
                live.remove(source)
                continue
            record = json.loads(line)
            of = f"mb:{record['id']}"
            for the, kind_of, value in facts(kind, record):
                writer.writerow([txn, "", the, of, kind_of, value])
                rows += 1
            entities += 1
            in_commit += 1
            if in_commit == per_commit:
                txn += 1
                in_commit = 0
    data = buffer.getvalue().encode("utf-8")
    with open(out, "wb") as file:
        file.write(data)
    digest = hashlib.sha256(data).hexdigest()[:16]
    print(f"{entities} entities, {rows} facts, {txn + (in_commit > 0)} commits; "
          f"dataset {digest}")


if __name__ == "__main__":
    main()
