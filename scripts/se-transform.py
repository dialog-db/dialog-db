#!/usr/bin/env python3
"""Transform a Stack Exchange site dump into a transaction-ordered fact log.

The Stack Exchange dumps are per-table XML with the edit history in a separate
file, so reconstructing "what did this post look like after each edit" means
joining PostHistory against Posts and replaying types. Doing that inside a
benchmark harness would mean every consumer re-derives the same thing, and any
two harnesses could disagree. This script does it once, and emits a flat file
the ingester reads top to bottom with no lookups.

The output is one row per asserted fact, ordered by transaction:

    txn,at,the,of,as,is

  txn  monotonic transaction ordinal, 1-based. Rows sharing a txn are ONE
       commit. This comes from the dump's own RevisionGUID, which Stack
       Exchange assigns to the rows of a single atomic edit, so commit
       boundaries are the site's real ones and not something inferred here.
  at   ISO-8601 timestamp of the transaction (the edit's CreationDate).
  the  attribute, e.g. `se.post/title`
  of   entity, e.g. `post:1` or `user:24`
  as   value type, matching the dialog CSV convention (text/natural/entity/...)
  is   value

Facts are asserted, never retracted: a later commit asserting the same
(the, of) supersedes the earlier value under cardinality-one, which is exactly
how an edit behaves.

Usage:
    se-transform.py <extracted-dump-dir> <output.csv> [--limit N]
"""

import argparse
import csv
import html
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# PostHistoryTypeId values we turn into facts. Stack Exchange splits the
# initial post and later edits into distinct type ids for the same field, so
# both map to the same attribute: an edit supersedes, which is the behavior we
# want to exercise.
TITLE_TYPES = {"1", "4"}
BODY_TYPES = {"2", "5"}
TAG_TYPES = {"3", "6"}

# Types that mark a state change rather than a content edit. Each becomes a
# boolean fact so the workload includes attribute flips, not just text
# replacement.
STATE_TYPES = {
    "10": ("se.post/closed", "true"),
    "11": ("se.post/closed", "false"),
    "12": ("se.post/deleted", "true"),
    "13": ("se.post/deleted", "false"),
    "14": ("se.post/locked", "true"),
    "15": ("se.post/locked", "false"),
}


def iter_rows(path):
    """Stream <row> elements, freeing each so a large file stays bounded."""
    for _, element in ET.iterparse(path, events=("end",)):
        if element.tag == "row":
            yield element
            element.clear()


def parse_tags(text):
    """`|a|b|c|` or `<a><b>` to a list. Both forms appear across dump vintages."""
    if not text:
        return []
    if text.startswith("|"):
        return [t for t in text.strip("|").split("|") if t]
    return [t for t in text.replace("><", ">|<").strip("<>").split("|<") if t]


def load_post_types(dump_dir):
    """PostId to PostTypeId, so a fact can say whether it is a question."""
    types = {}
    posts = Path(dump_dir) / "Posts.xml"
    for row in iter_rows(posts):
        post_id = row.get("Id")
        if post_id:
            types[post_id] = row.get("PostTypeId", "")
    return types


def collect_transactions(dump_dir):
    """Group PostHistory rows by RevisionGUID, ordered by time.

    A RevisionGUID is the dump's own transaction id: the rows of one atomic
    edit share it. Rows lacking one (rare, older entries) fall back to their
    own Id so they still form a singleton transaction rather than being
    dropped or merged into an unrelated edit.
    """
    groups = {}
    for row in iter_rows(Path(dump_dir) / "PostHistory.xml"):
        guid = row.get("RevisionGUID") or f"row-{row.get('Id')}"
        entry = {
            "type": row.get("PostHistoryTypeId", ""),
            "post": row.get("PostId", ""),
            "date": row.get("CreationDate", ""),
            "user": row.get("UserId", ""),
            "text": row.get("Text", ""),
        }
        groups.setdefault(guid, []).append(entry)

    # Order transactions by their earliest row: the file is roughly time
    # ordered already, but grouping must not depend on that.
    ordered = sorted(
        groups.values(),
        key=lambda rows: (min(r["date"] for r in rows), rows[0]["post"]),
    )
    return ordered


def facts_for_transaction(rows, post_types, seen_posts):
    """The facts one transaction asserts.

    The first transaction touching a post also asserts its identity facts
    (type, author), so an entity is introduced by the commit that created it
    rather than by a separate bulk load. That keeps the history shaped like
    the site's real accumulation.
    """
    facts = []
    post = rows[0]["post"]
    if not post:
        return facts

    entity = f"post:{post}"

    if post not in seen_posts:
        seen_posts.add(post)
        post_type = post_types.get(post, "")
        if post_type == "1":
            facts.append(("se.post/kind", entity, "text", "question"))
        elif post_type == "2":
            facts.append(("se.post/kind", entity, "text", "answer"))
        author = next((r["user"] for r in rows if r["user"]), "")
        if author:
            facts.append(("se.post/author", entity, "entity", f"user:{author}"))

    for row in rows:
        kind = row["type"]
        text = html.unescape(row["text"] or "")
        if kind in TITLE_TYPES and text:
            facts.append(("se.post/title", entity, "text", text))
        elif kind in BODY_TYPES and text:
            facts.append(("se.post/body", entity, "text", text))
        elif kind in TAG_TYPES:
            for tag in parse_tags(text):
                facts.append(("se.post/tag", entity, "text", tag))
        elif kind in STATE_TYPES:
            attribute, value = STATE_TYPES[kind]
            facts.append((attribute, entity, "boolean", value))

    return facts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dump_dir", help="directory of extracted dump XML")
    parser.add_argument("output", help="output CSV path")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="stop after this many transactions (0 = all), for a small fixture",
    )
    args = parser.parse_args()

    dump = Path(args.dump_dir)
    if not (dump / "PostHistory.xml").exists():
        sys.exit(f"no PostHistory.xml in {dump}")

    print("loading post types", file=sys.stderr)
    post_types = load_post_types(dump)

    print("grouping transactions", file=sys.stderr)
    transactions = collect_transactions(dump)
    print(f"{len(transactions)} transactions", file=sys.stderr)

    seen_posts = set()
    written = 0
    emitted_txns = 0

    with open(args.output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["txn", "at", "the", "of", "as", "is"])

        for rows in transactions:
            facts = facts_for_transaction(rows, post_types, seen_posts)
            if not facts:
                continue
            emitted_txns += 1
            at = min(r["date"] for r in rows)
            for attribute, entity, value_type, value in facts:
                writer.writerow([emitted_txns, at, attribute, entity, value_type, value])
                written += 1
            if args.limit and emitted_txns >= args.limit:
                break

    print(
        f"wrote {written} facts across {emitted_txns} transactions to {args.output}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
