# Record values: journal

What we tried, what it measured, and what we decided, newest last. The
[topic](../record-values.md) says why and sets the bars.

## Where things stand

**Status: Exploring.** See the [research index](../README.md) for what the
statuses mean.

- **Built:** nothing.
- **Decided:** records name their type; fields can be destructured in
  queries and read without decoding the whole; repeated strings are written
  once; payloads dialog does not interpret are carried under their type;
  equality is of canonical bytes, with token ids a shortcut inside one
  replica.
- **Open:** the four decisions in the topic (ordering, names on the wire,
  documents that grow, destructuring without an index).
- **Next:** a census of the records in tonk-team's history: how many, by
  type, their sizes, and how much of each repeats within the record and
  across records.

## Datasets

- **tonk-team:** the team space's pushed history, the 9,070 blocks #733
  fetched (568.5 MiB). Its records are revision records and tonk's view
  bindings and embeds. Not committed; read from a copy.
- **Stack Exchange:** the `retrocomputing` replay (`DIALOG_SE_CSV`). Its
  only records are revision records, one per commit, so it measures those
  at scale and nothing else.

## 2026-11-06 — Framing: what a record is, and where sharing lives

**Question.** What should a record value be, given a placeholder, three
ad hoc dag-cbor uses, and the single-file archive's tokens?

**Method.** Read `notes/record-value.md`, `Value::Record`'s keying
(`ordvalue.rs`, `key.rs`), every writer of a record in dialog and tonk,
#636 and #733, and the fressian and schemaboi documentation.

**Results.**

- Every record today is dag-cbor, and each writer invents its own type
  tag: the attribute for revisions, a `kind` field for tonk's bindings.
- Fressian, schemaboi and FlexBuffers each let the writer choose an
  encoding, so none is canonical as it stands. Fressian's cache and
  schemaboi's schema depend on the stream or file they sit in.
- A record's bytes are hashed, sent in blocks and signed, so they must
  stand alone. Sharing across records belongs to the file, as it does for
  nodes.

**Decision.** A record names its type and has one canonical, standalone
encoding; equality is of those bytes, and token ids compare them inside a
replica. Field names live in the record, written once, with later
occurrences referring back. Where the shared strings sit (one region or
interleaved) and whether references are offsets is for the spike. The bars
in the topic are proposed and await the maintainer.

## How to reproduce

Nothing to run yet.
