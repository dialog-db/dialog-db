# Record values: journal

What we tried, what it measured, and what we decided, newest last. The
[topic](../record-values.md) says why and sets the bars.

## Where things stand

**Status: Exploring.** See the [research index](../README.md) for what the
statuses mean.

- **Built:** nothing.
- **Decided:** records are flat and atomic. Fields are named by
  attributes and destructured as `{lat ?lat}`. Equality follows tokens as
  key comparison does, resolving them across sources, and the wire carries
  the canonical form. Repetition across records is shared through the
  replica's tokens, not inside a record. Payloads dialog does not
  interpret are carried as field values.
- **Open:** whether a record's attributes name its type, or it carries a
  tag; a value, or claims on an entity identified by its pairs (which
  decides whether fields are indexed); many-valued attributes in a record;
  documents that grow.
- **Stale:** the [topic](../record-values.md) still states the first
  entry's framing (a type tag, a string table inside the record, free
  field names). It is revised once the open three are answered.
- **Next:** settle the open three by reasoning, then a census of the
  records in tonk-team's history: how many, their sizes, and how much
  repeats across them.
- **Blocked:** neither dataset is in this session's container.

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

## 2026-11-06 — Equality follows tokens, and fields are attributes

**Question.** What a record's canonical form has to carry, once the
replica shares tokens across records, and how its fields are named.

**Method.** Discussion; nothing measured.

**Results.** As the maintainer stated them:

- Equality follows the pattern of key comparison: records read over the
  same source compare by their token references, records from different
  sources resolve their tokens, and the wire carries the resolved,
  canonical form.
- Repetition is expected across records, not within one.
- Fields are named by attributes: in `{lat ?lat}`, `lat` is an attribute
  reference, not a free name.
- Records are flat for now; nesting may come later.

Consequences raised, not yet decided:

- `Hash` cannot take the same-source shortcut: equal records from
  different sources must hash alike, so it runs over the canonical form,
  computed once and kept with the value.
- A sorted run of attribute and value pairs is an EAV run without its
  entity. Laid out as key tails, a record would compare, find a field and
  split into file tokens with the code and the tokens keys already use.
- Such a record is a concept instance without `this`. A concept's identity
  is the sorted set of its attributes' identities (`notes/concept.md`), so
  the attributes may already name the record's type.
- The alternative is claims on an entity identified by the hash of its
  pairs: atomic through the claim that refers to it, every field indexed,
  at (n+1) claims in three orderings per write, the cost #636 measured.

**Decision.** The four results above are settled. They replace the first
entry's string table inside the record and its "later occurrences
referring back": sharing moves from the record to the replica's tokens.
Not decided: whether attributes name the type, value or entity, and
many-valued attributes.

Commits: this entry.

## How to reproduce

Nothing to run yet.
