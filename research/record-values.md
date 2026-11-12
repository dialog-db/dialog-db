# Record values

Status: Exploring. See the [research index](README.md) for what the
statuses mean, and the [journal](record-values/journal.md) for where the
research stands and how each decision below was reached. The
[proposed design](record-values/design.md) answers each open question
below; the [first spike's results](record-values/results/2026-11-11-spike.md)
are the numbers it cites.

## Question

Can a record be an atomic value made of attribute and value pairs, which
a query destructures and the replica stores in the tokens its claims
already use, at a cost no higher than today's dag-cbor records?

## Hypothesis

A record laid out as its pairs, sorted, each encoded as a key's tail (the
attribute, the value's type byte, the value's order-preserving encoding),
compares, finds a field and splits into tokens with the code and the
tokens keys already use. Its attributes and values are then the same
tokens as the replica's claims, so in the single-file archive a record
costs mostly references, far less than its dag-cbor bytes, and
destructuring one field costs less than decoding the whole record.

In the key, a record is that sequence of tails itself, ended by a byte no
attribute starts with, not the sequence escaped as one byte string as a
`record:` value is keyed today. Its parts are then the key's own parts,
and the file splits a record the way it splits a key. A record then has
the two forms a tree node already has in the file: canonical bytes, which
are hashed and sent, and rows over the file's tokens, which local reads
use and which realize the canonical bytes.

## Why

Some values are compound but atomic: a location is a latitude and a
longitude that change together, a revision describes itself in one signed
record, a view carries its resolved bindings as one table. Split into
claims, such a value can be left half updated by concurrent writers, so it
has to be one value.

`Value::Record(Vec<u8>)` is a placeholder, keyed exactly like
`Value::Bytes`. Every record written today is dag-cbor with a type tag
its writer invented: `RevisionRecord` is typed by its attribute, tonk's
view `Bindings` and `Embeds` by a `kind` field checked on decode
(`tonk-template/src/bindings.rs`). A rule's source avoids `Record`
altogether and is stored as `Bytes`. None can be destructured in a query,
none can be read a field at a time, and none share anything with other
records or with claims. #636 measured the revision record at about 2.2 kB
of the 2.6 kB a one-fact commit adds to the root, written inline into both
the EAV and AEV keys.

## What a record is

Decided, each in a journal entry. The structured value is named a
**composite** (`Value::Composite`, `composite:`), a value type of its own;
"record" below means it, and opaque `record:` values stay as they are.

- **An atomic value.** It is written and replaced whole, and concurrent
  records compete as whole claims, by revision.
- **A set of attribute and value pairs.** A field may hold several values.
  Pairs are sorted, without duplicates.
- **No tag naming its kind.** A composite is one value type; nothing in it
  says which kind of composite it is (tonk's `kind`, the revision record's
  attribute). Dialog stores no type beyond each value's own type byte: an
  attribute is a relation read under an optional type and a pick,
  and concepts are read, not written. A record's fields are typed the same
  way, and a record satisfies whichever concepts its fields do. An
  attribute with no `as:` takes any value.
- **Fields hold any value.** That includes an entity, whose own claims are
  found by an ordinary join, and a nested record, which is destructured.
- **Destructuring is not selection.** Selecting `geo/lat` finds only the
  `geo/lat` claims the replica stores; holding a record with a `geo/lat`
  field changes nothing. `{ location: { geo/lat: ?l } }` reads a field out
  of a record already bound, and never scans. A record cannot stand where
  an entity is expected (`of` is typed `entity`).
- **Fields are named by attributes**, which bring their pick and their
  type, as a concept's fields do. Every value in a record shares the
  record's version, so `last` over several values ends on the tiebreak
  every election ends on when the version cannot decide: the hash of the
  value (proposed: of `Value::identity_key`; for a record, of its
  canonical form). Dialog's elections tie the same way since #752.
- **Equality follows tokens**, as key comparison does: records read over
  the same source compare by their token references, records from
  different sources resolve their tokens, and the wire carries the
  canonical form. `Hash` cannot take that shortcut, so it runs over the
  canonical form, computed once and kept with the value.
- **Repetition is expected mostly across records**, and is shared through
  the replica's tokens, not inside a record.

## Constraints

- **Canonical.** One logical record, one byte string: block hashes,
  spilled-value references, rule identity and signatures are all over it.
- **Blocks do not change.** One node is one block under its existing hash.
  A record's sharing with others lives in the file, below the hash; a
  record's canonical bytes never refer outside themselves.
- **Signed records are frozen.** A revision's signature is over its
  dag-cbor bytes, so revision records keep that encoding, and every record
  written today stays readable as it is.
- **Cross-target.** The encoding builds and reads on wasm; only the file
  archive is native.
- **A breaking change, said so.** It changes what a replica stores, so it
  gets a changelog entry under Compatibility and migration.
- **Splittable.** The file splits a key into tokens only where its parts
  delimit themselves and compare part by part in the key's order (key
  tails do; an escaped byte string has to be unescaped first). A record's
  canonical form has to be splittable the same way, recursively, for its
  parts to be the file's tokens.

## Not in scope

- **A merge function per type.** A replica that does not know a type
  cannot run its merge.
- **Selecting record fields.** By the rule above, a field that has to be
  found by value is a claim of its own.
- **Holding claims in query bindings as records.** Bindings live in memory
  and need no encoding; that is a separate change to `Match`.

## Open

- **The type in destructuring** is inferred from the pick applying, and is
  to be confirmed.
- **The surface of destructuring:** a pattern in the notation, a formula
  reading a path (jq's paths, naming attributes, not string keys), or
  both over one premise.
- **Documents that grow,** such as automerge: one value rewritten per edit,
  or its changes as a set under an `all` attribute, which needs no record
  at all (the journal's third entry).
- **Requiring records to satisfy a concept,** as `conforms` does for an
  entity field.
- **How the file holds a record's parts.** A file row has a fixed shape (a
  key's form, at most four parts, a datum), and a record has any number of
  pairs. A value token made of token names, or a section of rows of its
  own.
## What it is measured against

Today's records as they are: dag-cbor, on main's `fs` archive, and as one
token each in #733's file archive. Each variant runs on the same dataset:

| | encoding | storage |
|---|---|---|
| main | dag-cbor | `fs` |
| file, whole | dag-cbor | file archive, one token per record |
| candidate, whole | pairs as key tails | file archive, one token per record |
| candidate, tokenized | pairs as key tails | file archive, attributes and values as the file's tokens |

The measurements re-encode signed records to size them; nothing re-encodes
them in a replica. Before the variants, a census of the dataset's records:
how many, their sizes, how many spill past `inline_n`, and how much of each
repeats across records and with the replica's claims (a revision record's
parents, for one, are versions the history keys already hold as tokens).

Two costs the file has measured bear on the tokenized variant: splitting
nodes into rows is already the file's own largest cost (about 13% of a
warm command), and a node's tokens sit on hundreds of pages written across
the whole history. Splitting records adds to the first, and reading one
record from its parts may touch more pages than reading it whole. Both
are measured: the cost of a commit, and a cold destructure.

## Success

Proposed, for the maintainer to set before anything is measured:

- **Canonical.** Every record round-trips byte for byte through decode and
  encode, and through import into the file and rebuild, over generated
  records and every record in the dataset. A fuzzer finds no second
  encoding of one value.
- **Size on the wire.** The canonical record is at most 25% larger than
  the dag-cbor of the same data. Attributes are full relation names where
  CBOR keys are often short, so this one may not hold.
- **Size in the file.** Tokenized, records take under half the bytes they
  take as one token each.
- **Destructuring.** Reading one field costs less than decoding the whole
  dag-cbor record, at every record size in the dataset.
- **Commits.** The perf gate holds.

## Failure

The canonical record is more than 50% larger than dag-cbor, tokenizing
saves under 2× in the file, or reading one field is no cheaper than
decoding the whole. A spike that lands there says records stay dag-cbor,
typed by their attributes, and the file shares them whole, which is also
an answer.

In between, the maintainer judges the ratio on the numbers.

## Prior art

To learn from, not to lift:

- **Datomic tuples:** a fixed-length value of scalars, sortable and
  indexed; the value side of the value-or-entity choice, where component
  entities are the other.
- **FoundationDB's tuple layer:** an order-preserving tuple encoding, the
  same idea as laying pairs out as key tails.
- **[Fressian](https://github.com/Datomic/fressian/wiki):** tagged values
  naive readers read generically, and a cache of repeated values that is
  stateful across a stream ("readers have to start reading from where
  caching began"), which is why sharing here lives in the file.
- **[schemaboi](https://github.com/josephg/schemaboi):** the schema per
  file or message, unknown fields kept as foreign so a round trip loses
  nothing; not canonical.
- **FlexBuffers and CBOR stringref:** read in place by offsets, and
  strings written once and referred back to; neither canonical as writers
  use them.
- **jq:** paths, and a field of several values producing a result each,
  for the surface of destructuring.
