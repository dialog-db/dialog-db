# Record values

Status: Exploring. See the [journal](journal.md) for where the research
stands.

## Idea

Dialog stores `{the, of, is}` claims where `is` is one value. Some values
are compound but atomic: a location is a latitude and a longitude that
change together, a revision describes itself in one signed record, a view
carries its resolved bindings as one table. Splitting such a value into
claims lets concurrent writers leave it half updated, so it has to be one
value. We have been working around this for a long time, and it is time
to support it properly.

`Value::Record(Vec<u8>)` is a placeholder. It is keyed exactly like
`Value::Bytes`, and what is inside is each writer's own convention:

- `RevisionRecord` is dag-cbor, and its type is implied by its attribute
  (`dialog.db/revision`).
- tonk's view `Bindings` and `Embeds` are dag-cbor with a `kind` field
  inside, checked on decode (`tonk-template/src/bindings.rs`).
- A rule's source is dag-cbor stored as `Value::Bytes`, because `Record`
  "isn't yet supported end-to-end" (`dialog-repository/src/rules.rs`).

Every one of them invents a type tag, because each wants a schema
invariant without inventing a data encoding. None of them can be read a
field at a time: CBOR is decoded whole. And none share anything: the same
field names and the same issuer DID are written again in every record.
#636 measured what that costs: the revision record is about 2.2 kB of the
2.6 kB a one-fact commit adds to the root, written inline into both the EAV
and AEV keys.

What I want is a record value that:

1. **Names its type.** One mechanism for what tonk's `kind` and the
   attribute-implied revision type do today.
2. **Can be destructured in queries.** A query binds a field of a record
   without a formula written per type, as `dialog/revision` is today.
3. **Reads a field without decoding the whole.** Ideally in place, through
   a mapping of the file.
4. **Writes repeated strings once.** A field name or a repeated value is
   written at its first occurrence, and later occurrences refer back to it.
5. **Carries foreign payloads.** A record, or a field of one, can hold
   bytes dialog does not interpret, an automerge document for instance,
   under a type that says what they are.
6. **Imports into the single-file archive.** The file
   (`research/single-file-format/`, PR
   [#733](https://github.com/dialog-db/dialog-db/pull/733)) interns a
   record's strings as its own tokens, replacing them with offsets into
   the file, and rebuilds the record's canonical bytes for sync and hashing.

## Equality

Equality is equality of a record's canonical bytes. Block hashes, spilled
value references, rule identity and signatures are all over those bytes,
so a canonical encoding is required even though it is not what the file
stores. Inside one replica, comparing token ids is a shortcut for it, and
the wire carries the canonical form with every token resolved. The
shortcut holds only if:

- **a canonical value has exactly one token.** Two ids for the same bytes
  make equal values compare unequal. The writer's token dictionary is still
  open in #733.
- **ordering never uses token ids.** Ids follow insertion; the tree sorts
  by canonical bytes and must stay deterministic across replicas.

Equality inside a record must also agree with equality outside it: `3`
and `3.0` are one value in the number band, so they are one value in a
field too, and fields compare regardless of the order they were written
in. A field value is a dialog `Value` in dialog's encoding, not a CBOR
scalar, which also makes a DID in a record the same file token as the same
DID in a claim.

## Constraints

- **Canonical.** One logical record, one byte string. Formats that let the
  writer choose (whether to cache a value, whether to share a key, a packed
  or unpacked integer) are canonical only under a profile that fixes every
  choice by rule.
- **Blocks do not change.** One node is one block under its existing hash
  (the single-file constraint). A record's sharing with other records lives
  in the file, below the hash, never in the record's bytes: a record cannot
  refer to anything outside itself.
- **Signed records are frozen.** A revision's signature is over its
  dag-cbor bytes, so it is never re-encoded. Today's records stay readable
  as they are, forever, as a type whose payload is foreign.
- **Cross-target.** The encoding builds and reads on wasm; only the file
  archive is native.
- **A breaking change, said so.** A type in `Value::Record` changes what a
  replica stores and how a value hashes, so it gets a changelog entry under
  Compatibility and migration.

## Not in scope

- **A merge function per type** (`notes/record-value.md`). A replica that
  does not know a type cannot run its merge, and passing unknown types
  through is a goal.
- **Indexing fields.** Destructuring projects a field once a claim is
  found; a field that has to be looked up by value is a claim of its own.
- **Holding claims in query bindings as records.** Bindings live in memory
  and need no encoding; that is a separate change to `Match`.

## Open decisions

These are the maintainer's, not the spike's:

1. **Ordering.** An encoding with offsets does not sort by meaning, so
   records become equality-only in the value index. Acceptable, or do some
   records need to sort (an order-preserving tuple encoding)?
2. **Names on the wire.** The file can store a field name once across all
   records; the wire still carries it in every record. Does the wire cost
   matter, or only storage?
3. **Documents that grow.** A record is immutable, so every edit to an
   automerge document writes the whole document as a new value, and the
   file shares whole tokens only. Record, asset a record points to, or a
   record chunked so unchanged chunks are shared?
4. **Destructuring without an index.** `where lat > 30` scans every record
   under the attribute. Enough?

## Questions for the spike

- **Where the repetition is.** Within one record (lists of structs) or
  across records (field names, issuers)? Repeats within a record are what
  backlinks remove; repeats across records only the file removes.
- **Whether reading a field pays.** Decoding a 700-byte record whole costs
  about a microsecond. Which records are both large and destructured?
- **Where shared strings sit.** In one region of the record (a header or a
  footer, which read the same for a record written whole) or interleaved at
  first occurrence. Offsets or ordinals decide random access: an ordinal
  backlink (CBOR stringref, fressian's cache) needs a scan from the start.
  One region is the easier import: intern each entry once, remap the ids.
- **What import and rebuild cost.** #733 measured rebuilding a node's block
  at about 10× reading its rows. A record in the file is a frame inside a
  frame; its rebuild is paid on every send.

## What it is measured against

Today's records as they are: dag-cbor, on main's `fs` archive and as whole
value tokens in #733's file archive. Variants, each on the same dataset:

| | encoding | storage |
|---|---|---|
| main | dag-cbor | `fs` |
| file, records whole | dag-cbor | file archive, one token per record |
| candidate, whole | the new encoding | file archive, one token per record |
| candidate, tokenized | the new encoding | file archive, strings as file tokens |

Candidates: dag-cbor with a type (the conservative answer), CBOR with
stringref, a canonical profile of FlexBuffers, and a layout of our own.

## Success

Proposed bars, to be set before anything is measured:

- **Canonical.** Every record round-trips byte for byte, through decode
  and encode and through import into the file and rebuild, over generated
  records and every record in the dataset. A fuzzer finds no second
  encoding of one value.
- **Size.** The canonical encoding is no larger than dag-cbor for the same
  records. Tokenized in the file, records take under half the bytes they
  take as whole tokens.
- **Field reads.** Reading one field costs less than decoding the record
  whole with dag-cbor, at every record size in the dataset, and at least 5×
  less for records over 1 KiB.
- **Commits.** The perf gate holds.

## Failure

The canonical form is more than 20% larger than dag-cbor, tokenizing saves
under 2× in the file, and reading a field is no cheaper than decoding the
whole for records under `inline_n`. A spike that lands there says records
are typed dag-cbor and the file shares them whole, which is also an answer.

In between, the maintainer judges the ratio on the numbers.

## Prior art

To learn from, not to lift:

- **[Fressian](https://github.com/Datomic/fressian/wiki):** tagged values
  that naive readers read generically, and caching of repeated values and
  struct types. The cache is stateful across a stream ("readers have to
  start reading from where caching began") and optional, so it is not
  canonical.
- **[schemaboi](https://github.com/josephg/schemaboi):** the schema per
  file or message, self-describing data, local and remote schemas merged,
  unknown fields kept as foreign so round trips lose nothing. Experimental,
  and not canonical ("different files storing the same data model may be
  encoded differently").
- **FlexBuffers:** schemaless, read in place by offsets, the root at the
  end, keys shared by offset. The closest existing shape to goals 3 and 4,
  not canonical as writers use it.
- **CBOR stringref (tags 25 and 256) and Packed CBOR:** first occurrence
  inline, later ones by reference, under a deterministic rule. Canonical,
  but ordinal references are read in order.
- **Cap'n Proto's canonical form:** a format read in place by offsets that
  specifies one canonical encoding for signing.
- **Amazon Ion's symbol tables:** field names interned per stream.
- **FoundationDB's tuple layer:** an order-preserving tuple encoding, if
  decision 1 says records sort.
