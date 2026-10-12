# Identity, storage and notation

Status: proposal for 0.3.0. Nothing here is implemented yet.

## Problem

One serde shape does four jobs today. The descriptor types (`AttributeDescriptor`,
`ConceptDescriptor`, `DeductiveRuleDescriptor`, `Term`, `Value`) are:

- the JSON notation people and tonk write;
- the stored rule body (dag-cbor of the descriptor);
- the in-memory structure the compiler reads;
- the bytes identities hash (`AttributeDescriptor::to_cbor_bytes`,
  `ConceptDescriptor::to_cbor_bytes`, `DeductiveRule::try_this` over
  `canonical_descriptor()`).

So a change to any one of them changes the other three. In one release cycle,
spelling picks as entities, naming types as entities and tagging constants each
changed the JSON notation, the stored format and every identity at once.

The shared shape is also where the ambiguity bugs came from. `Value` serialized
untagged, so text, an entity and a symbol of one spelling encoded the same, and
a stored rule's constant decoded as whichever type its spelling resembled. A
rule matching the text `https://example.com` derived nothing once committed, and
two rules matching different values shared an identity.

## Proposal

Four representations, each with one job:

| Representation | Job | Constraint |
|---|---|---|
| Notation (JSON, tonk's YAML) | what people write and read | ergonomic; never hashed |
| AST | what the compiler reads | typed Rust values; no encoding concerns |
| Identity | what names a definition | a function of the AST value, never of an encoding |
| Storage | what a replica keeps and syncs | deterministic, versioned, fast to read |

Identity is the one that must not move once released, so it gets defined first,
over the AST and nothing else.

## Identity: merkle-reference v2

Identities are references in a second version of
[merkle-reference](https://github.com/Gozala/merkle-reference/blob/main/docs/spec.md),
implemented as a `merkle-reference` crate in this repository. The crate knows
nothing about dialog: it defines references, the shapes of composites and
canonical content, and dialog defines its own types on top of it.

v2 keeps v1's properties:

- identity is a function of the value;
- a composite value is identified through its parts' references;
- **a part stored inline and a part stored apart and linked by its reference are
  indistinguishable**, so any part can be moved out of its parent, or in, without
  changing the parent's reference, and a part's inclusion can be proved.

It changes how a value's type enters its reference, and it halves the hashing a
reference takes.

### A type is a view over bytes

Every value is bytes read under a type, and a type is an encoding of those bytes.
The type is not a tag byte in the content. It is the key the content is hashed
under:

```text
reference(v : bytes) = BLAKE3(v)
reference(v : T)     = BLAKE3-keyed(key = reference(T), content_T(v))
```

- **Plain bytes** hash in BLAKE3's plain mode, so a blob's reference is
  `blake3(blob)`. That is what dialog already keys spilled values and blobs by
  (`make_reference`), so a blob's reference is its store key.
- **Every other type** hashes in keyed mode, with the type's own reference as the
  key. A different key means a different IV, so the same bytes under two types
  give two unrelated references. No tag is prepended, and no keyed reference can
  equal the plain hash of any byte string. BLAKE3's root over more than one chunk
  is itself a parent of chunk chaining values, so a crafted byte string could
  otherwise share an id with a structured value.
- **A type is itself a value**, of type `type`, so `reference(T)` is the keyed
  hash of `T`'s definition under `reference(type)`. The recursion ends at
  `reference(type)`, which is a constant: `BLAKE3-derive_key("merkle-reference v2
  type", "")`.

**Types are named by URI**, and the crate defines no scalar types: only raw
bytes (plain BLAKE3) and the three shapes of a composite, `list:`, `map:` and
`set:`. What a scalar is, and how its content is written, belongs to whoever
names it. (An earlier revision kept v1's scalars as core types; they were
dropped because a hashing crate that fixes what a number is forces that choice
on everyone, and dialog's numbers already disagree with IEEE 754 bit patterns.)
Users plug in their own types by implementing the crate's `ToReference` for
the types they own; the crate's collections of them follow.

**Dialog's types** are values of `type`, so the crate needs nothing to add one.

Value types, which a stored value or a constant has:
- `number`: every number, in the one-band encoding (see
  [Numbers](#numbers-one-band-typed-views));
- `text:` (UTF-8 as written) and `boolean:`;
- `entity` (text that parses as a URI) and `symbol` (`namespace/name` text);
- `boolean`, `bytes` and `record`.

Views, which an attribute or a slot declares with `as:`:
- `integer` (whole numbers);
- `natural` (whole numbers that are not negative);
- `float` (every number, read as a float).

Views have no values of their own: they select and convert numbers. A view is
still a type with a reference, so an attribute's identity hashes the view it
declares.

AST nodes: attribute, concept, rule, premise and term.

**A type is identified by its name:** `reference(T)` is the keyed hash of `T`'s
name, a URI, under `reference(type)`. Dialog's names are its type entities
(`number:`, `integer:`, `entity:`, …), so a type dialog already names by entity
keeps that name.

### Composites hash their parts' references

A composite's content is always its parts' references, in order, never their
inline bytes:

```text
content_list(v) = reference(v[0]) ‖ reference(v[1]) ‖ …
content_map(m)  = reference(k0) ‖ reference(m[k0]) ‖ reference(k1) ‖ …   (keys in order)
```

A struct (an AST node) is a map from field names to values, or a list in a fixed
field order. Because the parent depends only on its parts' references:

- **Storage chooses freely** where each part lives. A rule body can embed the
  concepts it reads or link them by reference, and the rule's reference is the
  same either way. That is the indistinguishability property.
- **Proofs are per part.** BLAKE3 hashes the parent's content as a tree of 1 KiB
  chunks (32 references each). A proof that a part belongs to its parent is the
  chunk holding the part's reference, plus the chaining values up to the root.
  Verifying needs the parent's type reference, which is the key. Whether an
  existing Bao implementation supports keyed mode is to be checked. Otherwise
  `blake3::hazmat` can verify, because its merges take a `Mode`.
- **A part shared by many parents is hashed once.** A concept that ten rules read
  is hashed once and its reference reused.

**Why it is cheaper than v1.** Counting BLAKE3 compressions, each of which hashes
a 64-byte block:

| | v1 | v2 |
|---|---|---|
| Scalar of up to 64 bytes | 2: hash the content, then fold it with the cached hash of its format tag | 1: hash the content keyed by its type |
| Composite of n parts | about n: n − 1 pairwise folds, plus the tag fold | about n/2: one keyed hash over n × 32 bytes, two references per block, plus one parent per 32 parts |

So v2 roughly halves the hashing while keeping v1's tree semantics: every part
still has a reference that its parent hashes.

**What I first proposed instead, and dropped.** An earlier draft of this note let
a part be written inline as bytes, with no reference of its own, to skip hashing
it. That saved a hash per scalar, but a parent's reference then depended on
whether a part was inline or linked, which breaks the property above. Every part
is now referenced.

**Why not fold with BLAKE3's internals.** `blake3::hazmat` (in 1.8.2, our locked
version) can merge subtrees, but a subtree's chaining value depends on its input
offset (`set_input_offset`). A part folded that way would have no reference that
holds regardless of position, so a concept referenced by two rules would hash
differently in each. Hashing the parts' references as content keeps references
position-independent and stays out of `hazmat` for computing them.

### Canonical content

A reference is stable only if each type's content is a function of the value.
Two families of choices:

- **No lengths in identity.** Each value's content is a whole hash input, and a
  composite's content is a sequence of 32-byte references, so identity needs no
  length prefixes. Lengths and counts appear only in storage, which uses bijou:
  [`bijoux`](https://docs.rs/bijoux), from Ink & Switch's Subduction work. Bijou
  is bijective (every integer has exactly one encoding), takes one byte for
  values up to 247, and decodes 2 to 10 times faster than LEB128.
- **Dialog's `number`** has as its content its encoding in the
  `dialog-number` crate, which is SQLite4's decimal key encoding unchanged
  (see [Numbers](#numbers-one-band-typed-views) and the crate's README):
  - a class byte holding the sign and the size of the exponent, and the exponent
    itself when it is 0 to 10;
  - otherwise the exponent as SQLite4's order-preserving varint;
  - the base-100 digits, each `2X + 1`, the last `2X`;
  - every byte after the class inverted for a negative number.

  These are the bytes the value index will key numbers by once it moves to one
  band, so a number has one encoding for both identity and index key. Its
  consequences, the same in both:
  - `5` and `5.0` are one value, whichever of natural, integer or float holds
    it, so they join and a retraction of one retracts the other;
  - a float is the decimal it prints as (its shortest round-trip decimal): the
    float `0.1` is `0.1`, and the float `1e23` is 10²³;
  - integers of any width are exact;
  - `-0.0` is `0`, and NaN is one value. Today dialog keeps NaN payloads
    distinct, and a payload can change when a value passes through JavaScript.
- **Maps** order entries by their keys' references; a set of premises, by their
  references after labeling.
- **Structs** (AST nodes) are maps from field names to values, with an absent
  optional field left out. A field added later then changes no existing
  identity.
- **Local variables keep the canonical labeling** `rule/canonical.rs` does today,
  which is datalog's counterpart to de Bruijn indices: variables are join points,
  not lexical scopes.
- **Concept field names stay out of identity**, as they are today. A concept
  is its set of attributes: a field name only binds a variable to an
  attribute, and a rule installed under other names is re-headed by attribute
  (see `attribute-heads.md`). An earlier draft of this note said the opposite,
  which was wrong.
- **Descriptions stay out**, as they are today.
- **Text is hashed as written**, with no Unicode normalization. The database
  compares text byte for byte, so identity does the same. If two texts are equal
  in the index, their references are equal, and the reverse.

### Constants

A constant's reference is keyed by its type, so the type is part of identity
whatever the slot declares. Identity therefore tracks meaning even if slot
inference changes in a later release.

## Storage

Measured in the `rule-install` perf scenario (100 rules committed, then read back;
206.6M instructions):

| Step | Instructions | Share |
|---|---|---|
| Parsing stored bodies (dag-cbor) | 3.2M | 1.5% |
| Encoding bodies | 3.7M | 1.8% |
| Computing identities | 10.3M | 5% |
| Compiling decoded descriptors | 26.7M | 13% |

The byte format is not where read time goes, so the storage format is chosen for
determinism, evolvability and size, and speed comes from not recompiling.

- **Body format 3:** a binary encoding modelled on the WebAssembly binary format,
  with bijou in place of LEB128. It is described below.
- **Compiled-rule cache keyed by rule reference:** in process first, persistent
  later if it measures. This is what removes the 13%.
- **Values outside rules** (`Changes` batches, the session-overlay handoff):
  every value carries its type in storage. This fixes integers coming back as
  floats, which is a live bug in tonk's overlay handoff today.

### Body format 3

A body is laid out like a WebAssembly module. The layout below is a sketch; the
section ids and field order are settled when it is implemented.

```text
body    := magic version section*
magic   := 0x00 'd' 'l' 'g'
version := u32 little endian (3)
section := id:u8 size:bijou content
```

| Id | Section | Content | Required |
|---|---|---|---|
| 1 | references | the references the body names (types of `any` values), 32 bytes each | when the body has any |
| 2 | strings | relation names, field names and text constants, each length-prefixed UTF-8 | when the body has any |
| 3 | definitions | the attributes and concepts the rule uses, each written once | when the body has any |
| 4 | head | the conclusion: a concept by its index in the definitions | yes |
| 5 | premises | each premise: its kind, then its operands | yes |
| 6 | reduce | the folds of an aggregating rule | no |

**Operands point into the tables.** A premise reads a concept by its index in
the definitions section and a field by its index in the strings section. Each
definition and name is stored once per body, and decoding does no lookups.

**Schema-driven values.** The schema says what type each slot holds, so a stored
value carries no type where the slot declares one. In an `any` slot it carries
the index of its type's reference in the references table. That mirrors identity,
which names types by reference, and the notation's boxing rule. It keeps bodies
smaller than self-describing CBOR, and no value is read as a type its spelling
resembles.

**A body is self-contained.** Everything needed to load and run the rule is in
its body: no part is stored apart and linked by reference. Identity would allow
linking (a parent's reference is the same with a part embedded or linked), and
linking would store a concept read by many rules once. It is not done, because
a linked body cannot load until every part it names has synced, and a part
could not be retracted while any rule names it. Within a body, the definitions
table writes each concept once, as a WebAssembly module is self-contained and
names its imports.

**The authored form is a separate claim.** Identity leaves out three things the
author wrote: the names of local variables (identity hashes canonical labels),
the order of premises (identity sorts them), and descriptions (of the rule, its
concepts and their attributes). All three are kept outside the body, under their
own attribute on the rule's entity:

- the label each variable had in the source;
- the source position of each premise, as a permutation of the canonical order;
- the descriptions.

Loading a rule never reads it. Display and diagnostics read it when they need
it: an error names variables as the author did by loading it on the error path,
and shows canonical labels when it is missing. The head's field names stay in
the body, since identity hashes them. A premise binds by attribute, so its field
names are not in identity either; whether they move out too is settled with the
encoding.

Two rules that differ only in names, premise order or descriptions are one rule, with one
reference and one body, so a later install of the same rule can bring a
different authored form. Only the newest is kept: the attribute is read under
`last`, so a re-install replaces the authored form and leaves the body alone.
Retracting a rule retracts both claims.

**Compatibility.** An unknown section id below 128 is an error. An unknown id
of 128 or above is skipped, which is how later versions can add optional data
without a new format number.

**A canonical layout.** WebAssembly's encoding is not canonical: it allows
over-long LEB128 and some freedom in section order. Bodies are not hashed, but a
body should still be a function of its rule, so equal rules store equal bytes and
dedupe. Bijou removes the over-long encodings by construction, and the rest is
layout:

- sections appear in ascending id order, each at most once;
- the references and strings tables are sorted and hold no duplicates;
- premises appear in canonical order (by their encoded bytes, after labeling).

With the authored form outside the body, a body is a function of its rule: two
spellings of one rule store the same bytes.

**What it is not.** A body's indices are local to that body, as a WebAssembly
module's are. The same concept has a different index in different bodies, so a
body's bytes are never an identity. The rule's reference is computed from its AST,
with indices resolved back to references.

**Validation.** Bodies arrive from untrusted peers, so a decoder validates in one
pass and refuses a malformed body instead of panicking. That means bounds-checked
sizes, indices within their tables, and every required section present.

### Ideas taken from schemaboi

[schemaboi](https://github.com/josephg/schemaboi) is a binary format for data
whose schema keeps changing. It is not canonical ("different files storing the
same data model may be encoded differently"), so it is a source of ideas, not of
code:

- **Data names the schema it was written under.** Schemaboi stores the schema
  with the data. Here a body's types are references, so a body names its schema
  exactly. A reader with a newer schema merges the two instead of guessing.
- **Unknown data round-trips.** Schemaboi keeps fields and variants it does not
  understand as foreign data and writes them back on save. Bodies would do the
  same with the optional sections above 128, and any open enum (a premise kind or
  value type a newer release adds) would keep a variant it does not know as
  foreign. A rule holding foreign parts can be stored and synced, but not
  compiled.
- **Open and closed enums.** A closed enum cannot gain variants; an open one
  must expect variants it does not know. That makes which parts of the AST may
  grow an explicit decision for each type.
- **Renames stay local.** Schemaboi keeps a field's stored name forever and maps
  it to a display name locally. That is worth weighing for concept fields, whose
  names are in identity: a rename today is a new concept.
- **Widening a struct into an enum is compatible**, because "a struct is just an
  enum with 1 variant". That fits a premise or term shape that later gains
  alternatives.

## Numbers: one band, typed views

**The direction, assumed by the identity design above.** Numbers share one band
in the value index, in SQLite4's decimal key encoding (the `dialog-number`
crate), and dialog's number types are views over it. 0.3.0 adopts the encoding
for identity, so that when storage follows, a number's identity content and its
index key are the same bytes.

**Decimal, not binary (decided).** The prototype below used binary digits, which
keep every float's exact binary value but cannot hold `0.1`. The crate uses
decimal digits instead, SQLite4's choice:
- a float is taken as the decimal it prints as, so JSON text, a JavaScript
  number and a person's `0.1` are one number, and fixed-point views
  (`decimal(2)` for money) become possible;
- what it gives up is a float's binary value (`1e23` is 10²³);
- arithmetic stays in floats for now, and its results are approximate.

Measured against the binary encoding, integers cost about the same (16–65 ns to
encode or decode), floats cost more (50–118 ns to encode, 41–85 ns to decode),
and keys are smaller (a price 3.9 bytes against 10.7). Scans compare bytes and do
not decode. In WebAssembly the crate adds about 8 KB beside `serde_json`, which
already brings the float printer and parser it uses. The crate's README has the
numbers and the view model.

**The idea.** Store every number in one band of the value index, so 5 and 5.0
sort together, and make dialog's number types views over that band:

- an integer view reads every number, and a number with a fraction as its whole
  part;
- a natural view reads every number that is not negative, the same way;
- a float view reads every number as a float;
- a number view reads all of them as they are.

How views read and write is under [Views read and write](#views-read-and-write).

**How dialog stores numbers today.** Each type has its own band: natural,
integer, float, each a type byte then a fixed-width payload, the same encodings
TerminusDB uses. Consequences:

- A query for one type never finds another: integer 5 misses a stored natural 5.
- JSON already splits integers across bands. A non-negative number decodes as a
  natural and a negative one as an integer, so an untyped field holds both.
- A range over a variable that may be any number is not pushed into the scan. The
  scan reads all of the attribute's values and filters them row by row.

**TerminusDB** (`tdb-succinct`, the typed dictionary `terminusdb-store` uses)
also keeps one sorted segment per datatype and never compares across types, so
adopting it would not give these views. Its fixed-width encodings are dialog's.
Its decimals keep the decimal string's digits, so `0.5` and `0.50` are distinct
entries. Its encoders run 31 to 280 ns per value, mainly because each call
allocates.

**One band is viable.** A prototype encoding:
- writes sign class, then the power of two of the leading bit, then the
  remaining bits in 7-bit groups with a continuation bit (the prototype also
  ended with a byte saying how the number was written; 0.3.0 drops it, see
  below);
- inverts the bytes of negative numbers;
- is the binary counterpart of SQLite4's numeric keys, which share one encoding
  for integers and floats.

Over 200,000 mixed values (including subnormals, ±infinity, i128 and u128) it
round-trips exactly, its byte order equals exact numeric order, and whether a
value is whole can be read from its bytes. Measured against today's bands:

| | Today | One band |
|---|---|---|
| Encode / decode | 2–3 ns | 12–36 ns, about 0.3% of a scanned row's cost (~20,000 instructions in `read-scan`) |
| Small integers, EAV key (front-coded) | 32.1 B | 20.6 B |
| Timestamps, EAV key | 30.1 B | 23.1 B |
| Floats, EAV key | 24.1 B | 26.7 B |
| Integer-view range of 10 (in memory) | 3 seeks, 3.0 µs | 1 seek, 1.7 µs |
| Integer-view range of 1,250 rows (in memory) | 25 µs | 49 µs, a per-row whole-number check |

The in-memory rows overstate per-row costs relative to dialog, where a row costs
thousands of instructions and a seek far more. So one band likely wins selective
lookups and roughly ties wide scans, but that has to be measured in dialog: the
perf suite has no numeric scenario today.

**What it would decide:**

- **Numeric equality.** Decided: 5 and 5.0 are one value. A trailing byte saying
  how a number was written would keep "written as a float" queryable, but then
  5 and 5.0 would not join, and retracting one would leave the other. Those bite
  users; the written-as distinction is a leftover of Rust's types.
- **Zero and NaN.** `-0.0` becomes `0`, and NaN is one value.
- **Identity.** Dialog's number types become views over one `number` content,
  these bytes, keyed by the view's type reference, consistent with "a type is a
  view over bytes" above.
- **Migration, once, outside the read path (decided).** The read path knows only
  the one band; nobody pays for dual reads. Instead:
  - a tree in the new encoding carries a critical manifest field (an odd code),
    so a build that predates it refuses to read the tree rather than misread it.
    A tree without the field holds the old bands;
  - migrating a tree rewrites every number into the one band, keeping each
    claim's version and cause, and sets the field. It is a one-time data
    migration, done with the 0.3.0 identity migration so users migrate once;
  - the code that reads the old bands and migrates lives behind a feature (or a
    separate crate), so only builds that meet old data carry it. A build without
    it refuses a tree that lacks the field and says why;
  - migration must be a deterministic function of the tree's contents. Then two
    replicas that migrate independently produce the same tree, a merge with a
    peer's unmigrated tree can migrate that tree and the merge base alike before
    merging, and a claim one side retracted before migrating cannot come back
    through the other side's migrated copy;
  - older revisions stay in the old bands, so reading history across the
    migration needs the migration code too.

  The earlier design here, a transitional version that read both bands and
  re-encoded claims as they were written, would have made every read pay during
  the transition, and an "only the new band" version would have needed every
  replica of a space to finish first, which nothing in a local-first system can
  guarantee.

  Three hazards to design for:
  - **merge:** a claim one replica moved to the new band and another retracted
    under its old key must not come back, so merge translates old-band keys to
    the new encoding before comparing;
  - **claim identity:** a move carries the claim's version and causal history to
    its new key, so it is a re-key, not an assert;
  - **single-valued reads:** a `last` cell may hold one value in an old band and
    one in the new band. Election already scans a whole cell across value types,
    but this needs a test.

  The simplest safe start never moves claims: old bands are frozen, a retraction
  removes the old key, and the dual reads stay until a later release compacts the
  old bands.

**The alternative**: the same views over today's three bands. An integer view
scans the natural, integer and float bands, filtering floats to whole ones. It
needs no storage change, but takes three seeks per lookup and keeps cross-type
equality in the query layer.

**Measured in dialog (the assessment, not in 0.3.0).** A prototype on the
branch `claude/number-band-prototype` adds the band beside today's bands: a
critical manifest field picks which band a tree writes and reads, and the
`dialog-perf` suite gained numeric scenarios. Against the per-type bands, in
instructions:

| Scenario | One band |
|---|---|
| Point lookup | −1% |
| Scan | +1.0% |
| Narrow range | +2.1% |
| Commit | +2.6% |
| Range over mixed number types | −57% |
| Wide range | +105% |

- The wide range is an artifact of the scenario: its attributes interleave in
  the value index, and the scan reads the other attributes' numbers too. The
  per-type bands hide this because each band holds fewer of them. Scanning by
  attribute and filtering by value (estimate-based index routing) is the fix,
  and it is independent of the band.
- What closed the gap was parsing a key once per row rather than once per
  component, and the length scan: a scan compares bytes and never decodes.
- On tonk's real workload, `rule_load` ran 20% fewer instructions with the
  band. The seed commit makes 4.4 times fewer tree edits, which is not yet
  explained; capturing it as a `dialog-perf` scenario is the next step.
- The first tonk run returned no rows for natural fields under the band, until
  views read every stored number. A band without views silently loses rows.

Conclusion: identity keeps `dialog-number`. The band is viable, and adopting it
needs numeric equality on `Value`, the type lattice over views (below), writes
through a view, and the migration.

## Number types

Types an attribute can declare, beside the stored types:

- **`number:`** holds every number, as stored. It types an attribute or a
  slot, never a stored value or an index key.
- **Fixed-width types**, one for each Rust number: `natural:8` to
  `natural:128`, `integer:8` to `integer:128`, `float:32`, `float:64`. They
  are named by kind and width rather than by Rust's names, so they read as
  narrowings of the kind.
- **`natural:`, `integer:` and `float:` stay unbounded.** They are not
  aliases of the 128- and 64-bit types, so they can hold bignums later
  without changing identity.

A fixed-width type is a view, read as [Views read and write](#views-read-and-write)
says: it coerces where it only loses precision (a float through `natural:8`
truncates, a decimal through `float:32` rounds to nearest), and it leaves out
what it cannot hold. A write refuses a value outside the type. Until storage
moves to one band, a view reads the per-type bands and stores in the band of
its kind.

`#[derive(Attribute)]` declares the type from the Rust field: `u32` declares
`natural:32`. `usize` declares `natural:`, since its width differs between
native and WebAssembly and an identity must not.

**Every target reads a view alike.** The `float:32` view gives one NaN, and
tests pin the bits of each view's result, run on native and in WebAssembly.

**Not yet:** the query type lattice keeps `natural:`, `integer:` and `float:`
disjoint, so `natural:` does not include `natural:8`. A slot declaring a
fixed-width type gets no interval pushdown, because the scan has to admit
rows of every stored type through the view. `Value`'s equality still tells 5
from 5.0.

## Views read and write

A view (an attribute's `as:`) is a lens over the one numeric domain. It applies
when reading and when writing, and two apps may read and write one relation
through different views.

**Reading:** a view coerces where it can only lose precision, and filters where
the stored value is outside its range. An integer view reads 34.5 as 34; a
natural view does not see −3 at all. Coercing −3 to 0 instead would hide a real
difference, as the write rule below shows.

**Writing:** a write through a view reads the cell first, through the same view,
as a write under a choosing pick already does to find the claim it succeeds.
- If the read already yields the value being written, the write is a no-op. A
  natural-view app writing 34 over a stored 34.5 leaves 34.5.
- Otherwise the write succeeds the claim with exactly the value asserted. The
  natural-view app writing 35 stores 35.
- Under `all`, a write is a no-op when the set read through the view already
  holds the value, and appends otherwise.

So the store holds only values someone asserted: no write merges its value into
another's (writing 35 over 34.5 never stores 35.5). This is the existing rule
that writing a value the cell already holds writes nothing (`succession.rs`),
with equality judged through the view. A no-op judged on a stale read already
fails at publish (`batch.rs`).

Each view must satisfy two laws, which make it a well-behaved lens:
- writing what you read changes nothing (the no-op);
- reading what you wrote gives it back, so the view's coercion is idempotent on
  its own values (floor of 35 is 35).

The cost: a view cannot express a change it cannot see. Through a natural view,
"exactly 34" and "unchanged" are the same write; dropping the half takes a view
that sees it. That is acceptable only where what the view hides is precision,
which is why a view filters, rather than coerces, a value outside its range.

**Where it is judged:** for cells no rule derives, the tree elects the claim in
the descent that applies the write, comparing exact values. The no-op has to be
judged there too, so the instruction carries the attribute's view. This is a
storage change, not part of 0.3.0's identity work.

## Notation

JSON (and tonk's YAML) is a projection of the AST, never hashed, so it can favour
whoever writes it.

- **The reader takes types from slots.** A bare constant takes the type its slot
  declares (`5` under `integer:` is a signed integer, and `"home:"` under `text:`
  is text). It is boxed only where no slot decides: `{"integer:": 5}`,
  `{"entity:": "did:…"}`, `{"symbol:": "person/name"}`.
- **A bare string is text.** No type is guessed from a spelling.
- **The writer** emits bare wherever the reader would read it back as the same
  value.

## Unchanged

- **Replica and branch entities** (`EntityExt::of` in
  `dialog-repository/src/schema.rs`) hash fixed Rust structs, which have no type
  ambiguity. Changing them would migrate every replica.
- **Search-tree node encoding:** already deterministic ("a pure function of the
  entry list", `node/codec.rs`).
- **Stored facts:** their values sit in index keys with a type tag and never go
  through `Value`'s serde.

## Migration: once

- Users hold pre-0.2 identities (what tonk main ships). 0.2.0 reached no user.
- 0.3.0 moves pre-0.2 identities straight to references:
  - `dialog_query::migration::*_v0` still computes the old identities;
  - tonk's seed upgrade re-keys from them as it does now;
  - `Branch::upgrade_rules` re-installs any rule stored under a non-identity
    entity.
- **tonk-labs/tonk#1058 must not merge pinned to `v0.2.0`.** Users would then hold
  0.2.0 identities, and we would migrate them twice. It moves to 0.3.0 instead.
- **dialog-db/dialog-db#593 becomes the 0.3.0 vehicle.**
  - Picks as entities stay: that change is in the notation and the AST.
  - Its tagged dag-cbor identity is superseded by references.
  - Its decoding fixes stay: the direct value visitor, `Changes` read by first key,
    and constants conformed to their slot's type.

## Plan

Each step lands with tests and a perf sweep:

1. **The `merkle-reference` crate**: `type` as a value named by URI, keyed
   references, and composites (list, map, set) over part references,
   with the v2 spec written beside it (`rust/merkle-reference/spec.md`).
   Pinned by tests:
   - plain bytes equal `blake3(bytes)`;
   - the same bytes under two types give two references;
   - no keyed reference equals the plain hash of its content;
   - a parent's reference is the same with a part embedded or linked;
   - a part's inclusion proof verifies against its parent's reference, once
     the verifier exists (not in 0.3.0);
   - the result is the same however a value was built.
2. **Attribute and concept references.** Done:
   - a definition's type is named by the scheme of its entity: `the:`,
     `concept:`, and `rule:` for step 3;
   - an attribute maps `the` (its relations, best first, as symbols), `as`,
     `pick` and `among`. The declared type and the pick are hashed as the
     entities that name them (`text:`, `last:`), as constants are, not as
     type references: a declared type is a constant, and `bytes:` has no type
     reference of its own;
   - a concept maps each attribute's reference to the field's options
     (`optional`, `conforms` by the target's reference).

   Two things this step surfaced:
   - **Rules embed whole concept descriptors** in today's identity, in their
     wire form, which carries attribute descriptions. So a description can
     change a rule's identity even though it changes no attribute's or
     concept's. Step 3 fixes this by hashing concepts by reference.
   - **Identity is ahead of the index for numbers.** A natural, an integer
     and a float of the same number are one value in identity and three in
     the index until it moves to one band. Two attributes listing `1` and
     `1.0` share an identity and read different claims. The same will hold
     for rule constants in fields that declare no type once step 3 lands. The
     alternatives were to keep the band in identity (a third written-as
     value, which bakes the band in and needs another migration to drop) or
     to make the index read both bands now (the encoding work deferred).
3. **Rule references** over the canonical AST, referencing concepts. Done
   (`rule/reference.rs`): a rule is a map under `rule:` from its head (concept
   by reference, fields bound to canonical variables), `when` and `unless` as
   sets, and `reduce`. A concept premise binds by field (attribute and
   options) rather than by field name. Every other premise is its wire form
   read as a merkle structure, a constant `{"<type entity>": payload}` taken
   as its value, so a constant has one identity wherever it appears.
   Attribute descriptions no longer reach rule identity.
4. **Body format 3**, the WebAssembly-style layout above, self-contained, with
   the authored form as its own claim. The 0.3.0 migration rewrites every stored
   body into it, so bodies are rewritten once. Formats 0 to 2 are still read. Pinned by a round-trip
   test, a test that equal rules store equal bytes, and tests that feed the
   decoder truncated, oversized and out-of-range input. The repo has no fuzzing
   setup yet; a fuzz target for the decoder would be worth adding with it.
5. **The compiled-rule cache.**
6. **The JSON reader and writer** that take types from slots.
7. **A migration test:** a pre-0.2 fixture (from tonk main) upgrades to 0.3.0
   references in one pass. The re-keyed home and template spaces resolve every
   definition.

## Decided

- **`natural`, `integer` and `float` are views** over one `number` value type.
- **3 and 3.0 are one value**, in identity and, once storage follows, in the
  index.
- **Numbers are decimal** (SQLite4's encoding, the `dialog-number` crate): a
  float is its shortest round-trip decimal, and float arithmetic is approximate.
- **Stored numbers migrate once**, by a deterministic rewrite whose code is kept
  out of the read path.
- **Writes through a view are literal or no-ops**, never merges (see
  [Views read and write](#views-read-and-write)).
- **Text is hashed as written**, with no normalization, because the database
  compares text byte for byte.
- **Structs are maps from field names**, with absent optional fields left out.
- **merkle-reference defines no scalars**; types are named by URI, and the
  shapes are list, map and set. Proofs follow Bao's chunk tree (spec, "Proofs").
- **Inclusion proofs:** the structure supports them, but the verifier is not part
  of 0.3.0.
- **Types are identified by name**, and dialog's names are its type entities.
- **`number:` is declarable**, and fixed-width number types are named by kind
  and width (`natural:8`, `integer:64`, `float:32`). `natural:`, `integer:` and
  `float:` stay unbounded. The derive infers the sized type from the Rust field.
- **Identity keeps `dialog-number`** after the storage assessment above.
- **Rules are stored as atomic records:** a body holds everything needed to
  load the rule, and links no part by reference.
- **Body format 3 ships in 0.3.0**, so the migration rewrites bodies once.
- **The authored form is a separate claim** on the rule: variable names, premise
  order and descriptions. It keeps only the newest install's, and loading a rule
  does not read it.

## Open questions

- **A smaller variant, not adopted:** a part whose encoding is shorter than 32
  bytes could stand in for its own reference, as Ethereum's tries do. That still
  preserves indistinguishability, since the rule depends only on the value. It
  saves the hash of small scalars, but every such part in an `any` slot must then
  carry its type, and verification gets a second case. Worth measuring after the
  plain version works.
