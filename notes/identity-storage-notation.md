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
nothing about dialog: it defines references, a small set of core types and
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

**Core types**, defined by the crate, keep v1's set:
- scalars: bytes, null, text, boolean, integer of any size, and float;
- composites: list and map.

Fixed-width integers are not core types.

**Dialog's types** are values of `type`, so the crate needs nothing to add one.

Value types, which a stored value or a constant has:
- `number`: every number, in the one-band encoding (see
  [Numbers](#numbers-one-band-typed-views));
- `text`, the crate's core text;
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
name under `reference(type)`. The crate's core types have its own names, and
dialog's are its type entities (`number:`, `integer:`, `entity:`, …), so a type
dialog already names by entity keeps that name.

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
- **Core integer of any size:** a header byte `0x80 + k` for a magnitude of
  `k < 127` bytes (`0xFF` and then `k − 127` as an integer for longer ones),
  then the minimal big-endian magnitude, with every byte inverted for a
  negative integer. It follows TerminusDB's big integers without their padding
  bit and negative zero. Longer numbers sort after shorter ones, so the bytes
  sort numerically. Canonical form needs one rule: no leading zero byte.
- **Dialog's `number`** has the one-band encoding as its content, SQLite4's
  numeric key encoding with binary digits in place of decimal ones:
  - a class byte (−∞, negative ≥ 1 in magnitude, negative < 1, zero,
    positive < 1, positive ≥ 1, +∞, NaN), so the exponent's sign is in it;
  - the magnitude of the power of two of the leading bit, in an
    order-preserving bijective varint after bijou's: one byte below 248, two
    bytes otherwise, inverted for a number less than one. Every 128-bit integer
    has a one-byte exponent;
  - the remaining bits in 7-bit groups with a continuation bit;
  - every byte after the class inverted for a negative number.

  These are the bytes the value index will key numbers by once it moves to one
  band, so a number has one encoding for both identity and index key. Its
  consequences, the same in both:
  - `5` and `5.0` are one value, whichever of natural, integer or float holds
    it, so they join and a retraction of one retracts the other;
  - `-0.0` is `0.0`;
  - NaN is one value. Today dialog keeps NaN payloads distinct, and a payload can
    change when a value passes through JavaScript.

  A decimal type, if dialog wants one, can be added later as its own type without
  changing the crate. TerminusDB stores decimals as the decimal string's digits,
  which is not canonical.
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
| 1 | references | the references the body points to (concepts, attributes, types of `any` values, values stored apart), 32 bytes each | when the body references any |
| 2 | strings | relation names, field names and text constants, each length-prefixed UTF-8 | when the body has any |
| 3 | head | the conclusion: a concept by reference index, or its fields inline | yes |
| 4 | premises | each premise: its kind, then its operands | yes |
| 5 | reduce | the folds of an aggregating rule | no |
| 128 | names | the names the author gave local variables, for display only | no |
| 129 | order | the order the author wrote premises in, as a permutation of the canonical order, for display only | no |

**Operands point into the tables.** A premise reads a concept by its index in
the references section and a field by its index in the strings section. Each
reference and name is stored once per body, and decoding does no lookups.

**Schema-driven values.** The schema says what type each slot holds, so a stored
value carries no type where the slot declares one. In an `any` slot it carries
the index of its type's reference in the references table. That mirrors identity,
which names types by reference, and the notation's boxing rule. It keeps bodies
smaller than self-describing CBOR, and no value is read as a type its spelling
resembles.

**Inline or linked is storage's choice.** Because identity hashes parts'
references, a body may embed a part or store only its reference, and the rule's
reference is the same either way. Concepts read by many rules are linked so they
are stored once. Scalars and premises are embedded.

**The authored form lives apart.** Canonicalization throws away two things the
author wrote: the names of local variables (identity hashes canonical labels),
and the order of premises (identity sorts them). Two optional sections keep them:

- **names** holds the label each variable had in the source;
- **order** holds the source position of each premise, as a permutation of the
  canonical order.

With both, a decoded rule prints as the author wrote it. Nothing reads them
except display and diagnostics, and they work like the WebAssembly name section.
Two rules that differ only in names or premise order are one rule with one
reference, so a later install of the same rule can bring a different authored
form. The preference is that the newest install wins. A body is stored as a
claim (`dialog.rule/source`), so read under `last`, a re-install replaces the
authored form and leaves the reference unchanged. This is settled independently
of the rest of this design.

**Compatibility.** An unknown section id below 128 is an error. An unknown id
of 128 or above is skipped, which is how later versions can add optional data
without a new format number. The names (128) and order (129) sections are the
first such sections.

**A canonical layout.** WebAssembly's encoding is not canonical: it allows
over-long LEB128 and some freedom in section order. Bodies are not hashed, but a
body should still be a function of its rule, so equal rules store equal bytes and
dedupe. Bijou removes the over-long encodings by construction, and the rest is
layout:

- sections appear in ascending id order, each at most once;
- the references and strings tables are sorted and hold no duplicates;
- premises appear in canonical order (by their encoded bytes, after labeling).

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
in the value index, in the SQLite4-style encoding below, and dialog's number types
are views over it. The storage change itself is not part of 0.3.0. 0.3.0 adopts
the encoding for identity, so that when storage follows, a number's identity
content and its index key are the same bytes.

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
- **Migration, gradual.** Node layout versions cannot do it alone: to the tree a
  key is opaque bytes in one global order, and the encoding changes where a
  number sorts, so re-encoding a value moves its key. Instead:
  - the new encoding gets its own type byte, a `number` band beside the old
    natural, integer and float bands, so the tree's single order is never broken;
  - new writes go to the new band, and old values stay readable where they are;
  - typed views read the new band and the old bands until the old bands are
    empty, which is the alternative below, used only during the transition;
  - re-encoding happens when a claim is next written, or in a background sweep,
    and a later release can declare the old bands empty and drop the dual reads.

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

**Before the storage change** (not in 0.3.0):
- numeric scenarios in `dialog-perf` (point, narrow range, wide range, mixed
  types), which the suite lacks today;
- the one-band slot behind dialog's value encoder, measured on them.

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

1. **The `merkle-reference` crate**: the core types, `type` as a value, keyed
   references, composites over part references, and the integer of any size,
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
3. **Rule references** over the canonical AST, referencing concepts.
4. **Body format 3**, the WebAssembly-style layout above, with definitions
   stored by reference. Formats 0 to 2 are still read. Pinned by a round-trip
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
- **Writes through a view are literal or no-ops**, never merges (see
  [Views read and write](#views-read-and-write)).
- **Text is hashed as written**, with no normalization, because the database
  compares text byte for byte.
- **Structs are maps from field names**, with absent optional fields left out.
- **The crate's core float keeps v1's encoding.** Dialog's numbers use `number`,
  not the core float.
- **Inclusion proofs:** the structure supports them, but the verifier is not part
  of 0.3.0.
- **Types are identified by name**, and dialog's names are its type entities.

## Open questions

- **A smaller variant, not adopted:** a part whose encoding is shorter than 32
  bytes could stand in for its own reference, as Ethereum's tries do. That still
  preserves indistinguishability, since the rule depends only on the value. It
  saves the hash of small scalars, but every such part in an `any` slot must then
  carry its type, and verification gets a second case. Worth measuring after the
  plain version works.
