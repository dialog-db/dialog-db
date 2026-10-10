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
- scalars: bytes, text, boolean, integer of any size, and float;
- composites: list and map.

Fixed-width integers are not core types.

**Dialog's types** are values of `type`, so the crate needs nothing to add one:
- `integer` (i128), `natural` (u128) and `real` (f64), each with its own content
  encoding (see below);
- `entity` (text that parses as a URI) and `symbol` (`namespace/name` text);
- `record`;
- the AST nodes: attribute, concept, rule, premise and term.

Dialog's type entities (`integer:`, `entity:`, …) name those references.

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

- **Lengths and counts: bijou.** The bijou encodings ([`bijoux`](https://docs.rs/bijoux)),
  from Ink & Switch's Subduction work, are bijective: every integer has exactly
  one encoding, so content is canonical without a "minimal" rule to enforce.
  Values up to 247 take one byte, they decode 2 to 10 times faster than LEB128,
  and unsigned ones sort numerically. Bijou has fixed-width families only (up to
  128 bits), so it encodes lengths, counts and the storage format's integers. It
  does not encode the core integer of any size.
- **Core integer of any size:** a sign and length prefix, then the minimal
  big-endian magnitude, with negative magnitudes bit-inverted, as TerminusDB
  encodes its big integers. Longer numbers sort after shorter ones, so the bytes
  sort numerically. Canonical form needs one rule: no leading zero byte.
- **Dialog's fixed-width types use the bytes dialog already writes in index keys
  (`ordkey.rs`):**
  - `integer`: 16 bytes, big-endian, sign bit flipped;
  - `natural`: 16 bytes, big-endian;
  - `real`: 8 bytes, a negative number with every bit flipped and a non-negative
    one with only its sign bit flipped, big-endian.

  The same bytes then serve as a value's index key and as its identity content,
  so a type has one encoding, and those bytes sort in value order. Up to 64 bytes
  hash in one compression, so fixed width costs nothing. The storage format can
  still write values compactly in bijou.
- **Floats** (dialog's `real`) follow what dialog does today, with one change:
  - `-0.0` and `0.0` stay distinct, as dialog's equality and hashing already
    treat them (by bit pattern) and as IEEE 754's total order does;
  - **NaN collapses to one canonical quiet NaN**, at construction. Today
    dialog keeps NaN payloads distinct, and a payload can change when a value
    passes through JavaScript, so the same value could drift to a different
    identity.
  - A decimal type, if dialog wants one, can be defined later as its own type
    without changing the crate. TerminusDB stores arbitrary-precision decimals as
    continued fractions, which compare in byte order.
- **Maps** order entries by their keys' encoded bytes; a set of premises, by
  their references after labeling.
- **Local variables keep the canonical labeling** `rule/canonical.rs` does today,
  which is datalog's counterpart to de Bruijn indices: variables are join points,
  not lexical scopes.
- **Concept field names stay in identity**, because they are semantic.
- **Descriptions stay out**, as they are today.
- **Text** needs a decision on Unicode normalization (see open questions).

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

**Names live apart.** Identity hashes the canonical labels of local variables,
which loses the names the author wrote. The names section keeps them, so a
decoded rule prints as it was written. The section is optional, and nothing
reads it except display and diagnostics. It works like the WebAssembly name
section.

**Compatibility.** An unknown section id below 128 is an error. An unknown id
of 128 or above is skipped, which is how later versions can add optional data
without a new format number. The names section (id 128) is the first such
section.

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
   with the v2 spec written beside it. Pinned by tests:
   - plain bytes equal `blake3(bytes)`;
   - the same bytes under two types give two references;
   - no keyed reference equals the plain hash of its content;
   - a parent's reference is the same with a part embedded or linked;
   - a part's inclusion proof verifies against its parent's reference;
   - the result is the same however a value was built.
2. **Attribute and concept references.**
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

## Open questions

- **`natural` as its own type:** dialog has `natural` (u128) and `integer`
  (i128) today. Keep both, or have one `integer` and treat non-negativity as a
  constraint?
- **Text content:** normalize Unicode (NFC) before hashing, or hash the bytes as
  written? Normalizing makes visually equal text one value. Not normalizing keeps
  what the author wrote.
- **Struct encoding:** a map from field names (self-describing, survives added
  fields) or a list in fixed field order (smaller)? Field numbers would make
  renames free, at the cost of a schema mapping names to numbers.
- **Core float:** v1 has one; dialog's `real` uses its own sortable encoding. The
  core float could take the same encoding, or stay as v1 wrote it.
- **Inclusion proofs in 0.3.0:** the structure supports them either way. The
  question is only whether we build and test the verifier now.
- **A smaller variant, not adopted:** a part whose encoding is shorter than 32
  bytes could stand in for its own reference, as Ethereum's tries do. That still
  preserves indistinguishability, since the rule depends only on the value. It
  saves the hash of small scalars, but every such part in an `any` slot must then
  carry its type, and verification gets a second case. Worth measuring after the
  plain version works.
