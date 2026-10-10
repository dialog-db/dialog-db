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
nothing about dialog: it defines references, types and canonical content, and
dialog defines its own types on top of it.

v2 keeps v1's properties:
- identity is a function of the value;
- composite values are identified through their parts;
- a part's inclusion can be proved.

It changes two things: how a value's type enters its reference, and how much
hashing a reference takes.

### A type is a view over bytes

Every value is bytes read under a type, and a type is an encoding of those bytes.
Text is UTF-8, a natural number is a bijou-encoded integer, and an entity is the
UTF-8 of a URI that must parse. The type is not a tag byte in the content. It is
the key the content is hashed under:

```text
reference(v : bytes) = BLAKE3(v)
reference(v : T)     = BLAKE3-keyed(key = reference(T), encode_T(v))
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

The crate defines the core types: bytes, text, boolean, natural, integer, float,
list, map and struct. Dialog defines its own as values of `type`:
- entity: text that parses as a URI;
- symbol: text of the form `namespace/name`;
- record;
- the AST nodes: attribute, concept, rule, premise, term.

Dialog's type entities (`text:`, `entity:`, …) name those references. A type
dialog adds later needs nothing from the crate.

### Less hashing

v1 hashes every value as its own node:
- a scalar is a fold of a format-tag hash and a content hash;
- a list folds its elements' references pairwise;
- a map folds a pair per entry.

A rule with a few dozen scalars takes a few hundred small hash calls. v2 takes
one call per value that has its own identity:

- **Parts are inlined unless their slot says otherwise.** A struct field, list
  element or map entry is encoded inline, under the type its schema slot
  declares, with lengths in bijou. Only a slot typed `reference<T>` holds a
  32-byte reference instead. A rule's constants, variables and premise shapes are
  inline. The concepts it reads and the attributes a concept reads are
  references, because they are definitions in their own right, as in Unison.
- **One BLAKE3 call per identified value.** Inline parts add bytes, not hash
  calls. BLAKE3's own chunk tree still makes the content a merkle tree: a 1 KiB
  chunk is one leaf, and Bao proves a chunk's inclusion.
- **A slot that admits any type** (`any`) holds the value's type reference, then
  its inline encoding. The type is identified by reference, as everywhere else,
  never by a byte code.

What v2 gives up: an inline part is not a node of the tree, so it has no
reference inside its parent. Its reference is still computable from its type and
bytes, and its inclusion is provable at chunk granularity rather than per value.
A part that needs its own provable identity gets a `reference<T>` slot.

**Why not fold with BLAKE3's internals.** `blake3::hazmat` (in 1.8.2, our locked
version) exposes `merge_subtrees_root` with a `Mode`. But a subtree's chaining
value depends on its input offset (`set_input_offset`), so a part folded that way
has no reference that holds regardless of position. A concept referenced by two
rules would hash differently in each. Keyed hashing over inline content and part
references keeps references position-independent, and stays out of `hazmat`,
whose own docs warn that it is "hazardous material".

### Canonical content

A reference is only stable if `encode_T` is a function of the value:

- **Integers and lengths: bijou.** The bijou encodings ([`bijoux`](https://docs.rs/bijoux)),
  from Ink & Switch's Subduction work, are bijective: every integer has exactly
  one encoding, so content is canonical without a "minimal" rule to enforce.
  Values up to 247 take one byte, and the first byte gives the length. They come
  in u128 and i128 (zigzag) formats, which dialog's values need, and they decode
  2 to 10 times faster than LEB128.
- **Maps and sets** are ordered by their entries' encoded keys; premises, by
  their encoded bytes after labeling.
- **Local variables keep the canonical labeling** `rule/canonical.rs` does
  today, which is datalog's counterpart to de Bruijn indices: variables are join
  points, not lexical scopes.
- **Concept field names stay in identity**, because they are semantic.
- **Descriptions stay out**, as they are today.
- **Floats and text need a rule:** one NaN, a decision on `-0.0`, and a decision
  on Unicode normalization. See open questions.

### Constants

A constant's reference is keyed by its type. So the type is part of identity
whatever the slot declares, and identity tracks meaning even if slot inference
changes in a later release. Inside a rule, a constant in a typed slot is inline
and untagged (its type comes from the slot). One in an `any` slot carries its
type reference.

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

**Schema-driven values.** The schema says what type each slot holds, so a value
carries no type where the slot declares one. In an `any` slot it carries the index
of its type's reference in the references table. That mirrors identity, which
names types by reference, and the notation's boxing rule. It keeps bodies smaller
than self-describing CBOR, and no value is read as a type its spelling resembles.

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
   references, bijou content and inline/`reference<T>` slots, with the v2 spec
   written beside it. Pinned by tests:
   - plain bytes equal `blake3(bytes)`;
   - the same bytes under two types give two references;
   - no keyed reference equals the plain hash of its content;
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

- **Float content:** one canonical NaN; is `-0.0` distinct from `0.0`?
- **Text content:** normalize Unicode (NFC) before hashing, or hash the bytes as
  written? Normalizing makes visually equal text one value. Not normalizing keeps
  what the author wrote.
- **Which slots are references:** concepts and attributes, certainly. Formulas?
  Ranked `as:` lists, which can be long?
- **Inclusion proofs:** if nothing needs them yet, skip Bao. Chunk-level
  structure is there either way.
- **Struct field order inside content:** by field name or by a stable field
  number? Field numbers make renames free, at the cost of a schema that maps
  names to numbers.
- **Signed integers in sorted positions:** bijou's i128 sorts in zigzag order,
  not numeric order. That does not matter for identity, and index keys keep their
  own order-preserving encoding, but it does for any sorted section that holds
  signed integers.
