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

## Identity: references

A reference is a 32-byte BLAKE3 output, derived from the canonical AST in the
style of [merkle-reference](https://github.com/Gozala/merkle-reference/blob/main/docs/spec.md):
every node is identified by its kind and its content, and a composite node by the
references of its children.

- **Bytes leaf:** `blake3(bytes)`, plain mode. It is the hash dialog already keys
  spilled values and blobs by (`make_reference`), so a blob's reference is its
  store key.
- **Every other node:** BLAKE3 in `derive_key` mode, under one context string for
  dialog nodes, over `kind ‖ content`:
  - `kind` is the reference of the node kind's name (`text:`, `integer:`,
    `attribute:`, `rule:`, …), so kinds are entities, as types already are;
  - `content` is the scalar's encoding (UTF-8, LEB128, IEEE 754) for a scalar, or
    the concatenation of the children's references for a composite.

**Domain separation.** `derive_key` mode uses a different IV from plain mode, so
no node's reference can equal the plain BLAKE3 of any byte string. Without it,
untagged bytes would bring back the ambiguity this design removes. BLAKE3's root
over more than one chunk is itself a parent of chunk chaining values, so a crafted
byte string could share an id with a structured value.

**Why not fold with BLAKE3's internals.** `blake3::hazmat` (in 1.8.2, our
locked version) exposes `merge_subtrees_root` with a `Mode`. But a subtree's
chaining value depends on its input offset (`set_input_offset`), so a child folded
that way has no reference that holds regardless of position. A concept referenced
by two rules would hash differently in each. Hashing the concatenation of child
references instead gives position-independent references, and BLAKE3's own chunk
tree still supplies merkle structure: a 1 KiB chunk holds 32 references, and Bao
can prove one child's inclusion. It also keeps us out of `hazmat`, whose own docs
warn that it is "hazardous material".

**Composition.** A rule's content holds the references of the concepts it reads,
and a concept's holds those of its attributes. Today a rule's canonical form
inlines whole concept descriptors. A rule's identity then changes only when
something it references changes, which is Unison's property.

**Canonical form.**
- Local variables keep the canonical labeling `rule/canonical.rs` already does,
  which is datalog's counterpart to de Bruijn indices: variables are join points,
  not lexical scopes.
- Concept field names stay in identity, because a concept's fields are named
  semantically.
- Descriptions stay out, as they are today.
- Sets (premises) and maps (fields) are ordered by their members' references.
  That replaces the dag-cbor sort keys `canonical.rs` computes now, but labeling
  still runs first, since premise references depend on variable labels.

**Constants.** A constant's node kind is its type. That puts the type inside the
constant's reference whatever the slot declares, so identity tracks meaning even
if slot inference changes in a later release.

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

- **Body format 3:** a binary encoding modelled on the WebAssembly binary format.
  It is described below.
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
section := id:u8 size:leb128 content
```

| Id | Section | Content | Required |
|---|---|---|---|
| 1 | references | the references the body points to (concepts, attributes, values stored apart), 32 bytes each | when the body references any |
| 2 | strings | relation names, field names and text constants, each length-prefixed UTF-8 | when the body has any |
| 3 | head | the conclusion: a concept by reference index, or its fields inline | yes |
| 4 | premises | each premise: its kind, then its operands | yes |
| 5 | reduce | the folds of an aggregating rule | no |
| 0 | names | the names the author gave local variables, for display only | no |

**Operands point into the tables.** A premise reads a concept by its index in
the references section and a field by its index in the strings section. Each
reference and name is stored once per body, and decoding does no lookups.

**Schema-driven values.** The schema says what type each slot holds, so a value
carries no type tag where the slot declares a type. It carries one (a kind byte)
only where the slot admits any type, which is the notation's boxing rule. That
keeps bodies smaller than self-describing CBOR, and no value is read as a type
its spelling resembles.

**Names live apart.** Identity hashes the canonical labels of local variables,
which loses the names the author wrote. The names section keeps them, so a
decoded rule prints as it was written. The section is optional, and nothing
reads it except display and diagnostics. It works like the WebAssembly name
section.

**Compatibility.** An unknown section id below 128 is an error. An unknown id
of 128 or above is skipped, which is how later versions can add optional data
without a new format number. The names section (id 0) is one such optional
section.

**A canonical subset.** WebAssembly's encoding is not canonical: it allows
over-long LEB128 and some freedom in section order. Bodies are not hashed, but a
body should still be a function of its rule, so equal rules store equal bytes and
dedupe:

- every LEB128 is minimal;
- sections appear in ascending id order, each at most once, with the names
  section last;
- the references and strings tables are sorted and hold no duplicates;
- premises appear in canonical order (by reference, after labeling).

**What it is not.** A body's indices are local to that body, as a WebAssembly
module's are. The same concept has a different index in different bodies, so a
body's bytes are never an identity. The rule's reference is computed from its AST,
with indices resolved back to references.

**Validation.** Bodies arrive from untrusted peers, so a decoder validates in one
pass and refuses a malformed body instead of panicking. That means bounds-checked
sizes, indices within their tables, and every required section present.

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

1. **The reference function.** Pinned by tests:
   - bytes leaves equal `blake3(bytes)`;
   - no node reference equals the plain hash of its own encoding;
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

- **Kind tags:** the reference of the kind's name (self-describing, kinds are
  entities) or a small integer code (cheaper)? This proposal takes references; one
  extra hash per kind can be computed once.
- **Order of a map's entries:** by key reference (uniform) or by key spelling
  (readable)? The proposal takes references.
- **Inclusion proofs:** if nothing needs them yet, skip Bao. The structure is
  there either way.
- **Integers:** LEB128 (merkle-reference) or fixed width, for integers inside
  content?
- **References for record values in facts:** probably not in 0.3.0.
