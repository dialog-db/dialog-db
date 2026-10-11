# merkle-reference

Identifiers that are a function of a value, the same whichever of the value's
parts are stored inline and whichever are stored apart and linked by reference.

This is version 2 of [merkle references][v1]. The format is specified in
[`spec.md`](./spec.md); this README says why it exists, how to use it, and what
it chose.

## Why

Content identifiers usually identify how data is *encoded*, not what it *is*: the
same structure gets one identifier as DAG-CBOR, another as DAG-JSON, and another
again when one of its parts is moved into a block of its own. That couples
identity to storage decisions that should be free to change.

A merkle reference is computed from the value's structure:

- **Encoding-independent.** The reference depends on the value, not on how it is
  serialized.
- **Partition-independent.** A part stored inline and a part stored apart,
  linked by its reference, contribute the same thing to their parent. Storage
  can embed or split parts, value by value, without changing any identity.
- **Provable.** Each part has a reference its parent hashes, and a composite's
  content lines up with BLAKE3's chunk tree, so a part's inclusion, a key's
  absence from a map, or a slice of a list can be proved Bao-style.

Dialog uses it for the identities of its definitions (attributes, concepts,
rules) and of its values.

## How it works

A value is bytes, its content, read under a type:

```text
reference(v : bytes) = BLAKE3(v)
reference(v : T)     = BLAKE3-keyed(key = reference(T), content_T(v))
```

- A type is named by a URI (`text:`, `https://example.com/person`), and its
  reference is its name's, so anyone can define a type by naming it.
- The crate defines no scalar types. It defines the three shapes a composite can
  take, `list:`, `map:` and `set:`, whose content is always the parts'
  references, never their bytes. That is what makes an inline part and a linked
  part indistinguishable.

## Using it

```rust
use std::sync::LazyLock;
use merkle_reference::{Reference, ToReference, Type};

// Name your types. A type's reference is a hash, so derive it once.
static TEXT: LazyLock<Type> = LazyLock::new(|| Type::named("text:").unwrap());
static PERSON: LazyLock<Type> = LazyLock::new(|| Type::named("person:").unwrap());

// Implement `ToReference` for the types you own.
struct Text(String);

impl ToReference for Text {
    fn to_reference(&self) -> Reference {
        TEXT.reference_of(self.0.as_bytes())
    }
}

struct Person {
    name: Text,
    friends: Vec<Text>,
}

impl ToReference for Person {
    fn to_reference(&self) -> Reference {
        // A struct is a map from field names to values, under its own type.
        PERSON.map([
            (TEXT.reference_of(b"name"), self.name.to_reference()),
            (TEXT.reference_of(b"friends"), self.friends.to_reference()),
        ])
    }
}

// Collections of your types are lists, maps and sets by themselves; a part
// known only by its reference gives its parent the same reference.
let friends = vec![Text("Bob".into())];
let linked: Vec<Reference> = friends.iter().map(ToReference::to_reference).collect();
assert_eq!(friends.to_reference(), linked.to_reference());

// Raw bytes are their BLAKE3 hash.
let blob = Reference::hash(b"raw bytes");
```

- **`Reference`**: 32 bytes. `Reference::hash(bytes)` is the BLAKE3 of a blob;
  `from_bytes` and `from_hex` wrap existing ones.
- **`Type`**: `Type::named(uri)` (it refuses a name that is not a URI), the
  shapes as `Type::LIST`, `Type::MAP` and `Type::SET`, and on any type
  `reference_of(content)` for a scalar and `list`, `map` and `set` for a
  composite over its parts' references.
- **`ToReference`**: implemented for `Reference` itself, for slices, arrays and
  `Vec`s (lists), `BTreeMap` and `HashMap` (maps), `BTreeSet` and `HashSet`
  (sets), and through references and smart pointers.

### Your own types

The crate implements `ToReference` for no scalar Rust type, because it defines
no scalar types: whether `"3"` is text, a symbol or a number is yours to say. You
implement it for the types you own, and the crate's collections of them follow.
Rust's orphan rule keeps you from implementing it for a type you do not own,
such as `str`; wrap one in a newtype, or call your type's `reference_of`
directly.

Should a project need to give foreign scalars meaning (`str` as its `text:`,
`u64` as its `number:`), the trait can later take a vocabulary parameter,
`ToReference<V = ()>`. The orphan rule allows `impl ToReference<MyVocabulary> for
str` in the project's crate, and today's implementations stay valid, since they
mean `ToReference<()>`.

The crate depends only on `blake3` and builds for WebAssembly.

## Choices, and what they cost

**Types key the hash; they are not tagged into the content.** Keyed mode gives
the three guarantees with no extra bytes to hash: the same bytes under two types
give unrelated references, no typed reference equals the plain hash of any byte
string, and raw bytes stay plain BLAKE3. A type header in the content would make
content self-describing, but measured, it costs a second compression for scalars
of 33–64 bytes and about 50 ns per small composite, and padding it to keep map
entries chunk-aligned costs a second compression on every scalar. Keyed hashing
is a MAC keyed by the type, so a variant for another hash would use HMAC under
the same rules.

**No scalar types.** A hashing crate that fixed what a float or a number is would
force that choice on everyone; dialog, for one, identifies `3`, `3.0` and the
float `3.0` as one number, where IEEE 754 bit patterns would not. Two systems
share scalar references when they agree on scalar type names.

**Three shapes, sets included.** A set is not a map to a dummy value: that would
double its size and admit values that mean nothing. Dialog needs sets for a
rule's premises.

**Every part is referenced, even small scalars.** A parent always hashes its
parts' 32-byte references. Letting short parts stand in for themselves would save
about a third of the hashing of small structs but needs a second rule everywhere
a part is read; it is an open question in the spec.

**No lengths in identity.** Each content is a whole hash input and a composite is
a sequence of fixed-size references.

**Suited to values, not large mutable collections.** Appending to a list is cheap,
but inserting into the middle of a large map or set rehashes everything after the
insertion. Definitions and values fit; large collections that change in place
belong in a structure built for it.

## Performance

On one core with `blake3` 1.8: a scalar of up to 64 bytes 56–71 ns, a composite
of 16 parts about 0.9 µs, of 1000 parts about 10 µs. In dialog, an attribute's
reference takes about 0.6 µs, and a concept's identity is 25–45% cheaper than the
dag-cbor-plus-BLAKE3 scheme it replaced.

Composites hand BLAKE3 their content in whole chunks, so it can hash several per
SIMD pass; feeding it one reference at a time cost over four times as much at a
thousand parts.

## Status

- The format is pinned by test vectors in `tests/vectors.rs`, which were also
  derived independently from the spec text with the Python `blake3` package.
- Verifying Bao-style proofs is not implemented yet. The spec describes the
  layout; a keyed-mode verifier can be built on `blake3::hazmat`.

[v1]: https://github.com/Gozala/merkle-reference/blob/main/docs/spec.md
