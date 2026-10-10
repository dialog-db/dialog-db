# merkle-reference

Identifiers that are a function of a value, the same whichever of the value's
parts are stored inline and whichever are stored apart and linked by reference.

This is version 2 of [merkle references][v1]. The format is specified in
[`spec.md`](./spec.md); this README says why it exists, how to use it, and what
it chose.

```rust
use merkle_reference::{Bytes, Refer, Reference, Type};

// Core values.
let text = "hello world".refer();
let list = vec![1, 2, 3].refer();
let blob = Bytes(b"raw bytes").refer(); // = BLAKE3("raw bytes")

// A part linked by its reference gives its parent the same reference.
let parts: Vec<Reference> = vec![1, 2, 3].iter().map(Refer::refer).collect();
assert_eq!(parts.refer(), list);

// A type of your own: name it, then give its values content.
let person = Type::named("person:");
let alice = person.map([("name".refer(), "Alice".refer())]);
```

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
- **Provable.** Each part has a reference its parent hashes, so a part's
  inclusion in a value can be proved with the hashes along the way.

Dialog uses it for the identities of its definitions (attributes, concepts,
rules) and of its values.

## How it works

A value is bytes, its content, read under a type:

```text
reference(v : bytes) = BLAKE3(v)
reference(v : T)     = BLAKE3-keyed(key = reference(T), content_T(v))
```

- A type is identified by its name, hashed under the root type, so anyone can
  define a type by naming it.
- A composite's content is its parts' references, never their bytes: a list's
  are its items' references in order, a map's its entries' key and value
  references ordered by key reference. That is what makes an inline part and a
  linked part indistinguishable.
- The core types are null, boolean, integer of any size, float, text, list and
  map. Everything else, including dialog's own types, is defined by name on top.

## Using it

- `Reference`: 32 bytes; `Reference::of_bytes` is the plain BLAKE3 of a blob.
- `Type`: `Type::named(name)` for your own type, the core types as constants
  (`Type::TEXT`, `Type::LIST`, …). A type is a hash to derive, so a type you use
  often is best kept as a constant: `Type::from_reference(Reference::from_hex(…))`,
  with a test that it equals `Type::named(…)`.
- `Type::refer(content)` for a scalar of your type, `Type::list(parts)` and
  `Type::map(entries)` for a composite of your type over its parts' references.
- `Refer`: implemented for the core Rust types (`str`, `String`, integers but
  `u8`, `f64`, `bool`, `Vec`, slices, arrays, `BTreeMap`, `HashMap`), for
  `Bytes(..)` as plain bytes, and for `Reference` itself, which is how a linked
  part goes into its parent.
- `Integer`: the core integer of any size, with its canonical content, which
  also sorts as the integers do.

The crate depends only on `blake3` and builds for WebAssembly.

## Choices, and what they cost

**Types key the hash; they are not tagged into the content.** Keyed mode gives
two guarantees at no extra hashing: the same bytes under two types give unrelated
references, and no typed reference equals the plain hash of any byte string, so a
blob can never pass for a typed value. Plain bytes stay plain BLAKE3, so a blob's
reference is the hash every BLAKE3 tool computes for it.

The alternative is to prefix a type header to the content and hash plainly,
`H(type ‖ content)`. It would work with any hash function and make hashed
content self-describing. But a raw blob whose bytes happen to be `type ‖
content` would then have the same reference as that typed value, unless raw bytes
also get a header, which gives up `reference(blob) = BLAKE3(blob)`. The keyed
form is itself an instance of a keyed hash (a MAC keyed by the type), so a
non-BLAKE3 variant could use HMAC under the same rules.

**Every part is referenced, even small scalars.** A parent always hashes its
parts' 32-byte references. Letting short parts stand in for their own references
would save hashing but needs a second rule everywhere a part is read; it is noted
in the spec's open questions and not adopted.

**No lengths in identity.** Each content is a whole hash input and a composite is
a sequence of fixed-size references, so nothing needs a length prefix. Lengths
belong to storage formats.

**Integers of any size; floats as IEEE 754 bit patterns.** The core float keeps
version 1's encoding, so `-0.0` and NaN payloads are distinct. A user that wants
numbers to be values rather than bit patterns defines its own type: dialog
identifies numbers under `number:` with the [`dialog-number`][dialog-number]
encoding, where `3`, `3.0` and the float `3.0` are one value.

**Hashing cost.** A scalar of up to 64 bytes takes one BLAKE3 compression, and a
composite of `n` parts about `n / 2`. Version 1 took about twice that, since it
folded every value with a hash of its format tag.

**Not compatible with version 1.** Every reference differs.

## Status

- The format is pinned by test vectors in `tests/vectors.rs`, which were also
  derived independently from the spec text with the Python `blake3` package.
- Verifying inclusion proofs is not implemented yet. The structure supports it:
  BLAKE3 hashes a parent's content as a tree of 1 KiB chunks of 32 references.

[v1]: https://github.com/Gozala/merkle-reference/blob/main/docs/spec.md
[dialog-number]: ../dialog-number/README.md
