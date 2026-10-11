# Merkle references, version 2

A merkle reference identifies a value by what it is, not by how it is stored. It
keeps the properties of
[version 1](https://github.com/Gozala/merkle-reference/blob/main/docs/spec.md):

- a reference is a function of the value;
- a composite value is identified through its parts' references;
- a part stored inline and a part stored apart, linked by its reference, give
  their parent the same reference, so a part can move in or out of its parent
  without changing the parent's reference, and a part's inclusion can be proved.

Version 2 changes how a value's type enters its reference, names types by URI,
and takes about half the hashing version 1 does. It is not compatible with
version 1: every reference differs.

## References

A reference is 32 bytes of [BLAKE3](https://github.com/BLAKE3-team/BLAKE3-specs)
output. A value is bytes, its *content*, read under a *type*, and its reference
is:

```text
reference(v : bytes) = BLAKE3(v)
reference(v : T)     = BLAKE3-keyed(key = reference(T), content_T(v))
```

- Raw bytes hash in BLAKE3's plain mode, so a blob's reference is its BLAKE3
  hash.
- A value of any other type hashes in keyed mode, keyed by its type's
  reference. The type is not a tag inside the content. Two types give the same
  bytes two unrelated references, and since keyed mode flags every compression,
  no keyed reference equals the plain hash of any byte string: a blob cannot
  pass for a typed value.

## Types

A type is named by a URI: a scheme (a letter, then letters, digits, `+`, `-` or
`.`), a colon, and the rest, with no whitespace or control characters. `text:`,
`https://example.com/person` and `urn:isbn:0451450523` are type names; `text` is
not. A type is a value of the type `type`, so:

```text
reference(type) = BLAKE3-derive_key(context = "merkle-reference v2 type", key material = "")
reference(T)    = BLAKE3-keyed(key = reference(type), UTF-8(name of T))
```

`reference(type)` is `6c1c13119c7904d1293af5aebc87fdaa284de8c36bc9b0d0e7802cd13dbd6c4e`.

Anyone can define a type by naming it. Choosing names that do not collide is up
to the definer; a URI gives the means.

### Scalars

This specification defines no scalar types. What a number, a boolean or a piece
of text is, and how its content is written, is the definer's choice, and two
systems share references for scalars exactly when they agree on those types. The
one rule for a scalar type: its content must be a function of the value, so that
equal values have equal references.

For reference, dialog defines `text:` (UTF-8, as written), `boolean:` (one byte,
`0x00` or `0x01`), `number:` (its `dialog-number` encoding), `entity:`, `symbol:`
and `record:`.

### Shapes

A composite's content is always its parts' references, never their bytes, in one
of three shapes. The crate names one type for each:

| Shape | Type name | Type reference | Content |
|---|---|---|---|
| list | `list:` | `23429cd4…bdcb65e5` | the items' references, in order |
| map | `map:` | `37fcd51f…059db149` | each entry's key reference then value reference, entries ordered by key reference |
| set | `set:` | `07ccc542…732456e8` | the members' references, ordered, each once |

```text
content_list([v0, v1, …])  = reference(v0) ‖ reference(v1) ‖ …
content_map(m)             = reference(k0) ‖ reference(m[k0]) ‖ reference(k1) ‖ …    (k0 < k1 < …)
content_set({v0, v1, …})   = reference(v0) ‖ reference(v1) ‖ …                       (v0 < v1 < …)
```

References are ordered as bytes. A user type with parts uses one of these
contents under its own type: a struct is a map from field names to values under
the struct's type, with an absent optional field left out, so adding a field
later changes no existing reference. A map holds no two entries with one key; an
implementation given such entries anyway orders them by value reference, so its
output is still a function of its input.

No content carries a length: each content is a whole hash input, and a composite
is a sequence of 32-byte references.

## Why parts are indistinguishable from links

A composite's content is its parts' references, so its reference depends on
nothing else about a part. A part held inline and a part held elsewhere, known
by its reference, contribute the same 32 bytes. Storage is free to choose, value
by value, which parts to embed.

Aligning parts to BLAKE3 chunks would not let the parts' references fall out of
the parent's own hash tree: a chunk's chaining value depends on its position, a
standalone hash is finalized as a root while a subtree is not, and a subtree is a
power of two chunks. Each part is hashed on its own; the parent hashes the
results.

## Proofs (Bao)

BLAKE3 hashes a parent's content as a binary tree of 1 KiB chunks, the tree
[Bao](https://github.com/oconnor663/bao/blob/master/docs/spec.md) streams and
slices. Because a composite's content is a sequence of fixed-size references, the
tree lines up with the parts:

- a chunk holds exactly 32 list items or set members, or 16 map entries, and no
  part spans two chunks;
- a proof that a part belongs to its parent is the chunk holding the part's
  reference and the chaining values from that chunk to the root: for a parent of
  `n` parts, one chunk and about `log₂(n / 32)` sibling chaining values;
- because map entries and set members are ordered, the same proof also shows a
  key's or member's absence: the chunk where it would sit holds its neighbours
  and not it;
- a slice of a list (items `i` to `j`) is proved by the chunks that hold it and
  the chaining values around them, so a reader can verify part of a large list
  without fetching the rest;
- once the part's reference is verified, the part itself is verified by hashing
  it under its type.

Verification needs the parent type's reference, which is the BLAKE3 key. Bao's
own encoder and decoder are written for unkeyed BLAKE3; a verifier for keyed mode
merges chaining values with `blake3::hazmat` (`merge_subtrees_non_root` and
`merge_subtrees_root` take a `Mode::KeyedHash`). The crate does not provide one
yet.

The cost of this layout is in editing large composites: appending to a list
changes only the tree's right edge, but inserting into the middle of a large map
or set shifts every later chunk, so it costs a hash of everything after the
insertion. Merkle references suit values whose composites are small or append
mostly; large collections that change in place belong in a structure built for
it, such as a prolly tree.

## Cost

Counting BLAKE3 compressions of 64-byte blocks:

| | Version 1 | Version 2 |
|---|---|---|
| A scalar of up to 64 bytes | 2: hash the content, then fold it with its format tag | 1: hash the content keyed by its type |
| A composite of n parts | about n: n − 1 folds plus the tag fold | about n / 2: two references per block, plus one parent per 32 parts |

Measured with `blake3` 1.8 on one core: a scalar of up to 64 bytes 56–71 ns, a
composite of 16 parts about 0.9 µs, of 1000 parts about 7–10 µs. An
implementation should hand BLAKE3 a composite's content in whole chunks: feeding
it one reference at a time costs over four times as much at a thousand parts.

A type's reference costs a hash, so an implementation keeps the types it uses as
constants.

## Test vectors

`text:` and `entity:` below are types a user defines; their content is UTF-8.

| Value | Reference |
|---|---|
| bytes `01 02 03 04` | `63781d171425a36312fa058d8712d5d05135a991ec20351ce9d65cdb19a05432` |
| the type `text:` | `ab31fce38517675ffb0055997c0e925eaa057459986eb71969a3e914bfc06f87` |
| `text:` `"hello world"` | `9c8321ef7176250ce8c036545f034d40a0e00d9d2fa2eccf4d4978e993cab657` |
| `text:` `""` | `695633a4ac641447b3ea93ef43b0b2445b879324a7c88fab1ee09d370d6a7313` |
| `entity:` `did:key:alice` | `9cb91eb8813601e7a8f5a53b993053cc77c77f1e318fc33cef318202d7934213` |
| list of the texts `"a"`, `"b"`, `"c"` | `a9f5c01fa32908502ba0f2485a8da7764b5d8e5460f6159167678f81d6c30092` |
| empty list | `10fc213d5e98202cf24d703ca1eb188a1620d49206822f4560efedcd24dc645c` |
| map of texts `{"from": "@gozala", "to": "@mikeal", "payload": "Hi"}` | `befa44ef2225f206a213ae0e8f54bc36e85c0b2180cecc576f13ec3c7cf234b2` |
| empty map | `15401fcccef0b6088e064e0583d32bca06e3204522df130b032d434a6a9f2c69` |
| set of the texts `"a"`, `"b"`, `"c"` | `91bd53901c8ce0bf5119289067f87c16c45b6e2a50a1bec7c0ab00d55b93a827` |
| empty set | `42d6ea87f2aac355034b6c00ab0703ad59ba8fe28cff3f0f1c91229c95b912be` |

`tests/vectors.rs` pins the same values. They were also derived independently
from this text with the Python `blake3` package.

## Open questions

- **Short parts as their own references.** A part whose content is shorter than
  32 bytes could stand in for its own reference, as Ethereum's tries do. It keeps
  parts indistinguishable from links, since the rule depends only on the value,
  and would save about a third of the hashing of small structs, but every such
  part must then carry its type in its bytes, and readers get a second rule.
  Worth measuring if hashing shows up in profiles.
