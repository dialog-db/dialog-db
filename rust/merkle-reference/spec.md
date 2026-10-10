# Merkle references, version 2

A merkle reference identifies a value by what it is, not by how it is stored. It
keeps the properties of
[version 1](https://github.com/Gozala/merkle-reference/blob/main/docs/spec.md):

- a reference is a function of the value;
- a composite value is identified through its parts' references;
- a part stored inline and a part stored apart, linked by its reference, give
  their parent the same reference, so a part can move in or out of its parent
  without changing the parent's reference, and a part's inclusion can be proved.

Version 2 changes how a value's type enters its reference, and takes about half
the hashing version 1 does. It is not compatible with version 1: every reference
differs.

## References

A reference is 32 bytes of [BLAKE3](https://github.com/BLAKE3-team/BLAKE3-specs)
output. A value is bytes, its *content*, read under a *type*, and its reference
is:

```text
reference(v : bytes) = BLAKE3(v)
reference(v : T)     = BLAKE3-keyed(key = reference(T), content_T(v))
```

- Plain bytes hash in BLAKE3's plain mode, so a blob's reference is its BLAKE3
  hash.
- A value of any other type hashes in keyed mode, keyed by its type's
  reference. The type is not a tag inside the content. Two types give the same
  bytes two unrelated references, and since keyed mode flags every compression,
  no keyed reference equals the plain hash of any byte string.

## Types

A type is a value of the type `type`, and is identified by its name:

```text
reference(type) = BLAKE3-derive_key(context = "merkle-reference v2 type", key material = "")
reference(T)    = BLAKE3-keyed(key = reference(type), UTF-8(name of T))
```

| Type | Name | Reference |
|---|---|---|
| type | | `6c1c13119c7904d1293af5aebc87fdaa284de8c36bc9b0d0e7802cd13dbd6c4e` |
| null | `null` | `63dba2b2bec2ed479d938dd6133eb89f5423b43844b83f39ed208dd3e93fcb02` |
| boolean | `boolean` | `7f7f177fd3fcde3f4baabe1e2cc39374fbc6a2efad84a0dfdf53f067945e349b` |
| integer | `integer` | `fdc9909fb1f4f66fb4a878b4a4279bfc1b63e7504860d39ef0cd05fd692d2626` |
| float | `float` | `d620bf8e93b9cc079024a551f4d1bb38e4fe2ea0c3cfe03ed2a8ce456161a60b` |
| text | `text` | `51531dc865323c8fc012200946b9e63147ba0bdb4175b0d62568c1dedc479b78` |
| list | `list` | `684b01901abbcf0ccade0c97ae922b053f02ae46669632680f8aa51ae85b44e8` |
| map | `map` | `642836d5bd219ed6ad72a31913814819ea3da8f480de93483a7ea5a2821d4d24` |

These are the core types. Bytes have no type reference: they are the one value
hashed in plain mode.

Anyone can define a type by naming it. Its content is whatever its definer says,
with one constraint: content must be a function of the value, so equal values
have equal references. A type whose values have parts should give it the
content of a list or a map (below), so its parts keep the properties above.
Choosing names that do not collide, for example by namespacing them, is up to
the definer.

## Content

No content carries a length: each value's content is a whole hash input, and a
composite's content is a sequence of 32-byte references.

**null**: empty.

**boolean**: one byte, `0x00` for false and `0x01` for true.

**integer**, of any size: a header, then the magnitude as big-endian bytes with
no leading zero byte.

- An integer that is not negative, with a magnitude of `k` bytes, has the header
  `0x80 + k` when `k < 127`, and otherwise the byte `0xFF` followed by the
  content of the integer `k − 127`.
- A negative integer's content is its magnitude's content with every byte,
  header included, inverted.

Zero is `80`, 1 is `81 01`, 256 is `82 01 00`, −1 is `7E FE`. Each integer has
one content, contents sort as their integers do, and a reader knows where an
integer ends from its header, so one can be read from the start of longer bytes.
The scheme follows [TerminusDB's](https://terminusdb.org/blog/2022-11-21-terminusdb-internals-3/)
big integers, without its padding bit and negative zero.

**float**: IEEE 754 binary64, little-endian, bit for bit, as in version 1. Negative
zero and each NaN payload are distinct values. A user that wants other numeric
semantics defines its own number type.

**text**: UTF-8, as written. Text is not normalized, so two texts have one
reference exactly when their bytes are equal.

**list**: the items' references, in order.

```text
content_list([v0, v1, …]) = reference(v0) ‖ reference(v1) ‖ …
```

**map**: each entry's key reference then value reference, entries ordered by key
reference, compared as bytes.

```text
content_map(m) = reference(k0) ‖ reference(m[k0]) ‖ reference(k1) ‖ reference(m[k1]) ‖ …
```

Keys may be values of any type. A map has no two entries with one key; an
implementation given such entries anyway orders them by value reference, so its
output is still a function of its input.

A struct, as a user type, is best given the content of a map from field names to
values, leaving an absent optional field out, so adding a field later changes no
existing reference.

## Why parts are indistinguishable from links

A composite's content is its parts' references, so its reference depends on
nothing else about a part. A part held inline and a part held elsewhere, known
by its reference, contribute the same 32 bytes. Storage is free to choose,
value by value, which parts to embed.

## Inclusion proofs

BLAKE3 hashes content as a tree of 1 KiB chunks, each holding 32 references. A
proof that a part belongs to its parent is the chunk holding the part's
reference and the chaining values up the tree to the root, and verifying it
needs the parent type's reference, which is the key. This crate does not yet
provide a verifier.

## Cost

Counting BLAKE3 compressions of 64-byte blocks:

| | Version 1 | Version 2 |
|---|---|---|
| A scalar of up to 64 bytes | 2: hash the content, then fold it with its format tag | 1: hash the content keyed by its type |
| A composite of n parts | about n: n − 1 folds plus the tag fold | about n / 2: two references per block, plus one parent per 32 parts |

A type's reference costs a hash, so an implementation keeps the types it uses as
constants.

## Test vectors

| Value | Reference |
|---|---|
| bytes `01 02 03 04` | `63781d171425a36312fa058d8712d5d05135a991ec20351ce9d65cdb19a05432` |
| null | `4d619613f9ee44957e317b85876d62717eeefaddb41f5ee68801438ed1c2e4e8` |
| true | `d24b4fe8353c1bc4a7ef19901d3481e63b31c90b37eb2476ef4b17d479446f4c` |
| false | `535caf2484de229f732843678f9fac746e13fa25bf8a420e408a2b67b46872b8` |
| 0 | `cc5d8e0c2767bfa09919b54ad8e17eb0b8ab6cbbeb5aa42285692bbb562747fe` |
| 1985 | `414d885d26a2392adcbd2261e08f3625dc2772d4447fdbf557ab3f8836de7920` |
| −1985 | `fd8059021fa8e283abcb14e13be2aacd8fa9cbd20b360118d145a4f981534fec` |
| 2¹²⁸ | `db91d4412c3e18440ac8edd0f7f15a2e8c3a0dad5ef6317a6a7c916b061a8061` |
| a 300-byte integer, every byte `AB` | `1870673dfeafd85c87f08a86a38d9a124604ed692e597ceec6cb887d5fddf64e` |
| 18.033 | `ab68cdd21eee3bf07e204a30201c41de815ebf2dad8b972e9bb56d62fd2be606` |
| `"hello world"` | `c4afdeb66d3e84f3b0826fc3eeace55f9b2417626eb50f7053cf21437c5cfe10` |
| `[1, 2, 3]` | `56ae0e72e59af3d455399350d3ff76ac93454052e361effa1d7eba916ac1ae3a` |
| `[]` | `91690ab5f7abf1771f141be0523fcdaa7af82d24d80aee003375cf75c1f67c4c` |
| `{"from": "@gozala", "to": "@mikeal", "payload": "Hi"}` | `cbfcea0f9f6d2f4c571caa4189b2a980507b5b219356370db100a734debb4722` |
| `did:key:alice` under the type named `entity:` | `9cb91eb8813601e7a8f5a53b993053cc77c77f1e318fc33cef318202d7934213` |

`tests/vectors.rs` pins the same values. They were also derived independently
from this text with the Python `blake3` package.
