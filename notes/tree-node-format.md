# Tree node format: tagged nodes and an extensible manifest

Status: draft for review. Nothing here is implemented yet. The code table lives in [`rust/dialog-search-tree/format/table.csv`](../rust/dialog-search-tree/format/table.csv).

## Why

Every tree node carries the tree's format manifest (branching parameter, spill threshold, pacing knobs), so any node hash is a complete, self-describing tree root. Today the manifest is a fixed rkyv struct inlined into the node body: 28 bytes of positional values plus padding.

That layout cannot evolve. The struct's size is part of every node's layout, so adding a field (even an optional one) changes how every existing node decodes. A positional tail of optional fields would allow additions, but it assumes one linear history: fields cannot be dropped, and two implementations that each append a field give the same position two meanings.

We also need to keep reading every node already stored, and a reader has to learn a node's layout before it can read anything inside the node, so the answer cannot live inside the rkyv body.

## Goals

- Old nodes stay readable forever, with no migration.
- New fields can be added without coordinating a single version line, and fields can be retired.
- An older program keeps working with a newer peer's tree: fields it does not understand cost it a suboptimal tree shape, never a refusal, unless the field changes how data is read.
- The same node always has the same bytes (content addressing).
- The body stays zero-copy rkyv, read in place.
- Decoding a tree's manifest happens once per distinct manifest, not once per node read.

Encryption is out of scope here; the layout leaves room for it as a future version.

## Layout

A tagged node is:

```
version        bijou64   node layout version (0x02 for this layout)
kind           bijou64   0x00 segment (leaf), 0x01 index
header length  bijou64   byte length of the fields that follow
fields         the tree's manifest: code, length, value, repeated
padding        zero bytes up to the next 16-byte boundary
body           rkyv bytes: an archived Segment or Index, chosen by kind
```

Like an HTTP message: a short fixed preamble, headers, then a body of plain bytes.

- `version` picks how everything after it is read. It is a node layout version, not a manifest version: a change to the Segment or Index body layout is a new version. `0x01` is reserved for the legacy untagged layout and never written as a tag.
- `kind` says which body follows, so a reader chooses the archived type before touching rkyv, and code that only needs "is this a leaf" reads one byte. The body is the concrete archived struct, not an enum over both kinds. A kind is must-understand: a node of an unknown kind is unreadable.
- `header length` lets a reader skip the fields without parsing them, and makes the field bytes directly the key for the manifest decode memo (below).
- `fields` hold only the tree's manifest, which is identical on every node of a tree. Per-node facts (the kind, and a level if we ever add one) live in the preamble, so a tree's nodes share byte-identical field bytes.

### Integers

Every integer in the preamble and the fields (version, kind, lengths, codes, integer values) is encoded as [bijou64](https://www.inkandswitch.com/tangents/bijou64/): values 0-247 are one byte, and first bytes 248-255 announce 1-8 following big-endian bytes, offset so the ranges never overlap. Each number has exactly one encoding, which content addressing needs and LEB128 does not give without a minimality check on every read. It also decodes faster than LEB128 at about the same size. Byte lengths per value:

| Bytes | Range |
|---|---|
| 1 | 0 - 247 |
| 2 | 248 - 503 |
| 3 | 504 - 66,039 |
| 4 | 66,040 - 16,843,255 |
| 5-9 | larger, up to `u64::MAX` |

The `bijou64` crate (MIT/Apache-2.0) has been superseded by `bijoux`; we should depend on whichever is maintained, or vendor the few dozen lines of the codec.

### Alignment

Node bytes live in an `AlignedVec` (16-byte aligned), and rkyv reads the body in place, so the body must start at a 16-byte-aligned offset. The writer pads with zero bytes after the fields up to the next multiple of 16. Zero padding is deterministic, so the bytes stay canonical. The whole preamble plus an empty header plus padding is 16 bytes.

## The manifest fields

The manifest is a sequence of `code, length, value` entries, all bijou64 except the value bytes themselves, with integer values bijou64-encoded inside the value.

Canonical form (a writer must produce it, and a node that is not canonical is not a valid tagged node):

- Codes strictly ascending, no duplicates.
- A field equal to its default is omitted. The default manifest therefore encodes to zero field bytes.
- Integers in their only bijou64 encoding; lengths exact.

Every value is length-prefixed, so a reader skips a field it does not know without knowing its type.

Example: a tree that differs from the defaults only in `fanout_n = 4` has the field bytes `02 01 04` (code 0x02, length 1, value 4).

### The code table

[`table.csv`](../rust/dialog-search-tree/format/table.csv) is the registry, in the spirit of the multicodec table: one row per code with its name, what it tags (`version`, `kind` or `manifest`), status, the default a missing field means, and a description. The Rust manifest type is checked against it (or generated from it), and a build bundles the table it was built with.

Rules:

- A code is never reused. A field that is no longer wanted is marked `deprecated` and its code stays retired.
- A default never changes. A missing field means "the table's default", so changing a default would silently change the meaning of every existing header. A different default is a new field with a new code.
- Codes are assigned by a pull request to the table. That is the only coordination a new field needs.

Code ranges, aligned with bijou64 lengths:

| Range | Encoded size | Use |
|---|---|---|
| 0x00 - 0xF7 | 1 byte | core dialog fields |
| 0xF8 - 0x101F7 | 2-3 bytes | further registered fields, including third-party ones added to the table |
| 0x300000 and up | 4+ bytes | private and experimental use: no registration, no collision guarantee |

### Critical and shape fields

Some fields only shape the tree: ignoring one gives a tree shaped differently from its canonical form, which costs work when replicas compare but never loses or corrupts data. Others change how bytes are read: `inline_n` and `spill_prefix` decide how a value is laid out inside its key, and a reader that assumed its own values would build different query keys and miss facts.

A reader that has never seen a code does not have its table row, so the distinction is carried by the code itself:

- **Even codes are shape fields.** An unknown one is ignored (and kept, below).
- **Odd codes are critical.** An unknown one makes the tree unreadable for this build. This is the one failure a newer peer's tree can cause an older program, and it is confined to trees that actually use such a field.

### Unknown fields are kept

The in-memory manifest holds the fields this build knows as typed values and every other field as raw `(code, bytes)`. It writes all of them back when it persists. Without that, an older program editing a newer peer's tree would strip settings it does not understand. An unknown shape field shapes nothing locally; it just survives the edit.

### Defaults and the constants they replace

Every tunable that shapes the tree is a field whose default is today's value, including the weight overheads that are code constants today (`entry_overhead` 64, `key_overhead` 32, `link_overhead` 16). A tree that never changes them stores nothing for them.

## Reading a node

1. If the bytes start with a known `version`, and the kind, header length, fields, padding and body all parse and validate, the node is a tagged node of that version.
2. Otherwise it is a legacy node, read with the current decoder. Its inline manifest maps onto the same fields in memory, so legacy and tagged nodes produce the same `Manifest` value.

There is no magic prefix. An old node's first byte can be anything, including a known version, but then it has to pass every later check too: a length that fits, strictly sorted fields, exactly zero padding, and a body that passes rkyv's full validation where it lands. Arbitrary old-node bytes fail within the first few bytes and fall through to the legacy decoder. A deliberate "collision" is just a well-formed tagged node, and the rule is deterministic, so every peer reads the same bytes the same way. Keeping lengths exact and padding strictly zero is what makes the rejection fast and deterministic.

Field bytes are identical on every node of a tree, so the decoded manifest is memoized by its field bytes: after the first node of a tree, reading its manifest is one lookup, and the default manifest (empty fields) needs no parse at all. Body validation is separate and already done once per node by the cache of checked nodes (PR #545).

## Writing

- Every new node is written tagged. An edit to a legacy tree writes tagged nodes along the edited path and leaves untouched legacy subtrees as they are. Each node describes itself, so trees mixing both layouts are fine.
- Rewriting existing data in the new layout changes its hashes, so it will not deduplicate against legacy copies of the same data. Untouched legacy nodes keep sharing as before.

## Rollout

A build that predates this layout cannot read tagged nodes, and peers run different builds. So:

1. Ship the reader first: builds that read both layouts but still write legacy nodes.
2. Once those builds are what peers run, switch writing to the tagged layout.

## Open questions

- A per-node **level** (leaf 0, each index level above +1) would let sync and diffing know a subtree's depth without descending. Nothing needs it today; it would be one more preamble byte, added with a new version.
- Whether the table stays a CSV the code is checked against, or becomes a schema file the Rust type is generated from (the schemaboi direction), with the CSV derived from it.
- Integer codec: depend on `bijoux`, or vendor the codec.
- Whether the empty tree's node stays a segment with no entries (as now), or gets its own kind.
