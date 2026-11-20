# A composite value that holds a syntax tree and is read in place

**Phase: Building.** A built-in composite value, `Value::Composite`: a map
(a record its sub-band) or a list, stored compactly, referenced by its
merkle reference, and read without decoding what is not asked for. It
replaces the draft #812, which reached a field by a scan of every name and
wrote a nested composite again to read it, and the dag-cbor composite on
the notation's line, which is decoded whole.

The maintainer's goals, which this is measured against: "a builtin
representation of composite value that offer random access and can be
stored more compactly and could be read without decoding it all";
"Records should be referenced canonically by merkle-references", "giving
canonical identities even if on wire / on disk bytes may be different";
"all records are just views over the same memory buffer"; and it must hold
a syntax tree.

## What the encoding has to serve

The syntax trees of every `.clj` the notation has (its sketches, examples
and prelude, `wip/engine-fold` at `7b7484a8c`), counted by
[census.py](composite-value/census.py). It reads each document with the
reader's rules (`form.rs`) and builds the value `Syntax::value` builds
(`syntax.rs`: every node a map that says its `type`, a call's parts a list
and its labels a map, a struct's entries a list of `[key, value]` pairs).
The shapes are exact; the dag-cbor bytes are computed from CBOR's heads and
the composite's link layout, not written by the notation's code.

| measure (133 forms, 4,267 composites) | min | median | p95 | max |
|---|--:|--:|--:|--:|
| composites per form | 11 | 28 | 53 | 213 |
| fields per map (3,554 maps) | 0 | 2 | 3 | 9 |
| items per list (713 lists) | 0 | 1 | 4 | 10 |
| nesting depth | 7 | 7 | 16 | 19 |
| dag-cbor bytes per form | 339 | 893 | 1,833 | 7,026 |

One map and two lists hold more than 8. A scan is the common path; a table
of offsets is the rare one, and is kept as small as it can be.

## Measurements

`record_spike access` (`rust/dialog-repository/examples/record_spike/access.rs`),
over the trees `CENSUS_DUMP` writes (sha256 `362444d5754e6c43…`), release build, 9 rounds, ns a read, the median over every field or item in
turn; two runs agree within 15%. A 4-core Xeon at 2.1 GHz, shared with two
other builds (load 3 to 4), so differences under about 20 ns are noise.
"#812" is its codec kept as `pairs812.rs`; "scan" is this layout with no
offsets; "offsets" is it with offsets in every sequence of two or more.

| one field of a map of n | #812 | scan | offsets | | one item of a list of n | scan | offsets |
|--:|--:|--:|--:|---|--:|--:|--:|
| 2 | 105 | 118 | 151 | | 2 | 48 | 46 |
| 4 | 120 | 138 | 155 | | 4 | 50 | 46 |
| 8 | 169 | 174 | 200 | | 8 | 62 | 45 |
| 12 | 225 | 206 | 217 | | 12 | 82 | 47 |
| 16 | 281 | 240 | 223 | | 16 | 89 | 44 |
| 64 | 905 | 627 | 287 | | 64 | 257 | 43 |
| 256 | 3,343 | 2,108 | 355 | | 256 | 1,053 | 46 |

Values are text. Where each value is a three-field node, #812 takes 1,192
to 6,803 ns at every size, because it writes the node it returns again;
the scan takes 85 at 2 and 1,848 at 256, and offsets 123 and 323.

| a field three composites down, s siblings a level | #812, written again at each level | view |
|--:|--:|--:|
| 1 (185 bytes) | 13,661 | 362 |
| 3 (369 bytes) | 32,301 | 382 |
| 32 (3,343 bytes) | 365,799 | 760 |

| the syntax trees | dag-cbor | composite | offsets everywhere |
|---|--:|--:|--:|
| 133 forms | 142,609 | 95,127 (0.67) | 101,141 (0.71) |

What they say:

- In a map, a scan wins up to about 12 entries and offsets win from 16; in
  a list, offsets win from 4, by less than 20 ns below 8. `SCAN_LIMIT` is
  8: either way the loss below it is tens of nanoseconds, and the
  syntax trees reach it three times in 4,267 composites. Offsets
  everywhere would cost 6% more bytes for nothing they read.
- A view reads three levels down in under a microsecond at any size; #812
  pays for everything it writes again, 38 to 480 times more.
- Every tree is two thirds of its dag-cbor, keys being kept once for a tree.

## Decided

The maintainer's, unless marked:

- A composite is a value: whole, replaced, never edited in place. No
  fractional indexing, no HAMT, no tree of nodes inside it.
- Kinds: a **map** of arbitrary keys, and a **record**, the sub-band of the
  map whose every key is an attribute; a **list**, ordered, with repeats
  (a kind of its own: the coordinating agent's).
- A map's key is any value but a composite, text the usual one, ordered by
  its encoded bytes (the coordinating agent's). A key may hold several
  values, held as a set, as the journal of #748 decided (2026-11-06, "a
  record is a set of pairs, sorted, without duplicates"). The notation's
  own maps refuse a key twice and must enforce that themselves.
- Identity is the merkle reference, never the bytes: a map under
  `composite:` from each key's reference to its value's or to the `set:` of
  its values'; a list the `list:` of its items' references. A record's
  identity is what #812 gave it.
- Bytes are canonical for now, so a composite is compared and keyed in the
  tree by them; any other bytes are refused.

## The layout

```
 composite := table · body                     a buffer of one composite
 table     := seq(0) of key*                   every key the composite and
                                               all it nests use, once, sorted
 body      := seq(1) of (index · slot)*        a map: by key, then by value
            | seq(2) of slot*                  a list: in order
 slot      := type · payload                   a scalar as a tree key holds it
            | 22 · body                        a nested composite
 seq(kind) := head · count · size · offsets · items
 head      := kind | width << 2                width: none, or 1, 2 or 4 bytes

 {type: :tuple, items: ["a" "b"]} :

 00 02 0d │ 03 items 00 │ 03 type 00 │ 01 02 14 │ 00 16 02 02 06 03 a 00 03 b 00 │ 01 01 tuple: 00
 table: 2 keys in 13 bytes,          map: 2     key 0: a list of 2 in       key 1: an entity
 text type 03                        entries    6 bytes, text type 03
                                     in 20
```

- **Scan or offsets.** A sequence of more than `SCAN_LIMIT` items carries
  the offset of each item after the first, big endian, in the narrowest
  width that holds the last; a shorter one carries none. The rule fixes
  the width, so there is one encoding per value; offsets where there should
  be none, none where there should be some, a wider width, or an offset not
  where its item is are each refused, by name, in the tests.
- **Stepping over.** Every nested body says its size, so a field or an item
  past it is reached without reading it.
- **Values under one key** are ordered by their canonical bytes on their
  own, so their order does not depend on the table of the composite
  holding them.
- **Keys once.** The table serves the composite and everything nested in
  it, attributes and text keys alike: a syntax tree writes `type` once.

**Record as a sub-band.** One kind, told by its keys and never by a flag:
the table sorts by type byte first, so the attributes are one run of it,
and a map is a record exactly when its first and last entries' keys are
attributes. A map built with only attributes for keys and the same record
built by `Composite::record` are one value, one byte string and one
reference (tested). The types mirror the numbers: `composite:` (byte 22)
is the band every composite is keyed under, as `number:` is; in it
`composite:map`, `composite:record` and `composite:list` (23 to 25) type
slots and never keys, as `natural:8` does, `composite:record` stored as a
map as `natural:8` is stored as a natural. The query layer's `Primitive`
gives a map and a list a bit each and puts a record on the map's, so only
an attribute's declared type tells a record from a map.

**The view.** `Composite { buffer: Arc<[u8]>, body: usize }`: the buffer
that holds it and where its body is. A nested composite, a field or an
item read out shares the buffer, never written again. `Arc` and a place
rather than a lifetime, because `Value` owns what it holds, crosses
threads and await points, and is stored in bindings that outlive any
borrow of a block; a clone is a reference count. `ValueView::Composite`
holds the same view, so a scanned row binds a composite in place.
Equality and hashing walk the content through the table, so a view equals
its copy on its own; ordering compares canonical bytes, written again only
for a nested view.

**What is refused**, each a `CompositeError` asserted by name: bytes that
do not read (`Malformed`: no bytes, bytes past the end, a long varint, a
composite in the table, a slot of a type no composite holds, an offset not
where its item is); bytes that read but are not the canonical encoding
(`NotCanonical`: keys or entries out of order or twice, a key no entry
uses, offsets present, absent or too wide, a number not in the band's
encoding); a composite as a key (`CompositeKey`); NaN (`NotANumber`);
nesting past 128 (`TooDeep`). Random and mutated bytes never panic.

## Tests

In `dialog-corpus` (`model/composite/tests.rs`, 38, and `tree.rs`, 1):
against a plain model of maps and lists over 3,000 generated composites
(construction in any order and with repeats, every key and item, length,
identity); every layout read alike and only the canonical one accepted;
order total, equal to the canonical bytes and to the key's bytes in the
tree, no key a prefix of another; a nested view equal to, hashing and
ordering as its copy, sharing the buffer; a read decoding only what it
returns (a counter); the record told by its keys and typed by the band;
a map of attributes and a record one value; the bytes the drawing shows;
every refusal by its variant; a varint past `u32::MAX` refused; the bytes
and the merkle reference of six fixed composites (a record, a map with
text keys, a list with two-byte offsets, one nested three levels deep, an
empty map and an empty list) pinned as constants, which the wasm32 build
run in a browser matches as the 64-bit one does; 20,000 random and mutated byte strings
without a panic; every serde form; a tree storing records, maps and lists,
one spilled. In `dialog-repository` (`branch/select/composite_tests.rs`,
3): a round trip through a branch loaded afresh, by entity, attribute and
value, on the volatile provider and on the filesystem provider; and the
syntax tree below.

**The syntax tree.** `(todo ?t [1 2] state: :open tags: {"errand" _ "home" _}
where: {:author ?who})`, built from plain data in the shape `syntax.rs`
writes, is stored in a branch and read back; `operand.terms.where.entries[0][1].name`
is reached five composites down with one decode, the name returned; built
with every map's fields and the labels in the other order, it is one value
with one reference.

## What a composite costs a value that is not one

Nothing the Perf gate can see. The first push of this change (63075926c)
cost scenarios that build no composite up to 4%: `export-walk` +3.97%
instructions, the number ranges +2.4 to +3.6%, and 0.9 to 2.3% more bytes
and up to 1.5% more calls into the allocator in a dozen others. The types
did not grow: `Value`, `Option<Value>`, `ValueView`, a checked row's value
and the tree's entry are 32, 32, 32, 32 and 184 bytes on a 64-bit target
on main and here, since a `Composite` (a shared buffer and an offset, 24
bytes) is no larger than the `Vec` a `Value::Bytes` holds; a test pins
`Value` and `ValueView` at the size their scalars give them. Callgrind put
the cost in three places:

- **A scan's upper bound.** With the composite the largest type a key
  carries, the bound for an unset value became the composite type and a
  256-byte filler, in an allocation of its own, for every bound of every
  scan. Every composite's bytes start with its table's head, a byte below
  `0x10`, so one filler byte and the terminator bound them all, in the
  two-byte allocation the bound reserves. Main reserved 257 bytes for its
  one-byte infinity, so the bytes allocated are now under main's.
- **A key's value slot.** The composite's type byte in the key parser
  moved the reading of a value slot out of line, a call on every key
  parsed (`parse_key_layout`'s closure, 1.75M instructions in
  `export-walk`). The composite's byte is told in a cold function and the
  slot is read inline again.
- **A refinement's views.** A set of sized number views walked every
  type, 26 now against 22, testing each for being a view
  (`Refinement::admits`, +674K in `number-range-wide`). It walks the
  twelve sized number types alone.

Measured with `perf sweep` under callgrind, as the gate measures: the
first push against the main it was measured on (875cc7f15), and this
change merged on main (181b8bd61) against that main. Instructions outside
the allocator:

| scenario | main 875cc7f15 | 63075926c | main 181b8bd61 | now |
|---|--:|--:|--:|--:|
| export-walk | 22,723,071 | 23,624,289 | 22,723,073 | 22,563,042 (-0.70%) |
| number-range-wide | 20,173,192 | 20,905,591 | 20,179,484 | 19,931,486 (-1.23%) |
| number-range-narrow | 4,277,749 | 4,404,134 | 4,276,754 | 4,197,557 (-1.85%) |
| number-range-mixed | 6,638,925 | 6,799,851 | 6,640,044 | 6,597,610 (-0.64%) |

Bytes allocated, the calls into the allocator back to main's within one:

| scenario | main 875cc7f15 | 63075926c | main 181b8bd61 | now |
|---|--:|--:|--:|--:|
| query-member | 28,043,552 | 28,690,824 | 28,048,738 | 27,793,719 (-0.91%) |
| number-commit | 421,037,100 | 428,206,560 | 419,364,868 | 418,371,888 (-0.24%) |
| transaction-reads | 80,350,950 | 81,732,930 | 79,556,140 | 79,179,424 (-0.47%) |
| subscribe-repoll | 7,842,709 | 7,975,061 | 7,555,755 | 7,527,947 (-0.37%) |
| rule-point | 30,785,124 | 31,199,484 | 30,439,096 | 30,234,008 (-0.67%) |

No scenario of the 31 grows past the gate's tolerance: the most any grows
is 32,933 instructions (`query-join`, +0.07%, under the gate's 50,000) and
5 calls into the allocator.

## What bytes allowed to vary would take

Identity is already apart from the bytes. To accept other layouts (offsets
everywhere, keys inline, a buffer's shared table): `from_bytes` would check
structure and not compare with the canonical encoding (`from_layout` does
this now, for measurement); equality and hashing already walk content, so
they hold; the tree would key a composite by its canonical bytes written
again on the way in, or by its reference, which loses range order and a
spilled prefix; a block's hash would then cover bytes two writers can
spell differently, so whatever hashes or signs a block of composites would
have to canonicalize first.

## Prior work

- **#748, the record values research** (`worker/laughing-maxwell-lzfhyy`,
  `research/record-values*`). This layout rests on its findings: a header
  of names once per record (2026-11-11b layouts: tonk's bindings 1.02 of
  dag-cbor against 1.89 inline), every record a view over its buffer, the
  merkle reference as identity, a composite as a type of its own (byte
  22), keyed whole first. It extends them: that research found taking a
  nested record out of a header layout cost 6 to 7 times a copy and
  listed "a view (a buffer and a span)" as not measured; the view is that,
  and costs nothing. It contradicts one: "fields are named by attributes
  ... not a free name" (journal, 2026-11-06), overruled by the maintainer's
  arbitrary keys of 2026-11-20. Its harness is extended here with
  `access` and `pairs812.rs`.
- **#812** (`worker/laughing-maxwell-lzfhyy-composite`): the first build,
  the starting point; its record identities are unchanged.
- **`feat/composite-types`**: drops an unused type-lattice `Composite`
  (product and variant) for concept conformance. About typing entities,
  not values; not merged; nothing taken.
- **`feat/open-record`**: a `Directory<T>`, symbol-keyed, as a type-system
  shape only, on the code before the corpus. Not merged; nothing taken.
- **`decision/record-value`**: `notes/record-value.md`, an opaque record
  with a format trait, superseded by #748.
- **`worker/spill-revision-records`**, **`worker/rival-merge-records`**:
  spilling revision records' dag-cbor, and pushing against a rival. Not
  about composites; not merged.
- **`worker/laughing-maxwell-lzfhyy-value-hash-tiebreak`**: elections
  broken by the value's hash, which #748 says landed as #752.

## Left for the patterns and the notation

They will call `Composite::map`, `Composite::record`, `Composite::list`,
`kind`, `is_map`, `is_list`, `is_record`, `len`, `first`, `get`, `item`,
`entries`, `items`, `first_view`, `item_view`, `reference` and
`to_bytes`, `Value::Composite` and `ValueDataType::{CompositeMap,
CompositeRecord, CompositeList}`. To do there:

- replace `data.rs`'s `Composite::{Map, List}` in `Value::Record` with
  `Value::Composite`, and `entries`/`items` with `first` and `item`;
- refuse a key twice in the notation's own maps, since a composite map
  keeps a repeated key's values as a set;
- spell a set so `Syntax::value` keeps it (today `{: a : b}` repeats the
  empty key and is refused);
- move a definition's entity, made of its signature's reference, to the
  new reference;
- keep a struct's entries as a list of `[key, value]` pairs (a key is a
  node, and a composite's key is a scalar), as `syntax.rs` does now.

Not done here: a composite in a key is still its canonical bytes escaped
as one string, so a key cannot be read in place and a tree's node does not
split a composite into the archive's tokens (#748, design sections 1 and
4).
