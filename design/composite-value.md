# A composite value that holds a syntax tree and is read in place

**Phase: Building.** A built-in composite value, `Value::Composite`: a map
(a record its sub-band) or a list, stored compactly, referenced by its
merkle reference, and read without decoding what is not asked for. It
replaces the draft #812, which reached a field by a scan of every name and
wrote a nested composite again to read it, and the dag-cbor composite on
the notation's line, which is decoded whole.

## Goals, in order

The maintainer's, stated 2026-11-23; every section below answers to them.

1. **Compression by sharing.** The only reason this is not CBOR: many
   tokens repeat across many composite values (and within a syntax tree),
   so stored in the Chronicle format they are shared and compress very
   well. In his words: "if it is tokenized, it can be interned. If it's
   not, then impossible." So a composite's bytes are tokenized, its atoms
   addressable in a table, which makes interning possible wherever a store
   supports it. Today that is a Chronicle file (`dialog-chronicle`, native
   only): a composite in a key there is its atoms' token ids and its body.
   Tree blocks in every other store, in the browser among them, and blocks
   sent to a peer carry each composite whole, its own table shared with
   nothing; "it's okay if we don't do the sharing in non-chronicle setups.
   In fact, that's kind of the expectation at the moment."
2. **Reading without decoding.** A property or a path inside a composite
   is read without decoding the whole value, and what is read is a view
   into the buffer.
3. **The format can change without changing the database's.** A newer
   dialog reads every older layout and may write the new one; nobody is
   forced to upgrade a database for it.
4. **Not goals.** Identity preserved across layouts (nice to have only),
   and one value across two layouts. Joining on whole composites is not
   something the system cares about: composites are taken apart, and a
   field that needs an index gets an index of its own.

Earlier statements of the same aims: "a builtin representation of
composite value that offer random access and can be stored more
compactly and could be read without decoding it all"; "all records are
just views over the same memory buffer"; and it must hold a syntax tree.

## What the encoding has to serve

The syntax trees of every `.clj` the notation has (its sketches, examples
and prelude, `wip/engine-fold` at `7b7484a8c`), counted by
`design/composite-value/census.py` on the `research/composite-value`
branch, which holds the measurement harness. It reads each document with the
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

Every number here is of the layout at `11dc23c` (atoms in the table,
scalars referred to), measured 2026-11-23 on a 4-core Xeon shared with a
callgrind sweep (load about 3), so differences under about 30 ns are
noise. The harness is `record_spike` on the `research/composite-value`
branch (`rust/dialog-repository/examples/record_spike/access.rs`), built
against this layout in a local checkout with `Composite::layout` and
`Composite::from_layout` made public again (they are test-only here):
`cargo run --release -p dialog-repository --features helpers --example
record_spike -- access 9`. ns a read, the median over every field or item
in turn. "#812" is its codec, kept as `pairs812.rs` there; "scan" is this
layout with no offsets anywhere, the table included; "offsets" is it with
offsets in every sequence of two or more. The canonical layout is "scan"
up to 8 items and "offsets" past them.

| one field of a map of n, values text | #812 | scan | offsets | | one item of a list of n, items text | scan | offsets |
|--:|--:|--:|--:|---|--:|--:|--:|
| 2 | 156 | 227 | 253 | | 2 | 66 | 65 |
| 4 | 189 | 232 | 202 | | 4 | 76 | 61 |
| 8 | 200 | 306 | 245 | | 8 | 90 | 56 |
| 12 | 256 | 391 | 264 | | 12 | 101 | 54 |
| 16 | 310 | 465 | 259 | | 16 | 142 | 67 |
| 64 | 1,097 | 1,387 | 335 | | 64 | 394 | 61 |
| 256 | 3,643 | 4,959 | 419 | | 256 | 1,593 | 65 |

Where each value is a three-field node, #812 takes 1,452 to 9,276 ns,
because it writes the node it returns again; the scan takes 122 at 2 and
4,938 at 256, and offsets 159 and 375.

| a field three composites down, s siblings a level | #812, written again at each level | view |
|--:|--:|--:|
| 1 (178 bytes) | 21,492 | 548 |
| 3 (310 bytes) | 42,770 | 793 |
| 32 (2,514 bytes) | 498,209 | 1,075 |

What they say:

- A scalar is now read through the table: the field's key is found in
  it, then the value's atom by its place. At the syntax trees' common
  size, a map of two text fields, that is 227 ns against 118 when values
  sat in their slots (the layout before atom references, measured the
  same way, under a similar load): the price of keeping each scalar once.
  Past 8 entries the table and the body have offsets and a read stays
  flat (419 ns at 256).
- `SCAN_LIMIT` stays 8: below it, offsets win or lose by tens of
  nanoseconds, and the syntax trees reach it three times in 4,267
  composites.
- A view reads three levels down in about a microsecond at any size; #812
  pays for everything it writes again, 39 to 460 times more.

**What a read costs on the path a query takes.** A value bound from a key
goes through `decode_value_view`: the payload is unescaped, then
`Composite::from_bytes` reads every part and writes the composite again
to check its bytes are canonical, then the field is read. For a chain of
maps of two fields (`type` and `body`), the deepest `type` read through
its path, release, 20,000 rounds (`cargo test --release -p dialog-corpus
--lib -- --ignored --nocapture measure_reading_a_field`):

| depth | bytes | whole read | unescape | `from_bytes` | view, no check | the field |
|--:|--:|--:|--:|--:|--:|--:|
| 7 | 118 | 7,974 ns | 328 ns | 6,237 ns | 27 ns | 831 ns |
| 13 | 166 | 15,174 ns | 395 ns | 12,032 ns | 31 ns | 1,473 ns |
| 19 | 217 | 20,023 ns | 392 ns | 16,160 ns | 28 ns | 2,051 ns |

The canonical check is about 80% of a read and the field 10%: a stored
composite is not yet read "without decoding it all", since the check
decodes all of it. It guards bytes nobody checked: a block from a peer or
from storage is content addressed, which says the bytes are the ones its
hash names, not that they are canonical, while a composite built here is
canonical by construction. Where the check belongs (when a block is taken
in, or on every read as now) is the maintainer's call; nothing here
changes it.

**The archive.** A file of syntax trees with their atoms shared as tokens
against each composite kept whole, in `dialog-chronicle` (`cargo test
--release -p dialog-chronicle --lib -- --ignored --nocapture
measure_an_archive_of_syntax_trees`). The census's own trees could not be
read here (the notation's documents are not in this checkout), so these
are 133 generated trees of the census's shape (a map per node that says
its `type`, two fields at the median, 7 to 11 deep, names from a
vocabulary of 150), 141,182 bytes of composites, one fact each:

| | tokens | their bytes | token records | frame records | index pages |
|---|--:|--:|--:|--:|--:|
| atoms shared | 765 | 107,523 | 116,246 | 1,450 | 8,434 |
| kept whole | 299 | 141,661 | 146,415 | 1,444 | 0 |

Shared, the tokens' records are 0.79 of whole and all records but the
preallocated extent 0.85 (126,130 against 147,859 bytes): every token
costs a pad byte, padding and a length, and two and a half times the
tokens grow the token index out of its first page. Atoms shared across
many versions of a tree, or many trees, save more; this file holds each
tree once.

**Not measured at head.** The census's dag-cbor comparison: the trees
were 0.67 of their dag-cbor size on the layout before atom references;
measuring the layout at head needs the trees `CENSUS_DUMP` writes from the
notation's documents (sha256 `362444d5754e6c43…`), which this checkout
does not have.

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
 table     := seq(0) of atom*                  every key and scalar value the
                                               composite and all it nests use,
                                               once, sorted by canonical bytes
 body      := seq(1) of (index · slot)*        a map: by key, then by value
            | seq(2) of slot*                  a list: in order
 slot      := 26 · index                        a scalar's place in the table
            | 22 · body                        a nested composite
 seq(kind) := head · count · size · offsets · items
 head      := kind | width << 2                width: none, or 1, 2 or 4 bytes

 The table holds complete scalar key/value encodings. Every scalar slot in
 every nested body is a type-26 reference to that table; repeated strings
 such as two `"hello world"` values occupy one table entry.
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
- **Atoms once.** The table serves the composite and everything nested in
  it, for keys and values alike: a syntax tree writes `type` once, and the
  same string used in several fields appears once.

**Chronicle.** An atom is a scalar in the bytes a tree key holds it in,
so the single-file archive (`dialog-chronicle`) keeps a composite in a
key as a packed token: `22 · FF 'C' 'T' 01 · count · id… · body`, each
`id` the four-byte token id of an atom, in the table's order, and the body
as the canonical bytes hold it. Each atom is the token a key holding that
scalar alone names, so a text that a thousand composites and a plain fact
hold is one token (`it_shares_a_composite_atom_with_every_value_that_holds_it`).
Reading a key, a segment rebuilt from its rows, or a node served in place
realizes the packed token to the escaped canonical bytes again. Its limits:

- A composite that spilled, whose key holds only a prefix of its bytes,
  is kept whole, told by the key (`key_value_is_spilled`), not by a parse
  that fails.
- Every well formed composite a key holds whole is packed. A composite is
  packed only when the packed token realizes to the very key bytes; that
  check is a guard the tests assert is never used for a composite the
  corpus wrote (500 random composites in every key form, and every
  shape). It can fire only for key bytes the corpus would not write,
  which a block from a peer can hold: a value that is not one escaped
  byte string, bytes without a table and a body, or a layout that is not
  the canonical one (offsets where none belong, a wider offset width).
  Such a value is kept whole, which is what was given. The check costs
  173 ns for a two-field map and 0.8 to 2.0 µs for a syntax tree, once a
  value: a draft remembers the token each value was packed as, so the
  four keys a fact puts its value in (three orderings and history) pack
  it once. A commit of 1,000 syntax-tree facts packs 3.96 times a fact
  without that and 0.99 with it, and appends in 48.6 µs a fact against
  27.9 (`cargo test --release -p dialog-chronicle --lib -- --ignored
  --nocapture measure_a_commit_of_syntax_trees`, 2026-11-23).
- An index node's separators are whole keys, kept whole: a composite in a
  separator shares nothing.
- A packed token's second byte, `FF`, is one no composite kept whole
  starts with: escaped canonical bytes start with the table's head, below
  `0x10`, escaped `00 FF` when zero (tested on random, empty, spilled and
  offset-tabled composites).
- A spilled composite shares nothing with the file. Its key holds a
  64-byte prefix (a value spills when its key encoding passes
  `inline_n`, 4,096 bytes by default), and its whole bytes go to the
  space's blob store through the blob effect, which Chronicle does not
  answer: a Chronicle space keeps blobs in the filesystem provider, a
  file each. Within one composite the atom table already keeps a repeated
  scalar once; across values a spilled one gets nothing. Of 1,000
  generated trees of the census's shape 11 spill; the census's largest
  form is 7,026 bytes of dag-cbor, so at most a few of its 133 forms do.
  Sharing their atoms needs the blob in the file: Chronicle answering the
  blob effects (a blob store with range reads in the file, packing a blob
  that realizes back as a composite), or the corpus writing a spilled
  composite as an archive block, which moves where a replica keeps and
  syncs it. Neither is in this change.
- Every token costs a pad byte, padding to 16 and a four-byte length, so
  a small atom shared once costs more than it saves; the archive's size
  below says what that comes to.
- Token ids are file offsets and a token is never moved or dropped once
  written, so the ids a packed token holds stay valid. A packed token
  names atoms written before it (tested on every file the tests write),
  so a file Chronicle wrote never holds one naming an atom it lacks:
  `PackedError::MissingAtom` and `Truncated` come only from damaged
  bytes. A token record's checksum is checked when the file is opened
  for the records past its last sync point, not on every read, so damage
  to an older record reaches a reader; these errors are what a packed
  token gives then, and a read never panics on it. An append prepared
  before another one names new atoms by drafted ids; laid out, it looks a
  packed token up under its atoms' final ids, so it is not written twice
  (`it_reconciles_a_packed_composite_another_append_wrote_first`).

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

In `dialog-corpus` (`model/composite/tests.rs` and `tree.rs`): against a
plain model of maps and lists over 3,000 generated composites
(construction in any order and with repeats, every key and item, length,
identity); every layout read alike and only the canonical one accepted;
order total, equal to the canonical bytes and to the key's bytes in the
tree, no key a prefix of another; a nested view equal to, hashing and
ordering as its copy, sharing the buffer; a read decoding only what it
returns (a counter); the record told by its keys and typed by the band;
a map of attributes and a record one value; the bytes the drawing shows;
every refusal by its variant, a scalar held in a body instead of
referred to among them; byte 26 reserved, a key starting its value with
it refused; a composite split into atoms and body and joined back to the
same bytes; a varint past `u32::MAX` refused; the bytes and the merkle
reference of six fixed composites (a record, a map with text keys, a list
with two-byte offsets, one nested three levels deep, an empty map and an
empty list) pinned as constants, which the wasm32 build run in a browser
matches as the 64-bit one does (the bytes moved with the atom table; the
six references did not); 20,000 random and mutated byte strings without a
panic; every serde form; a tree storing records, maps and lists, one
spilled.

In `dialog-chronicle` (`node/composite/tests.rs`, and one in
`archive.rs`): a composite in a key split into atom tokens; the value
found as the last token in all five key forms; a spilled composite kept
whole on purpose; a composite that would not come back the same kept
whole, its atoms dropped; a corrupted packed token (a count past its ids,
an id the file lacks, cut short, a drafted id unresolved) an error of a
named kind; 500 random composites in every form back byte for byte; a
segment of every shape and form realized from its tokens; the atom shared
once across two composites and a plain value; every form and shape read
back after reopening, from atoms new in the commit and held from an
earlier one, whole and as served in place; a rewritten node reusing its
predecessor's rows of composites; and the append prepared before another.

In `dialog-repository` (`branch/select/composite_tests.rs`): a round trip
through a branch loaded afresh, by entity, attribute and value, on the
volatile provider and on the platform's durable one, one body for both:
the filesystem natively, IndexedDB and OPFS in the browser; and the
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
structure and not compare with the canonical encoding (the test-only
`from_layout` does this now); equality and hashing already walk content, so
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
`to_bytes`, `Value::Composite` and `ValueDataType::{Map,
CompositeRecord, List}`. To do there:

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
