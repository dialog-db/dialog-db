# Single-file archive: design

The decision for building [the single-file format](single-file-format.md),
from what [the spike](single-file-format-spike.md) measured. Numbers below
are from that census: Stack Exchange at 4096 commits, and tonk-team's whole
pushed history (9,070 blocks from R2).

## The decision

The single-file format is an **archive provider**. It answers
`archive::Get`, `archive::Put` and `archive::Import` by hash, as the `fs`
and IndexedDB providers do, and fills the archive slot of a `Space`. The
tree, the query engine, sync and the capability model do not change: they
keep reading and writing blocks by hash.

Inside the file a tree node is stored one of two ways:

- **whole**: its wire block, byte for byte. A read is one positioned read,
  the same cost as a file per block.
- **as a frame**: its rows of token ids, written against the frame it
  supersedes, with a whole frame every 32 versions. A read decodes the chain
  and realizes the block.

Every node arrives whole. A node becomes a frame when a newer node
supersedes it, or when the whole nodes exceed their budget and nobody has
read it lately. So the nodes queries read stay whole, and history is
compact. Which nodes stay whole is policy, not format, and can change
without changing the file.

## Why this and not the alternatives

Measured on the live segments:

| | SE | tonk-team |
|---|---|---|
| read a block from a file (+ blake3) | 11–15 µs (21–24) | 13 µs (21) |
| decode a frame's rows | 4–27 µs | 5–17 µs |
| realize a block from rows, of which: | 134–225 µs | 139–169 µs |
| · rebuilding entries from tokens | 79 µs | 82 µs |
| · the tree's encoder | 61 µs | 71 µs |
| validating bytes into a node (every read pays it) | 4 µs | 7 µs |

- **Frames only** (what the spike measured) is the smallest file, but every
  node cache miss pays the encoder, 4–5× a file read even with a perfect
  rebuild. A database that reads cold is the case that matters most for
  "open the file and query it", so that cost lands on every first query.
- **A tree that reads rows** removes the cost, but it means a second node
  representation throughout `dialog-search-tree`, and it breaks the
  "single-file format is an archive" requirement: the tree would have to
  know which store it is reading.
- **Hashing a canonical row encoding instead of the wire block** removes
  realizing altogether, but changes every node's address: a storage and
  sync format change for every replica, for a gain only this provider
  needs.
- **Whole hot nodes, frames for the rest** keeps the archive interface,
  costs a file read for what queries read, and pays the realize only for
  history. It costs space: the file holds roughly the live tree whole on
  top of the frames.

## Where it lives

A new crate, `dialog-archive-file`, above `dialog-artifacts`. It has to
split artifact keys into entity, attribute and value, and realize nodes
with the search tree's encoder, so it cannot live in `dialog-storage`.
Native first, on one file through positioned reads and appends. The web
needs OPFS, which is later work.

It frames blocks of the `index` catalog that parse as tree nodes. Every
other block, of any catalog, is stored whole and never converted.

## The file

An append-only sequence of records, each `kind (u8) | length (varint) |
crc32c | body`:

| record | body |
|---|---|
| token | one token's bytes; its id is its order among tokens |
| whole | a block's catalog and bytes |
| frame | the node's hash, kind, the node id it is written against (or none), and its sections as columnar rows |
| retire | the node ids whose whole records a frame now replaces |
| checkpoint | the index state as of this record (see below) |
| footer | the position of the latest checkpoint, at a fixed offset from the end |

- **Node ids.** Every node gets a dense id when it first lands. Frames name
  their children and their predecessor by node id, not by file position,
  because a node's record moves when it goes from whole to frame. The index
  maps id to hash and to its current record.
- **Rows.** As in the spike: per leaf entry, the form and the token ids of
  entity, attribute, value and datum. Per index link, the separator, the
  child's node id and the scale. Per buffered op, the same as an entry with
  no datum for a retraction. Each column is zigzag-delta varints. Against
  its predecessor a section is copy runs and literal rows.
- **Predecessor.** The latest node of the same kind that starts at the same
  first key. The provider needs nothing from the tree to find it; on both
  datasets the frames come out the same size as with the tree's own
  lineage.

## Reads

`Get(hash)`: fingerprint lookup in the in-memory index, then

- whole: one positioned read of the record, returned as the block;
- frame: decode up to 32 frames back to a whole frame, realize the block.

`LoadBlock` checks the hash and the node cache keeps what was read, both as
they do today. The provider does not cache realized blocks itself; the
node cache already does.

Realizing starts as the tree's encoder fed from rows, about 140 µs. A
purpose-built realizer that writes the node's columns straight from tokens
skips the 80 µs of rebuilding entries, and is checked against the tree's
encoder by a property test over random nodes. That is an optimization
inside the provider, not a format change.

## Writes

`Put` and `Import` append each block as a whole record and add it to the
index. Nothing is encoded on the commit path, so a commit costs appends.

Turning nodes into frames happens after the import returns:

1. For each new node, find its predecessor. If the predecessor is whole,
   it is due to become a frame.
2. Whole nodes beyond the budget (default: 64 MiB, or twice the largest
   head the provider has seen) are due too, least recently read first.
3. For each node due: split it into rows, encode the frame against its own
   predecessor, **realize the frame and compare it to the block**, and only
   if they match append the frame and a retire record. A node that does
   not realize byte for byte, such as a legacy-layout node, stays whole.
   That check makes realize correct by construction: no frame is ever
   written that does not rebuild its block.

Durability matches the `fs` provider: no fsync per block. An import's
records become visible when appended, and a crash can lose only the tail
after the last complete record. The branch head is published by the memory
provider after the import returns, so a torn tail can only lose blocks of a
commit that was never published. A sync point can be added if an embedder
needs one.

## Index, checkpoints, recovery

In memory:

- block fingerprint (8 bytes) to node id and record position;
- token id to position, plus token bytes to id for writes;
- predecessor map: kind and first key to the latest node id;
- read recency of whole nodes, for the budget.

At 16 bytes a block the index is about 140 KB for tonk-team and 240 KB for
SE. The token map is the largest: 11 to 14 MiB of tokens on these datasets.

Every 64 imports, or 16 MiB of appends, the provider writes a checkpoint
record (the index and maps, sorted and serialized) and a footer pointing at
it. Opening reads the footer, loads the checkpoint, then scans the records
after it. A record with a bad checksum ends the scan, and the file is
truncated there.

## Reclaiming space

A whole record that a frame replaced is dead. Native: punch a hole over it
(`fallocate(PUNCH_HOLE)` on Linux, `F_PUNCHHOLE` on APFS), so the file
keeps its offsets and the filesystem gets the space back. Whole records
are aligned to 4 KiB so the holes free whole pages. Where punching is not
available, compact: when dead bytes exceed the live ones, copy the
non-dead records into a new file and rename it over the old.

## What it should weigh

From the census, the frames and tokens plus the whole nodes the budget
keeps (taking the live tree as what stays whole):

| | file per block | frames + tokens | + whole live tree | ratio |
|---|---|---|---|---|
| SE, 4096 commits | 1169 MiB | 13.4 | about 33 | 35× |
| tonk-team | 568.5 MiB | 25.4 | about 53 | 11× |

Both grow with the live facts, not with commits. A smaller budget trades
cold reads of the head for space.

## Not in this design

- **Blobs.** Spilled values and uploads go through `blob::*`, a separate
  slot of a `Space`. They can be whole records in the same file later; they
  never become frames.
- **Branch heads and credentials.** The memory and credential slots stay
  where they are. A record kind for memory cells would make the file the
  whole space, and is the natural next step once the archive works.
- **Finding old roots.** Revision records naming their parents' roots (see
  the discussion on the spike) make every revision's tree findable; that is
  a change to `RevisionRecord`, independent of this provider.
- **BAO verified ranges, multi-process writers, the web.**

## Build order

1. The file and its records: append, checksum, footer, checkpoint, open and
   recover a torn tail. Tested on its own.
2. The provider with every node whole: `Get`, `Put` and `Import` over the
   file, passing the tests the `fs` and volatile archive providers pass.
   This is already a one-file archive at file-per-block speed.
3. Frames: tokens, rows, predecessors, the verified conversion, and reads
   through the chain. The census's checks become its tests.
4. The budget, hole punching and compaction.
5. The perf gate's scenarios over it, against the `fs` archive, before it
   is offered as a provider. Then the direct realizer, if history reads
   matter.
