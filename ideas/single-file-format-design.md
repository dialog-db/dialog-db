# Single-file archive: design

The decision for building [the single-file format](single-file-format.md),
from what [the spike](single-file-format-spike.md) measured. Numbers below
are from that census: Stack Exchange at 4096 commits, and tonk-team's whole
pushed history (9,070 blocks from R2).

## The decision

The file serves **nodes** to local reads and **blocks** to sync.

- A query, an edit, a traversal or a difference reads a node: the tree asks
  for it through `Nodes::load_node`, as it does today, and the file answers
  with a node decoded from its frame. Nothing is serialized into a wire
  block on the way.
- Sync, and anything else that moves blocks between replicas, reads and
  writes blocks through `archive::Get`, `Put` and `Import`, as today. A
  `Get` realizes the block from the frame; that is the only path that pays
  for realizing.

Inside the file every tree node is a **frame**: its rows of token ids,
written against the frame it supersedes, with a whole frame every 32
versions. There are no whole copies of hot nodes: a local read never needs
the wire form, so keeping one buys nothing.

The file is **append-only, always**. Nothing is overwritten, punched or
compacted in place. One writer appends at a time; any number of readers,
in any process, read the prefix that ends at a footer, which never changes
again (see [Concurrency](#concurrency)).

### What changed from the first version of this decision

The first version kept the archive interface as the only interface: every
read was a `Get` of a wire block, so a frame had to be realized (150–225
µs) on every node cache miss, and the design kept hot nodes whole to avoid
that. Two observations removed both:

- Local reads have no use for the wire form. Its bytes exist to be hashed
  and sent; a query wants keys and values. The tree already reads through
  its own seam (`Nodes::load_node`), not through the archive effect, so a
  store can answer with a node that was never a block.
- Whole copies would have to be retired when superseded, by punching holes
  or compacting. Both rewrite the file under readers, which is exactly what
  an append-only file with lock-free readers must not do.

## Why

Measured on the live segments:

| | SE | tonk-team |
|---|---|---|
| read a block from a file (+ blake3) | 11–15 µs (21–24) | 13 µs (21) |
| decode a frame's rows | 4–27 µs | 5–17 µs |
| realize a block from rows | 134–225 µs | 139–169 µs |
| validate a block into a node (every read of a block pays it) | 4 µs | 7 µs |

Decoding rows costs about a file read; realizing costs the tree's encoder
on top. A node read from rows skips the encoder, the blake3 of the block
and the validation, and is the only form in which the file's size win and
fast reads hold together.

It is also the memory win the [memory census](single-file-format-spike.md#memory-while-querying)
points at: a cached node today holds its block plus its decoded keys (about
twice the block). A node over rows holds 4-byte ids into a token region
every node shares.

What it weighs is the frames-only number, not frames plus a hot copy:

| | file per block | the file | ratio |
|---|---|---|---|
| SE, 4096 commits | 1169 MiB | 13.4 MiB | 87× |
| tonk-team | 568.5 MiB | 25.4 MiB | 22× |

## What it takes from the tree

`PersistentNode` today is its block: `body()` returns rkyv-archived views
of an index or a segment, and the tree reads through them at about 60
places. A node read from rows needs those reads to work on rows as well.
The surface is small:

- an index: `len`, `hash_at`, `separator`, `scale_at`, `links`, `route`,
  `children_spanning`, `selector`, `range_scale`, and its novelty
  (`novelty_len`, `buffer_for`);
- a segment: `len`, `entries`, `value_at`, and its key cursor.

So the change is: those accessors become the node's interface, with two
backings, the archived block (what every store but this one returns) and
rows over a shared token region (what this one returns). Keys compare
component by component against token bytes, as the columnar segment
already compares against its arenas; a value is the datum token's bytes,
archived on their own and aligned so they read in place.

Writes do not change. A persist still encodes each new node, because a
node's address is the blake3 of its wire form; the store receives the
block and the entries it was encoded from, and writes the frame from the
entries rather than parsing the block back.

## Concurrency

SQLite runs one writer at a time in both of its modes. In rollback-journal
mode a writer locks readers out while it commits. In WAL mode readers and
the writer proceed together: a writer appends pages to the write-ahead
log, each reader reads the database plus the log up to the point it
started (its snapshot), and a checkpoint copies the log back into the
database only as far as no reader still needs. Writers still take turns
on one lock.

This file is WAL mode without the checkpoint, because nothing is ever
copied back:

- **Readers** take no lock. A reader's snapshot is a footer: everything
  before it is complete and never changes. To see newer writes, a reader
  checks the file's length and reads the records past its footer into its
  index. A reader in another process does the same.
- **Writers take turns** on an exclusive lock on the file (`flock`, or a
  lock on a byte range past the end on platforms without it). A writer
  that takes the lock first reads any records other processes appended
  since its last turn, so its index is current, then appends its records
  and a footer, then releases the lock. Within a process the existing
  mutex orders writers; across processes the file lock does.
- **What can conflict is not here.** Blocks are content-addressed, so two
  writers storing the same block store the same bytes, and the second skips
  it. The conflict that matters is two writers moving the same branch head,
  and the memory provider settles that with its compare-and-swap, not the
  archive. If branch heads move into the file later, a head is a record
  written under the same lock, and its compare-and-swap is a check against
  the latest head record before appending.

A turn is short: a commit appends a few hundred KB. Turn-taking writers
therefore behave like concurrent ones in practice, as SQLite's do. What
the design gives up is two processes writing at the same instant, which
SQLite gives up too.

Not supported: a file on a network filesystem (locks and lengths there are
not reliable, the same caveat SQLite gives), and two writers on one file
from two machines.

## Where it lives

`rust/dialog-archive-file` (native first). It splits artifact keys into
entity, attribute and value and builds nodes from rows, so it depends on
`dialog-artifacts` and `dialog-search-tree`, not the other way round. The
node interface it needs is in `dialog-search-tree`.

## The file

An append-only sequence of records, each `kind (u8) | length (u32) |
crc32 | body`:

| record | body |
|---|---|
| block | a block's catalog and bytes: anything that is not a tree node, kept whole |
| token | one token's bytes; its id is its order among tokens |
| frame | the node's hash, kind, the node id it is written against (or none), and its sections as columnar rows |
| checkpoint | the index state as of this record |
| footer | the offset of the latest checkpoint; ends every append |

- **Node ids.** Every node gets a dense id when it lands. Frames name their
  children and their predecessor by node id; the index maps an id to its
  hash and its frame.
- **Rows.** As in the spike: per leaf entry, the form and the token ids of
  entity, attribute, value and datum; per index link, the separator, the
  child's node id and the scale; per buffered op, an entry with no datum
  for a retraction. Each column is zigzag-delta varints; against a
  predecessor a section is copy runs and literal rows.
- **Predecessor.** The latest node of the same kind that starts at the same
  first key. The store needs nothing from the tree to find it; on both
  datasets the frames come out the same size as with the tree's lineage.
- **Datum tokens** are aligned in the token region so an archived datum
  reads in place.

## Reads

Token bytes are read in place through a mapping of the file; the index
keeps where each token is, not its bytes. `Nodes::load_node(hash)`: the node cache, else the index, then decode the
frame chain (at most 32 frames) into rows and return a node over them.
`archive::Get(hash)`: the same rows, realized into the wire block and
verified against the hash. A frame is only written once its rows realize
to the exact block it stands for, so `Get` always returns the block that
was stored.

## Writes

`Put` and `Import` of tree nodes append a frame per node; anything else is
appended whole. A frame is checked by realizing it and comparing with the
block before it is appended; a node that does not round-trip (a legacy
layout, say) is appended whole. Durability matches the `fs` provider: no
fsync per append, and `sync` for a durable point.

## Index, checkpoints, recovery

In memory: block fingerprint to node id and record; token id to offset
(and token bytes to id, for the writer); the predecessor map. Every 16 MiB
of appends a checkpoint record and a footer; opening reads the checkpoint
the last footer names and the records after it; a file that does not end
in a footer is read from the start and its torn tail is dropped. (Built:
`rust/dialog-archive-file`.)

## Not in this design

- **Blobs** go through `blob::*`, a separate slot of a `Space`. They can be
  block records in the same file later; they never become frames.
- **Branch heads and credentials** stay in the memory and credential
  slots. A head record kind, under the writer lock, would make the file
  the whole space.
- **Finding old roots** is a change to `RevisionRecord` (each revision
  names its parents' roots), independent of this file.
- **Reclaiming space.** Nothing is reclaimed: history is the point. If a
  space ever needs to shrink, it is copied into a new file by a process
  holding the writer lock, and readers move to it, as SQLite's `VACUUM`.
- **The web.** OPFS gives one writer per file and synchronous access in a
  worker; it is later work.

## Build order

1. **Done** (`rust/dialog-archive-file`). The file and its records: append,
   checksum, footer, checkpoint, open and recover a torn tail.
2. **Done.** The archive effects over the file with every block whole,
   opened from a space's `Location`. A repository commits, reopens and
   queries through it (`tests/repository.rs`).
3. **Done.** Writers across processes: the writer lock, catching up on
   records other processes appended, and readers that follow the file's
   growth (`tests/repository.rs` runs two processes appending at once).
4. **A node over rows** in `dialog-search-tree`: the index and segment
   accessors as an interface with two backings, the archived block and
   rows over a token region, tested by reading every node of the spike's
   datasets both ways and comparing every accessor.
5. **Done, with reads realizing.** Frames: tokens, rows, predecessors and
   the checked conversion in the file; `Get` realizes. Until step 4, a
   tree read of a frame realizes too.
6. **The perf gate's scenarios** over it, against the `fs` archive.

## Measured, steps 1–3 and 5

`examples/file_archive_bench.rs` in dialog-baseline replays the Stack
Exchange log through a repository, one branch commit per transaction, into
three spaces that differ only in their archive. Then it reopens each and
reads, cold (an empty node cache, a warm OS page cache) and warm.

Every identity the run mints is derived from one seed: the profile and
space keys, and the line's identifier. These end up in the tree (a
revision's record is keyed by its branch and issuer), so with fresh keys
the same replay wrote 11,780 to 14,916 blocks from run to run. Seeded,
every store writes the same 14,900 blocks and ends on the same root, and
the bench refuses to report stores that disagree.

4096 commits, 9,915 facts:

| | `fs` | file, whole | file, frames |
|---|---|---|---|
| on disk | 1147 MiB, 14,900 files | 1173 MiB | **13.2 MiB** |
| commit | 5.8 ms | 6.5 ms | 7.2–8.9 ms |
| cold scan, 8 attributes | 39 ms | 30 ms | 36–47 ms |
| cold point reads, 256 | 10 ms | 7 ms | 18–25 ms |
| warm scan / points | 22 / 2 ms | 21 / 2 ms | 12–19 / 2–3 ms |
| archive index in memory | — | 0.8 MiB | 4.0–4.1 MiB, tokens mapped |
| reopen | — | 8 ms | 30 ms |

Repository-level times still move 10–25% between identical runs on a
shared container. The archive's own timers are firmer.

What it says:

- **Size holds up through a real repository**: 87× smaller than the `fs`
  archive. 14,900 frames take 2.7 MiB and 33k tokens 10.2 MiB, so the
  tokens are the data.
- **Writing a frame costs about 600 µs a node**, 2.3 ms a commit, which is
  the whole gap from the whole-block file:
  - about 340 µs splits the node into rows. Per node, on the kept file:
    101 µs decoding keys, 73 µs looking up tokens, 45 µs re-archiving each
    datum, 7 µs validating the block;
  - about 260 µs verifies, mostly rebuilding the block (176 µs);
  - the file write itself drops from 0.5 s (whole) to 0.1 s.
  Splitting keys without allocating changed nothing measurable. The cost
  is re-deriving what the tree had in hand when it encoded the node. The
  fix is the one this design names under Writes: the store receives the
  entries a node was encoded from. That removes the decode and, for nodes
  this process encoded, the rebuild that checks them.
- **A cold read pays rebuilding** (about 140–180 µs a node), so cold point
  reads are 2× slower than `fs`, and warm reads are the same: the node
  cache above the archive holds rebuilt nodes. Step 4 is what removes that
  cost, and the census bounds what it can buy: decoding rows costs about
  what reading a block file does, so cold reads reach parity with `fs`,
  not better. What step 4 buys beyond that is memory: a cached node over
  shared tokens instead of its block plus its decoded keys.
- **Tokens are read through a mapping of the file.** Holding them in
  memory made the index 23.4 MiB, mostly the 10.2 MiB of token bytes plus
  growth slack, so it would not scale past memory. Mapped, the index is
  where each token, node and block sits: 4.0 MiB for a reader, which opens
  from a checkpoint without reading a token. The table that finds a token
  by its bytes is built by a writer, on its first write. The pages a read
  touches belong to the OS page cache, which can drop them.
