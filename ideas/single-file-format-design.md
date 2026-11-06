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
4. **Done.** A node over rows in `dialog-search-tree`: `IndexView` and
   `SegmentView`, backed by a block or by a store's `NodeRows`, which a
   `NodeSource` the environment holds hands over before any block is
   loaded. Tested by reading every node both ways and comparing every
   accessor (`tests/repository.rs`; by hand, all 14,900 nodes of a
   4096-commit Stack Exchange file).
5. **Done.** Frames: tokens, rows, predecessors and the checked conversion
   in the file; `Get` realizes. The archive is a `NodeSource`.
6. **The perf gate's scenarios** over it, against the `fs` archive.

## Reading in place (step 4)

What the tree reads, and where from, when the archive serves a node:

- **Keys** are the tokens a row names, compared part by part where the
  file keeps them (`KeyParts::cmp_bytes`). A cursor that has to hand out a
  key as one slice writes it into one buffer it reuses, as the columnar
  segments already do; a binary search over a leaf (a point read, the
  entry point of a range) compares in place and writes nothing.
- **Values** are tokens too: each is a standalone archive of a
  `State<Datum>`, aligned to 16 in the file, read as its archived type
  where it sits. The archive checks each once, the first time a node that
  holds it is served.
- **Children** are named by hash, read from the child's frame header in
  the file, or from a hash token for a child the file holds no frame of.
- **Rows** are not read in place. A frame's ids are varint columns written
  against its predecessor, so serving a node decodes its rows once
  (through a cache of decoded rows) and records where each part is: about
  70 bytes an entry, the node's footprint in a node cache. Fixed-width rows
  would read in place but take several times the bytes; the decode is
  part of the 9–12 ms a cold read of 256 points takes in all.
- **Blocks** are built only on asking (`PersistentNode::buffer`): sync,
  push, and a load through the repository's own block path.

What it does not remove: the tree's scans yield owned values
(`into_owned` on yield), so a value read in place is deserialized when a
query takes it, as on main. Removing that is a change to the tree's query
API, not to its storage.

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
  - about 340 µs splits the node into rows. Per node, on the kept file
    (136 entries a node on average): 74 µs looking up tokens, 47 µs
    re-archiving each datum, 36 µs decoding keys, 12 µs splitting them, 8
    µs taking each value out of the block, 7 µs validating the block;
  - about 260 µs verifies, mostly rebuilding the block (166 µs);
  - the file write itself drops from 0.5 s (whole) to 0.1 s.
  No one step dominates: the cost is about 4 µs an entry, paid for every
  entry of every node written, though a commit changes a handful. Handing
  the store the decoded keys would save 36 µs of the 600. What would pay is
  not re-processing the entries a node shares with the one it supersedes,
  and that needs the tree to say which entries those are.
- **A cold read pays rebuilding** (about 140–180 µs a node), so cold point
  reads are 2× slower than `fs`, and warm reads are the same: the node
  cache above the archive holds rebuilt nodes. Step 4 removes that cost
  (below).
- **Tokens are read through a mapping of the file.** Holding them in
  memory made the index 23.4 MiB, mostly the 10.2 MiB of token bytes plus
  growth slack, so it would not scale past memory. Mapped, the index is
  where each token, node and block sits: 4.0 MiB for a reader, which opens
  from a checkpoint without reading a token. The table that finds a token
  by its bytes is built by a writer, on its first write. The pages a read
  touches belong to the OS page cache, which can drop them.

## Measured, step 4: nodes served in place, against main

The same bench, with a fourth store, `file/served`: the frames file, with
the environment holding it as its node source. `fs` is main's archive.
4096 commits, the same 14,900 blocks and root in every store:

| | `fs` (main) | file, served in place | file, rebuilding blocks |
|---|---|---|---|
| on disk | 1147 MiB | **13.5 MiB** | 13.5 MiB |
| cold scan, 8 attributes | 32–37 ms | **29–37 ms** | 51 ms |
| cold point reads, 256 | 11–12 ms | **9–12 ms** | 16 ms |
| warm scan / points | 14–19 / 2 ms | 15–16 / 2 ms | 19 / 2 ms |
| node cache after the scans | 56 nodes, 2.9 MiB | 56 nodes, **1.0 MiB** | — |
| heap the scans leave held | 6.2 MiB | **4.3 MiB** | — |
| heap the open takes | 7.8 MiB | 13.0 MiB | — |
| commit | 5.8–6.8 ms | 7.6–10.4 ms | 9.6 ms |

What it says:

- **Cold reads match main's.** Served in place, a cold read costs what
  reading a block file does, as the census predicted; rebuilding the block
  made it about 1.5× to 2× slower.
- **A cached node is a third of a cached block**: 18 KB against 53 KB, for
  the same 56 nodes. The rest of what reads leave held (about 3.3 MiB) is
  shared by both stores: other caches, not nodes.
- **The open costs 5 MiB more**: the archive's index (4 MiB), what main
  does not have since a directory is its index. That is where each token,
  node and block is; it grows with the file, not with what is read.
- **Commits still pay the frame's write cost**, about 2.3 ms more than a
  whole-block file. A commit that reads through served nodes skips the
  rebuild on its own reads, so it lands between the whole-block file and
  the rebuilding one; the spread between runs is as large as that gap.
- Warm point reads first came out at 5 ms: the walker streamed a served
  leaf from its first key, as it must for a block's columnar keys. It now
  enters a served leaf by a binary search in place (`keys_from`).

## Where the block index lives: layouts, replayed

The index maps every block's hash to where its record is, for every block
the file holds (tree nodes, versions no tree reaches any more, spilled
values, revision records). Two ideas carry over from Hyperbee whatever the
layout: the root is at the tail (the footer names it, so opening reads the
footer and nothing else), and a key can be a pointer to the record that
holds it. From Hypercore: a tree's roots merge like a binary counter's
carries, which is a schedule for merging sorted runs. From our own tree:
hitchhiker buffers, which turn many small writes into few large ones.
Every tree below names its children by position: the index is not content
addressed, so nothing hashes its nodes.

`single_file_census` (`DIALOG_CENSUS_INDEX`) replays a history one append
at a time into each layout, then, at sixteen points of the history, looks
up every 7th block written by then. It counts the 4 KiB pages the index
reads before the block's own record, cold (nothing cached) and warm (the
top cached, as it is once a reader has looked anything up: a tree's root,
every run's header and fences).

- tonk-team: the 5,925 blocks the modern-only file holds, 444 appends,
  about 2.6 KB of file per block;
- Stack Exchange: 4,096 commits, about 13–15k blocks (the replay mints
  fresh keys, so the count moves between runs), about 930 bytes per block.

| layout | tonk MiB | tonk warm (p99) | tonk cold | SE MiB | SE warm (p99) | SE cold | largest append (SE) |
|---|---|---|---|---|---|---|---|
| B-tree, 8 a node, keys as pointers, paths copied | 0.58 | 13.3 (17) | 15.6 | 2.13 | 13.2 (18) | 16.8 | 5.2 KB |
| B-tree, 8 a node, 8-byte prefixes, paths copied | 1.20 | 1.8 (3) | 3.5 | 4.33 | 1.6 (4) | 4.1 | 10.3 KB |
| B-tree, 32 a node, prefixes, paths copied | 1.74 | 1.2 (3) | 2.6 | 7.15 | 1.2 (3) | 3.0 | 15.6 KB |
| B-tree, 32 a node, against last version, whole every 8 | 0.50 | 2.6 (8) | 9.1 | 1.80 | 1.7 (5) | 10.5 | 4.5 KB |
| hitchhiker, 32 a node, buffers of 64, root whole | 0.70 | 0.9 (3) | 3.2 | 4.75 | 0.7 (2) | 3.4 | 6.4 KB |
| hitchhiker, 32 a node, buffers of 256, root against its last version, whole every 8 | 0.40 | 1.3 (3) | 7.3 | 2.38 | 1.1 (3) | 6.1 | 8.1 KB |
| **hitchhiker, 32 a node, buffers of 256, root against its last version, whole every 32** | **0.32** | **1.2 (3)** | 17.5 | **1.46** | **1.1 (2)** | 6.6 | **8.0 KB** |
| hitchhiker, 64 a node, buffers of 512, root against its last version, whole every 64 | 0.27 | 1.4 (4) | 29.3 | 1.11 | 2.0 (4) | 26.3 | 17.7 KB |
| sorted runs, merged 2 at a time | 0.73 | 1.0 (1) | 5.6 | 2.76 | 1.0 (1) | 3.8 | 443 KB |
| sorted runs, merged 4 at a time | 0.40 | 1.0 (1) | 7.6 | 1.51 | 1.0 (1) | 4.9 | 288 KB |
| sorted runs, merged 8 at a time | 0.26 | 1.0 (1) | 11.4 | 1.10 | 1.0 (1) | 7.3 | 243 KB |

The data the index points at is about 5.7 MiB (tonk-team's tree) and
13 MiB (Stack Exchange).

- Copying whole paths is cheap to read and dear to write: every append
  rewrites a path per new block, and Stack Exchange's many small appends
  make that a third of the data or more.
- Keys as pointers halve that, and cost a page per comparison.
- Writing every node against its last version is small, but a cold
  lookup reads every version back to the last whole one.
- Sorted runs read one page warm, but an append that completes a carry
  rewrites everything below it (288 KB on Stack Exchange at base 4), a
  spike that grows with the index.
- A hitchhiker tree keeps the writes small without the spike: an append
  rewrites the root (its buffer gains the append's entries) and only now
  and then the nodes a full buffer flushes into. The root is the one node
  every lookup reads, so it is the one that can be written against its
  last version with a long chain: warm, a lookup reads about one page; cold,
  the first lookup also reads the root's chain. At 32 a node, buffers of
  256 and the root whole every 32 versions, it is the smallest layout
  without a spike, 6% (tonk) and 11% (Stack Exchange) of the data.
