# Single-file format: spike results

The design these results led to is [single-file-format-design.md](single-file-format-design.md).

Spike for [single-file-format.md](single-file-format.md). It measures the
idea's size claim, and what a read pays for it, before any provider
exists. It does not build the provider, so it has no perf gate numbers.

## Verdict

**With our own encoding: size yes, growth yes, reads only if the tree reads
rows.** Two encodings were measured. The idea as written (a frame of 4-byte
token offsets per node) is about 20× smaller than file-per-block but still
copies every rewritten node forward, so it grows with commits. A columnar
frame written against the version it supersedes (runs copied from the
predecessor's rows, the rest as zigzag-delta varint columns, a whole frame
every 32 versions) removes that copy: on Stack Exchange it is 87× smaller
than file-per-block and grows with the live tree, and on tonk-team's pushed
history it is 22× smaller.

Reading is where it stops being free. Decoding a node's rows costs 5–27 µs,
about a file read (15 µs, 24 µs with blake3). Rebuilding the block from
them with the tree's encoder costs 150–225 µs, and that is the tree's
encoder, not the format. So the format reads fast only if the tree can work
on rows, keeping blocks for sync and verification.

Nodes in the tree's legacy layout (36% of tonk-team's pushed history) do
not rebuild with today's encoder. That layout predates the current tree
encoding and is out of scope for this format.

Against the success bars:

| bar | offsets (the idea as written) | columnar/32 (our encoding) |
|---|---|---|
| ≥ 10× smaller than file-per-block | **met on SE** (20×); 9.9× on tonk-team's history, just under | **met**: 87× on SE, 22× on tonk-team's history |
| grows with live facts, not commits | **not met**: 2.76× for 2× commits on SE, live tree 2.14× | **met on SE**: 2.27× for 2× commits, live tree 2.14× |
| get by hash still works and verifies | **met**: every node in the current layout rebuilds byte for byte (legacy-layout nodes are out of scope) | same |
| reads near the block store | **not met**: realizing costs 130–175 µs, against 14 µs to read a file | **met for rows** (5–27 µs), **not met for blocks** (150–225 µs) |
| writes | not timed | encode + append 10–30 µs per node; splitting a node into rows 200 µs; a file per block 290–460 µs, all without fsync |
| perf gate within tolerance | **not measured**: there is no provider yet | same |
| crash leaves the last commit intact | **not measured** | same |

zstd models stay in the tables as a reference for how much redundancy there
is. They say nothing about what our own encoding costs, and our encoding
matches or beats them: 13.4 MiB against 13.9 for zstd-delta frames on SE,
25.4 against 25.2 on tonk-team.

## Our own encoding: columnar frames written against their predecessor

A frame is a node's rows: per leaf entry its form, the token ids of its
entity, attribute and value, and the id of its datum; per index link its
separator id, the child frame's position in the file and its scale;
per buffered op the same as an entry, with no datum for a retraction.
Token ids are dense, in order of first appearance. Rows are written column
by column, each id as a zigzag delta against the same column's previous
row, so a sorted or repeating column costs about a byte a row.

Against the frame it supersedes (same depth, same left separator), each
section is written as ops: copy a run of the predecessor's rows, or take
the next rows from this frame's columns. A rewrite that changes one entry
of a segment is two copies around one literal row. Every `K`th version is
written whole so a read decodes at most `K` frames. `K = 1` is the columnar
packing alone.

A store does not need the tree's lineage to find a frame's predecessor.
The census finds it by depth and left separator from every root; a store
that only sees the blocks it is given can take the latest frame of the
same kind that starts at the same key. The frames come out the same size
(1.0 MiB on SE at 2048 commits, 2.5 MiB on tonk-team's pushed history), and
the two agree on 99% and 96% of the predecessors lineage names.

Stack Exchange, 4096 commits, MiB on disk, cumulative:

| commits | live tree | fs | offsets (idea) | zstd-delta frames/32 | **columnar/1** | **columnar/8** | **columnar/32** |
|---|---|---|---|---|---|---|---|
| 512 | 2.3 | 53.5 | 3.5 | 1.4 | 2.1 | 1.4 | **1.4** |
| 1024 | 4.8 | 160.4 | 9.2 | 3.0 | 5.2 | 3.2 | **2.9** |
| 2048 | 9.2 | 418.7 | 21.7 | 6.1 | 12.0 | 6.5 | **5.9** |
| 3072 | 14.2 | 762.1 | 39.2 | 9.7 | 21.4 | 10.5 | **9.3** |
| 4096 | 19.7 | 1160.7 | 59.1 | 13.9 | 32.5 | 15.2 | **13.4** |

At 4096 commits the 13.4 MiB is 10.6 MiB of tokens and dictionary, 2.5 MiB
of frames and 0.2 MiB of frame index. The frames that were 48 MiB as
offsets are 2.5 MiB: 14,159 of 15,031 are written against a predecessor.
What is left is the tokens, which track the live facts. Per commit between
2048 and 4096: 3.7 KB, against 5.2 KB of live tree growth.

Reads, mean µs per live node at 4096 commits (371 nodes, 19.7 MiB, warm,
noisy to about ±20%):

| | decode rows | realize the block | realize + blake3 |
|---|---|---|---|
| fs file read | | 14.7 | 23.7 |
| offsets (idea) | | 173 | 183 |
| columnar/1 | 5.3 | 178 | 188 |
| columnar/8 | 14.0 | 205 | 237 |
| columnar/32 | 27.0 | 225 | 262 |

Decoding rows is the format's own cost: a copy chain of up to `K` frames,
each a few µs. Realizing is the tree's encoder building an 80 KB node from
those rows, the same in every model. A reader that wants rows (a scan, a
lookup inside a node) pays a file read or less. A reader that wants the
block (sync, get by hash) pays the encoder.

Writes, mean µs per node, no fsync anywhere:

| | µs |
|---|---|
| fs: a file per block | 457 |
| log: append the block | 271 |
| split a node into rows (shared by every K) | 210 |
| columnar/1: encode + append | 29.8 |
| columnar/8: encode + append | 15.0 |
| columnar/32: encode + append | 10.4 |

Splitting into rows is measured from the block, which a writer would not
start from: the tree has the entries in hand. Most of it is serializing
each datum and looking its bytes up in the token dictionary, which a writer
pays either way. The fs and log timings are page-cache writes, and they
vary several-fold between runs (290 and 37 µs on the tonk-team run); they
show order of magnitude, not a gate.

## tonk-team's pushed history

The R2 archive behind tonk-team holds 9,070 blocks, 568.5 MiB: 8,768 tree
nodes under 770 roots, and 302 other blocks. `fetch_blocks` in tonk
fetched all of them through a joined agent. Their file times are fetch
times, so the roots are ordered by the number of history records under
each (the history region only grows; `DIALOG_CENSUS_ORDER=history`). The
302 other blocks land after the last root, so the last row includes their
8.9 MiB.

| commits | live tree | fs | offsets (idea) | zstd-delta frames/32 | **columnar/1** | **columnar/8** | **columnar/32** | stream |
|---|---|---|---|---|---|---|---|---|
| 192 | 9.9 | 80.7 | 9.6 | 4.9 | 6.5 | 5.0 | **4.9** | 1.7 |
| 385 | 18.3 | 221.2 | 23.1 | 10.3 | 14.9 | 10.8 | **10.5** | 4.2 |
| 577 | 24.0 | 367.4 | 34.9 | 14.0 | 21.9 | 14.9 | **14.3** | 6.2 |
| 770 | 27.9 | 568.5 | 57.3 | 25.2 | 37.6 | 26.5 | **25.4** | 10.8 |

- **Size.** 22× smaller than fs. Of the 25.4 MiB, the tokens are 13.8 MiB,
  and 9.5 MiB of those are opaque history and coverage keys referenced
  3.7 times each; frames are 2.5 MiB; the other blocks 8.9 MiB. The
  history keys are now the largest part, and splitting them into
  components is the next lever.
- **Reads.** Decoding rows 5.0–14.2 µs, against 13.6 µs for a file read;
  realizing 150–165 µs.
- **Old blocks do not round-trip.** 3,145 of the 8,768 nodes, holding 180
  MiB, realize to bytes that differ from their block: segments 40 bytes
  shorter, index nodes 16. They are one era, roots 31 to 356 of 770; the
  head tree's 685 nodes all round-trip. They are exactly the nodes in the
  tree's legacy layout (`LEGACY_LAYOUT`), which the tree still reads; a
  frame rebuilt with today's encoder comes out in the tagged layout, with a
  different hash. The legacy layout predates the current tree encoding,
  so the format does not need to carry it.
- **The 2 GB.** R2 holds 568.5 MiB for this space, and some blocks in a
  local replica are not on R2 at all (4 of 8 sampled). The 2 GB replica is
  a device's, and it holds blocks that never reached R2.

## tonk-team, replayed

The same census on tonk-team, rebuilt from its head tree's history region
(see "Running it on a real replica" below) and replayed through today's
code: 1457 revisions from 108 origins, 44,362 claims. The replayed live
tree is 24.7 MiB; the real head on disk is 30 MB, so the reconstruction is
close. Its 653 MB of blobs (uploads and spilled values) stay outside every
model, as they do in both stores.

MiB on disk, cumulative:

| commits | live tree | fs | log+zstd | log+delta/32 | **tokens+frames** | tokens + zstd frames | tokens + delta frames/32 | stream |
|---|---|---|---|---|---|---|---|---|
| 364 | 9.6 | 88.4 | 18.8 | 4.4 | **9.6** | 5.5 | 4.0 | 1.5 |
| 728 | 18.4 | 221.5 | 46.0 | 10.4 | **23.4** | 13.8 | 9.8 | 3.6 |
| 1092 | 20.4 | 316.7 | 65.2 | 13.0 | **30.2** | 17.2 | 11.3 | 4.6 |
| 1457 | 24.7 | 493.3 | 104.1 | 18.4 | **42.6** | 23.3 | 13.8 | 6.9 |

Per commit over the second half, in KB: fs 373, tokens+frames 27 (frames
21.6), tokens + delta frames/32 5.6, live tree growth 8.9.

- **Size: only just met.** 11.6× smaller than fs, against 20× on Stack
  Exchange. Of its 11.5 MiB of tokens, 7.7 MiB are opaque keys referenced
  about 3 times each: the history and coverage regions, whose keys are
  unique per record and which the idea's tokenizer treats as one token.
  tonk writes a lot of history per fact; a tokenizer that split those keys
  (origin, edition, entity, attribute, value) would share more of them.
- **Growth: not met, same as Stack Exchange.** Doubling the commits from
  728 to 1457 grew the live tree 1.34× and the tokens 1.34×, but the
  frames 2.09× and fs 2.23×.
- **Where the bytes come from:** segments again, 436 of 493 MiB (88%).
- **Reads:** realizing a node costs 75–89 µs, against 9 µs to read its
  file (16 µs with blake3).
- **Per commit, today's code writes about 340 KB** on this history, not
  the 2 MB the idea cites for the 2 GB replica. Either that replica's
  commits were larger, or the tree has improved since it was written. It
  is worth knowing which before using 2 GB as the case for the format.

Caveats: 108 writers' revisions replay as one line in edition order, not
as the merges they were; 6,653 claims in the reserved `dialog.` namespace
(delegations and the like, which dialog writes itself) replay under a
namespace of the same length.

## How it was measured

`rust/dialog-baseline/examples/single_file_census.rs` replays the real
`retrocomputing` log (`scripts/se-transform.py` on the April 2024 dump)
through an in-memory branch. It keeps every distinct block the archive
is asked to store, in write order, and models each store on the same
bytes:

- **fs**: one file per block, as the `fs` provider writes them; also
  rounded up to 4 KiB.
- **log**: every block appended to one file, plus a 40-byte get-by-hash
  entry. This is what DCAA was.
- **log+zstd**: each block compressed on its own.
- **log+delta/K**: each node compressed with zstd against the node it
  supersedes (same depth, same left separator), with a keyframe every `K`
  versions.
- **tokens+frames**: the idea. Entity, attribute, value (its type byte
  and payload together) and the rkyv datum are each a token written
  once. An entry is a form byte and four 4-byte offsets. An index frame
  points at its children's frames by offset and carries its own hash.
  The total includes the token dictionary (12 bytes per token) and the
  get-by-hash index (16 bytes per frame). Every frame is decoded and
  re-encoded with the tree's own encoder, and the census counts how many
  match the block they stand for.
- **tokens + zstd frames / delta frames/K**: the idea with frames
  compressed alone, or against the frame they supersede.
- **stream**: the whole log through one long-window zstd stream. There
  is no random access, so this is a reference, not a design. Its 128 MiB
  window stops it being a true floor past about 2000 commits.
- **columnar/K**: our own encoding, described above. Every columnar frame
  is decoded through its copy chain and realized like the offset frames,
  and the census counts how many rebuild their block byte for byte.

```sh
DIALOG_SE_CSV=retro-facts.csv cargo run --release -p dialog-baseline \
  --example single_file_census -- 4096 512,1024,2048,3072,4096
```

The block count varies by about 2% between runs; the versions a commit
stamps carry wall-clock time.

## Size

MiB on disk, cumulative:

| commits | live tree | fs | log | log+zstd | log+delta/32 | **tokens+frames** | tokens + zstd frames | tokens + delta frames/32 | stream |
|---|---|---|---|---|---|---|---|---|---|
| 512 | 2.3 | 54.6 | 54.6 | 12.4 | 2.6 | **3.5** | 2.3 | 1.4 | 1.0 |
| 1024 | 4.8 | 159.2 | 159.3 | 37.9 | 5.7 | **9.0** | 5.7 | 3.1 | 2.4 |
| 2048 | 9.2 | 414.1 | 414.3 | 97.7 | 12.4 | **21.3** | 13.6 | 6.1 | 5.4 |
| 3072 | 14.2 | 767.8 | 768.1 | 179.1 | 20.7 | **38.8** | 24.8 | 9.7 | 9.5 |
| 4096 | 19.7 | 1176.0 | 1176.5 | 275.3 | 29.7 | **58.7** | 38.0 | 13.9 | 14.3 |

Rounding to 4 KiB adds 2.5% to fs (blocks average 80 KB), so filesystem
overhead is not where the 9× comes from. Putting the blocks in one file
saves nothing, as DCAA found.

Where the idea's file goes at 4096 commits: 10.6 MiB of tokens plus
dictionary, 47.8 MiB of frames. Per commit, between 2048 and 4096:

| | KB per commit |
|---|---|
| live tree growth | 5.2 |
| fs | 381 |
| tokens + frames | 18.7 (tokens 2.9, frames 15.7) |
| tokens + delta frames/32 | 3.9 |

Tokens track the live facts: they come to about half the live tree, and
every token is referenced hundreds of times (a datum 442 times, a value
257, an entity 358). The idea gets the token sharing it promised. What it
does not remove is the frame copied forward on every rewrite: a segment
frame is about 190 entries × 17 bytes, rewritten whole for a change to
one entry.

## Where the bytes come from

Segments, not roots. Of 1176 MiB written by 4096 commits, leaf segments
are 894 MiB (10,684 writes, about 86 KB each) and index nodes 282 MiB
(4,095 writes). That is 2.6 leaf rewrites per commit, for commits that
average 2.4 facts (each fact writes four entries: EAV, AEV, VAE and
history). The doc's picture of the root frame re-encode as the source
holds for index nodes, but they are a quarter of the bytes on this
workload.

The doc also gives the live tree at 2048 commits as about 49 MB and the
store as about 9× that. This run measures the store at the doc's number
(414 to 423 MiB), but the live tree, the nodes reachable from the head
root, at 9.2 MiB: a retained-to-live ratio of 45×, not 9×. Either the
49 MB counted something else (spilled values live in the blob store,
not the archive), or the tree has changed since. It is worth settling,
because the ratio is the headline of the motivation.

## Reads

Mean time to get each of the 375 live nodes at 4096 commits (19.7 MiB),
warm, on this 4-core container. The timings are noisy to about ±20%: the
realize row without blake3 came out slower than the row with it.

| | µs per node |
|---|---|
| fs file read | 10.6 |
| fs file read + blake3 | 23.8 |
| log+zstd | 51.6 |
| log+delta/8 | 221 |
| log+delta/32 | 472 |
| log+delta/inf | 1133 |
| tokens: realize | 124 |
| tokens: realize + blake3 | 103 |

Realizing is unoptimized: it deserializes each datum and allocates each
key. But it rebuilds and re-encodes an 80 KB node on every read the node
cache misses, which no tuning makes free. Delta chains are worse, and an
unbounded chain is not viable. Delta-coded frames (the best size) would
pay a short chain decode and then the realize.

The question "must a key comparison not cost a hop" did not need the
file at all on this workload: the whole token region is 10.6 MiB at 4096
commits and would sit in memory.

## What this says about the idea

- The duplication the idea targets is real but secondary. Tokens shared
  across indexes and versions collapse to about half the live tree.
  Most of the redundancy is version-to-version: a node rewritten with
  one entry changed. Tokenizing shrinks each copy about 20× and still
  writes the copy.
- If old revisions must stay cheap, the format has to share structure
  between versions of a node, HyperDB style, not only tokens. On this
  data that is the difference between 15.7 and about 1 KB of frames per
  commit. The columnar model above does it: a frame copies its
  predecessor's runs of rows, and frames fall from 48 to 2.5 MiB.
- The store can only compress what the tree insists on writing. At 381
  KB written per 2.4-fact commit, the cheapest question may be on the
  tree's side: smaller leaves, or keeping more in the buffers, cut fs
  bytes without a new provider and cut every encoding above with them.
  `node_size_sweep` and `live_census` are the starting points.

## Memory while querying

`rust/dialog-baseline/examples/memory_census.rs` reads a replica's head
through the paths a query uses (`scan`, `scan_owned`) under a counting
allocator: every fact once through the attribute index, then every fact
materialized. tonk-team's head (20,247 facts) and the 4096-commit Stack
Exchange replay written to disk (12,294 facts):

| | tonk-team | SE |
|---|---|---|
| node cache: blocks it holds | 5.6 MiB (135 nodes) | 6.4 MiB (124 nodes) |
| node cache: what the nodes derive, uncounted by its budget | 5.6 MiB | 7.5 MiB |
| result of every fact | 25.7 MiB, 1,331 B a row | 18.7 MiB, 1,593 B a row |
| · of which entity, attribute and value bytes | 11.8 MiB | 5.3 MiB |
| the same result as shared tokens and rows of ids | 10.6 MiB (2.4× less) | 5.1 MiB (3.6× less) |

Per row of the result:

| | tonk-team | SE |
|---|---|---|
| attribute | 137 B, for about 20 B of name | 128 B |
| entity | 157 B, for about 45 B of URI | 130 B |
| value | 666 B, for about 540 B of payload | 631 B |
| cause | 53 B | 43 B |
| the row itself | 96 B | 96 B |

- **The duplication is in decoding, not storage.** Every decoded row gets
  its own `Arc` for its attribute and its entity, though tonk-team has 549
  distinct attributes (37 rows each) and SE 9 (1,366 rows each). An
  attribute costs a row about 130 bytes where an id would cost 4.
- **Values are mostly distinct** on both datasets (2.1 and 1.6 rows per
  value) and are most of the bytes, so sharing values saves less than
  sharing entities and attributes.
- **A warm node costs about twice its block.** Nodes memoize their decoded
  keys, key hashes and summary, which the cache does not charge to its
  budget. The decoded keys repeat entity, attribute and value bytes for
  every entry, in each of the three indexes.

What the format changes and what it does not: tokens with ids make
interning free (an id indexes a table of shared values), and a node's
decoded keys could be rows of ids into the token region instead of key
bytes. But neither needs the file. An interner on the decode path, keyed by
the entity's and attribute's key bytes, removes most of the per-row
overhead today, with any archive.

## Running it on a real replica

The census also reads a replica's file-per-block archive directly, every
block it ever stored:

```sh
cargo run --release -p dialog-baseline --example single_file_census -- \
  archive path/to/replica/archive
```

Nothing records which commit wrote a block, so the order is rebuilt: a
commit's root is a tree node no other node links to, roots go in order of
their file times, and each root's new nodes are appended children first.
Checked against the replay: a 1024-commit replay written to disk
(`single_file_census disk 1024`) scans as exactly 1024 roots, and every
model agrees with the in-memory replay within run-to-run noise (fs 160.4
vs 160.3 MiB, tokens+frames 9.1 vs 9.0 MiB). A replica that pulled its
history from a peer wrote those files when it pulled, so its order, and
with it the delta models, is only as good as its file times.

The census can also rebuild a replica's commits when its archive holds
only the head tree, as a replica that pulled its history does:

```sh
cargo run --release -p dialog-baseline --example single_file_census -- \
  history path/to/replica
```

`archive` mode also reads a partial replica, one that fetched nodes lazily
and holds only what it read: a link to a child it does not hold carries
the child's hash in the frame instead of an offset, and the live tree is
what of it the replica holds.

`path/to/replica` holds `archive/` and `blob/`. The head tree's history
region keeps one record per claim every revision made, under that
revision's version, so grouping records by version and replaying the
groups in edition order reproduces the commit sequence through today's
code. Concurrent writers' revisions replay as one line, not as merges.
Checked the same way: the 1024-commit replay on disk rebuilds as exactly
1024 revisions and 2590 claims, and replays to 159.5 MiB written against
160.3 MiB for the original.

## Not done

- **The 2 GB replica itself.** tonk-team's pushed history is measured
  above, and its replay through today's code. The replica that reached
  2 GB holds blocks that never reached R2, and only that device has them.
- **A tree that reads rows.** The reads above say the format is only as
  fast as a file read if the tree works on decoded rows rather than
  realized blocks. Whether the tree can, and what it would keep as blocks
  (for sync and get by hash), is the design question this spike leaves.
- **Split history keys.** On tonk-team they are the largest part of the
  file.
- A provider, the perf gate, crash recovery, BAO verification, memory
  cells. None of them is worth building until the frame representation
  is settled, because each one depends on it.
