# Single-file format: spike results

Spike for [single-file-format.md](single-file-format.md). It measures the
idea's size claim, and what a read pays for it, before any provider
exists. It does not build the provider, so it has no perf gate numbers.

## Verdict

**Size: yes, barely on tonk. Growth: no. Reads: a real cost.** A
tokenized file comes out about 20× smaller than file-per-block on the
Stack Exchange replay and 11.6× on tonk-team's history, which clears the
order-of-magnitude bar, the second only just. But the frames, not the tokens,
are 80% of that file, and they grow with commits the way the directory
does. The 20× is a constant factor on the same growth curve, not a curve
that follows the live facts. What follows the live facts is not copying
a node forward at all: delta-coding each frame against the version it
supersedes. The doc treats that as a second tier. On this data it is
where most of the win is.

Against the success bars:

| bar | result |
|---|---|
| ≥ 10× smaller than file-per-block | **met**: 19× at 2048 commits, 20× at 4096; 11.6× on tonk-team |
| grows with live facts, not commits | **not met**: 2.76× for 2× commits; the live tree grew 2.14× and the directory 2.84× |
| get by hash still works and verifies | **met in the model**: all 14,779 frames realize to their original block byte for byte, so their hashes match |
| reads near the block store | **not met as built**: realizing a node costs 100–125 µs, against 11 µs to read its file (24 µs with blake3) |
| perf gate within tolerance | **not measured**: there is no provider yet |
| crash leaves the last commit intact | **not measured** |

## tonk-team

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
  re-encoded with the tree's own encoder, and the run fails unless it
  matches the block it stands for.
- **tokens + zstd frames / delta frames/K**: the idea with frames
  compressed alone, or against the frame they supersede.
- **stream**: the whole log through one long-window zstd stream. There
  is no random access, so this is a reference, not a design. Its 128 MiB
  window stops it being a true floor past about 2000 commits.

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
  commit. A frame that refers to its predecessor's runs of entries,
  rather than a zstd delta, would get most of it with a bounded number
  of hops. It is the obvious next model to try.
- The store can only compress what the tree insists on writing. At 381
  KB written per 2.4-fact commit, the cheapest question may be on the
  tree's side: smaller leaves, or keeping more in the buffers, cut fs
  bytes without a new provider and cut every encoding above with them.
  `node_size_sweep` and `live_census` are the starting points.

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

`path/to/replica` holds `archive/` and `blob/`. The head tree's history
region keeps one record per claim every revision made, under that
revision's version, so grouping records by version and replaying the
groups in edition order reproduces the commit sequence through today's
code. Concurrent writers' revisions replay as one line, not as merges.
Checked the same way: the 1024-commit replay on disk rebuilds as exactly
1024 revisions and 2590 claims, and replays to 159.5 MiB written against
160.3 MiB for the original.

## Not done

- **The 2 GB replica itself.** tonk-team's history is measured above,
  replayed through today's code. The replica that reached 2 GB held
  blocks written by older code, and only that device has them.
- A provider, the perf gate, crash recovery, BAO verification, memory
  cells. None of them is worth building until the frame representation
  is settled, because each one depends on it.
