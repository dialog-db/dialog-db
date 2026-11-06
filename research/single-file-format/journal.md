# Single-file format: journal

What we tried, what it measured, and what we decided, newest last. The
[idea](idea.md) says why; the [spike](spike.md) holds the first round of
measurements in full; the [design](design.md) is the current decision.
Work happens on the `worker/pensive-edison-fu3bx5` branch, PR
[#733](https://github.com/dialog-db/dialog-db/pull/733).

## Where things stand

**Status: Building.** See the [research index](../README.md) for what the
statuses mean.

- **Built:** `dialog-archive-file`, a space's blocks in one append-only
  file, tree nodes as frames over shared tokens, read by the tree in place
  through a mapping of the file; writers take turns across processes.
  `dialog-migration` moves a replica the `fs` provider wrote into it.
- **Size:** tonk-team's 5,623 modern tree nodes take 5.7 MiB, against
  379.4 MiB as a file per block (66×). Stack Exchange at 4096 commits takes
  13.2 MiB against 1147 MiB (the history-key split below has not been
  re-measured there yet).
- **Speed, against main's `fs` archive:** cold reads level, warm reads the
  same, cached nodes a third of the memory; commits about 2.3 ms slower.
- **Next:** the [evaluation](evaluation.md) that decides the block index's
  layout on queries, writes, sync, memory and storage; a hitchhiker tree
  leads on size.
- **Open:** the writer's token dictionary (in memory today, as a cache);
  skipping unchanged entries on write; values yielded in place by the
  tree's scans; Windows; the web (OPFS); verified range reads.

## Datasets

- **Stack Exchange:** the `retrocomputing` dump transformed into facts,
  replayed one branch commit per transaction (`DIALOG_SE_CSV`). Up to 4096
  commits, 9,915 facts, about 14,900 blocks.
- **tonk-team:** the team space's pushed history, all 9,070 blocks fetched
  from R2 (568.5 MiB; R2's own listing matches the fetched blocks block for
  block). 770 roots, ordered by the history records they hold. 3,145 nodes
  are in the old layout and do not rebuild with today's encoder: they are
  kept whole, and left out of "modern" figures.

## 2026-11-04 — The spike: is one file of tokens and frames worth it?

**Question.** Would one append-only file, each distinct token once and
each node a frame of references, beat a file per block on size, without
losing reads?

**Method.** `single_file_census` replays Stack Exchange and models stores
over the blocks a repository writes: the file per block, an append log,
zstd, zstd against the previous version, and frames of token offsets.

**Results.** Frames of 4-byte token offsets: 20× smaller than a file per
block on Stack Exchange, 9.9× on tonk-team, but every rewritten node is
copied forward, so the file grows with commits. zstd against the previous
version reached 13.9 MiB on Stack Exchange.

**Decision.** Worth pursuing, with an encoding of our own: zstd says how
much redundancy there is, not how to read it.

Commits: `d792cc7`, `b2eccd9`, `bd26f78`.

## 2026-11-05 — Columnar frames written against their predecessor

**Question.** Can our own encoding match zstd and still read fast?

**Method.** A frame is rows of dense token ids, each column zigzag-delta
varints; each section is written against the node it supersedes, as runs
copied from its rows around the rows that are new, and whole every K
versions. The predecessor is the latest node of the same kind starting at
the same first key, which a store can find without the tree's help (it
agrees with the tree's own lineage 96–99% of the time).

**Results.**

| | Stack Exchange, 4096 commits | tonk-team, 770 roots |
|---|---|---|
| file per block | 1160.7 MiB | 568.5 MiB |
| offsets (the idea as written) | 59.1 | 57.3 |
| zstd against the previous version | 13.9 | 25.2 |
| columnar, whole every 32 | **13.4 (87×)** | **25.4 (22×)** |

Decoding a node's rows: 5–27 µs, about a file read. Rebuilding its block
from rows: 150–225 µs, which is the tree's encoder, not the format.

**Decision.** Columnar frames, whole every 32. The 3,145 tonk-team nodes
that do not rebuild are the old layout and out of scope.

Commits: `37508df`, `eed8061`, `6d159cf`, `b7011ac`, `27a6220`.

## 2026-11-05 — Memory while querying

**Question.** Facts repeat entities and values everywhere; what does that
cost in memory at query time?

**Method.** `memory_census`, with a counting allocator: the node cache
after a scan, the rows a scan returns, and the same data as a token model.

**Results.** A warm node holds about twice its block (its decoded keys are
kept beside it). Result rows cost about 130 bytes per attribute and per
entity, from a fresh `Arc` per decode. A token model is 2.4–3.6× smaller.

**Decision.** The file should serve nodes, not blocks, to local reads:
the wire form exists to be hashed and sent.

Commits: `674e2f7`, `03aed53`.

## 2026-11-05 — The archive, whole blocks first

**Built.** `dialog-archive-file`: records (`kind | length | crc32 | body`),
a footer ending every append, checkpoints, recovery of a torn tail, the
archive effects (`Get`, `Put`, `Import`), opened from a space's `Location`.
A repository commits, reopens and selects through it.

Commit: `ccee000`.

## 2026-11-05 — Design: nodes to local reads, blocks to sync; writers take turns

**Question.** How does a single file serve both, and how do several
processes share it?

**Decision.** Local reads get nodes read from rows; sync gets blocks
realized on asking. Concurrency is SQLite's write-ahead log without the
checkpoint copy: writers take turns on an exclusive file lock and first
read what others appended; readers take no lock, since everything up to a
footer never changes.

Commit: `ad60ac1`.

## 2026-11-05 — Frames in the archive, and a bench against main

**Built.** Tree nodes kept as frames, written only once they read back and
rebuild their exact block; tokens through a mapping of the file; writers
across processes (a test runs two processes appending at once, and fails
with the catch-up turned off).

**Method.** `file_archive_bench` replays Stack Exchange through a
repository into spaces that differ only in their archive, reopens each and
reads cold, then warm.

**Results (4096 commits).** 13.2 MiB against 1147 MiB. Rebuilding a block
on every cold read made cold point reads about 2× slower than main. A
frame costs about 600 µs to write, about 2.3 ms a commit. Holding token
bytes in memory made the index 23.4 MiB; mapped, 4.0 MiB.

**Found along the way.** The same replay wrote 11,780 to 14,916 blocks from
run to run: a replica's keys reach its tree. Seeding every identity made
every store write the same 14,900 blocks and end on the same root.

Commits: `cfef28c`, `3077330`, `f6f344a`, `c1ae556`.

## 2026-11-05 — Reading nodes in place

**Question.** Can the tree read a node from the file without its block?

**Built.** The tree reads a node through `IndexView` and `SegmentView`,
backed by a block or by a store's rows (`NodeRows`); an environment that
holds a `NodeSource` is asked before any block is loaded. The archive
serves nodes whose keys compare part by part and whose values are read as
archives where they sit (tokens are aligned to 16).

**Results, against main.** Cold reads level (scan 29–37 ms against
32–37, points 9–12 ms against 11–12); a cached node 18 KB against 53 KB.
All 14,900 Stack Exchange nodes read the same both ways, accessor by
accessor.

**Not removed.** The tree's scans yield owned values, so a value read in
place is deserialized when a query takes it, as on main.

Commits: `b76b351`, `6af4544`, `2db465b`.

## 2026-11-05 — What the in-memory index holds

**Results (Stack Exchange, 4096 commits).** About 4.8 MiB, 35% of the
file: every block by catalog and hash (1.6 MiB), the node table (1.2 MiB),
node ids by hash (1.0 MiB), the token table (1.1 MiB). About 250 bytes a
node, while a node's frame averages 190.

Naming tokens and children by file position instead of id would drop the
node table and the token table, for frames about 26% larger.

Commit: `9e90672`.

## 2026-11-05 — tonk-team in the file, and an index estimate

**Results.** The modern blocks: 24.2 MiB in the file (of which 8.9 MiB are
blocks that are not tree nodes, kept whole) against 388.3 MiB as files.
The 5,623 modern nodes alone: 15.3 MiB, of which 12.3 MiB were tokens.

Commit: `32ffd4d`.

## 2026-11-05 — Migrations

**Built.** `dialog-migration`, a crate of migrations, each behind a
feature, run by an application before it opens a replica (`Migrations`,
`Step`) or by `dialog-migrate`. Its `archive-file` step moves every space
of a replica into the file, writing it under another name and renaming it
once complete, so an interrupted run resumes. tonk-team moves in 5.4 s.

Commits: `4333312`, `93d6a2c`, `9d508f8`.

## 2026-11-06 — Splitting history keys

**Question.** Tokens were 80% of tonk-team's nodes. Why?

**Finding.** History and coverage keys have no column schema in the tree
(`Schema::opaque`), so the archive kept each whole, as one token of its
own: none was ever shared.

**Built.** The archive splits them into version, entity, attribute and
value tokens (`split_record_components`), so a record shares its fact's
tokens and a revision's records share one version. The tree's own leaf
encoding is untouched.

**Results (tonk-team, modern nodes).**

| | before | split |
|---|---|---|
| tokens | 80,014, 12.29 MiB | 29,322, 3.48 MiB |
| frames | 1.66 MiB | 1.94 MiB |
| total | 15.3 MiB | **5.7 MiB (66×)** |
| index loaded at open | 3.4 MiB | 1.8 MiB |

All 5,623 nodes still read the same both ways and rebuild their blocks.

Commit: `3ceb925`.

## 2026-11-06 — What a writer that just opened the file pays

**Question.** If the token dictionary stays in memory, how different is a
cold writer from a hot one?

**Results (tonk-team).** Opening: 8.9 ms (it scans every record, since no
checkpoint was written yet). Building the dictionary from the mapped
tokens: 1.4 ms for 29,322 tokens. An append: 5.7 ms. So a cold writer pays
less than a quarter of an append, once; about 48 ns a token, so tens of
milliseconds at ten times this data, and about half a second at a gigabyte
of tokens.

**Decision.** Keep the dictionary as a writer-side cache for now; revisit
when replicas grow large enough for it to matter.

Commit: `c69e733`.

## 2026-11-06 — Where the block index lives

**Question.** The index has to find any block by hash, including blocks no
tree reaches any more, without being loaded into memory. What layout?

**Prior art.** Hyperbee keeps a B-tree in its log: each block carries the
nodes its put changed, keys are pointers to the blocks holding them, and
the root is in the last block. Hypercore's merkle tree is positional
(flat in-order, no pointers, roots merging like binary carries), so it
cannot find a block by hash, but its carries are a schedule for merging
sorted runs. Our own tree's hitchhiker buffers turn many small writes
into few large ones.

**Method.** `single_file_census` with `DIALOG_CENSUS_INDEX` replays each
history one append at a time into each layout, then at sixteen points of
the history looks up every 7th block written by then, counting the 4 KiB
pages the index reads: cold, and warm (the top cached).

**Results.**

| layout | tonk MiB | tonk warm pages | SE MiB | SE warm pages | largest append |
|---|---|---|---|---|---|
| B-tree, keys as pointers, paths copied | 0.58 | 13.3 | 2.13 | 13.2 | 5 KB |
| B-tree, hash prefixes, paths copied (8 a node) | 1.20 | 1.8 | 4.33 | 1.6 | 10 KB |
| B-tree against last version, whole every 8 | 0.50 | 2.6 | 1.80 | 1.7 | 5 KB |
| sorted runs, merged 4 at a time | 0.40 | 1.0 | 1.51 | 1.0 | 288 KB |
| **hitchhiker, 32 a node, buffers of 256, root against its last version, whole every 32** | **0.32** | **1.2** | **1.46** | **1.1** | **8 KB** |

The full table is in the [design](design.md#where-the-block-index-lives-layouts-replayed).

**Leading, not decided.** A hitchhiker tree in the file, children named by
position: the smallest layout without a merge spike, 6–11% of the data,
about a page a lookup once its root is cached. The first lookup after
opening reads the root's chain of versions (7–17 pages), once. Sorted runs
merged 4 at a time are the other finalist. Size and pages are one axis;
the evaluation decides on the rest.

Commits: `e368291`, `66eb85d`.

## 2026-11-06 — How we decide: five axes

**Question.** The index layouts were compared on size and pages a lookup
reads. What else must a decision weigh?

**Plan.** The [evaluation](evaluation.md): queries (real ones, including
tonk's), writes (tonk's commands creating issues and memos, inductive
rules and all), sync (pulling another replica's changes), memory (readers
and writers apart) and storage growth, for `fs`, the file as built today,
and the two index finalists built for real. It needs the index behind one
interface, a store per run in `dialog-perf` and `dialog-soak`, and tonk
patched to build against this branch.

**Status.** Planned, not run.

## How to reproduce

```sh
# Stack Exchange through a repository, against main's archive
DIALOG_SE_CSV=retro-facts.csv DIALOG_BENCH_COMMITS=4096 \
  cargo run --release -p dialog-baseline --example file_archive_bench

# a directory of blocks into the file, with the index layouts replayed
DIALOG_CENSUS_ORDER=history DIALOG_CENSUS_FILE=out DIALOG_CENSUS_INDEX=1 \
  cargo run --release -p dialog-baseline --example single_file_census -- \
  archive path/to/blocks

# a replica into the file
cargo run --release -p dialog-migration --features archive-file \
  --bin dialog-migrate -- path/to/profile
```
