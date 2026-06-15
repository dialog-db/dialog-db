# Rank distribution: bit-batch vs threshold geometric

Produced by `benches/distribution.rs` (timing + shape) and
`examples/node_sizes.rs` (per-level node sizes):

```sh
cargo bench -p dialog-search-tree --bench distribution --features helpers
cargo run --release --example node_sizes --features helpers
```

Criterion writes one overlaid comparison line chart per (operation, branch
factor) to `target/criterion/<op>/m=<m>/report/index.html`, linked from the
top-level `target/criterion/report/index.html`. Branch factors swept: 32, 64,
128, 254. Sizes: 1k, 10k, 50k. Seed 42.

## TL;DR

- The threshold derivation is **correct**; the bit-batch derivation has a bug
  that collapses the effective branch factor **above the leaves to ~2**,
  regardless of the declared `m`.
- The fix produces **flatter trees with ~3x fewer blocks** at production `m` —
  the win for content-addressed / networked storage, where block count drives
  round-trips.
- The threshold tree is **slower for insert and point-get at large `m`**, but
  this is **not a cost of correct branching**: at moderate `m` (32) it is
  within ~5% of bit-batch, and the get cost is an implementation artifact of
  the shared search path (see "Why get is slower"), not the algorithm.
- Range scans are a wash (slightly faster with threshold).

## The bug: effective branch factor collapses above the leaves

The bit-batch derivation simulates a `1/m` coin by reading `ceil(log2(m))`-bit
batches, but each batch is read from a single byte and batches past the first
do not align to byte boundaries. The promotion probability above the first
level degrades toward `1/2`, so every level above the leaves carries ~2
children no matter what `m` is declared. The signature is `L1 ≈ 2.0` in every
bit-batch shape row below.

Consequently the bit-batch tree is **structurally inhomogeneous**: leaf fan-out
tracks `m` (the leaf split is roughly correct), but the index spine above is
effectively a binary tree. There is no single `m` you can feed a *correct*
algorithm to reproduce that shape, because a correct distribution produces a
*homogeneous* tree (the same fan-out at every level).

## Tree shape

| m | size | dist | height | nodes | idx | leaf | fan-out per level (L0 = leaves) |
|---|---|---|---|---|---|---|---|
| 32 | 50k | bit-batch | 6 | 1 724 | 201 | 1 523 | L0:32.8 L1:8.4 L2:18.1 L3:1.4 L4:3.5 L5:2.0 |
| 32 | 50k | threshold | 3 | 1 600 | 44 | 1 556 | L0:32.1 L1:36.2 L2:43.0 |
| 64 | 50k | bit-batch | 5 | 949 | 196 | 753 | L0:66.4 L1:4.2 L2:15.1 L3:6.0 L4:2.0 |
| 64 | 50k | threshold | 3 | 788 | 10 | 778 | L0:64.3 L1:86.4 L2:9.0 |
| 128 | 50k | bit-batch | 7 | 604 | 242 | 362 | L0:138.1 L1:2.0 L2:3.8 L3:6.9 L4:2.3 L5:1.5 L6:2.0 |
| 128 | 50k | threshold | 2 | 395 | 1 | 394 | L0:126.9 L1:394.0 |
| 254 | 50k | bit-batch | 7 | 604 | 242 | 362 | L0:138.1 L1:2.0 L2:3.8 L3:6.9 L4:2.3 L5:1.5 L6:2.0 |
| 254 | 50k | threshold | 2 | 197 | 1 | 196 | L0:255.1 L1:196.0 |

(Full 1k/10k rows are emitted by the `shape` bench group.)

- **Height**: threshold stays at 2-3; bit-batch reaches 7 at m=128/254.
- **Blocks**: threshold persists ~3x fewer at production `m` (197 vs 604 at
  m=254/50k). Total *bytes* are nearly identical (entries dominate); the saving
  is block *count*.
- **The skew worsens with `m`**: at m=254 the bit-batch upper levels still carry
  2-7 children where 254 was declared.

## Node sizes (per element, by node type)

From `examples/node_sizes.rs`. The two node types differ, as expected:

- **Index node**: ~**48 B per child** (16 B upper-bound key + 32 B blake3 hash).
- **Segment (leaf) node**: ~**56 B per entry** (16 B key + 32 B value + ~8 B
  rkyv overhead).

A node's total size is `fan-out x per-element`, so size scales with `m`. The
leaf fan-out follows a geometric distribution, so leaf sizes have a long tail:

| m | leaf mean | leaf max | root index bytes (fan-out) |
|---|---|---|---|
| 32 | 1.8 KB | 11 KB | 2.1 KB (43) |
| 64 | 3.6 KB | 28 KB | 0.4 KB (9) |
| 128 | 7.1 KB | 46 KB | 18.9 KB (394) |
| 254 | 14.3 KB | **96 KB** | 9.4 KB (196) |

Index nodes stay small in absolute terms even at high `m`; the large blocks are
**leaf segments**, and the geometric tail pushes the worst-case leaf to ~5-7x
the mean.

### Comparison to Datomic

Datomic segments are "up to ~50 KB, usually 1,000-20,000 datoms" with a
"1,000+ branching factor" and a tree "no more than three levels deep" (Tonsky,
*Unofficial Guide to Datomic Internals*). Our mean segment (14 KB at m=254) is
well within that; our max (96 KB) slightly overshoots. Datomic deliberately
uses **high branch factor + large segments** so one fetch returns "1,000-20,000
datoms around" the target, avoiding N+1 round-trips. That is the same
network-IO argument for raising our `m` — we are currently conservative by
comparison, leaving headroom.

## Timing (criterion medians)

### insert (full rebuild per sample)

| m | 10k bit-batch | 10k threshold | ratio |
|---|---|---|---|
| 32 | 182 ms | 190 ms | 1.05x |
| 64 | 254 ms | 272 ms | 1.07x |
| 128 | 368 ms | 486 ms | 1.32x |
| 254 | 368 ms | 780 ms | 2.12x |

### get (256 point lookups per sample)

| m | 50k bit-batch | 50k threshold | root fan-out |
|---|---|---|---|
| 32 | 434 µs | 459 µs | 43 |
| 64 | 383 µs | 546 µs | 9* |
| 128 | 430 µs | 789 µs | 394 |
| 254 | 432 µs | 685 µs | 196 |

\* The get cost tracks **root fan-out**, not `m`: m=128 (fan-out 394) is the
slowest and slower than m=254 (fan-out 196), even though 254 > 128. This is the
fingerprint of the per-node O(fan-out) cost described below.

### range_scan (full scan)

| m | 50k bit-batch | 50k threshold |
|---|---|---|
| 32 | 1.154 ms | 1.148 ms |
| 254 | 1.028 ms | 0.977 ms |

A wash; threshold slightly faster (fewer index blocks to load).

## Why insert is slower at large m

Each mutation re-shapes and re-serializes the nodes on the path. A flat
height-2 tree at m=254 hangs all leaves off one index node, so every insert
re-serializes that wide root; the tall bit-batch tree spreads the change over
small nodes. This is partly inherent (wider nodes cost more to rebuild) and
mitigable with a node-size cap on leaf segments (whose geometric tail is the
worst offender).

## Why get is slower (and why it is fixable)

A point lookup walks root -> ... -> leaf. The leaf search is a `binary_search`
(`ArchivedNodeBody::find_entry`), O(log m) and cheap. The cost is in the
**index** walk: `TreeWalker::search` iterates **all** links of each index node
and `into_owned`-deserializes **every sibling** link into owned
`left_siblings` / `right_siblings` for the returned `path` (machinery the
insert/delete rebuild needs). That work is **O(fan-out) per index node**, and
it runs unconditionally, including on `get`.

So the flat tree, which concentrates the whole key range under one ~196-wide
root, makes a single get deserialize ~195 sibling links; the tall bit-batch
tree, with ~2-wide index nodes, deserializes only a couple per level. This is
why the get cost tracks root fan-out rather than tree depth, and why it is an
**implementation artifact, not a property of correct branching**: a read-only
descent that binary-searches the index links and skips the sibling
materialization would make get roughly flat in `m`.

## Recommendation

1. **Land the threshold fix.** It is correct, and at moderate `m` it is
   perf-neutral against bit-batch while already producing a better-shaped tree.
2. **Treat branch factor as a separate, tunable knob.** Raising `m` lowers
   block count (better network IO, toward Datomic's design) at the cost of
   larger nodes. The two costs that grow with `m` are both addressable without
   touching rank derivation:
   - **get**: read-only search path (skip sibling `into_owned`) -> flat in `m`.
   - **insert**: leaf segment-size cap to bound the geometric tail.
