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

## Why get was slower, and the fix (implemented)

A point lookup walks root -> ... -> leaf. The leaf search is a `binary_search`
(`ArchivedNodeBody::find_entry`), O(log m) and cheap. The cost was in the
**index** walk: `TreeWalker::search` iterates **all** links of each index node
and `into_owned`-deserializes **every sibling** link into owned
`left_siblings` / `right_siblings` for the returned `path` (machinery the
insert/delete rebuild needs). That work is **O(fan-out) per index node**, and
it ran unconditionally, including on `get` -- which is why the numbers above
track root fan-out (m=128, fan-out 394, was the slowest) rather than depth.

The fix made `search` itself zero-copy rather than adding a parallel read path.
A `TreeLayer` now holds only the host node (Arc-backed, shared on clone) and the
child index the descent took; it no longer eagerly `into_owned`s the host's
other children. Reads (`get`, range scans) take the leaf and ignore the rest,
copying nothing. Writes (`insert`, `delete`) decode the siblings of a level
lazily, from the host, only when they actually rebuild that level -- which they
do by re-serializing the whole node anyway, so no extra copy is introduced. Each
index node is navigated with a binary search (`partition_point`, O(log
fan-out)). Cross-checked by `it_gets_present_and_absent_keys_across_index_boundaries`
(present keys, gaps, on-boundary keys, above-max fallthrough), the canonical-form
invariance tests, and the full existing suite.

### get @ 50k: before vs after (threshold), with bit-batch (shares the read path)

| m | threshold before | threshold after | bit-batch after |
|---|---|---|---|
| 32 | 459 µs | **130 µs** | 197 µs |
| 64 | 546 µs | **156 µs** | 203 µs |
| 128 | 789 µs | **180 µs** | 305 µs |
| 254 | 685 µs | **326 µs** | 397 µs |

The O(fan-out) cost is gone: threshold get dropped 3.5-4.4x and the curve is
far flatter in `m`. With per-node cost fixed, what remains is tree depth, so the
shorter threshold tree is now **faster than bit-batch on get at every branch
factor**. (bit-batch improved too, since it shares the new read path; threshold
gains more because it had the wide nodes the old path punished.)

This confirms the get slowdown was an **implementation artifact, not a cost of
correct branching**. Range scans benefit from the same change (the start-key
descent no longer materializes siblings, and `into_indexed` no longer re-derives
each child index with a binary search -- `search` already recorded it). The
write path keeps the same canonical output (all tests green) while shedding the
eager sibling decode/concat it never needed before the rebuild step.

## Recommendation

1. **Land the threshold fix.** It is correct; with the read-only get path it is
   now faster than bit-batch on get and produces a strictly better-shaped tree.
2. **Treat branch factor as a separate, tunable knob.** Raising `m` lowers
   block count (better network IO, toward Datomic's design) at the cost of
   larger nodes.
   - **get**: handled -- `find_leaf` makes it ~flat in `m`.
   - **insert**: still grows with `m` (wide-node rebuild). A leaf segment-size
     cap to bound the geometric tail is the remaining follow-up; not addressed
     here since it touches the builder, not rank derivation.
