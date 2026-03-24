# Node Size Bounds

## The Problem

The prolly tree splits nodes using a geometric distribution: each entry's hash is tested against a threshold, and entries that fall below become chunk boundaries. The average chunk size equals the branch factor (254), but individual chunks can be any size. A single entry can end up alone in a node, and a run of 1000+ entries with no boundary is entirely possible.

This is the variance problem. The geometric distribution with parameter `p = 1/m` has standard deviation approximately equal to the mean. For `m = 254`, roughly 13% of nodes will be twice the target size or larger, and about 0.4% of entries will be singleton nodes. In practice this means:

1. **Oversized nodes.** A node with 800 entries must be fully deserialized to read any one of them. The serialized size is unpredictable, which makes I/O planning difficult and wastes bandwidth during sync. A single large node can dominate the cost of an otherwise fast lookup.

2. **Undersized nodes.** A node with 1 or 2 entries wastes a hash, a storage round-trip, and a tree level on almost no data. Small nodes also reduce structural sharing: two trees that differ by one entry in a region of tiny nodes will share fewer nodes than they would with reasonably sized ones.

3. **Sync amplification.** During differential sync, changed nodes are transmitted whole. An oversized node that changes by one entry forces retransmission of all its entries. An undersized node transmits almost no useful data per round-trip. Both extremes degrade sync efficiency.

The underlying cause is that the geometric distribution has no memory. Whether the current run is 1 entry or 500, the probability of a boundary at the next entry is always `1/m`. The algorithm does not adapt to how much data has accumulated.

## Context: Content-Defined Chunking

This is a well-studied problem in content-defined chunking (CDC), used in deduplication systems, backup tools, and content-addressable storage. The standard solution is to enforce minimum and maximum bounds on chunk size:

- **Minimum bound.** Ignore boundary candidates until at least `min` entries have accumulated since the last split. This collapses tiny nodes into their neighbors.
- **Maximum bound.** Force a split after `max` entries regardless of the hash. This caps the worst-case node size.

Between the bounds, the normal geometric distribution applies. Boundaries are still content-defined (determined by the hash), so structural sharing is preserved for the vast majority of nodes. Only nodes near the min/max edges lose some content-sensitivity, and even those tend to re-converge quickly after the forced boundary.

FastCDC and similar algorithms typically use `min = mean/2`, `max = mean*2` or `min = mean/4`, `max = mean*4`. The tighter the bounds, the lower the variance, but also the lower the deduplication ratio. For prolly trees the tradeoff is between consistent node sizes and structural sharing across versions.

## Design: Bounded Geometric Distribution

### Parameters

Given a branch factor `m`:

```
min_size = m / 4    (default: 63 for m=254)
max_size = m * 4    (default: 1016 for m=254)
```

These are tunable but should be fixed for a given tree configuration, since changing them produces a different tree shape for the same data.

### Algorithm

The boundary decision changes from:

```
is_boundary = rank > minimum_rank
```

to:

```
is_boundary = (count >= min_size && rank > minimum_rank) || count >= max_size
```

where `count` is the number of entries accumulated since the last boundary.

When `count < min_size`, boundary candidates are suppressed. The entry still has a rank (its hash does not change), but the rank is not acted on. When `count >= max_size`, a boundary is forced regardless of rank. Between the two bounds, the existing logic applies unchanged.

### Where It Applies

The boundary decision happens in the `collect` function (in both `dialog-search-tree` and `dialog-prolly-tree`). Today `collect` accumulates children and splits when it sees a child with `rank > minimum_rank`. The bounds add two conditions around that check:

```rust
fn collect<Child>(
    children: NonEmpty<(Child, Rank)>,
    minimum_rank: Rank,
    min_size: usize,
    max_size: usize,
) -> Result<RankedNodes<Key, Value>, Error>
where
    NodeBody<Key, Value>: TryFrom<Vec<Child>, Error = Error>,
{
    let mut output: Vec<(Node<Key, Value>, u32)> = vec![];
    let mut pending = vec![];

    for (child, rank) in children {
        pending.push(child);

        let dominated = pending.len() < min_size;
        let saturated = pending.len() >= max_size;
        let boundary = rank > minimum_rank;

        if saturated || (!dominated && boundary) {
            let node = Node::new(Buffer::from(
                NodeBody::try_from(std::mem::take(&mut pending))?.as_bytes()?,
            ));
            output.push((node, rank));
        }
    }

    if !pending.is_empty() {
        let node = Node::new(Buffer::from(
            NodeBody::try_from(pending)?.as_bytes()?,
        ));
        output.push((node, minimum_rank));
    }

    // ...
}
```

The same change applies to `join_with_rank` in `dialog-prolly-tree`, which follows the identical accumulate-and-split pattern.

### What Does Not Change

- **Rank computation.** The `compute_geometric_rank` function is unchanged. Every entry still gets a deterministic rank from its hash. The bounds only affect whether a rank triggers a split.
- **Tree structure above the leaves.** Index nodes are built from links using the same `collect` function. The bounds apply uniformly at every level.
- **The `distribute` method.** It calls `collect` for each level. The bounds are passed through.

### Effect on Structural Sharing

Structural sharing is preserved in the middle of each node, where the geometric distribution operates freely. At the edges:

- A forced max-boundary can split at a different point than the hash would have. The two resulting nodes may not match nodes in another version of the tree. But the next boundary (determined by hash) will re-anchor, and everything after it will share again.
- A suppressed min-boundary means an entry that would have been a boundary in the unbounded case gets absorbed into its node. The resulting larger node may not match the smaller node in another version. Again, the next unsuppressed boundary re-anchors.

In both cases the damage is local. One or two nodes near the forced/suppressed boundary may differ, but the rest of the tree converges. This is the same tradeoff that every CDC system makes, and in practice the impact on deduplication is small when the bounds are within 4x of the mean.

### Effect on Variance

With `min = m/4` and `max = m*4`:

| Metric | Unbounded | Bounded |
|---|---|---|
| Mean node size | 254 | ~254 (slightly shifted) |
| Std dev | ~253 | ~120 (estimated) |
| Min node size | 1 | 63 |
| Max node size | unbounded | 1016 |
| P(node > 2x mean) | ~13.5% | 0% (capped) |
| P(singleton node) | ~0.4% | 0% (floored) |

## Decision

**Add min/max size bounds to the `collect` function in both tree implementations.**

### Incremental Path

1. **Now**: Add `min_size` and `max_size` parameters to `collect` and `join_with_rank`. Use `m/4` and `m*4` as defaults derived from the branch factor. The geometric distribution and rank computation are untouched.

2. **Later**: Consider making bounds configurable per tree instance, which would allow different bounds for segment nodes (where values are large and smaller nodes are preferable) vs index nodes (where entries are small and larger nodes are cheap).

### Rationale

1. **Unbounded variance wastes I/O.** Oversized nodes amplify reads and sync. Undersized nodes waste round-trips and storage overhead. Bounding both eliminates the extremes.

2. **Content-defined boundaries are preserved where they matter.** Between the bounds, splitting is still hash-determined. Structural sharing is only affected at the edges of forced/suppressed boundaries, and re-converges quickly.

3. **The change is local to `collect`.** Rank computation, tree traversal, differential sync, and the storage layer are all unaffected. The only change is the split condition inside the accumulation loop.

4. **The same technique extends to per-level tuning.** Once bounds exist, using different bounds for leaf vs index levels is a parameter change, not an architectural one.
