# Transient tree: batched edits with one rebuild

## Goal

Apply a batch of inserts/deletes paying the per-node serialize + hash cost once
per touched node, not once per operation. Inspired by Clojure transients: a
short-lived mutable view over an otherwise immutable tree.

The tree may be only partially replicated, so a batch touches only the subtrees
its keys reach: untouched children stay referenced by hash and are loaded
lazily, only if a descent enters them.

## Node model

A `NodeEdit` is a node in the batch, of one of two kinds. The names follow the
persistent/transient data structure vocabulary, not Rust's borrow/own one: both
variants own their data and neither holds a reference into the original tree, so
the type needs no lifetime.

```
enum NodeEdit<Key, Value> {
    /// A sealed node of the durable tree, named by its Link (hash plus upper
    /// bound). Never copied; seals back to itself for free.
    Persistent(Link<Key>),
    /// A node edited in this batch: held in memory, no hash yet, with its upper
    /// bound cached for ordering within its parent.
    Transient { upper_bound: Key, body: TransientBody<Key, Value> },
}

enum TransientBody<Key, Value> {
    Index(Vec<NodeEdit<Key, Value>>),   // ordered child edits
    Segment(Vec<Entry<Key, Value>>),    // ordered entries
}
```

This is the node-granularity transient: the unit of copy is a node. The first
edit that descends through a node lifts it from `Persistent` to `Transient`,
decoding its direct children (as `Persistent` links) or entries into memory
once. Later edits in the same batch mutate that transient node in place. Sibling
subtrees the batch never enters stay `Persistent` and are never decoded.

## apply (insert / delete)

`Tree::transient()` holds the root as a `NodeEdit`. Each op descends by key,
lifting persistent nodes to transient as it passes, and at the leaf splices the
entry into (or removes it from) the transient segment's sorted vector.

The descent runs in two phases so no borrow spans an `await`: first walk down
recording the child index taken at each level (reborrowing from the root each
step, lifting as it goes), then follow the recorded indices to the segment and
apply the change, refreshing cached upper bounds back up the path.

apply does **not** re-group nodes at rank boundaries. Node boundaries are a pure
function of entry ranks, so they are derived once, at persist, from the final
contents. This keeps apply cheap and makes a batch's result independent of the
order its ops were applied.

## persist (seal)

`seal` walks the transient frontier bottom-up. A `Persistent` node passes its
link through untouched (no decode, no re-hash). A transient node's children are
reduced to a canonical, still-ungrouped run of child links, then a parent groups
that run into nodes one level up.

The subtle part is fusing runs across dissolved boundaries. A delete can remove
the key that separated two adjacent segments; those segments must then re-fuse,
because a from-scratch build of the surviving keys would. So `merge_children`
treats each maximal run of adjacent **transient** children as one unit: their
entries (at the leaf level) or child links (higher up) are concatenated and cut
with the canonical rank rule as a whole. A `Persistent` child terminates a run:
its upper bound was a rank boundary in the durable tree, so it never fuses with
a neighbor. Grouping uses the same thresholds and cut rule as the sequential
`TreeShaper::collect` (cut after each child whose `rank(upper_bound)` exceeds the
level threshold; `BOTTOM_RANK` at leaves, `+1` per level up).

`persist` then groups the root's children at least once (so a lone surviving
segment gains its index wrapper, as the sequential delete leaves it), folds up
until a single root link remains, and finally `collapse_root` strips any
non-canonical chain of single-child index-over-index nodes that hollowed-out
upper levels can leave, keeping the legitimate index-over-lone-segment root. The
result is the new root hash plus a delta of exactly the nodes the batch sealed,
laid on top of the tree's carried-forward pending changes.

## Correctness contract

`transient -> apply* -> persist` yields the same root hash as the same ops
through `Tree::insert` / `Tree::delete` one at a time. Verified by
`it_matches_sequential_inserts`, `it_matches_sequential_with_deletes`,
`it_matches_sequential_when_deletes_collapse_the_tree`, and a randomized
`it_matches_sequential_for_random_mixed_batches` sweeping seeds, base sizes, and
delete/insert mixes.

## Out of scope (follow-ups)

- Novelty buffer / write-ahead amortization layered on top of this batch
  primitive.
- In-place value replacement for layout-preserving edits.
