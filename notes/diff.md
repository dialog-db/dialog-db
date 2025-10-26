# Tree Differentiation Algorithm

## Overview

The differentiation algorithm computes the set of changes between two search trees by performing a parallel tree walk with hash-based pruning. The output is a stream of `Change` events representing entries that were added or removed.

```rust
pub enum Change<Key, Value> {
    Add(Entry<Key, Value>),
    Remove(Entry<Key, Value>),
}

pub fn differentiate(checkpoint: Tree, current: Tree) -> Stream<Change>
```

---

## Core Principles

### 1. Hash-Based Pruning

The fundamental optimization: **if two nodes have the same hash, their entire subtrees are identical and can be skipped**.

```yaml
checkpoint@abc         current@abc
├─ node1@xyz          ├─ node1@xyz
│  └─ ...             │  └─ ...
└─ node2@def          └─ node2@def
   └─ ...                └─ ...
```

If `checkpoint.hash == current.hash`, we emit **no changes** and return immediately.

### 2. Ordered Comparison

Both trees store entries in sorted order by key. We can walk them in parallel using a two-cursor algorithm, similar to merging sorted arrays.

### 3. Upper Bound Navigation

Each node (whether IndexNode/Branch or Segment) has an `upper_bound` - the maximum key in that node or subtree. This allows us to:
- Align cursors between trees
- Detect boundary shifts
- Navigate efficiently through misaligned ranges

### 4. Range-Based Comparison

When boundaries don't align between trees (due to boundary insertions or removals), we identify the misaligned **range** and compare all references within that range.

**Boundary shifts occur when**:
- **Boundary removed**: Children from the removed boundary shift to the next parent
- **Boundary inserted**: A range splits into multiple boundaries

---

## Algorithm Structure

```
differentiate(checkpoint, current)
  ├─ If both None → return empty stream
  ├─ If checkpoint None → add_all(current)
  ├─ If current None → remove_all(checkpoint)
  └─ diff_nodes(checkpoint.root, current.root)
```

### `diff_nodes(before: Node, after: Node)`

Compares two nodes and dispatches to the appropriate handler:

```
diff_nodes(before, after)
  ├─ If before.hash == after.hash → Skip (identical)
  ├─ If both segments → diff_entries(before, after)
  ├─ If both branches → diff_ranges(before, after)
  └─ If one branch, one segment → extract entries from both, diff_entries
```

**Note**: Trees can have different heights if one has grown significantly. When comparing a branch against a segment, we extract all entries from the branch's subtree and compare them against the segment's entries.

---

## Entry Comparison: `diff_entries`

At the leaf level (or when comparing extracted entries), we use a two-cursor walk of sorted entry lists.

### Example: Modified Entries

```yaml
checkpoint                current
├─ Entry(k1, v1)         ├─ Entry(k1, v1)
├─ Entry(k2, v2)         ├─ Entry(k2, v2')    (value changed)
├─ Entry(k3, v3)         ├─ Entry(k4, v4)     (k3 removed, k4 added)
└─ Entry(k5, v5)         └─ Entry(k5, v5)
```

**Two-cursor walk**:

| Step | Checkpoint | Current | Comparison | Output |
|------|-----------|---------|------------|--------|
| 1 | `k1:v1` | `k1:v1` | Entries equal | Skip, advance both |
| 2 | `k2:v2` | `k2:v2'` | Keys equal, values differ | `Remove(k2:v2)`, `Add(k2:v2')`, advance both |
| 3 | `k3:v3` | `k4:v4` | `k3 < k4` | `Remove(k3:v3)`, advance checkpoint |
| 4 | `k5:v5` | `k4:v4` | `k5 > k4` | `Add(k4:v4)`, advance current |
| 5 | `k5:v5` | `k5:v5` | Entries equal | Skip, advance both |

**Output stream**:
```rust
Remove(Entry { key: k2, value: v2 })
Add(Entry { key: k2, value: v2' })
Remove(Entry { key: k3, value: v3 })
Add(Entry { key: k4, value: v4 })
```

---

## Range Comparison: `diff_ranges`

This is the core of the algorithm when comparing IndexNode children. We walk two sorted lists of references, identifying aligned and misaligned ranges.

### Key Insight: Boundary Shifts

When a boundary entry is removed or added, the geometric distribution changes, causing boundary shifts:
- **Boundary removed**: Children shift to the next parent
- **Boundary inserted**: Range splits into multiple nodes

### Example: Boundary Removal

**Initial state** (checkpoint):
```yaml
checkpoint
├─ ref_a [bound=k3]
│   ├─ ref_a1 [bound=k0]
│   ├─ ref_a2 [bound=k1]
│   └─ ref_a3 [bound=k3]
├─ ref_b [bound=k5]
│   ├─ ref_b1 [bound=k4]
│   └─ ref_b2 [bound=k5]
└─ ref_c [bound=k7]
    ├─ ref_c1 [bound=k6]
    └─ ref_c2 [bound=k7]
```

**After removing entry `k4`** (current):
```yaml
current
├─ ref_a [bound=k3]
│   ├─ ref_a1 [bound=k0]
│   ├─ ref_a2 [bound=k1]
│   └─ ref_a3 [bound=k3]
└─ ref_c [bound=k7]
    ├─ ref_b2 [bound=k5]  ← Shifted from ref_b!
    ├─ ref_c1 [bound=k6]
    └─ ref_c2 [bound=k7]
```

**What happened**:
- Removing entry `k4` caused boundary `ref_b@k5` to disappear
- Children of `ref_b` shifted to `ref_c`
- Simply calling `remove_all(ref_b)` would be wrong!

### Range-Based Algorithm

When we encounter misaligned boundaries, we collect references in the misaligned range and compare their children.

**Walk sequence** for the example above:

| Step | Checkpoint Cursor | Current Cursor | Action |
|------|------------------|----------------|--------|
| 1 | `ref_a@k3` | `ref_a@k3` | Bounds match, compare normally |
| 2 | `ref_b@k5` | `ref_c@k7` | **Misalignment detected!** |
| | | | Checkpoint range: `(k3..k5]` |
| | | | Current range: `(k3..k7]` |
| | | | Checkpoint refs: `[ref_b@k5]` |
| | | | Current refs: `[ref_c@k7]` |
| | | | Load checkpoint children: `[ref_b1@k4, ref_b2@k5]` |
| | | | Load current children: `[ref_b2@k5, ref_c1@k6, ref_c2@k7]` |
| | | | **Compare these child lists** |

Wait, this isn't quite right. Let me reconsider...

Actually, looking at the structure again:
- Checkpoint has `[ref_a@k3, ref_b@k5, ref_c@k7]`
- Current has `[ref_a@k3, ref_c@k7]`

When we hit the misalignment:
- We're at `ref_b@k5` in checkpoint
- We're at `ref_c@k7` in current
- We need to collect until we find matching hashes

But `ref_c@k7` in both trees might not have the same hash (because current's `ref_c` now contains `ref_b2`).

So the range should be:
- Checkpoint: collect `[ref_b@k5, ref_c@k7]` 
- Current: collect `[ref_c@k7]`
- Load all their children and compare

### Cases for Range Comparison

#### Case 1: Bounds Match, Hashes Match
```yaml
checkpoint: ref_a@k5 (hash=abc)
current:    ref_a@k5 (hash=abc)
```

**Action**: Skip (identical subtree), advance both cursors.

#### Case 2: Bounds Match, Hashes Differ

```yaml
checkpoint: ref_a@k5 (hash=abc)
current:    ref_a@k5 (hash=xyz)
```

**Action**: Range contains just these nodes' children
- Load children from both refs
- Compare the children lists

#### Case 3: Misaligned Boundaries (Different Bounds)

```yaml
checkpoint: [ref_a@k3, ref_b@k5, ref_c@k7]
current:    [ref_a@k3, ref_k@k6, ref_c@k7]
```

If `ref_c@k7` has the same hash in both trees, we can stop collecting at the previous boundary:

**Action**:
- Checkpoint range: `(k3..k5]` → refs: `[ref_b@k5]`
- Current range: `(k3..k6]` → refs: `[ref_k@k6]`
- Load children from both refs
- Compare the children lists
- Skip `ref_c@k7` (identical)

If `ref_c@k7` differs, we include it in the range:

**Action**:
- Checkpoint range: `(k3..k7]` → refs: `[ref_b@k5, ref_c@k7]`
- Current range: `(k3..k7]` → refs: `[ref_k@k6, ref_c@k7]`
- Load children from all refs
- Compare the children lists

#### Case 4: One Side Exhausted

```yaml
checkpoint: [ref_a@k3]
current:    [ref_a@k3, ref_b@k5, ref_c@k7]
```

**Action**:
- After comparing `ref_a`, checkpoint cursor is exhausted
- Current has remaining refs: `[ref_b@k5, ref_c@k7]`
- Load all children from current refs
- Call `add_all` on those children

### Range Definition

A **range** is defined by two bounds:
- **Start**: Previous reference's upper_bound (exclusive), or tree minimum if first ref
- **End**: Last reference's upper_bound before the next matching ref (inclusive)

For example, `(k3..k5]` means:
- All keys `> k3` and `<= k5`
- In byte array terms: lexicographically greater than k3, up to and including k5

---

## Implementation Approaches

### Recursive Approach

The pseudocode shown earlier uses recursion: when we find a range, we recursively call `diff_reference_lists` on the children.

### Stack-Based Approach

Alternatively, we can use a stack to avoid recursion:

```rust
fn differentiate(checkpoint: Tree, current: Tree) -> Stream<Change> {
    let mut stack = vec![(checkpoint.root, current.root)];
    
    stream! {
        while let Some((before, after)) = stack.pop() {
            if before.hash() == after.hash() {
                continue; // Skip identical subtrees
            }
            
            if before.is_segment() && after.is_segment() {
                // Leaf level - emit changes
                for change in diff_entries(before, after) {
                    yield change;
                }
            } else {
                // Identify ranges at this level
                let ranges = identify_ranges(before, after);
                
                // Load children for each range and push to stack
                for (before_refs, after_refs) in ranges {
                    let before_children = load_all_children(before_refs);
                    let after_children = load_all_children(after_refs);
                    
                    // Push child pairs onto stack
                    for (b, a) in pair_children(before_children, after_children) {
                        stack.push((b, a));
                    }
                }
            }
        }
    }
}
```

**Benefits of stack-based approach**:
- No recursion depth limits
- Easier to control traversal order
- Can implement breadth-first or depth-first easily
- Clearer separation of "finding ranges" vs "processing ranges"

---

## Performance Characteristics

### Time Complexity

- **Best case**: O(log n) - Trees differ only at root, identical hashes below
- **Worst case**: O(n) - Every entry differs, must walk entire tree
- **Average case**: O(k log n) - k changed entries scattered across log n levels

### Space Complexity

- **Streaming**: O(1) constant memory for the stream itself
- **Stack depth**: O(log n) for recursion or explicit stack
- **Node loading**: Only loads nodes that differ or are in misaligned ranges
- **Collected refs**: Temporary O(m) where m is refs in a misaligned range

### Optimizations

1. **Hash pruning**: Entire subtrees skipped with single hash comparison
2. **Lazy loading**: Only loads nodes that potentially differ
3. **Streaming output**: No need to materialize full diff in memory
4. **Smart range detection**: Only collect until finding matching hashes

---

## Edge Cases

### Empty Trees

```rust
differentiate(empty, tree_with_data)
// → add_all(tree_with_data)

differentiate(tree_with_data, empty)
// → remove_all(tree_with_data)
```

### Identical Trees

```rust
differentiate(tree@abc123, tree@abc123)
// → Empty stream (root hash match)
```

### Height Mismatch

```yaml
checkpoint (small tree):
└─ segment [bound=k1]
   └─ Entry(k1, v1)

current (large tree):
└─ IndexNode [bound=k1]
   └─ IndexNode [bound=k1]
      └─ segment [bound=k1]
         └─ Entry(k1, v1)
```

Both extract to `[Entry(k1, v1)]`, so the diff is empty.

---

## Error Handling

### Fatal Errors (Fail Fast)

1. **Missing blocks**: If a node reference can't be loaded from storage
2. **Structural inconsistencies**: If tree invariants are violated

All errors propagate immediately, stopping the stream.

### No Partial Results

If differentiation fails halfway through, the entire operation fails. There are no partial diffs - it's all or nothing.

---

## Integration with Sync Protocol

From sync.md, differentiation is used in the merge process:

```rust
// Compute what changed locally
let changes = differentiate(branch.base, branch.current);

// Apply those changes to the fetched remote tree
let merged = remote_tree.integrate(changes).await?;
```

This enables:
- ✅ Only loads nodes that differ
- ✅ Handles boundary shifts correctly
- ✅ Cached subtrees remain cached (no refetch if unchanged)
- ✅ Deterministic conflict resolution via integrate
