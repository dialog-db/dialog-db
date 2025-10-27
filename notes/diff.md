# Tree Differentiation Algorithm

## Overview

The differentiation algorithm computes the set of changes between two search trees using a **sparse tree** representation with prune-expand-stream approach. The output is a stream of `Change` events representing entries that were added or removed.

```rust
pub enum Change<Key, Value> {
    Add(Entry<Key, Value>),
    Remove(Entry<Key, Value>),
}

pub fn differentiate(before: &Tree, after: &Tree) -> impl Differential
```

---

## Core Principles

### 1. Hash-Based Pruning

The fundamental optimization: **if two nodes have the same hash, their entire subtrees are identical and can be skipped**.

```yaml
before@abc           after@abc
├─ node1@xyz         ├─ node1@xyz
│  └─ ...            │  └─ ...
└─ node2@def         └─ node2@def
   └─ ...               └─ ...
```

If `before.hash == after.hash`, we emit **no changes** and return immediately.

### 2. Sparse Tree Representation

A **sparse tree** is a subset of nodes from the original tree that represents only the portions that differ. Instead of walking the entire tree structure, we maintain only the nodes that are relevant to the diff.

### 3. Prune-Expand-Stream Cycle

The algorithm alternates between:
- **Prune**: Remove nodes with identical hashes from both sparse trees
- **Expand**: Replace branch nodes with their children to expose more detail
- **Repeat**: Until only differing segment (leaf) nodes remain
- **Stream**: Two-cursor walk over remaining segments to yield changes

### 4. Ordered Comparison

Both trees store entries in sorted order by key. Once we reach segments, we can walk them in parallel using a two-cursor algorithm, similar to merging sorted arrays.

---

## Algorithm Structure

### High-Level Flow

```
differentiate(before, after)
  ├─ If both None → return empty stream
  ├─ If before None → add_all(after)
  ├─ If after None → remove_all(before)
  └─ delta = Delta::from((before, after))
     ├─ delta.expand() // Prune and expand until only segments remain
     └─ delta.stream() // Two-cursor walk to yield changes
```

### Delta Structure

The `Delta` struct maintains two sparse trees representing the differing portions:

```rust
/// Represents the difference between two prolly trees.
///
/// `Delta` maintains two [`SparseTree`]s representing the differing portions of
/// two trees being compared. The first tree is the "before" state, and the second
/// is the "after" state.
pub(crate) struct Delta<'a, ...>(
    SparseTree<'a, ...>,  // before
    SparseTree<'a, ...>,  // after
)
```

### Expand Phase

The expand phase alternates between pruning and expanding until only segments remain:

```rust
impl Delta {
    async fn expand(&mut self) -> Result<()> {
        let Self(before, after) = self;
        loop {
            // Prune shared nodes using two-cursor walk
            before.prune(after);

            // Try to expand both sides
            let before_expanded = before.expand().await?;
            let after_expanded = after.expand().await?;

            // If neither side expanded, we're done expanding
            if !before_expanded && !after_expanded {
                break;
            }
        }

        // Final prune after reaching segments
        before.prune(after);

        Ok(())
    }
}
```

**Prune step:**
- Compare nodes at the same position in both sparse trees
- If hashes match, remove both nodes (identical subtree)
- If hashes differ, keep both nodes for further expansion

**Expand step:**
- For each branch node remaining in the sparse trees
- Load its children from storage
- Replace the branch node with its children in the sparse tree

**Termination:**
- Loop ends when nothing is left to expand (we reached segments)
- At this point, all branches have been expanded or pruned

### Stream Phase

Once only segments remain, perform a two-cursor walk:

```rust
fn stream(&self) -> impl Stream<Item = Result<Change>> {
    let mut before_cursor = self.before.segments();
    let mut after_cursor = self.after.segments();

    loop {
        match (before_cursor.peek(), after_cursor.peek()) {
            // Keys match
            (Some(b), Some(a)) if b.key == a.key => {
                if b.value != a.value {
                    // Value changed
                    yield Change::Remove(b);
                    yield Change::Add(a);
                }
                // Keys match - advance both
                before_cursor.next();
                after_cursor.next();
            }
            // Before key < after key
            (Some(b), Some(a)) if b.key < a.key => {
                yield Change::Remove(b);
                before_cursor.next();
            }
            // Before key > after key
            (Some(b), Some(a)) => {
                yield Change::Add(a);
                after_cursor.next();
            }
            // Only before remains
            (Some(b), None) => {
                yield Change::Remove(b);
                before_cursor.next();
            }
            // Only after remains
            (None, Some(a)) => {
                yield Change::Add(a);
                after_cursor.next();
            }
            // Both exhausted
            (None, None) => break,
        }
    }
}
```

---

## Example Walkthrough

### Setup

```yaml
before:                          after:
  IndexNode@abc                    IndexNode@xyz
  ├─ Segment@111 [k1,k2]          ├─ Segment@111 [k1,k2]    (same)
  └─ Segment@222 [k3,k4]          └─ Segment@333 [k3,k5]    (different)
```

### Iteration 1: Initial Prune

**Sparse trees:**
- before: `[IndexNode@abc]`
- after: `[IndexNode@xyz]`

**Prune check:**
- `abc != xyz` → keep both

**Result:** No pruning, proceed to expand

### Iteration 2: Expand Branches

**Expand:**
- Load children of `IndexNode@abc`: `[Segment@111, Segment@222]`
- Load children of `IndexNode@xyz`: `[Segment@111, Segment@333]`

**Sparse trees:**
- before: `[Segment@111, Segment@222]`
- after: `[Segment@111, Segment@333]`

### Iteration 3: Prune Segments

**Prune check:**
- Position 0: `Segment@111 == Segment@111` → remove both
- Position 1: `Segment@222 != Segment@333` → keep both

**Sparse trees:**
- before: `[Segment@222]`
- after: `[Segment@333]`

**Only segments check:** Yes → exit expand loop

### Stream Phase

**Two-cursor walk:**
- before: `[k3:v3, k4:v4]`
- after: `[k3:v3', k5:v5]`

| Step | Before | After | Action |
|------|--------|-------|--------|
| 1 | `k3:v3` | `k3:v3'` | Keys match, values differ → `Remove(k3:v3)`, `Add(k3:v3')` |
| 2 | `k4:v4` | `k5:v5` | `k4 < k5` → `Remove(k4:v4)` |
| 3 | Done | `k5:v5` | Only after → `Add(k5:v5)` |

**Output stream:**
```rust
Remove(Entry { key: k3, value: v3 })
Add(Entry { key: k3, value: v3' })
Remove(Entry { key: k4, value: v4 })
Add(Entry { key: k5, value: v5 })
```

## Remaining Problem: Different Tree Heights

> ⚠️  **Suboptimal Behavior with Height Differences**
>
> The current algorithm exhibits suboptimal behavior when comparing trees of different heights. When comparing nodes at different heights, the hash comparison will **never** discover equal nodes, causing the algorithm to expand all the way down to segments before discovering what to prune.
>
> **Example: Containment Scenario**
>
> Consider two trees where `tree1` has 100 entries and `tree2` has 1000 entries, with the first 100 entries identical:
>
> ```yaml
> tree1 (height=2):
>   IndexNode@abc
>   └─ Segments [k1..k100]
>
> tree2 (height=3):
>   IndexNode@xyz
>   └─ IndexNode@def
>      ├─ Segments [k1..k100]    (identical to tree1!)
>      └─ Segments [k101..k1000]
> ```
>
> **What happens:**
> 1. Compare `IndexNode@abc` (height 2) with `IndexNode@xyz` (height 3)
> 2. Hashes don't match (different heights, different structure)
> 3. Expand `tree2` one level: `IndexNode@def` vs `IndexNode@abc`
> 4. Still don't match (different heights)
> 5. Continue expanding until both are at segment level
> 6. **Only then** discover that segments for k1..k100 are identical
> 7. End up reading far more nodes than necessary
>
> **Impact:**
> - Reads O(height_difference × branching_factor) extra nodes
> - In the example above, we read the entire tree2 structure down to segments
> - Negates the benefit of hash-based pruning for the shared portion
>
> **Potential Solutions:**
> - Height-aware comparison: when heights differ, expand the taller tree first
> - Hash comparison at segment level: compare segment hashes even when nested at different depths
> - Hybrid approach: use range queries to skip ahead in taller trees

---

## Edge Cases


### Height Mismatch (Same Data)

```yaml
before (height=1):
└─ Segment [k1:v1]

after (height=3):
└─ IndexNode
   └─ IndexNode
      └─ Segment [k1:v1]
```

Both will expand to `[Entry(k1:v1)]`, resulting in an empty diff. However, this requires expanding the entire `after` tree (see performance warning above).

### Subset Relationship

```yaml
before: [k1, k2, k3]
after:  [k1, k2, k3, k4, k5]
```

If `after` fully contains `before` with additional entries, the algorithm will:
1. Identify shared subtrees through hash comparison (if at same height)
2. Yield only `Add(k4)` and `Add(k5)` changes
3. **BUT**: if trees have different heights, will expand all the way to segments (suboptimal)
