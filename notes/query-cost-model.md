# Query Cost Model

## The Problem

The query planner orders premises to minimize total execution cost. Each premise corresponds to a range scan over a prolly tree index. The size of that range determines how many tree nodes need to be traversed, and in a sparse replica, each node traversal may require a network roundtrip. Fewer nodes visited means fewer roundtrips. The cost model exists to give the planner a way to compare orderings.

Counting how many of the three triple components `{the, of, is}` are known is not sufficient. The actual scan cost depends on *which* components are known, because that determines which index is used, how much of the key is a contiguous prefix, and how many entries fall within the range.

Consider `{the, of}` vs `{the, is}`. Both have two components known. But `{the, of}` constructs a 129-byte key prefix that narrows to a single (entity, attribute) pair. `{the, is}` on the right index constructs a 97-byte prefix, on the wrong index it constructs a 65-byte prefix with the value post-filtered. These are not the same cost.

## Context: Key Layout

Dialog stores each artifact in a single prolly tree with three key layouts distinguished by a tag byte. Each key is 162 bytes:

```
EAV:  [tag 1B][entity 64B][attribute 64B][value_type 1B][value_ref 32B]
AEV:  [tag 1B][attribute 64B][entity 64B][value_type 1B][value_ref 32B]
VAE:  [tag 1B][value_type 1B][value_ref 32B][attribute 64B][entity 64B]
```

A range scan constructs a `(start_key, end_key)` pair. Known fields are set to their actual value in both keys. Unknown fields are set to `0x00..` in the start and `0xFF..` in the end. Only fields that form a contiguous prefix from the start of the key constrain the tree traversal. A known field after an unknown gap does not help; it gets post-filtered after fetching.

```
Example: {the, is} on AEV index

  AEV key:  [tag][attribute][entity][value_type][value_ref]
  Known:          ^^^^^^^^^^                    ^^^^^^^^^^
  Prefix:         65 bytes    ← attribute constrains the prefix
  Gap:                        ← entity unknown, 64 bytes of 0x00..0xFF
  Tail:                                         ← value known but after gap

  The tree walks all entities for this attribute. Value is checked
  after fetching each entry. The 65-byte prefix is all we get.

Example: {the, is} on VAE index

  VAE key:  [tag][value_type][value_ref][attribute][entity]
  Known:          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  Prefix:         97 bytes   ← value + attribute form contiguous prefix
  Tail:                                            ← entity unknown

  The tree walks only entities matching this (value, attribute) pair.
  97-byte prefix. Significantly tighter.
```

## Index Selection

Given a selector, choose the index whose key layout produces the longest contiguous prefix for the known fields.

```
entity known         → EAV  (entity leads the key)
attribute + value    → VAE  (value + attribute are contiguous, 97B prefix)
attribute only       → AEV  (attribute leads the key)
value only           → VAE  (value leads the key)
```

When entity is known, EAV always wins regardless of what else is known. Entity is 64 bytes and appears first. If attribute is also known, the prefix extends to 129 bytes. If value is known instead, it sits at the end of the EAV key and gets post-filtered, but EAV is still the best available option because no index places entity and value adjacent.

When entity is unknown but both attribute and value are known, VAE produces a 97-byte prefix while AEV produces only 65 bytes (value at the end, post-filtered). VAE wins.

## Cost Tiers

Costs are ordered from tightest range to broadest. The numeric values are intuitive weights, not measured quantities. Their purpose is to produce correct relative ordering during planning, not to predict actual latency. A constant named `SCAN` means a broader traversal than `READ`, which means a broader traversal than `SEGMENT`.

```
SEGMENT  = 100      1-2 tree nodes, near point-lookup
READ     = 200      small bounded range, a few nodes
SCAN     = 1000     broader range, multiple segments
FULL     = 5000     large portion of an index
VERIFY   = 100      per-match secondary lookup cost
CONCEPT  = 1000     overhead for rule evaluation in concept queries
```

### {the, of, is}: All three known

**Index:** EAV. **Prefix:** 162 bytes (full key).

Near point-lookup. At most one entry. Cardinality does not matter here because multiple entries only arise from concurrent writes, which is a transient condition.

**Cost:** `SEGMENT` for both ONE and MANY.

### {of, the}: Entity and attribute known

**Index:** EAV. **Prefix:** 129 bytes.

Scans values for a single (entity, attribute) pair.

For `Cardinality::One`, this is typically a single entry (the current winner). For `Cardinality::Many`, this is a collection, potentially tens to hundreds of entries, but the range is bounded tightly by the 129-byte prefix.

**Cost:** `SEGMENT` for ONE. `READ` for MANY.

### {the, is}: Attribute and value known

**Index:** VAE. **Prefix:** 97 bytes.

Finds entities that have a specific value for a specific attribute. For example: "find all people named Alice."

For `Cardinality::Many`, entries are yielded directly from the range.

For `Cardinality::One`, each entry from the VAE scan is a candidate that may or may not be the write-race winner for its (entity, attribute) pair. The VAE index does not group by (attribute, entity) in a way that allows a sliding window to pick winners. So each candidate requires a secondary lookup: scan EAV with the `[entity][attribute]` prefix (129 bytes, Tier 2) and check whether the winning value matches `is`. If it matches, yield. If not, skip.

**Cost:** `READ` for MANY. `READ + VERIFY` for ONE.

### {of}: Entity only

**Index:** EAV. **Prefix:** 65 bytes.

Scans all attributes for one entity. In practice, entities have O(10) attributes, so the range is narrow despite the shorter prefix.

**Cost:** `READ` for ONE. `SCAN` for MANY.

### {of, is}: Entity and value known

**Index:** EAV. **Prefix:** 65 bytes. Value post-filtered.

No index has entity and value adjacent. EAV gives a 65-byte entity prefix. The value constraint is checked by `matches_selector` after fetching each entry but does not reduce the number of tree nodes traversed.

This costs the same as `{of}` alone. The planner should not overvalue binding a value variable when the entity is already bound.

**Cost:** `READ` for ONE. `SCAN` for MANY.

### {the}: Attribute only

**Index:** AEV. **Prefix:** 65 bytes.

Scans all entities for one attribute. Attributes are typically shared by many entities ("person/name" applies to every person), so the range is wide despite having the same prefix length as `{of}`.

**Cost:** `SCAN` for ONE. `FULL` for MANY.

### {is}: Value only

**Index:** VAE. **Prefix:** 34 bytes.

Broadest single-field scan. Finds all (attribute, entity) pairs that reference this value.

For `Cardinality::Many`, entries are yielded directly.

For `Cardinality::One`, same secondary verification as `{the, is}`: for each (attribute, entity) from the scan, perform an EAV lookup on `[entity][attribute]` and check if the race winner matches `is`.

**Cost:** `FULL` for MANY. `SCAN + VERIFY` for ONE.

### Summary

```
Known       Index   Prefix    ONE              MANY
---------   -----   ------    ---------------  ---------------
{the,of,is} EAV     162B      SEGMENT (100)    SEGMENT (100)
{of,the}    EAV     129B      SEGMENT (100)    READ    (200)
{the,is}    VAE      97B      READ+V  (300)    READ    (200)
{of}        EAV      65B      READ    (200)    SCAN    (1000)
{of,is}     EAV      65B      READ    (200)    SCAN    (1000)
{the}       AEV      65B      SCAN    (1000)   FULL    (5000)
{is}        VAE      34B      SCAN+V  (1100)   FULL    (5000)
```

## Winner Verification for Cardinality One

When an attribute has `Cardinality::One`, only the write-race winner for each (entity, attribute) pair should be yielded. The strategy depends on the scan index.

**Sliding window.** When results are grouped by (attribute, entity) in key order, a sliding window can buffer the current group and emit the winner when the group boundary changes. This works for:
- EAV scans (entity leads, attributes follow, values are grouped)
- AEV scans with attribute known and value unknown (entities are grouped under the attribute)

**Secondary lookup.** When results are not grouped by (attribute, entity), each candidate is verified individually. For each candidate `{the, of, is}` from the primary scan, perform an EAV scan with the `[of][the]` prefix (129 bytes). Find the winner in that range. If the winner's value matches `is`, yield. Otherwise skip. This applies to:
- VAE scans for `{the, is}` (entities are in order but each is its own group since attribute is fixed; no multi-value grouping to resolve)
- VAE scans for `{is}` only (attribute varies, no grouping)

The condition for choosing sliding window vs secondary lookup:

```
sliding_window = entity_known OR (attribute_known AND NOT value_known)
```

## Premise Ordering

The planner assigns each premise a cost based on the tiers above, given the currently bound variables. It then selects premises greedily: at each step, pick the viable premise with the lowest cost, execute it, add its bindings to the environment, and re-estimate remaining premises.

### Greedy approach

The greedy algorithm runs in O(N^2) where N is the number of premises. For each of N steps, it scans remaining premises to find the cheapest viable one. This is simple to implement, simple to reason about, and performs well when the cheapest next step is clearly distinguished. For most real-world queries with a handful of premises, greedy produces an optimal or near-optimal ordering.

### Where greedy falls short

Greedy can produce suboptimal orderings when multiple premises tie on cost. With the index-aware cost model, ties are less frequent than with a naive count-based model. But ties still occur, particularly when two premises are both in the `{the}` tier with the same cardinality.

When premises tie, greedy picks one arbitrarily (first encountered). But the two tied premises may bind different variables, and one set of bindings may reduce downstream costs more than the other. Greedy cannot see this because it only looks one step ahead.

Example:

```
P1: (person/name, ?person, ?name)       {the} only → SCAN (1000)
P2: (dept/members, ?dept, ?person)       {the} only → SCAN (1000)
P3: (dept/budget, ?dept, ?budget)        {the} only → SCAN (1000)

Greedy picks P1 first (arbitrary tie). Binds ?person.
P2 becomes {the, is} → READ (200). P3 still {the} → SCAN (1000).
Total: 1000 + 200 + 1000 = 2200.

Better: P2 first. Binds ?dept and ?person.
P1 becomes {the, is} → READ (200). P3 becomes {the, is} → READ (200).
Total: 1000 + 200 + 200 = 1400.
```

The greedy algorithm cannot recover from picking P1 first because it never considers how P2's bindings would cascade to P3.

### Held-Karp DP as potential improvement

The Held-Karp algorithm finds the minimum-cost ordering by dynamic programming over subsets. The state is a bitmask of which premises have been evaluated. For each subset, it records the minimum total cost to evaluate that subset in some order.

```
state:  S ⊆ {P1, ..., Pn}     (bitmask of evaluated premises)
bound(S) = union of variables bound by premises in S, plus initial bindings

cost(∅) = 0
cost(S) = min over Pi ∈ S:
    cost(S \ {Pi}) + estimate(Pi, bound(S \ {Pi}))

Final answer: cost({P1, ..., Pn}), backtrack to recover the ordering.
```

Complexity is O(2^N * N) states times O(N) transitions. For typical query sizes this is negligible:

```
N    Greedy (N^2)    Held-Karp (2^N * N)
5    25              160
8    64              2,048
10   100             10,240
15   225             491,520
```

Planning runs once per adornment and the result is cached. Even for N=15, the DP table is computed in microseconds. The cost of one network roundtrip dwarfs the planning overhead by orders of magnitude, so slower planning that saves even one roundtrip is a net win.

A reasonable path forward: keep the greedy algorithm as the default for its simplicity and predictability. When greedy encounters ties (multiple premises share the lowest cost), apply Held-Karp over the tied subset and the remaining premises to break the tie optimally. This gives the best of both approaches: O(N^2) in the common case, O(2^N * N) only when it matters.

## Decision

**Build an index-aware cost model that reflects key prefix tightness.**

The cost model distinguishes which triple components are known, not just how many. Index selection routes `{the, is}` queries to VAE instead of AEV. Cardinality-one winner verification uses secondary lookups when the primary scan does not produce contiguous (attribute, entity) groups.

Start with the greedy premise ordering algorithm. Held-Karp DP is a future improvement for tie-breaking.
