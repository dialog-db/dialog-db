# Query Cost Model and Index Selection Improvements

## Overview

The query planner orders premises (rule conditions) to minimize total execution
cost. Cost reflects the size of key ranges scanned in the prolly tree, which
directly maps to network roundtrips when fetching tree nodes from a remote
store. Tighter key prefixes mean fewer nodes traversed.

This document specifies three coordinated changes:

1. **Cost model**: replace the constraint-count approach with an index-aware
   model that reflects actual key prefix tightness.
2. **Index selection**: update `Artifacts::select` to choose the index that
   produces the tightest prefix for the given selector.
3. **Cardinality-one verification**: adapt the winner-verification path for the
   new `{the, is}` index routing.

---

## Background: Key Layouts

Each artifact is stored in three indexes with different key orderings. The tag
byte distinguishes them. Field sizes:

| Field | Bytes |
|---|---|
| tag | 1 |
| entity | 64 |
| attribute | 64 |
| value_type | 1 |
| value_ref | 32 |

**Total key: 162 bytes.**

### EAV (Entity-Attribute-Value) — tag 0

```
[tag 1B][entity 64B][attribute 64B][value_type 1B][value_ref 32B]
```

Optimal when entity (`of`) is part of the prefix.

### AEV (Attribute-Entity-Value) — tag 1

```
[tag 1B][attribute 64B][entity 64B][value_type 1B][value_ref 32B]
```

Optimal when attribute (`the`) leads the prefix and entity may follow.

### VAE (Value-Attribute-Entity) — tag 2

```
[tag 1B][value_type 1B][value_ref 32B][attribute 64B][entity 64B]
```

Optimal when value (`is`) leads the prefix, optionally followed by attribute.

### How range scans work

Range scans construct a `(start_key, end_key)` pair. Known fields are set to
their actual value in both keys. Unknown fields are set to `0x00..` in the start
key and `0xFF..` in the end key. **Only fields that form a contiguous prefix
from the beginning of the key actually narrow the range.** A known field after
an unknown gap does not help the tree traversal — it is post-filtered by
`matches_selector`.

---

## Change 1: Index Selection

**File:** `rust/dialog-artifacts/src/artifacts.rs` — `Artifacts::select`

The current logic:

```rust
if selector.entity().is_some() {
    EAV
} else if selector.attribute().is_some() {
    AEV
} else {
    VAE
}
```

This fails for `{the, is}` (attribute + value known, entity unknown). It picks
AEV, which gives a 65-byte prefix (`[tag][attribute]`). But VAE would give a
98-byte prefix (`[tag][value_type][value_ref][attribute]`) — significantly
tighter.

### Proposed logic

```rust
if selector.entity().is_some() {
    // Entity is the first field in EAV. Whether attribute or value
    // are also known, entity always gives the best leading prefix.
    Self::scan_eav(index, selector)
} else if selector.attribute().is_some() && selector.value().is_some() {
    // Both attribute and value known, no entity.
    // VAE: [tag][value_type][value_ref][attribute] = 98B prefix
    // AEV: [tag][attribute] = 65B prefix (value is at end, post-filtered)
    // VAE wins.
    Self::scan_vae(index, selector)
} else if selector.attribute().is_some() {
    // Attribute only → AEV: [tag][attribute] = 65B prefix
    Self::scan_aev(index, selector)
} else {
    // Value only (or other) → VAE
    Self::scan_vae(index, selector)
}
```

**Note:** When entity is known, EAV always wins because entity is 64 bytes and
appears first. Adding attribute extends the prefix to 129 bytes. Adding value
instead only helps at the end (post-filtered), but that's still the best
available option since no index has `[entity][value]` adjacent.

---

## Change 2: Cost Model

**File:** `rust/dialog-query/src/schema.rs` — `Cardinality::estimate`

Replace the current constraint-count model with one that reflects which fields
are known and how that maps to key prefix tightness and expected result size.

### Cost tiers and rationale

Listed in order from cheapest to most expensive. Each tier describes: what's
known, which index is used, the effective key prefix, and why the cost is what
it is.

#### Tier 1: `{the, of, is}` — All three known

- **Index:** EAV
- **Prefix:** `[entity][attribute][value_type][value_ref]` — 162 bytes, full key
- **Behavior:** Near point-lookup. At most one entry matches (even for
  `Cardinality::Many`, multiple entries only arise from concurrent writes).
- **Cost:** `SEGMENT_READ` (100) for both ONE and MANY

#### Tier 2: `{of, the}` — Entity and attribute known

- **Index:** EAV
- **Prefix:** `[entity][attribute]` — 129 bytes
- **Behavior:** Scans values for a single (entity, attribute) pair.
  - `Cardinality::One`: typically 1 entry (the current winner), occasionally 2–3
    from concurrent writes. Near point-lookup.
  - `Cardinality::Many`: collection values for this entity. Could be tens to
    hundreds of entries, but the range is still tightly bounded.
- **Cost:** `SEGMENT_READ` (100) for ONE, `RANGE_READ` (200) for MANY

#### Tier 3: `{the, is}` — Attribute and value known

- **Index:** VAE (**changed from AEV** — see Change 1)
- **Prefix:** `[value_type][value_ref][attribute]` — 97 bytes
- **Behavior:** Finds entities that have a specific value for a specific
  attribute (e.g., "find all people named Alice").
  - `Cardinality::Many`: yields entries directly from the range.
  - `Cardinality::One`: each entry from the VAE scan is a *candidate* winner.
    Must perform a secondary lookup on EAV with `[entity][attribute]` prefix
    (129 bytes — tier 2 cost) to verify the candidate value actually wins the
    write race for that (entity, attribute) pair. If the winner matches `is`,
    yield; otherwise skip.
- **Cost:** `RANGE_READ` (200) for MANY, `RANGE_READ + SECONDARY_LOOKUP` (300)
  for ONE
- **Secondary lookup cost reasoning:** Each verification is a tier-2 lookup
  (SEGMENT_READ). The `SECONDARY_LOOKUP` constant (100) represents the
  amortized per-match cost. In practice, the number of matches N is small —
  few entities share the same value for a given attribute.

#### Tier 4: `{of}` — Entity only

- **Index:** EAV
- **Prefix:** `[entity]` — 65 bytes
- **Behavior:** Scans all attributes for one entity. Entities typically have
  O(10) attributes, so the range is narrow in practice despite the shorter
  prefix.
  - `Cardinality::One`: fewer entries (one per attribute).
  - `Cardinality::Many`: more entries (collections expand the result set).
- **Cost:** `RANGE_READ` (200) for ONE, `RANGE_SCAN` (1000) for MANY

#### Tier 4b: `{of, is}` — Entity and value known

- **Index:** EAV (no index has entity and value adjacent)
- **Prefix:** `[entity]` — 65 bytes (value is post-filtered)
- **Behavior:** Same scan as `{of}` alone. The value constraint is checked by
  `matches_selector` after fetching entries, so it does not reduce the number
  of tree nodes traversed.
- **Cost:** Same as `{of}` — `RANGE_READ` (200) for ONE, `RANGE_SCAN` (1000)
  for MANY
- **Planner implication:** binding a value variable when the entity is already
  bound does NOT reduce scan cost. The planner should not prefer a premise
  that binds `is` over one that binds `the` when `of` is already known.

#### Tier 5: `{the}` — Attribute only

- **Index:** AEV
- **Prefix:** `[attribute]` — 65 bytes
- **Behavior:** Scans all entities for one attribute. Attributes are typically
  shared by many entities (e.g., "person/name" applies to every person), so
  the range is wide.
  - `Cardinality::One`: one value per entity, but many entities.
  - `Cardinality::Many`: multiple values per entity, across many entities.
- **Cost:** `RANGE_SCAN` (1000) for ONE, `INDEX_SCAN` (5000) for MANY

#### Tier 6: `{is}` — Value only

- **Index:** VAE
- **Prefix:** `[value_type][value_ref]` — 34 bytes
- **Behavior:** Broadest single-field scan. Finds all (attribute, entity) pairs
  that reference this value.
  - `Cardinality::Many`: yields entries directly.
  - `Cardinality::One`: same secondary verification as tier 3, but N (number of
    matches to verify) is potentially larger because no attribute constraint
    limits the scan.
- **Cost:** `INDEX_SCAN` (5000) for MANY, `RANGE_SCAN + SECONDARY_LOOKUP`
  (1100) for ONE

### Proposed implementation

```rust
pub fn estimate(&self, the: bool, of: bool, is: bool) -> Option<usize> {
    match (the, of, is, self) {
        // Tier 1: all known — near point-lookup
        (true,  true,  true,  _)    => Some(SEGMENT_READ),

        // Tier 2: entity + attribute — 129B prefix
        (true,  true,  false, One)  => Some(SEGMENT_READ),
        (true,  true,  false, Many) => Some(RANGE_READ),

        // Tier 3: attribute + value — 97B prefix via VAE
        (true,  false, true,  Many) => Some(RANGE_READ),
        (true,  false, true,  One)  => Some(RANGE_READ + SECONDARY_LOOKUP),

        // Tier 4: entity only — 65B prefix, few attrs per entity
        (false, true,  false, One)  => Some(RANGE_READ),
        (false, true,  false, Many) => Some(RANGE_SCAN),

        // Tier 4b: entity + value — 65B prefix, value post-filtered
        (false, true,  true,  One)  => Some(RANGE_READ),
        (false, true,  true,  Many) => Some(RANGE_SCAN),

        // Tier 5: attribute only — 65B prefix, many entities
        (true,  false, false, One)  => Some(RANGE_SCAN),
        (true,  false, false, Many) => Some(INDEX_SCAN),

        // Tier 6: value only — 34B prefix, broadest scan
        (false, false, true,  Many) => Some(INDEX_SCAN),
        (false, false, true,  One)  => Some(RANGE_SCAN + SECONDARY_LOOKUP),

        // No constraints — unbound, rejected
        (false, false, false, _)    => None,
    }
}
```

### Constants

Keep the existing constants. The values are intuitive approximations of
relative cost rather than measured values, and the primary goal is correct
*ordering* of premises:

```rust
pub const SEGMENT_READ: usize = 100;      // ~point lookup, 1-2 tree nodes
pub const RANGE_READ: usize = 200;        // bounded range, a few tree nodes
pub const RANGE_SCAN: usize = 1_000;      // broader range, multiple segments
pub const INDEX_SCAN: usize = 5_000;      // near-full index traversal
pub const SECONDARY_LOOKUP: usize = 100;  // per-match verification cost
pub const CONCEPT_OVERHEAD: usize = 1_000; // rule evaluation overhead
```

Rename `SEGMENT_READ_COST` → `SEGMENT_READ`, `RANGE_READ_COST` → `RANGE_READ`,
`RANGE_SCAN_COST` → `RANGE_SCAN` for consistency (all other constants omit the
`_COST` suffix). Or keep names as-is to minimize churn — implementer's call.

---

## Change 3: Cardinality-One Verification Path

**File:** `rust/dialog-query/src/relation/query.rs` —
`RelationQuery::evaluate_cardinality_one`

Currently, the code decides between sliding-window (grouped scan) vs
secondary-lookup (per-match verification) based on whether entity or attribute
is known in the *Term*:

```rust
let entity_known = matches!(&self.of, Term::Constant(_));
let attribute_known = matches!(&self.the, Term::Constant(_));

if entity_known || attribute_known {
    Either::Left(self.select_winners(source, answers))
} else {
    // verify_winner path
}
```

With the index selection change, `{the, is}` queries now use VAE instead of
AEV. In the VAE index, entries are ordered by
`[value][attribute][entity]` — the (attribute, entity) pairs are NOT
contiguous when scanning by value+attribute, because different entities
interleave. However, for the `{the, is}` case on VAE, the scan range is
`[value_type][value_ref][attribute][min_entity..max_entity]`, which IS
grouped by entity. So the sliding window approach still works here —
entities appear in order within the attribute group.

**Wait — this needs careful analysis.** The sliding window in `select_winners`
groups by `(attribute, entity)`:

```rust
let same_group = candidate
    .as_ref()
    .is_some_and(|c| c.the == artifact.the && c.of == artifact.of);
```

In the VAE scan for `{the, is}`, attribute is fixed and entities are scanned in
order. Each entity appears at most once per attribute (they're unique keys), so
there's no grouping needed — each entry is its own group. The sliding window
would emit every entry, which is wrong for Cardinality::One — we need to verify
that each candidate actually wins.

### Proposed fix

Change the branch condition to reflect whether the scan produces contiguous
`(attribute, entity)` groups. This is true when the index orders by entity
first (EAV) or attribute first (AEV) with the leading field known. It is NOT
true when scanning VAE:

```rust
let entity_known = matches!(&self.of, Term::Constant(_));
let attribute_known = matches!(&self.the, Term::Constant(_));
let value_known = matches!(&self.is, Term::Constant(_));

// Sliding window works when results are grouped by (attribute, entity):
// - EAV scan (entity known): groups are [entity][attribute][values...]
// - AEV scan (attribute known, value unknown): groups are [attribute][entity][values...]
// It does NOT work for VAE scans because (attribute, entity) aren't contiguous.
let use_sliding_window = (entity_known || attribute_known) && !(!entity_known && value_known);

if use_sliding_window {
    Either::Left(self.select_winners(source, answers))
} else {
    // Secondary lookup path: for each candidate from the scan,
    // verify it wins via an EAV lookup on [entity][attribute]
    ...
}
```

Or more simply:

```rust
let can_group = entity_known || (attribute_known && !value_known);
```

This means:
- `{of, the}`, `{of}`, `{of, the, is}` → sliding window (EAV, grouped)
- `{the}` → sliding window (AEV, grouped)
- `{the, is}` → secondary lookup (VAE, not grouped by entity)
- `{is}` → secondary lookup (VAE, not grouped)

---

## Test Updates

Existing tests in `candidate.rs` and `planner.rs` assert specific cost values.
These will need updating to match the new model. Key tests affected:

- `it_costs_same_when_fully_bound`: currently asserts ONE=100, MANY=200 for 3/3
  constraints. New model: both are 100. **Update assertion.**
- `it_costs_one_constant_two_variables`: asserts RANGE_SCAN (1000) for `{the}`
  with ONE. Still correct in new model.
- `it_costs_more_for_cardinality_many`: asserts ONE=RANGE_SCAN, MANY=INDEX_SCAN
  for `{the}` only. Still correct.
- `it_reduces_cost_for_env_variables`: asserts SEGMENT_READ after binding
  entity (making it `{the, of}`). Still correct.
- `it_matches_fact_cost_value_bound`: asserts RANGE_SCAN for MANY with `{the,
  is}`. New model: RANGE_READ (200). **Update assertion.**
- `it_restores_cost_when_replanned_to_empty_scope`: asserts INDEX_SCAN (5000)
  for MANY with `{the}` only. Still correct.
- Various concept overhead tests: concept overhead is additive, so adjust the
  base cost expectations per the new tier and add CONCEPT_OVERHEAD.

Add new tests for cases that were previously indistinguishable:

- `{the, is}` ONE should cost RANGE_READ + SECONDARY_LOOKUP (300)
- `{the, is}` MANY should cost RANGE_READ (200)
- `{of}` ONE should cost RANGE_READ (200), not RANGE_SCAN (1000)
- `{of, is}` should cost the same as `{of}` (value post-filtered)
- `{the, of, is}` ONE and MANY should both cost SEGMENT_READ (100)

---

## Migration and Compatibility

These changes affect planning output (premise ordering) but not query
correctness. Any valid ordering produces the same results — only performance
differs. No data migration is needed.

The changes should be deployed together since the cost model assumes the index
selection routes `{the, is}` to VAE, and the cardinality-one verification
assumes VAE for that case.

## Summary of files to modify

| File | Change |
|---|---|
| `rust/dialog-query/src/schema.rs` | New `Cardinality::estimate` match arms |
| `rust/dialog-artifacts/src/artifacts.rs` | Index selection for `{the, is}` → VAE |
| `rust/dialog-query/src/relation/query.rs` | Cardinality-one branch condition |
| `rust/dialog-query/src/planner/candidate.rs` | Update test assertions |
| `rust/dialog-query/src/planner.rs` | Update test assertions |
