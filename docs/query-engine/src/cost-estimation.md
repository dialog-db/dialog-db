# Cost Estimation

Each premise provides an `estimate(env) -> Option<usize>` method returning a
cost that reflects expected I/O effort given which variables are currently bound.

## Cost Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `SEGMENT_READ_COST` | 100 | Direct segment lookup |
| `RANGE_READ_COST` | 200 | Bounded range read |
| `RANGE_SCAN_COST` | 1,000 | Scan with partial constraints |
| `INDEX_SCAN` | 5,000 | Broad scan, minimal constraints |
| `CONCEPT_OVERHEAD` | 1,000 | Rule evaluation overhead |

These are relative values for ordering, not wall-clock time.

## Attribute Query Costs

Cost depends on how many of `(the, of, is)` are bound and the cardinality.

### Cardinality::One

| Bound | Cost | Notes |
|-------|------|-------|
| 3 | 100 | Direct segment lookup |
| 2 | 100 | Narrow range within one segment |
| 1 | 1,000 | Scan all entities for this attribute |
| 0 | — | Rejected by planner |

When only the value (`is`) is known, the VAE index is used. For
`Cardinality::One` this requires a **secondary EAV lookup** to verify the
candidate is the current winner for its `(attribute, entity)` pair. This adds
`SEGMENT_READ_COST` to the base cost.

### Cardinality::Many

| Bound | Cost | Notes |
|-------|------|-------|
| 3 | 200 | Range read (multiple values possible) |
| 2 | 1,000 | Range scan over values |
| 1 | 5,000 | Scan all entities and values |
| 0 | — | Rejected by planner |

## Concept Query Costs

Sum of attribute lookup costs plus `CONCEPT_OVERHEAD`. When the entity is
unbound, the planner finds the cheapest lead attribute and estimates the rest
as if the entity were bound.

## Formula Costs

Formulas are pure computations — no I/O. Cost is a small fixed value from
`#[derived(cost = N)]`. They start as blocked candidates and become viable
after earlier premises bind their inputs.

## Constraint Costs

Constraints (`Equality`) have a cost of 1 — they only filter or infer bindings
without I/O. They require **one** of the two operands to be bound (not both).
If one operand is bound, the constraint infers the other; if both are bound, it
checks equality; if neither is bound, it cannot execute. Constraints use a
choice group, so the planner schedules them as soon as either operand is
available.

## How Cost Drives Planning

The greedy algorithm picks the cheapest viable candidate at each step:

1. **Constants first** — premises with more constants are cheaper and selected
   early, binding variables for later premises.
2. **Cascade effect** — each bound variable reduces subsequent costs.
3. **Formulas last** — require bound inputs, low cost, schedule after I/O.
4. **Negation last** — requires all variables bound, produces no new bindings.
