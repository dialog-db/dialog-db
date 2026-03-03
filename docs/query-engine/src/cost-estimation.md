# Cost Estimation

The planner needs a way to compare premises. Each premise type provides an
`estimate(env) -> Option<usize>` method that returns a cost reflecting the
expected I/O effort, given which variables are currently bound.

## Cost Constants

Defined in `dialog-query/src/schema.rs`:

| Constant | Value | Meaning |
|----------|-------|---------|
| `SEGMENT_READ_COST` | 100 | Direct lookup into a single tree segment |
| `RANGE_READ_COST` | 200 | Bounded range read (may span a few segments) |
| `RANGE_SCAN_COST` | 1,000 | Scan with partial constraints |
| `INDEX_SCAN` | 5,000 | Broad scan with minimal constraints |
| `CONCEPT_OVERHEAD` | 1,000 | Added cost for rule evaluation in concepts |

These are relative values chosen to establish ordering, not wall-clock time.
The important property is:

```
SEGMENT_READ < RANGE_READ < RANGE_SCAN < INDEX_SCAN
```

## Relation Query Costs

A `RelationQuery` has four terms: `the`, `of`, `is`, `cause`. The cost depends
on how many of `(the, of, is)` are bound (constants or variables already in the
environment). `cause` is always optional and does not affect cost.

### Cardinality::One

An attribute with `Cardinality::One` stores at most one value per
`(attribute, entity)` pair. The winning value is determined by causal ordering.

| Bound terms | Cost | Index used | Explanation |
|-------------|------|-----------|-------------|
| 3 (the + of + is) | 100 | EAV | Direct segment lookup |
| 2 (e.g., the + of) | 100 | EAV | Narrow range within one segment |
| 1 (e.g., the only) | 1,000 | AEV | Scan all entities for this attribute |
| 0 | unbound | n/a | Rejected by planner |

### Cardinality::Many

An attribute with `Cardinality::Many` can store multiple values per entity.
Cost is higher because more data must be examined.

| Bound terms | Cost | Index used | Explanation |
|-------------|------|-----------|-------------|
| 3 (the + of + is) | 200 | EAV | Range read (multiple values possible) |
| 2 (e.g., the + of) | 1,000 | EAV | Range scan over values |
| 1 (e.g., the only) | 5,000 | AEV | Scan all entities and values |
| 0 | unbound | n/a | Rejected by planner |

### VAE Penalty

When only the value (`is`) is known, the query uses the VAE index. For
`Cardinality::One`, this requires a **secondary lookup** to verify that the
found claim is actually the current winner for its `(attribute, entity)` pair.
This adds `SEGMENT_READ_COST` to the base cost.

## Concept Query Costs

A concept query expands into multiple relation queries (one per attribute).
The cost is estimated by simulating this expansion:

1. If `this` (the entity) is bound, sum the cost of each attribute lookup
   with the entity constrained.
2. If `this` is unbound, find the cheapest "lead" attribute (the one that will
   scan to discover entities) and add the costs of the remaining attribute
   lookups as if the entity were bound.

In both cases, `CONCEPT_OVERHEAD` is added to reflect the cost of rule
evaluation:

```
concept_cost = sum(attribute_costs) + CONCEPT_OVERHEAD
```

## Formula Costs

Formulas are pure computations, no I/O. Their cost is a small fixed value
declared in the `#[derived(cost = N)]` annotation. This ensures formulas are
cheaper than I/O operations and are scheduled after the premises that bind
their inputs.

Since formulas typically require all input parameters to be bound, they start
as `Blocked` candidates and only become `Viable` after earlier premises bind
their inputs.

## Constraint Costs

Constraints (equality checks between terms) have zero cost since they only
filter existing answers without I/O. However, they require both operands to be
bound, so they're scheduled after the premises that produce those bindings.

## Schema and Requirements

Each premise advertises a `Schema`, a map of parameter names to `Field`
descriptors:

```rust
pub struct Field {
    pub description: String,
    pub content_type: Option<Type>,
    pub requirement: Requirement,
    pub cardinality: Cardinality,
}
```

The `Requirement` enum tells the planner whether a parameter is a prerequisite
or a product:

```rust
pub enum Requirement {
    Required(Option<Group>),  // must be bound (possibly via choice group)
    Optional,                  // will be produced
}
```

A `Group` ties parameters together: if any member of the group is bound, the
whole group is satisfied. For a `RelationQuery`, the `(the, of, is)` parameters
share a choice group since knowing any one of them is enough to constrain the
query.

## How Cost Drives Planning

The planner's greedy algorithm always picks the cheapest viable candidate. This
produces good orderings because:

1. **Constants first**: Premises with more constants have lower costs and are
   selected early, binding variables for later premises.

2. **Cascade effect**: Each bound variable reduces the cost of subsequent
   premises. A premise that costs 1,000 with one constraint might cost 100
   with two.

3. **Formulas last**: Since formulas require bound inputs and have low cost,
   they schedule after the I/O premises that produce their inputs.

4. **Negation last**: Negated premises (`Unless`) require all their variables
   to be bound and don't produce new bindings, so they schedule at the end.

This greedy approach doesn't guarantee the globally optimal ordering, but it
works well in practice because the cost function captures the right tradeoff:
more constraints means cheaper I/O.
