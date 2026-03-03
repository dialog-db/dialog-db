# Relation Queries

A `RelationQuery` is the most fundamental premise type. It matches claims in the
store against a `(the, of, is, cause)` pattern and produces answers with
variable bindings for each match.

## Structure

```rust
pub struct RelationQuery {
    the: Term<The>,           // attribute selector
    of: Term<Entity>,         // entity
    is: Term<Any>,            // value
    cause: Term<Cause>,       // provenance
    cardinality: Option<Cardinality>,
}
```

Each position is a `Term`, either a constant that constrains the match or a
variable that captures the matched value.

## Evaluation Flow

For each incoming answer in the stream:

1. **Resolve variables**: Check if any of the query's variable terms are
   already bound in the incoming answer. If `?person` is bound to
   `Entity(alice)`, the `of` position becomes a constant constraint.

2. **Build selector**: Convert the resolved terms into an `ArtifactSelector`
   that the store understands. The selector specifies constraints on
   attribute, entity, and value.

3. **Choose index**: The store picks the index based on what's constrained:
   - Entity known -> **EAV** index (scan by entity, then attribute)
   - Attribute known (entity unknown) -> **AEV** index (scan by attribute)
   - Value known (entity and attribute unknown) -> **VAE** index (scan by
     value)

4. **Scan claims**: Iterate over matching claims from the chosen index.

5. **Handle cardinality**: Filter claims based on cardinality.

6. **Produce answers**: For each matching claim, clone the incoming answer
   and merge `Evidence::Relation` to bind the claim's components to their
   corresponding variables.

## Cardinality Handling

### Cardinality::Many

All matching claims are yielded. If an entity has three tags, the query
produces three answers.

### Cardinality::One

Only the "winning" claim per `(attribute, entity)` pair is yielded. The winner
is the claim with the latest causal ordering.

The winner-selection strategy depends on the index used:

**EAV/AEV scan** (entity or attribute known):
Claims for the same `(attribute, entity)` group are contiguous in the index.
The evaluator uses a sliding window to track the current group and picks the
winner within each group before yielding.

**VAE scan** (only value known):
Claims for different `(attribute, entity)` groups are not contiguous.
The evaluator produces candidates and performs a secondary EAV lookup for each
to verify that the candidate is actually the current winner. This secondary
lookup adds `SEGMENT_READ_COST` to the base cost, which is why VAE queries
are more expensive.

## Cost Estimation

The `estimate(&env)` method counts how many of `(the, of, is)` are bound
(either as constants or as variables present in the environment) and delegates
to `Cardinality::estimate(the_bound, of_bound, is_bound)`:

```rust
pub fn estimate(&self, env: &Environment) -> Option<usize> {
    let the = self.the.is_bound(env);
    let of  = self.of.is_bound(env);
    let is  = self.is.is_bound(env);

    let base = self.cardinality?.estimate(the, of, is)?;

    // VAE penalty for Cardinality::One
    if !the && !of && is && self.cardinality == Some(Cardinality::One) {
        Some(base + SEGMENT_READ_COST)
    } else {
        Some(base)
    }
}
```

## Schema

A `RelationQuery` advertises a schema with four fields in a single choice
group. The choice group means that binding *any one* of `(the, of, is)` is
sufficient:

```
Schema for RelationQuery:

  "the"   Required(Group A)   attribute
  "of"    Required(Group A)   entity
  "is"    Required(Group A)   value
  "cause" Optional            provenance

  Group A: satisfied if any member is bound
```

This means a relation query is never blocked by the planner. It can always
execute, though with varying cost depending on how much is bound.

## Example

```rust
// Find all names in the store
let all_names = RelationQuery::new(
    Term::from(the!("person/name")),  // the: constant
    Term::var("person"),               // of:  variable (free)
    Term::var("name"),                 // is:  variable (free)
    Term::blank(),                     // cause: don't care
    Some(Cardinality::One),
);
// Cost: RANGE_SCAN_COST (1,000), one constraint, needs AEV scan

// Find Alice's name
let alice_name = RelationQuery::new(
    Term::from(the!("person/name")),
    Term::from(alice_entity),          // of: constant
    Term::var("name"),
    Term::blank(),
    Some(Cardinality::One),
);
// Cost: SEGMENT_READ_COST (100), two constraints, direct EAV lookup
```
