# Attribute Queries

An `AttributeQuery` is the most fundamental premise type. It matches claims
against a `(the, of, is, cause)` pattern and produces matches with variable
bindings for each hit.

## Structure

```rust
pub enum DynamicAttributeQuery {
    All(AttributeQueryAll),    // Cardinality::Many — yield all matches
    Only(AttributeQueryOnly),  // Cardinality::One — yield winner per (attribute, entity)
}
```

Each position is a `Term` — either a constant that constrains the match or a
variable that captures the matched value.

## Evaluation Flow

For each incoming match:

1. **Resolve variables** — check if query variables are already bound.
   If `?person` is bound to `Entity(alice)`, the `of` position becomes a
   constant constraint.
2. **Build selector** — convert resolved terms into an `ArtifactSelector`.
3. **Choose index** — the store picks EAV, AEV, or VAE based on what's
   constrained (see [Indexes](./indexes.md)).
4. **Scan claims** — iterate over matching claims from the chosen index.
5. **Handle cardinality** — `Many` yields all claims; `One` yields only the
   winner per `(attribute, entity)` pair (latest causal ordering).
6. **Produce matches** — for each matching claim, clone the incoming match and
   bind the claim's components to corresponding variables.

## Winner Selection (Cardinality::One)

The winner-selection strategy depends on the index:

- **EAV/AEV scan** — claims for the same `(attribute, entity)` are contiguous.
  A sliding window picks the winner within each group before yielding.
- **VAE scan** — claims are not contiguous by `(attribute, entity)`. Each
  candidate requires a secondary EAV lookup to verify it is the current winner.
  This adds `SEGMENT_READ_COST` to the base cost.

## Schema

An `AttributeQuery` places `(the, of, is)` in a single choice group. Binding
*any one* is sufficient — the query is never blocked by the planner, though
cost varies with how much is bound.
