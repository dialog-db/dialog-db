# Aggregation

Aggregation over query results, modeled on Datomic: an aggregate is a property
of the **query projection** (Datomic's `:find`), never of a rule or a premise.
This is what makes a malformed aggregation *unwriteable* rather than
*rejected by the analyzer* — the concern the design is built around.

## The problem this avoids

The classic Datalog aggregation bug is a variable that is both aggregated and
also appears as a plain (grouping) term — e.g. `total(?dept, sum(?salary))`
where `?salary` also leaks in as a grouping key. Systems that put the aggregate
inside the rule head or body must then *check* that the grouped and aggregated
variable sets are disjoint, and reject rules that violate it. That is a
stringly-typed validation: the bad rule is representable, then refused.

Datomic removes the site where the bug could be written. Aggregation lives
only in `:find`; the `:where` body is ordinary pattern matching that has no
notion of aggregation at all. So:

- The body cannot express "aggregated yet also matched" — it is
  aggregation-free by construction.
- A `:find` element is either a plain variable (a grouping key) or an aggregate
  expression over a variable — a sum type, never both.
- The grouping set is **derived, not declared**: it is exactly the plain
  variables in `:find`. There is nothing to keep consistent, so there is
  nothing to check.

The whole class of "grouped and aggregated" bugs has no syntactic home. There
is no analyzer rule for it because there is no way to state the error.

## Where this lands in dialog

The map of the current rule/query model (`notes/` and the rule analyzer) shows
two facts that make the Datomic approach a clean fit:

1. A **rule head is a name-keyed record of typed fields** with no positional or
   role structure; head↔body binding is by name coincidence. Adding aggregation
   *into* rules would mean adding brand-new role information to
   `ConceptDescriptor` and a new cross-field disjointness check in `analyze()`.
   The Datomic model needs none of that: rules are untouched.
2. A query evaluates to a **stream of `Match` (bindings rows)**. The projection
   from that stream to results is currently *implicit* (a `Query<C>` shapes rows
   into concept instances by field name); there is no explicit `:find` spec.
   That stream is exactly the result table an aggregate folds over, and the
   projection boundary is exactly where Datomic's `:find` sits.

So aggregation is a new **projection layer at the query result boundary**, not
a change to `DeductiveRule`, `ConceptDescriptor`, `Premise`, or the analyzer.

## Design

A query gains an explicit projection spec (the `:find`-analog). Each projected
position is a sum type:

```
enum Find {
    Group(Variable),           // a grouping key
    Aggregate(Aggregate, Variable),  // fold over the variable within the group
}
```

- `Group | Aggregate` being a sum type is the entire invalid-by-construction
  story: a projected position is one or the other, never both. A variable
  cannot be tagged as grouped-and-aggregated because that state does not exist.
- The **grouping key set is derived**: the `Group` positions. Not declared, so
  not checkable-and-wrong.
- Evaluation: run the query as today to get the `Match` stream, then group the
  rows by the tuple of `Group` variables' values and fold each group with the
  `Aggregate` functions. A group-by-then-fold over the finished result table.

### Aggregate functions and output types

`count: any -> UnsignedInt`, `sum: numeric -> numeric`, `min/max: T -> T`,
`avg`, `count-distinct`, etc. The output type is computed from the projected
variable's already-inferred type — no new inference arm inside rule analysis,
just a function applied at the projection.

### Why stratification is not needed here

In Datomic (and here) aggregates run over the **finished result set** of the
query, after any recursive rules have reached fixpoint. The aggregate never
observes a relation mid-computation, so there is no "aggregate over a relation
still being derived" hazard — the negation-through-recursion stratification
machinery does not need an aggregate arm. This holds precisely because
aggregate results do **not** feed back into rule derivation. Not allowing that
feedback is the feature, not a limitation to work around.

## Scope

Phase 1: aggregation at the ad-hoc query projection only — the pure Datomic
model. No aggregate ever appears in a rule.

Deliberately out of scope (a separate, later decision): a *concept* whose
fields are aggregates (a materialized "DeptTotal" concept). Datomic does not
have this; it would reintroduce the "aggregate defines a head field" question
and its cross-field check. If wanted, it is an explicit extension beyond the
Datomic model, and should be named as one — not folded in silently here.
