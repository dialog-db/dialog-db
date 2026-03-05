# Unification

This chapter explains how variable bindings accumulate during query evaluation
and how conflicts eliminate matches.

## What Is a Match?

A `Match` is a set of variable bindings accumulated during evaluation. It maps
variable names to bound values with provenance tracking.

## The Evaluation Pipeline

Evaluation is a pipeline where each premise receives a stream of matches and
produces a (possibly larger or smaller) stream:

```
Empty match
    │
    ▼
┌───────────┐    ┌───────────┐    ┌───────────┐
│ Premise 1 ├───►│ Premise 2 ├───►│ Premise 3 ├───► Results
└───────────┘    └───────────┘    └───────────┘
```

Each premise is a filter-and-expander:
- Zero expansions → match eliminated
- Multiple expansions → match multiplied (one per hit)

## How Matches Expand

When a premise finds a match, it clones the incoming match and merges new
bindings:

```
Incoming: { ?person → alice }
Premise: (person/age, ?person, ?age)
Claim:   (person/age, alice, 30)
         ↓
Result:  { ?person → alice, ?age → 30 }
```

## How Matches Get Eliminated

**Unification failure** — a premise tries to bind a variable to a value
different from its existing binding. The match is discarded.

**No matches** — a premise finds no matching claims. The incoming match
produces no output.

**Negation** — an `Unless` premise inverts the logic: if the inner
proposition produces any match, the incoming match is eliminated; if not,
it passes through unchanged.

## How Joins Work

Shared variable names act as join keys:

```
Premise 1: (person/name, ?person, ?name)
Premise 2: (person/age,  ?person, ?age)
```

When premise 1 binds `?person` to `alice`, premise 2 must also match `alice`.
Any claim with a different entity fails unification.

## Provenance

Every binding carries a `Factor` recording how the value was obtained:

- **`Selected`** — from a specific claim matched by an attribute query
- **`Derived`** — computed by a formula, with references to input factors
- **`Parameter`** — provided externally as a query parameter

A variable can accumulate multiple factors that agree on the same value
(confirming the binding from different sources). Disagreement is a unification
failure.

## Seeding

Every query starts with a single empty match (the **seed**). This flows into
the first premise, which expands it into concrete matches. Without the seed,
no matches would flow through the pipeline.
