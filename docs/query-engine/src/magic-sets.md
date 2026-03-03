# Relation to Magic Sets

Dialog's query planning draws from the **magic set transformation**, a
classical optimization from deductive databases. This chapter explains the
connection and where Dialog's approach differs.

## Background: The Magic Set Transformation

The magic set transformation (Bancilhon, Maier, Sagiv & Ullman, 1986) solves a
problem in Datalog evaluation: how to avoid computing all possible derivations
when you only need a specific subset.

### The Problem

Consider a recursive rule for computing ancestors:

```
ancestor(X, Y) :- parent(X, Y).
ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).
```

A naive bottom-up evaluation computes *all* ancestor relationships in the
database, even if the query only asks for `ancestor(alice, ?who)`. This is
wasteful.

### The Solution

Magic sets transform the program to be **goal-directed**: they push the query's
constraints into the rules, generating "magic" predicates that filter
computation:

```
magic_ancestor(alice).
magic_ancestor(Z) :- magic_ancestor(X), parent(X, Z).
ancestor(X, Y) :- magic_ancestor(X), parent(X, Y).
ancestor(X, Y) :- magic_ancestor(X), parent(X, Z), ancestor(Z, Y).
```

Now only ancestors reachable from `alice` are computed.

### Adornments

The key concept is the **adornment**, a string of `b` (bound) and `f` (free)
markers that describes which parameters are known at query time:

```
ancestor^bf(alice, ?who)
          ^^
          |+- free (unknown)
          +-- bound (alice is known)
```

Each adornment generates a specialized version of the rule. Different calling
patterns produce different adornments and thus different specialized rules.

## How Dialog Relates

Dialog's approach mirrors magic sets in several ways:

### Adornments as Cache Keys

Dialog uses `Adornment`, a `u64` bitfield, to capture the binding pattern
when a concept is queried:

```rust
pub struct Adornment(u64);  // bit i = 1 if parameter i is bound
```

This serves the same role as the adornment string in magic sets: it identifies
which parameters are known, enabling specialization.

### Pushing Selections Into Joins

In classical magic sets, the adornment-specialized rule pushes selections
(constraints from bound parameters) into the join order. Dialog achieves the
same effect through **re-planning**:

```
Original plan (nothing bound):
  1. Scan person/name -> bind ?person, ?name     [cost: 1000]
  2. Look up person/age -> bind ?age              [cost: 100]

Re-planned with name bound:
  1. Look up person/name with name=Alice -> bind ?person  [cost: 100]
  2. Look up person/age -> bind ?age                       [cost: 100]
```

The planner pushes the bound parameter's constraint into the first step, same
as magic sets would generate a specialized rule.

### Lazy Compilation

Magic sets transform the program before evaluation. Dialog compiles plans on
demand: the first time a concept is queried with a particular adornment, the
plan is compiled and cached. Subsequent queries with the same adornment reuse
the cached plan.

```
Magic sets: transform program -> evaluate
Dialog:     query arrives -> derive adornment -> check cache -> compile if miss -> evaluate
```

## Where Dialog Differs

### Non-Recursive Rules

Dialog currently handles **non-recursive** rules only. For non-recursive rules,
the magic set transformation reduces to pushing selections into joins, which is
what Dialog's planner does. No "magic" predicates are needed because there's no
fixpoint computation.

### No Program Transformation

Classical magic sets rewrite the Datalog program itself, generating new rules.
Dialog doesn't transform rules. It re-plans existing rule premises for each
adornment. The rules stay the same; only the execution order changes.

### Greedy vs Optimal Planning

Magic sets produce an optimal join order for each adornment (assuming accurate
cost estimates). Dialog uses a greedy algorithm that picks the cheapest
available premise at each step. This is simpler but doesn't guarantee global
optimality. In practice, the greedy approach works well because the cost model
correctly ranks the important tradeoffs.

### Plan Caching vs Tabling

In recursive Datalog with magic sets, **tabling** (memoization of intermediate
results) is needed for termination and efficiency. Dialog's adornment cache
currently stores **plans** (execution strategies), not **results**.

However, the infrastructure is designed to support result-level caching
(tabling) in the future. The `Adornment` type already serves as a natural
cache key for memoized results, which would be needed for fixpoint evaluation
of recursive rules.

## Future: Recursive Rules

If Dialog adds support for recursive rules, the adornment system provides a
foundation:

1. **Adornments** already identify calling patterns
2. **Plan caching** can be extended to **result caching** (tabling)
3. **Fixpoint evaluation** would iterate until no new results are produced
4. **Stratification** (which Dialog already uses for negation) would determine
   evaluation order for mutually recursive rules

The design draws from Tekle & Liu (2011), "More Efficient Datalog Queries:
Subsumptive Tabling Beats Magic Sets," which shows how tabling with
adornment-based specialization can outperform traditional magic set
transformations.

## Summary

| Aspect | Magic Sets | Dialog |
|--------|-----------|--------|
| Binding pattern | Adornment string (bf) | `Adornment(u64)` bitfield |
| Specialization | Rewrite rules | Re-plan premises |
| When | Before evaluation | On demand (lazy) |
| Caching | Tabling (results) | Plan cache (strategies) |
| Recursion | Full support | Not yet (infrastructure ready) |
| Join ordering | Optimal per adornment | Greedy per adornment |
