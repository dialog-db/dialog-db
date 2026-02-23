# ECS-Style Query V2: Magic Set Optimization Review

## ECS-Style Query V2 Architecture

The `feat/ecs-style-query-v2` branch implements a Datalog-inspired query engine over DialogDB's append-only fact store. The "ECS" (Entity-Component-System) analogy maps to the data model:

- **Entity** = the `this` field (opaque ID identifying a subject)
- **Component** = an `Attribute` (e.g. `person/name`, `person/age`) — a named, typed property
- **System** = a `DeductiveRule` that derives virtual components by pattern-matching existing facts

### Key Layers

| Layer | Files | Role |
|---|---|---|
| **Concepts** | `concept.rs`, `application/concept.rs` | Group N attributes into a single entity "view" (like an ECS archetype). A `ConceptDescriptor` is a set of `AttributeSchema` entries. |
| **Facts / Relations** | `fact.rs`, `application/fact.rs`, `application/relation.rs` | Atomic EAV triples stored in prolly-tree indexes (AEV, EAV, VAE). |
| **Formulas** | `formula.rs`, `application/formula.rs` | Pure computation premises (string length, `LIKE`, etc.) — no I/O. |
| **Rules** | `rule.rs`, `predicate/deductive_rule.rs` | Datalog rules: `conclusion :- premise_1, premise_2, ...`. Conclusion is always a concept. Premises can be facts, concepts (recursive), formulas, or negations. |
| **Planner** | `planner.rs`, `analyzer.rs` | Cost-based greedy planner. Iteratively picks cheapest viable premise, extends environment, repeats. `Analysis` tracks viable/blocked status and cost. |
| **Execution** | `planner.rs` (`Chain`, `Fork`, `Join`) | `Join` = sequential pipeline of steps. `Fork` = union of alternative joins (one per rule). Evaluation is lazy via async streams. |

### Concept Evaluation Flow (`application/concept.rs:226-266`)

When querying `Match::<Employee>`:

1. Collect all rules for the concept's operator (the concept's own default rule mapping attributes to fact lookups, plus user-installed deductive rules).
2. Plan each rule via `Join::from(&rule.premises)` then `join.plan(&scope)` — re-ordering premises given current bindings.
3. Fork all plans into a `Fork` (union).
4. For each incoming answer, extract parameter bindings, evaluate the forked plan, merge results back.

This is **top-down, demand-driven evaluation** — rules expand at the point they're referenced.

## Where Magic Set Optimization Fits

### The Problem Magic Sets Solve

Magic set transformation optimizes **recursive Datalog** evaluated bottom-up. When a query has bound arguments, naive bottom-up computes *all* derivable facts even though only a subset matching the bindings is needed.

Example:
```
ancestor(X, Y) :- parent(X, Y).
ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).

?- ancestor("Alice", Y)
```

Naive bottom-up computes all ancestor pairs. Magic sets rewrite the program to propagate `X = "Alice"` into the recursive computation via synthetic "magic" predicates, pruning irrelevant derivations.

### Current State: Top-Down with Binding Propagation

The engine **already achieves the core property** magic sets provide — for non-recursive rules:

1. `ConceptApplication::evaluate` receives input with bound variables.
2. `extract_parameters` maps those bindings into the rule's internal parameter space.
3. `join.plan(&scope)` **re-plans** premises given those bindings — this is exactly "sideways information passing."
4. `Analysis::update` incrementally recalculates viability and cost as variables become bound.

**For non-recursive rules, the engine already does what magic sets would provide.**

### Where Magic Sets Would Add Value

Magic sets become relevant for:

1. **Recursive concepts** — if you define transitive closure or hierarchical rules, the current engine has no recursion detection or fixed-point iteration.
2. **Multi-rule sharing** — when multiple rules share sub-computations, magic sets factor out shared work. Currently each `Fork` branch evaluates independently.
3. **Semi-naive evaluation** — if `dialog-dbsp` evolves into an incremental maintenance layer, magic sets bridge goal-directed queries with bottom-up incremental maintenance.

## Tradeoffs

### Benefits

| Benefit | Detail |
|---|---|
| **Enables safe recursion** | No recursion support today. Magic sets + semi-naive evaluation is the standard approach for recursive Datalog with termination guarantees. |
| **Unlocks DBSP integration** | `dialog-dbsp` exists as scaffolding. Magic sets bridge top-down queries with bottom-up incremental maintenance. |
| **Better multi-rule optimization** | Rules sharing sub-computations could reuse intermediate results instead of redundant fact lookups across `Fork` branches. |
| **Principled binding propagation** | While `plan(&scope)` works, it's ad hoc. Magic sets formalize "which bindings flow where" provably correctly for arbitrary rule programs. |

### Costs

| Cost | Detail |
|---|---|
| **Significant complexity** | Requires: adornment analysis, SIPS, program rewriting with magic predicates, bottom-up evaluator with stratification. Large surface area for ~5K lines of query code. |
| **Non-recursive case already optimized** | `plan(&scope)` and `Analysis::update` already achieve binding propagation. Magic sets for non-recursive rules add overhead with no benefit. |
| **ECS model limits recursion depth** | Entity-component lookups and view derivation rarely need deep recursive traversals. May be premature optimization. |
| **Conflicts with streaming architecture** | Engine evaluates lazily via `async_stream`/`try_stream!`. Magic sets assume materialized intermediate relations. Hybrid model (lazy + materialized) is a significant change. |
| **Two planning strategies** | Existing top-down planner + magic-set bottom-up evaluator. Keeping both correct and consistent is non-trivial. |
| **Complicates cost model** | Current model is clean: `SEGMENT_READ_COST`, `RANGE_SCAN_COST`, `INDEX_SCAN`, `CONCEPT_OVERHEAD`. Magic intermediates need their own cost estimates. |

## Integration Points

If magic sets were integrated:

1. **`application/concept.rs:232-239`** — Rule collection and `Fork` construction. Detect recursive rules and route through magic set transformer.
2. **`predicate/deductive_rule.rs`** — `DeductiveRule` needs adorned variants. `compile()` needs adornment analysis.
3. **`planner.rs`** — New `MagicJoin` or similar for magic-set-transformed strata.
4. **`analyzer.rs`** — New `Analysis` variants for magic predicates (filters, not generators).
5. **`dialog-dbsp/`** — Natural home for semi-naive fixed-point evaluator.

## Recommendation

**Defer magic set integration.** The existing top-down planner with scope-aware replanning already achieves the primary benefit for the current feature set. Integrate when one of these triggers:

1. **Recursive rules become required** — transitive closure, hierarchical data, reachability queries.
2. **DBSP incremental evaluation materializes** — magic sets bridge goal-directed queries with bottom-up maintenance.
3. **Profiling reveals redundant work** across `Fork` branches sharing sub-computations.

The current architecture has clean separation between planning (`analyzer.rs`, `planner.rs`) and execution (`Chain`, `Fork`, `Join`) that would be disrupted by magic set rewriting. The ECS-style use cases don't typically require recursion.
