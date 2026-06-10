# Chapter one completion: analyzed rule plans per scope

> Design note. Closes out the operator-IR refactor (PR #347 line of work) by making the rule type
> hierarchy carry its guarantees: a descriptor is analyzed into a rule that is plannable by construction,
> and that rule produces a concrete evaluation plan per scope. Removes the layers that were added but
> never wired in. Behavior-preserving; NOT the cost redesign (a separate later chapter).

## The type hierarchy (the target)

Three layers, each a stronger guarantee than the last:

1. **`DeductiveRuleDescriptor`** — the parsed / serializable form (from the wire or a builder). No
   guarantees; just data.
2. **`DeductiveRule`** — *the analyzed rule*. Produced by analyzing a descriptor: verified for every
   invariant (type inference, required-head-not-optional, coalesce contract, conclusion grounding) and
   therefore **plannable by construction**. Holds the analysis — `{ conclusion, premises, types, graph }`
   — not a pre-baked plan.
3. **`Conjunction`** — *the concrete evaluation plan* for a *specific scope*, produced on demand by
   `DeductiveRule::plan(scope)`. **The plan is what evaluates**: `Conjunction::evaluate(...)` runs the
   ordered steps. The rule analyzes and plans; the plan it returns evaluates.

The same hierarchy holds for `InductiveRule` (and the `Rule` enum wraps either).

```
DeductiveRuleDescriptor --analyze--> DeductiveRule --plan(scope)--> Conjunction --evaluate--> results
   (data)                            (verified, plannable)          (scope-specific plan)
```

## What is wrong today, against this model

- **The constructor runs plan-then-analyze, inverted.** `DeductiveRuleDescriptor::compile` plans the
  premises *first* (`Planner::plan`), then analyzes the planned steps
  (`analyze(&join.steps)`, `DependencyGraph::from_steps(&[Plan])`). So the graph is an output of planning
  and cannot feed it. It should be **analyze first** (graph from premises, order-independent), then the
  rule plans per scope.
- **`DeductiveRule` holds a pre-baked `join: Conjunction`.** The plan is scope-specific, so storing one
  plan is wrong; the rule should hold the *analysis* and produce a plan on demand. (Replan today even
  reconstructs premises from the stored plan via `as_premise()` to re-run the planner.)
- **`AnalyzedRule` is a redundant side-record.** Step 2 added `analysis: Option<AnalyzedRule>` next to
  the `join`, because `DeductiveRule` was already "the thing holding a Conjunction." In this model
  `DeductiveRule` *is* the analyzed rule, so `AnalyzedRule` collapses into it — no separate type, no
  `Option`.
- **`adorn` and the graph are dead in production.** Wired in by this change: the planner consumes the
  graph, and `categorize`/`adorn` is its single feasibility path.

## Concrete changes

### Analysis is the constructor — and narrowing is its output
- `DeductiveRuleDescriptor::analyze(self) -> Result<DeductiveRule, TypeError>` (renamed from `compile`):
  converts `when`/`unless` into premises, runs type inference, **applies `apply_types` to narrow the
  premises once**, runs the invariant checks, builds the `DependencyGraph` from the narrowed premises,
  and returns the analyzed `DeductiveRule`. On any invariant failure it returns the error (with the
  in-progress rule for display) — so a constructed `DeductiveRule` is guaranteed valid.
- **Narrowing is an analysis output, not a planning step.** Today `apply_types` runs inside
  `Planner::plan` (on every plan/replan). Move it to analysis: the rule stores **already-narrowed
  premises**. Consequence: the planner needs neither `TypeEnv::infer` nor `apply_types` — it receives
  narrowed premises and only orders + lowers them. No `types` parameter threads into the planner.
  Verified safe: nothing downstream reads the inferred `types` after planning — they are used *only* for
  narrowing, so once narrowing is baked into the premises, `types` is purely analysis-internal.
- `DependencyGraph::from_steps(&[Plan])` becomes `DependencyGraph::from_premises(&[Premise])` (done).
- `TypeEnv::infer` consumes premises (done).

### `DeductiveRule` holds the analysis, plans per scope
- Fields: `{ conclusion, premises (narrowed), graph }` (the former `AnalyzedRule` content; `types`
  optional since nothing downstream reads them). No `join`, no `Option<AnalyzedRule>`.
- `plan(&self, scope) -> Conjunction`: orders the **already-narrowed** premises greedily using the
  `graph` for feasibility (via the shared `categorize`/`adorn`) and `estimate(scope)` for cost, lowering
  each chosen premise to a `Plan`. No narrowing, no inference here. This is the only planning entry; it
  does not rebuild the graph.
- `descriptor(&self)`: reconstructs `when`/`unless` from the stored **premises** directly — no
  `as_premise()` round-trip through a stored plan.
- No `evaluate` on the rule. Evaluation lives on the `Conjunction` that `plan(scope)` returns
  (`Conjunction::evaluate`, as today). A caller does `rule.plan(scope).evaluate(...)`.

### The planner consumes analysis
- The greedy ordering core takes premises + graph + types (i.e. a `&DeductiveRule` or its parts) + scope,
  and emits the ordered `Conjunction`. Feasibility comes from the graph + `categorize`; cost from
  `estimate`. Replanning (`Conjunction::plan` / concept adornment replan via `ConceptRules::plan`) goes
  through the rule's `plan(scope)`, never premise reconstruction.

### Standalone queries (no rule)
- `session.rs` / `negation.rs` plan a single ad-hoc premise (a query, not a rule). They analyze that one
  premise on the fly (cheap) then plan — so the single planning core still consumes analysis. A thin
  convenience can wrap analyze-then-plan to keep call sites small.

## Legacy to remove (the point of this note)
- `DeductiveRule.join` (stored pre-baked plan) and `analysis: Option<AnalyzedRule>` — replaced by the
  analyzed-rule fields.
- The separate `AnalyzedRule` type — collapsed into `DeductiveRule`/`InductiveRule`.
- `DependencyGraph::from_steps(&[Plan])` — replaced by `from_premises`.
- `as_premise()` uses in the descriptor/replan path that the stored-premises design makes dead (verify
  per call site; `as_premise` may remain where still genuinely needed).
- One of `adorn` / `categorize` if redundant once the planner calls the shared core directly — do not
  leave a tested-but-unused `adorn`.
- Any `Candidate` schema-walk the graph-driven planner makes redundant.

## Done when (chapter one complete)
1. `DeductiveRuleDescriptor::analyze -> DeductiveRule` (analyzed, plannable by construction); same for
   inductive. No pre-baked plan stored on the rule; no separate `AnalyzedRule`.
2. The planner plans **from the analysis graph**, per scope, not by reconstructing premises.
3. Analyzer + planner carry no vestigial code — every legacy surface the new model replaced is removed,
   verified.

Guarded throughout by the characterization, update-vs-adorn equivalence, plan-ordering, and cost-model
tests — behavior-preserving.

## Explicitly out of scope
Cost redesign (the 16-arm `Cardinality::estimate` table, magic constants, work-class/selectivity model).
Cost (`estimate`) and feasibility (`categorize`) are already decoupled — `estimate` reads `is_bound(env)`
directly, never `Requirement`/schema — so cost is a separate later chapter, not interleaved with this.

## The graph as a dependency index (not the ordering driver)

Clarification (user): the dependency graph's role is to encode *dependencies* so that, given an
`env`, we can identify which premises are *affected* by a binding — which become newly feasible, and
which premise depends on which. The `requires[i]` edges (premise `j` binds a variable premise `i`
needs) are exactly this: when a variable enters scope, the edges name the premises it unblocks,
without rescanning everything.

This separates two consumers:
- **Ordering** needs per-*scope* feasibility (`adorn`/`categorize`) because choice groups shift with
  the bound set; the static graph alone can't drive the greedy cost ordering.
- **Demand / incremental re-planning** needs the static dependency structure: "binding X affects
  premises {…}" — the affected-premises lookup the `requires` edges provide.

So the graph is retained as the **dependency index** the incremental-subscriptions / demand work
consumes, while `adorn` drives ordering. A greedy planner can also *use* the edges to re-check only
the premises affected by the round's newly-bound variables, instead of rescanning all remaining
premises each round — an optimization the edges enable.
