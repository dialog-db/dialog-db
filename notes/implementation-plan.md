# Implementation record: operator IR, SIPS-owned-by-analysis, sound optionals

> Sequencing record for the `feat/operator-ir` chapter. Turns the design in
> [`planning-adornment-and-cost.md`](./planning-adornment-and-cost.md) and
> [`incremental-subscriptions.md`](./incremental-subscriptions.md) into an ordered series of small,
> always-green steps. **Status: the chapter is complete** — steps 1, 2, 4, 5a, 5b, the
> analyze-then-plan inversion, and the sound-optionals milestone (scalar associative layer,
> `MaybeQuery`, one optionality encoding, checked types) all landed; 5c, 6, and 7 remain open and are
> tracked on the roadmap. Current-truth engine documentation lives in
> [`query-engine-design.md`](./query-engine-design.md); the as-built deltas from this plan are recorded
> at the end of this note.

## Guiding constraints

- **Always green.** Every PR leaves the workspace compiling and all tests passing. No PR is a
  "checkpoint" that needs the next one to be correct.
- **Behavior-preserving until proven otherwise.** Steps that restructure (IR lowering, graph wiring)
  must not change query results or plan ordering. Each ships with a test asserting equivalence where
  feasible (e.g. plan order unchanged, same rows out).
- **Each PR is one idea.** Reviewable in one sitting; a reviewer can hold the whole change in their head.
- **The seams already exist.** `ConceptRules::plan` already caches per-adornment plans (the JIT
  worklist). `DependencyGraph` already computes binds/needs/edges (the SIPS skeleton) but is discarded.
  `Binding`/`Match::bind` already implement the three-state cell merge. We are *connecting and
  generalizing* existing pieces, not greenfielding.

## Current state

Branch `feat/operator-ir` (off `feat/type-inference-v2`) has uncommitted WIP that lowered `Plan` from a
struct to an enum-with-`evaluate` (Scan/Formula/Constraint/Concept/Negate, each carrying a `Header`).
It compiles and is clippy-clean. The design has since shifted underneath it (feasibility-as-`adorn`,
analysis owns the SIPS, premise derivable not stored). **Decision for step 1:** keep the enum-IR shape
(it is sound and on the path) but land it minimally first, *before* the larger graph-wiring and
feasibility changes, so each lands as its own reviewable PR rather than one entangled change.

## The steps

### Step 1 — Land the operator IR (`Plan` enum with `evaluate`)

**Goal.** Move execution off the syntactic AST onto a compiled `Plan` enum. `evaluate` dispatches on the
variant; `Premise`/`Proposition` stop carrying `evaluate`.

**Scope.** The current WIP, finished and squared away:
- `Plan` is `enum { Scan, Formula, Constraint, Concept, Negate }`, each variant `(Header, payload)`.
- `Header { premise, cost, binds, env }` — **premise retained for now** (derivable; removed in step 4).
- Negation filter moves onto `Plan::Negate` (the seam later magic-sets work hooks).
- `Concept` variant delegates to `ConceptQuery::evaluate` (no concept lowering yet).
- Lowering happens once in `Planner::plan` after `apply_types`; consumers read `premise()` accessor.

**Done when.** Compiles, clippy-clean, `test:native:debug` green; `evaluate` gone from `Premise`/
`Proposition`/`DynamicAttributeQuery`'s pass-through layers (kept on leaf query types and the trait).
Dead `Negation::evaluate` removed if now unused.

**Risk.** Low. Behavior-preserving by construction (same leaf evaluators, same order).

### Step 2 — Make analysis own the SIPS (compute the graph, stop discarding it)

**Goal.** `analyze()` becomes load-bearing: it returns the `DependencyGraph` and that graph is carried
on the analyzed rule / conjunction, not recomputed and dropped.

**Scope.**
- `DeductiveRule` / `Conjunction` carry the `DependencyGraph` (or `AnalyzedRule`) produced at analysis.
- `analyze()` is wired into the rule-construction path so its result is *kept* (today only its `Err` is
  used, as a validation gate — `rule.rs:127`).
- No planner behavior change yet — the planner still computes what it computes; the graph is now
  *available* alongside. This isolates "compute + retain the SIPS" from "consume it."

**Done when.** The graph is reachable from a planned rule; a test asserts it matches what the planner
derives. Still behavior-preserving (planner unchanged).

**Risk.** Low–medium. Mostly plumbing; the graph already exists and is tested.

### Step 3 — DROPPED (mis-framed)

The original step 3 ("planner consumes the analysis-built graph instead of rebuilding it") rested on a
premise that does not hold once the code flow is examined:

- **The graph is an *output* of planning, not an input.** `DependencyGraph::from_steps(&join.steps)`
  and `analyze()` both run on the *already-ordered* steps, after `Planner::plan` has done the ordering.
  The `Candidate` machinery that orders premises runs *before* any graph exists, so for the initial plan
  there is no graph to consume.
- **The static graph cannot replace the planner's per-scope categorization.** `from_steps` computes
  `binds`/`needs` at empty scope (only *constant*-satisfied choice groups count as bound).
  `Candidate::from`/`update` compute the same baseline but then mutate it per scope — a blocked
  candidate's `requires` shrink as earlier steps bind variables, and choice groups satisfied by a *bound
  variable* flip required→binds. Substituting the static graph would change plan ordering.
- **On the path where consuming a precomputed graph would apply (replanning), the graph isn't reachable**
  (it lives on the rule, not the `Conjunction`), and the per-scope dynamics above still force a redo.

The genuinely shared, genuinely static work both the planner and `from_steps` do is the *per-premise
schema categorization*. Extracting that into one shared function is exactly what step 5 (`adorn`) does —
so the dedup belongs there, not in a separate "feed the graph to the planner" step. Step 2's retained
graph remains valuable as the SIPS the demand-reification work consumes later; it is simply not a
planner input. Proceed directly to step 4, then step 5.

### Step 4 — Drop the stored premise; derive it from the IR

**Goal.** Remove `premise` from `Header`. The leaf payload + the graph subsume it; analysis/replan/
descriptor read derived methods.

**Scope.**
- `Plan` gains `schema()` / `parameters()` / `estimate()` (from payload) and `as_premise()` /
  variant-introspection (for the `Assert`/`Unless` + inner-attribute consumers in `deductive.rs`,
  `inductive.rs`, `types.rs`).
- `analyze()` / `DependencyGraph::from_steps` / type inference read these instead of `step.premise()`.
- `Header { cost, binds, env }` — no premise.

**Done when.** No `premise` field; all 17 former `premise()` call sites read derived data; behavior
unchanged. This is the step that makes the IR *replace* the AST rather than shadow it.

**Risk.** Medium. Touches every former premise consumer. Lands after step 3 so the graph is already the
source of binding info; this step only removes the redundant copy.

### Step 5 — Separate feasibility from cost: introduce `adorn`

**Goal.** Split the fused `estimate(env) -> Option<usize>` into a **feasibility** verdict and a **cost**
number. `adorn(bound) -> Result<Binds, Infeasible>` answers can-it-run + what-it-binds + why-not; cost
is asked only of feasible premises.

`Requirement` is woven through ~72 sites in 12 files (schema, planner, every constraint/attribute leaf,
the formula cell model). Replacing it wholesale would be a big-bang change against the always-green
principle, so step 5 is itself a sub-series, each part shippable and green.

**Step 5a — introduce `adorn`, derived (DONE).** Add `Plan::adorn(&BTreeSet<String>) -> Result<Binds,
Infeasible>` plus the `Binds` and `Infeasible` types, *derived from the existing `Requirement` schema* —
the same two-pass categorization `Candidate::from` runs (constant/bound-satisfied choice groups, then
slot classification), generalized to an arbitrary `bound` set. Purely additive: nothing changes
behavior, `adorn` is a new tested view. `Infeasible::NeedsAll(set)` mirrors the planner's `requires`.
A test pins `adorn` to the planner: for each planned step, `adorn(step.env())` is `Ok` and binds exactly
`step.binds()`.

**Step 5b — planner consumes `adorn`.** `Candidate::from` sources viability/binds from `adorn` instead
of re-walking the schema inline. This is the dedup the dropped step 3 was reaching for, now done via the
per-premise function.

Key finding (characterization, committed first): `Candidate::update` is an *incremental optimization*,
not part of the contract. Its internal state — including a Viable-arm stickiness asymmetry on scope
shrink — is unreachable in real planning, because replanning rebuilds candidates fresh from premises
(`Conjunction::plan` -> `Planner::from(Vec<Premise>)`) and a forward pass only grows scope. So `adorn`
(a stateless recompute) need NOT match `update`'s internal state; it only needs to produce the same
*output plan*. That observable — plan order + per-step binds + total cost across replans — is pinned by
`mod plan_ordering` in `planner.rs`; `Candidate::from`'s construction categorization is pinned by `mod
characterization` in `candidate.rs`. With those guardrails, 5b is free to replace `from`/`update` with
`adorn`-based logic and prove equivalence at the plan level. Do not over-specify `update`'s internals.

**Step 5c — enrich the vocabulary.** Only once `adorn` is the feasibility path, reconsider the
`Requirement`/`Group` schema: introduce the richer `Infeasible` shapes (`NeedsAnyOf` for equality;
`NeedsKOf` for genuinely atomic k-of-n) and the declarable per-premise `Feasibility` descriptor that
replaces per-slot flags. `estimate` keeps cost only, called after `adorn` is `Ok`. Most multidirectional
cases are instead handled by decomposition (step 6), so `KOf` stays rare.

**Done when (5 overall).** Feasibility and cost are distinct; the planner's categorization goes through
`adorn`; new tests cover the `Infeasible` reasons.

**Risk.** Medium, but contained by the sub-series: 5a is additive, 5b is behavior-preserving dedup, 5c
is where the vocabulary actually changes.

### Step 6 — Decompose multidirectional constraints into directional sub-premises

**Goal.** Follow the propagator model: a multidirectional constraint (`math/sum`, `product`) lowers into
a set of directional sub-premises sharing variables, each a trivial one-output `Prefix`. The planner
runs whichever becomes feasible first. Removes the need for `KOf` feasibility.

**Scope.**
- Lowering for arithmetic/relational formulas emits N directional `Plan` nodes (adder/subtractor/…)
  instead of one k-of-n node.
- Confirm `Equality` already is this shape (two inverse copies); generalize the pattern.
- `KOf` stays only for genuinely atomic premises (likely none after this).

**Done when.** `math/sum` with any 2 of 3 bound plans and evaluates correctly via the matching
directional node; tests cover all three input patterns.

**Risk.** Medium–high. Changes formula lowering and possibly the formula/cell model. Sequenced last
because it depends on `adorn` (step 5) and is the most novel. Could be split into its own follow-up
series if it grows.

### Step 7 (deferred) — Cost as `(class, selectivity)`

Lift the magic-number constants onto an explicit work-class ladder, and add a selectivity tie-breaker
(narrower scan wins within a class). Deferred: the scalar ladder preserves current behavior; this is an
optimization that the demand-reification work will motivate. Tracked, not scheduled.

## Dependency order

```
1 (IR)  →  2 (analysis owns graph)  →  3 (planner consumes graph)  →  4 (drop premise)
                                                                         ↓
                                          5 (adorn: feasibility/cost split)
                                                                         ↓
                                          6 (decompose multidirectional)
                                                                         ↓
                                          7 (cost class+selectivity, deferred)
```

Steps 1–4 are the **IR + SIPS-ownership** track: behavior-preserving restructuring that makes the
compiled form the source of truth and connects the dormant analysis phase to planning. Steps 5–6 are
the **propagator-feasibility** track: they change the requirement vocabulary and constraint modeling.
Step 7 is cost refinement, deferred.

After step 6 the engine is positioned for the incremental-subscriptions work: `adorn` is the reified
demand function (which vars a premise needs/binds), the graph is the SIPS to demand-transform, and the
`Plan` IR is the place to attach incremental evaluation.

## What this plan deliberately does NOT do

- No eager enumeration of adornments (the papers forbid it; JIT+cache stays — `ConceptRules::plan`).
- No change to the merge lattice (equality-only `Present` stays; narrowing is incremental-subscriptions
  scope).
- No demand reification / incremental subscriptions here — that is the *next* project, unblocked by
  this one. This plan ends at "the planner is a SIPS-driven, feasibility/cost-separated, propagator-
  decomposed engine over a compiled IR."

## The analyze-then-plan inversion (folded chapter)

Mid-chapter, the constructor flow was inverted from plan-then-analyze to analyze-then-plan, giving the
rule hierarchy its guarantees:

```
DeductiveRuleDescriptor --analyze--> DeductiveRule --plan(scope)--> Conjunction --evaluate--> results
   (data)                            (verified, plannable)          (scope-specific plan)
```

- Analysis is the constructor: type inference, the invariant checks (required-head, Coalesce,
  negated-optional), and the `DependencyGraph` all run from the premises, before any execution order is
  chosen. A constructed `DeductiveRule` is plannable by construction.
- The rule holds the *analysis* and produces a `Conjunction` per scope; no pre-baked plan is stored,
  and replanning never reconstructs premises from a stored plan. `Candidate` and the
  premise-reconstruction replan path were removed.
- `DependencyGraph::from_steps(&[Plan])` became `from_premises(&[Premise])`.

### The graph as a dependency index (not the ordering driver)

Clarification (user): the dependency graph's role is to encode *dependencies* so that, given an `env`,
we can identify which premises are *affected* by a binding — which become newly feasible, and which
premise depends on which. The `requires[i]` edges (premise `j` binds a variable premise `i` needs) are
exactly this: when a variable enters scope, the edges name the premises it unblocks, without rescanning
everything.

This separates two consumers:

- **Ordering** needs per-*scope* feasibility (`feasibility::categorize`) because choice groups shift
  with the bound set; the static graph alone can't drive the greedy cost ordering.
- **Demand / incremental re-planning** needs the static dependency structure: "binding X affects
  premises {…}" — the affected-premises lookup the `requires` edges provide.

So the graph is retained as the **dependency index** the incremental-subscriptions / demand work
consumes, while feasibility drives ordering. A greedy planner can also *use* the edges to re-check only
the premises affected by the round's newly-bound variables — an optimization the edges enable.

## As-built deltas from this plan

- **Narrowing stayed at plan time, but consumes the analysis.** The folded chapter proposed storing
  *already-narrowed* premises on the rule. As built, the rule stores premises in authored form and
  `Planner::with_types` projects the analysis-inferred `TypeEnv` onto a working copy per `plan(scope)`
  call: inference runs exactly once, and the serialized descriptor round-trips unchanged (narrowed
  terms never leak into the wire form).
- **`AnalyzedRule` was composed, not dissolved.** `DeductiveRule { analysis: AnalyzedRule }` rather
  than flattened fields; the functional intent (rule holds analysis, plans per scope) is met.
- **`adorn` became `feasibility::feasible`/`categorize`.** The per-step method was dropped with
  `Candidate`; one shared SIPS binding function remains.
- **The sound-optionals milestone rode this chapter.** Optionality left the associative layer
  (`Resolution` and the is-term-driven fallback deleted); `MaybeQuery` (premise/plan construct) owns
  set-widening with the entity-bound contract in its schema; `Coalesce` declares its source as a hard
  requirement; `Absent` matches nothing in scalar slots in both polarities; kinds are enforced at
  scans, binds, and equality. See `notes/scalar-associative-layer.md`,
  `notes/polarity-and-negation.md`, and `rust/dialog-query/guide.md`.
- **Still open from this plan:** step 5c (richer `Infeasible` vocabulary + serializable per-premise
  `Feasibility` descriptor), step 6 (propagator decomposition of multidirectional formulas), step 7
  (cost as class + selectivity). These fold into the roadmap's type-checker milestone (formula
  generics / refinement predicates) and the eventual cost chapter.
