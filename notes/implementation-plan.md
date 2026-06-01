# Implementation plan: operator IR, SIPS-owned-by-analysis, propagator feasibility

> Sequencing note. Turns the design in [`planning-adornment-and-cost.md`](./planning-adornment-and-cost.md)
> and [`incremental-subscriptions.md`](./incremental-subscriptions.md) into an ordered series of small,
> independently-reviewable, always-green PRs. Each step compiles, passes `nix … test:native:debug`, and
> is shippable on its own; later steps depend only on earlier ones.

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

**Scope.**
- Per `Plan` variant: `adorn(&BTreeSet<String>) -> Result<Binds, Infeasible>` (the SIPS function `f`).
  `Infeasible::{ NeedsAnyOf, NeedsAll }` (+ `NeedsKOf` reserved). For Scan this is the feasibility
  column of the existing 16-arm table; for Constraint/Formula it is their input requirements.
- `estimate` keeps cost only, called after `adorn` is `Ok`.
- Planner uses `adorn` for viability + binds (replacing the graph's categorization with the per-premise
  function — note: this *subsumes* step 3's graph categorization, so the graph becomes order+edges
  only, feasibility comes from `adorn`). Step 3 and step 5 must be reconciled: step 3 wires the graph;
  step 5 moves feasibility into `adorn` and the graph keeps the *order/edge* structure.
- Required-bindings diagnostics now come from `Infeasible` (richer than today's `RequiredBindings`).

**Done when.** Feasibility and cost are distinct methods; the `Candidate::from` slot-categorization is
replaced by `adorn`; behavior-preserving on existing queries; new tests cover the `Infeasible` reasons.

**Risk.** Medium. This is where the per-slot `Requirement`/`Group` vocabulary is replaced. Equality's
`AnyOf({a,b})` is the first non-Prefix case to validate.

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
