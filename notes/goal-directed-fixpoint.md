# Goal-Directed Fixpoint Evaluation — Implementation Plan

Status: **planned, not implemented**. This document is a self-contained
specification for making recursive concept evaluation demand-seeded
(goal-directed) for linearly recursive rules, with a sound fallback to
the existing full-closure evaluation everywhere the optimization does
not apply.

## Problem

Recursive concepts (those on a dependency cycle, e.g.
`schema::RevisionAncestor` in `dialog-repository`) are evaluated by the
semi-naive fixpoint in
`rust/dialog-query/src/concept/query/fixpoint.rs`. That evaluator
computes the **entire** closure of the recursive concept over **all**
facts and joins the caller's bindings against the result afterwards
(see the module doc: "this evaluator computes the component's full
fixpoint and joins the caller's bindings against the result
afterwards", and `ConceptQuery::evaluate` in
`rust/dialog-query/src/concept/query.rs`, which calls
`fixpoint::evaluate(&app.predicate, analysis, env)` without passing the
caller's bindings, then filters via `fixpoint::join`).

For `ancestor`-shaped rules over a history of n revisions the full
closure is Θ(n²) rows (every `(descendant, ancestor)` pair), while a
query with the descendant bound only needs the O(n) rows reachable from
it, and a query with both ends bound needs one row. The fix: when the
caller binds operands, evaluate only the *demanded* part of the
closure. This is the classical magic-sets idea specialized to linear
recursion; the demand set is exactly the frontier of a top-down walk.

## Read these first (in order)

1. `rust/dialog-query/src/concept/query/fixpoint.rs` — the whole file.
   Pay attention to: `Row`, `row_key`, `InMemoryAnswerTable`,
   `SplitRule` / `Member` / `discover`, `evaluate_table` (seed round),
   `delta_rounds`, `collect_rule_rows`, `join`, `project`,
   `MAX_ROUNDS`, and `Continuation` at the bottom.
2. `rust/dialog-query/src/concept/query.rs` — `ConceptQuery::evaluate`
   (how `app.terms` + the input `Match` are available but unused by the
   fixpoint branch, and how `table` is computed once per selection),
   plus `extract_parameters` for how bound values are read out of a
   `Match`.
3. `rust/dialog-query/src/session/dependencies.rs` — `ProgramAnalysis`
   (`in_same_cycle`, `is_recursive`).
4. `rust/dialog-query/src/planner/` — enough of `Planner` to use
   `Planner::with_types(premises, types).plan(&scope)` and to read the
   resulting plan's bound-variable set.
5. Tests that must keep passing unchanged:
   `fixpoint.rs::tests` (chain / diamond / caller-binding / deep chain /
   multi-path family), `fixpoint.rs::derived_edge_tests`, and in
   `dialog-repository`:
   `repository::tests::it_derives_transitive_revision_ancestry`,
   `repository::branch::pull::history_tests::it_derives_merge_ancestry_across_both_parents`,
   `repository::branch::transaction::query::tests::it_resolves_derived_revision_concepts_in_a_transaction`.

## Key structural facts the design relies on

- **Head operands are body variable names.** A `DeductiveRule`'s
  conclusion operand `o` corresponds to the body variable named `o`:
  `fixpoint::project` reads `matched.lookup(&Term::var(operand))`. So
  "bind head operand `o` to value v" means "bind the variable named `o`
  to v" when evaluating that rule's body.
- **Recursive occurrences are already split out.** `discover` produces,
  per rule, `occurrences: Vec<ConceptQuery>` (in-component concept
  premises) and `base: Vec<Premise>` (everything else). A rule is
  *linear* iff `occurrences.len() == 1`; non-recursive iff `0`.
- **The final join stays.** `ConceptQuery::evaluate` joins every input
  `Match` against the returned rows (`fixpoint::join`). Demand
  filtering may therefore over-approximate freely — extra rows are
  filtered there — but must never under-approximate.
- **Adornment is stable across one selection.** The comment in
  `ConceptQuery::evaluate` documents that all matches in a selection
  share the same binding *pattern*; only the values differ. Static
  checks can be done once per evaluation; demand values vary per input
  row.
- **Rows and goals need canonical keys.** `Value` has no total order;
  use dag-cbor encoding (`row_key`) as the identity for sets/maps, as
  the answer table already does.

## Design

### 1. Goal extraction (`concept/query.rs`)

Add a helper next to `extract_parameters`:

```rust
/// The caller-bound concept operands: operand name -> bound value.
/// A `Term::Constant` binds; a named variable bound `Present` in the
/// input binds; everything else (unbound, blank, `Absent`) does not.
fn goal_of(
    terms: &Parameters,
    input: &Match,
    descriptor: &ConceptDescriptor,
) -> BTreeMap<String, Value>
```

Iterate `iter::once("this").chain(descriptor.with().keys())`; for each
operand look up `terms.get(operand)`:
- `Term::Constant(v)` → insert `v`.
- `Term::Variable { name: Some(_), .. }` where
  `input.lookup(term) == Ok(Binding::Present(v))` → insert `v`.
- anything else → skip.

### 2. Threading the goal into the fixpoint (`concept/query.rs`)

In `ConceptQuery::evaluate`'s fixpoint branch (the `rules.recursion()`
arm), replace the single cached `table` with:

- `full_table: Option<Vec<Row>>` — the existing shared full closure,
  still used when the goal is empty **or** demand planning declines
  (fallback), **and always used when a `Continuation` is attached**
  (`rules.continuation()`), because retained subscription tables must
  keep whole-closure semantics. Do not change the continuation path at
  all.
- `by_goal: HashMap<Vec<u8>, Vec<Row>>` — rows per goal, keyed by
  dag-cbor of the goal map, since different input rows in one selection
  can carry different bound values.

Per input row: compute `goal = goal_of(...)`. If continuation attached
or `goal.is_empty()` → use/compute `full_table` exactly as today.
Otherwise call the new `fixpoint::evaluate_demanded(&app.predicate,
analysis, env, &goal)`; on `Ok(Some(rows))` memoize in `by_goal` and
join; on `Ok(None)` (demand planning declined) fall back to
`full_table`. Memoize the *decline* too (a simple `bool` alongside
`full_table`) so one selection doesn't retry planning per row.

### 3. New entry point and demand plan (`fixpoint.rs`)

```rust
/// Demand-seeded evaluation: compute only the part of the component's
/// closure reachable from the caller's bound operands. Returns
/// `Ok(None)` when the rule shape does not support sound demand
/// seeding — the caller falls back to the full fixpoint.
pub async fn evaluate_demanded<'a, Env>(
    root: &ConceptDescriptor,
    analysis: &ProgramAnalysis,
    env: &'a Env,
    goal: &BTreeMap<String, Value>,
) -> Result<Option<Vec<Row>>, EvaluationError>
```

Steps:

**(a) Discover and gate.** Run `discover` as today. Decline
(`return Ok(None)`) unless ALL of:
- `members.len() == 1` (no mutual recursion — single-concept cycles
  only, which is the `ancestor` shape; mutual recursion falls back);
- every rule of the member has `occurrences.len() <= 1` (linear);
- `B` (the goal's operand-name set) is non-empty.

**(b) Static propagation check (once).** For each rule *with* an
occurrence, verify demand on `B` flows through the recursive call:

For each operand `o ∈ B`, look at `occurrence.terms.get(o)`:
- `Term::Constant(_)` → fine (checked per-tuple at runtime).
- `Term::Variable { name: Some(v), .. }` → `v` must be bound when the
  rule's base premises are evaluated with the head's `B`-variables
  bound. Check this by planning: build
  `Planner::with_types(split.base.clone(), split.rule.analysis().types.clone())
      .plan(&scope_B)` where `scope_B` is an `Environment` containing
  the variable names in `B`. If planning fails, decline. Then confirm
  `v` is in the plan's output bound-set — plans expose it as
  `pub fn binds(&self) -> &Environment`
  (`rust/dialog-query/src/planner/plan.rs:127`); check membership of
  `v` in that environment. A `v` that is
  itself named in `B` (head-bound, e.g. a constant-through recursion
  like the `ancestor`-bound adornment) counts as bound with no base
  evaluation needed.
- anything else (blank / missing) → decline.

Rules with **no** occurrence (base rules) need a plannability check
too: plan `split.rule`'s full premise list under `scope_B`. If that
fails, decline. (Seeded seed-round evaluation below relies on it.)

**(c) Demand iteration.** Compute the demand set `D` of `B`-tuples
(each tuple = `BTreeMap<String, Value>` restricted to `B`, keyed by
dag-cbor):

```text
D        := { goal restricted to B }
frontier := D
round    := 0
while frontier not empty:
    round += 1; if round > MAX_ROUNDS -> Err(FixpointDivergence)
    next := {}
    for each rule with an occurrence:
        for each tuple in frontier:
            matched := Match with var o := tuple[o] for o in B
            rows := evaluate the rule's base premises with `matched`
                    (empty base -> [matched] as-is), using the plan
                    from (b)
            for each row:
                tuple' := for o in B:
                    match occurrence.terms[o]:
                        Constant(c)  -> c    (skip row on later
                                              mismatch: nothing to
                                              propagate; the tuple' is
                                              just {o: c})
                        Variable(v)  -> row's value of v
                if tuple' not in D: add to D and next
    frontier := next
```

This is a bounded traversal: for `RevisionAncestor` with `this` bound
it visits exactly the ancestry of the bound revision (each step is the
edge premise evaluated with `this` bound — a point lookup); with
`ancestor` bound the occurrence term is head-bound so `D` stays a
singleton and the loop exits after one no-op round.

**(d) Seeded seed round.** As `evaluate_table`'s seed round, but per
demand tuple: for each rule with no occurrence, for each `tuple ∈ D`,
plan/evaluate the rule with the `B`-variables pre-bound (seed
`Match` from the tuple, plan under `scope_B` — reuse the plan from
(b), it is the same for every tuple). Stage projected rows into a fresh
`InMemoryAnswerTable`.

**(e) Delta rounds with a demand filter.** Run `delta_rounds` exactly
as today with one addition: in `stage_rule_rows` (or a wrapper), before
`table.insert`, drop any row whose `B`-projection (dag-cbor key of the
row restricted to `B`) is not in `D`. This is what keeps the table
bounded: the sideways join of a step rule can transiently propose rows
about non-demanded heads (children of demanded nodes that are not
themselves demanded); they can never be answers for this goal, and
without the filter they would seed further derivations. Thread the
filter as an `Option<&DemandFilter>` parameter so the existing
full-closure path passes `None` and is byte-for-byte unchanged.

**(f) Early exit for fully-bound goals.** If `B` covers *every* operand
of the concept, the goal names one exact row. After the seed round and
after every `table.advance()` in the delta rounds, check whether
`row_key(goal_row)` is present in the total; if so, stop and return
`vec![goal_row]`. (Returning only that row is sound: the caller's join
binds nothing new.) Thread this as `Option<&Row>` alongside the demand
filter.

**(g) Return** `Ok(Some(table.total(&root.this())))`.

### 4. What must NOT change

- The `Continuation` / subscription path (`Continuation::rows`,
  `extend`, `retract`): retained tables stay full-closure. The guard in
  step 2 makes demand seeding unreachable there.
- `MAX_ROUNDS` semantics (the demand loop gets the same valve).
- The stratification contract: demand seeding changes *which* tuples
  are derived, not rule semantics. Negated premises inside `base` are
  planned and evaluated exactly as before (they appear in the base
  plans of (b)/(d)).
- The final `fixpoint::join` in `ConceptQuery::evaluate` — it still
  runs; demand rows are an over-approximation of the answer only in
  operands outside `B`.

### 5. Soundness argument (keep in the module docs)

Claim: for a single-member component with linear rules passing check
(b), the demanded table contains every row of the full closure whose
`B`-projection equals the goal's.

Sketch: induct on the derivation height of a closure row `r` with
`r|B = goal|B`. Height 1: `r` derives from a base rule; the seeded seed
round evaluates that rule with `B` bound to `goal|B` (a restriction of
the unbound evaluation), so `r` is staged. Height k: `r` derives from a
step rule joining base premises with an occurrence row `r'`. By (b),
the occurrence's `B`-operands are determined by the head's `B`-values
through the base premises, so `r'|B ∈ D` (that is exactly the demand
iteration (c), seeded from `goal|B`). By induction `r'` is in the
table; the delta rounds join it with the same base premises and stage
`r`, and the demand filter admits it since `r|B = goal|B ∈ D`.

The demand filter never drops such an `r` because `r|B ∈ D` by
construction; it only drops rows whose `B`-projection is outside `D`,
which by the same propagation argument can never participate in a
derivation of a demanded row *through the occurrence position of B*.
(They could only re-enter via base premises, which do not read the
table.)

### 6. Tests

New engine tests in `fixpoint.rs` (model on the existing `tests`
module; reuse `ancestor_concept` / `ancestor_rules` and the
`family/parent` fixtures):

1. `it_answers_bound_this_from_the_demanded_region` — two *disjoint*
   families (chain A: 4 people, chain B: 4 people) in one branch. Query
   `ancestor` with `this` bound inside family A. Assert (i) rows equal
   the full-closure result filtered to that binding, and (ii) using a
   `Select`-counting env wrapper (wrap `TestEnv`, count
   `Provider<Select>::execute` calls), the bound query executes fewer
   selects than the same query unbound. This proves family B was never
   visited.
2. `it_answers_bound_ancestor_without_global_closure` — same fixture,
   bind only `ancestor` (the fb adornment; demand is a singleton).
   Parity with filtered full closure.
3. `it_early_exits_when_both_ends_are_bound` — deep chain (24 links);
   bind `this` = bottom, `ancestor` = one step up. Assert the counting
   env shows far fewer selects than the full-closure run (the walk
   stops after the first delta round finds the row).
4. `it_falls_back_for_nonlinear_rules` — a rule with two occurrences
   (e.g. `related(x,y) :- ancestor-of-common(x,z), ancestor(z,y)`
   shaped so two in-component premises appear in one body; simplest: a
   same-generation program `sg(x,y) :- flat(x,y); sg(x,y) :- up(x,a),
   sg(a,b), down(b,y)` is linear — instead use
   `path(x,y) :- edge(x,y); path(x,y) :- path(x,z), path(z,y)`).
   Bound query must return correct results (identical to unbound +
   filter), proving the decline-and-fallback path.
5. `it_matches_full_closure_on_every_existing_scenario` — for the
   chain, diamond, deep-chain and multi-path fixtures already in the
   test module, run each query once unbound and once with `this` bound
   per entity, asserting the bound result equals the filtered unbound
   result. (Parity harness, cheap to write as a helper.)
6. Divergence: a formula-driven infinite rule with a bound goal still
   errors with `FixpointDivergence` (valve applies to the demand loop).

`dialog-repository` additions (in `repository.rs` next to
`it_derives_transitive_revision_ancestry`):

7. `it_bounds_ancestry_reads_to_the_demanded_lineage` — two branches
   with divergent histories merged once; query `RevisionAncestor` with
   `this` bound to a mid-history revision and assert parity with the
   filtered unbound result. (A read-count assertion here is optional —
   engine tests 1/3 already pin the complexity claim.)

Regression suite: `cargo test --workspace` must stay green — notably
every test named in "Read these first §5", untouched.

### 7. Acceptance checklist

- [ ] `cargo test --workspace` green.
- [ ] `cargo test -p dialog-repository --features integration-tests` green.
- [ ] `cargo clippy --workspace --all-targets --all-features` clean.
- [ ] `cargo check --target wasm32-unknown-unknown -p dialog-query -p dialog-repository` compiles
      (no `std::thread`, no non-wasm sync primitives in new code).
- [ ] New tests 1–7 present and green.
- [ ] Full-closure path byte-compatible: no behavior change when the
      goal is empty, when a continuation is attached, or when demand
      planning declines.
- [ ] Module docs in `fixpoint.rs` updated: replace the "future work"
      note with a description of the demanded path + the soundness
      sketch (§5), stating the fallback conditions.

### 8. Out of scope (do not attempt)

- General magic sets: sideways-information-passing strategies,
  supplementary predicates, synthetic magic concepts. Not needed for
  linear rules and it strains the content-addressed
  `ConceptDescriptor` model.
- Mutual recursion (multi-member components) — falls back.
- Demand-aware `Continuation` tables (subscriptions keep full closure).
- Changing `Formula` to read the store. Formulas are pure
  (`Formula::compute(Input) -> Vec<Self>`, no env, synchronous), and
  the incremental-maintenance classifier (`classify_base`) counts them
  as `Inert` — an effectful formula would make additions/deletions
  invisible to `extend`/`retract` and silently corrupt standing
  queries.

### 9. Gotchas

- `Value` is not `Ord`/`Hash`-total (floats): always key sets/maps by
  dag-cbor bytes (`row_key` / `serde_ipld_dagcbor::to_vec`), never by
  `Value` directly.
- `Date::now`-style nondeterminism is irrelevant here, but keep the
  demand iteration deterministic (BTree structures / sorted keys) so
  test failures reproduce.
- When binding a seed `Match`, use `Term::<Any>::var(name)` — matching
  how `bind_occurrence` and `project` address variables.
- The base-premise plan for step rules is computed under `scope_B`
  but evaluated with a `Match` carrying the tuple values; this mirrors
  `collect_rule_rows` (plan under `scope`, evaluate with `matched.seed()`).
- Don't forget `wasm_bindgen_test` attributes on new async tests
  (copy the `cfg_attr` pattern from neighbors).
