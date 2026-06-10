# Recursion, Negation, and Stratification

A design report covering recursive rule evaluation, negation semantics, and
the cross-cutting challenges that arise from rules being stored in a
content-addressed, replicated database. Captures decisions made, alternatives
considered, and open questions for future work.

## Purpose

Dialog-DB needs deductive rules to derive new concepts from existing facts
(Datalog-style inference). The query engine today supports non-recursive
rules with positive and negated premises, and has scaffolding from the
magic-set paper (adornment-keyed plan caching) but does not yet support
recursive rules. Adding recursion brings two intertwined problems:

1. **Recursion** — rules whose body references their own head, directly or
   transitively (e.g. ancestor as the transitive closure of parent).
2. **Stratified negation** — rules where a `Premise::Unless` reference
   creates a cycle in the dependency graph, which classical Datalog rejects
   because no fixpoint semantics exists.

Both questions take on a new shape in a replicated setting where rules
themselves are stored in the database and can be installed independently
on multiple replicas. The naive single-author assumption that underlies
most Datalog literature does not hold.

## Background: what's in place today

- **Query engine** (`rust/dialog-query`) evaluates concept queries against
  an EAV fact store. The model is *top-down, streaming*: a query produces
  a stream of `Match` frames, threaded through `Conjunction`s of planned
  `Premise`s.
- **Rules** are compiled into `DeductiveRule` (head + planned body) and
  installed into a `RuleRegistry`. Multiple rules with the same head are
  evaluated as a `Disjunction` of `Conjunction`s.
- **Plan caching by adornment** — each binding pattern (what is bound vs
  free at call time) produces a specialised plan. This is the magic-set
  *adornment* machinery, minus the magic predicates and fixpoint.
- **Dependency graph + SCC analysis** — installed under
  `session::dependencies`. Each rule contributes edges from its head to
  each concept its body references. Tarjan's algorithm computes strongly
  connected components; non-trivial components mark concepts as recursive.
- **`RuleRegistry::validate()`** — checks the rule set for negation-
  through-recursion violations. Returns a list of `NegationViolation`s
  (rule + the negated concept that lands inside its own cycle). Callable
  any time: after install, after merge, before query.
- **`is_recursive()`** — query the analysis for a single concept's
  recursion status. Available on `ConceptRules`, `RuleRegistry`, and via
  the snapshot `RuleRegistry::analysis()`.

The engine does not yet have a fixpoint evaluator. Phase 2 will add one.

## Core problem: the replica-merge challenge

This is the constraint that pushed the design away from textbook Datalog.

Rules live in the database. Two replicas can each install rules locally,
each install fully valid in isolation, and then sync. The merged rule set
may not be valid — and there is no way for either replica to have prevented
this at install time.

Concrete example:

```
Replica A installs:
    safe(X) :- person(X), !blocked(X).

Replica B installs:
    blocked(X) :- safe(X), banned(X).

Each install is legal locally. After sync, the merged set has
safe → !blocked → safe — a cycle through negation. No stratified
semantics exists for this program.
```

The classical Datalog response is "reject the program." But neither
replica could refuse the install at the time, and rejecting *at merge*
would force replicas to disagree about whether the merge succeeded —
breaking CRDT-style convergence.

This drives the central design constraints:

- **`register()` must be infallible for stratification reasons.** Lock
  errors propagate; nothing else. Stratification is a whole-set property,
  not an install-time property.
- **Stratification is checked elsewhere** — either eagerly via
  `validate()` or lazily at query time.
- **Replicas converge** on the merged rule set regardless of stratifiability.
  Diagnostics surface as query-time errors; the rule storage itself
  remains consistent.

## Design constraints

These are non-negotiable for the recursion work:

1. **Replica convergence comes first.** Any design that requires replicas
   to disagree on installed rules is rejected.
2. **Bounded memory.** Recursive evaluation cannot let the AnswerSet grow
   without bound. The implementation must expose a swappable storage trait
   so an in-memory implementation can later be replaced with a disk-backed
   one without changing the evaluator.
3. **Streaming where possible.** Non-recursive queries stay fully
   streaming. Recursive sub-queries are internally round-based but yield
   per-round, not at the very end of the fixpoint.
4. **Fail loudly on divergence.** Iteration caps catch rule sets that
   would otherwise spin forever.
5. **No silent semantic changes from refactoring.** Adding negation to a
   rule body must not silently shift its temporal semantics (this killed
   the EDB-only-negation proposal — see "Rejected alternatives").
6. **`unless` and aggregation are checked uniformly.** Both are non-
   monotonic; both contribute negative-polarity edges; the same
   stratification machinery covers both (aggregation lands in a future
   phase).

## Approach: stratified Datalog + query-time validation

After extensive review of alternatives, the design we adopted:

1. **Rules with `unless` are accepted by `register()` unconditionally.**
   Stratification is not an install gate.
2. **`validate()` reports stratification violations** for the current
   rule set. Callers decide what to do — surface as warning, refuse to
   query, or ignore. Single-replica authors call it after install for
   immediate feedback; post-sync code calls it after merge.
3. **Query-time check (Phase 2)** — the recursive evaluator will run a
   targeted validation over the queried concept's dependency closure
   before evaluating. Ill-stratified queries fail with a structured
   `EvaluationError::NegationThroughRecursion`; well-stratified queries
   (the vast majority) proceed normally.
4. **Recursive evaluation uses seminaive bottom-up with magic-set-style
   binding propagation.** Each iteration computes Δ-answers by joining
   rule bodies with the previous iteration's deltas. Goal-directed via
   magic relations derived from the call adornment.
5. **`AnswerTable` trait wraps internal memoisation.** First implementation
   is in-memory; the trait is the swap point for disk-backed storage.

This approach preserves expressiveness (full IDB-aware negation when
stratified), satisfies replica convergence (every install accepted), and
gives a clean implementation path (existing planner stays in use; the
recursive evaluator wraps it in a fixpoint driver).

## Considerations and rejected alternatives

Several alternatives were evaluated. Each is recorded here so the reasons
are not lost.

### EDB-only negation (rejected)

Proposal: `!Query::<Concept>` looks only at stored EDB facts of that
concept, never at derived IDB ones. Stratification becomes trivially
satisfied because no negative edge can cross into the rule layer.

**Why rejected** — silent semantic surprises. Adding a rule for a
previously-stored concept changes what `!Query` matches downstream. A
fully-derived concept has an empty negation domain (vacuously true).
Concepts with mixed EDB and IDB facts give bizarre results. Library
composition is broken: an importer adding a rule for a library's negated
predicate changes the library's behaviour invisibly. Materialization
choices leak into semantics.

The asymmetry between positive and negative concept queries is load-
bearing for stratification but pushes too much implementation detail into
the user's mental model.

### `@next` / inductive rules only (rejected for now)

Proposal: forbid negation in deductive rules; allow it only in inductive
(`@next`) rules whose semantics involve time advancement. Bloom-style.

**Status** — we don't have wall-clock time or transaction-level tick
machinery, so inductive rules in the Bloom sense aren't available. We
also confirmed (after re-reading the specs) that Bloom does *not*
restrict negation to inductive rules — it permits stratified deductive
negation just like classical Datalog. The "Bloom is more constrained"
framing was a misremembering.

**Future** — if and when transaction-level temporal evolution is added,
explicit inductive rules become a useful escape hatch for cross-tick
recursion-through-negation. Not part of the immediate roadmap.

### Stable models / answer set programming (deferred)

Proposal: accept any rule set; queries return the *set* of stable models.
A non-stratified program just has multiple legal interpretations and the
user picks one (or unions them).

**Why deferred** — evaluation is NP-hard in general; queries can return
disjunctive answers; implementation complexity is much higher than
stratified Datalog. Possible long-term direction if the use case demands
it, but too big a jump for the first cut.

### Deterministic disable (deferred)

Proposal: detect unstratifiable cycles, pick a deterministic feedback set
(e.g. highest content-hash in each cycle), and treat those rules as
inactive. Replicas converge on the active subset.

**Why deferred** — the choice of which rule to disable is principled
(deterministic) but arbitrary from the user's perspective. Disabled rules
become silent. Useful as an opt-in policy for production deployments that
prefer "degraded but functional" over "fails until human fixes it," but
not the default.

### PomoLogic / Bloom / Dedalus / DBSP comparison

We surveyed prior art:

- **PomoLogic** — uses classical stratification with install-time
  rejection. Replica-merge is not addressed; the spec is silent on it.
- **Bloom / Dedalus** — same stratification model. Time-stratified
  recursion through `@next` is permitted but not required. CALM theorem
  applies to monotonic programs; neither system addresses concurrent
  rule installation across replicas.
- **DBSP** — an evaluation calculus, not a language. Assumes a fixed
  well-stratified program. Computes whatever circuit is given; non-
  stratified circuits may not converge.

None of these systems solves the replica-merge problem because none of
them assumes rules are installed concurrently from multiple sources. Our
setting is genuinely novel in this respect.

What we *do* borrow:

- **Stratification mechanics** — Apt–Blair–Walker dependency graph + SCC.
- **Adornment-keyed plan caching** — magic-set scaffolding already in
  place.
- **Seminaive Δ-joins** — the standard efficient bottom-up evaluator.
- **DBSP-shaped operators** — Phase 2 implementation aims to keep each
  step (Δ-join, fixpoint, magic-filter) as a one-step DBSP operator, so a
  future move to cross-transaction incremental view maintenance is
  additive rather than a rewrite.

### Lenses (Boomerang and successors)

Reviewed for relevance to bidirectional rule semantics. **Not directly
applicable** — lenses solve a different problem (update propagation back
through views). They reinforce the same insight as CALM: monotonicity is
the property that makes composition tractable. No new mechanism to
borrow.

## Monotonicity and CALM: honest accounting

Earlier framings of the work as "monotonic" were sloppy. The honest
position:

- Pure positive Datalog (no `unless`, no aggregation) **is** CALM-
  monotonic and coordination-free.
- Stratified Datalog with negation **is not** CALM-monotonic. Adding a
  fact to `blocked` can *remove* a derivation of `safe`. The system is
  *deterministic given the merged rule set + merged EDB* — replicas agree
  on results — but does not satisfy CALM coordination-freedom.
- Aggregation has the same property as negation: non-monotonic in
  general; deterministic given inputs.

The lattice-typed aggregation subset (count, sum-over-non-negative, min,
max) **is** CALM-monotonic in its respective lattice order. Composition
that respects the lattice (via lattice merge) is coordination-free.
Worth preserving as a future fast-path.

For the current work we accept that the system is deterministic-given-
inputs rather than fully CALM-monotonic. The trade is full IDB-aware
negation semantics in exchange for losing coordination-free composition.

## Inductive vs deductive rules in our model

A clarification that emerged late in design: in our query-time
evaluation model, "inductive" does not carry its Dedalus meaning of
"derivations appear at tick t+1." We do not have wall-clock time or
transaction-level ticks. Both kinds of rule are evaluated at query time;
their output is not persisted between queries.

The only operational difference between deductive and inductive rules in
our model is *stratum order during a single query's fixpoint*. Each
stratum boundary is internally a "tick" in the sense of "previous-stratum
fixpoint is the input to the next." The user observes the fully converged
result.

Consequence: **a non-recursive rule without negation has no functional
distinction between deductive and inductive.** The distinction matters
only for rules with non-monotonic operators (negation or, eventually,
aggregation), where the rule must live in a stratum strictly above its
non-monotonic dependencies.

Because of this, we do not currently expose `@next` syntax. A rule with
`unless:` is automatically placed in a higher stratum at evaluation time.
This is invisible to the user; queries return the converged answer.

If transaction-level temporal evolution is added later, explicit
`induce:` syntax can be introduced at that point to carry the persistence
semantics. Not designed for speculatively now.

## Aggregation: future work, same shape

Aggregation will eventually be supported. It rides on the same
stratification rails as negation:

- Non-monotonic in general; contributes negative-polarity edges; same
  `validate()` machinery catches cycles through aggregation.
- Lattice-typed subset is CALM-monotonic and can compose freely without
  stratification.

The DSL question — how aggregation appears in user-facing rule notation
— was discussed and resolved: **aggregation is its own rule kind**,
parallel to `deduce:`. Aggregating rules use a new keyword
(`count:`/`sum:`/`max:`/`min:`/`collect:`) and have a fixed shape:

```yaml
order-count-per-dept:
  count: order-count-per-dept
  with:
    dept: ?dept
    total: ?total
  of:
    - assert: order
      where:
        this: ?order
        dept: ?dept
  is: ?total
```

Other rules consume aggregating-rule conclusions positively, the same way
they consume any concept. **There is no aggregation premise inside
deductive rule bodies.** This mirrors the `or` removal: rather than mix
two scoping regimes in one body and create a class of compile errors,
the problematic construct gets its own rule kind. Variables local to an
aggregation are syntactically unable to escape the rule because the head
shape is fixed; "consumed variable" errors cannot arise because there is
no shared scope to consume from.

Concept types stay simple: a concept is still just a schema of attributes
with regular field types (`u64`, `String`, `Entity`, etc.). Aggregating
fields are not a thing in `#[derive(Concept)]`. The user authors a
derived concept whose fields happen to be populated by an aggregating
rule.

Set-valued aggregation (`collect:`) waits for collection-typed fields in
the schema, which is independent and useful regardless of aggregation.

## Implementation roadmap

### Phase 1 — completed

- Dependency graph + Tarjan SCC analysis (`session::dependencies`).
- `Dependency` enum (positive / negative) at the premise level.
- `RuleRegistry::register()` is infallible for stratification.
- `RuleRegistry::validate()` returns the full list of
  `NegationViolation`s.
- `is_recursive()` accessors on registry, concept rules, and via the
  analysis snapshot.
- Tests covering: self-recursion, mutual recursion across two concepts,
  transitive three-concept cycle, validation reporting, and the post-
  merge case (rule that closes a cycle on an existing rule's negation).

### Phase 2 — recursive evaluator

- Seminaive Δ-driver: each round joins recursive premises against the
  previous round's deltas; non-recursive premises stay on the existing
  top-down path.
- Magic relations from call adornment for goal-directed propagation.
- `AnswerTable` trait — in-memory hash-dedup first, swappable for disk-
  backed later.
- Per-round streaming output; iteration cap as a safety valve.
- Query-time stratification check over the queried closure: ill-
  stratified closures fail with `EvaluationError::NegationThroughRecursion`.
- Ancestor as the deliverable: tests ported from the legacy query-engine
  cover linear chains and diamond family trees.

### Phase 3 — aggregation

- Aggregating rule kind (`count:`/`sum:`/`max:`/`min:`/`collect:` in YAML
  notation).
- Built-in aggregation functions.
- Group-by semantics via implicit head bindings.
- Negative-polarity edge contribution to the dependency graph; same
  `validate()` machinery.
- Lattice-typed result subset for CALM-monotonic composition (optional
  refinement).

### Phase 4 — transaction-level temporal evolution (speculative)

- `induce:` syntax for genuinely temporal rules.
- Persistence of inductive output across transactions.
- Replica coordination on tick boundaries (open question).
- DBSP-style cross-transaction incremental view maintenance.

Phases 4 builds on the operator vocabulary established in Phase 2, so the
move is additive rather than a rewrite. Each Phase 2 operator (Δ-join,
fixpoint, magic-filter) is intended to map cleanly to a one-step DBSP
operator for the eventual transition.

## Open questions

- **Lattice typing for aggregation.** When does the lattice subset
  warrant first-class type-system support vs. living as a documentation
  convention? Decision deferred to when aggregation is being implemented.
- **Iteration cap default.** What's a reasonable upper bound for
  fixpoint iterations before failing the query? Should it be configurable
  per-query, per-session, or global? Decision deferred to Phase 2.
- **Deterministic disable as opt-in policy.** Useful for production
  deployments wanting "degraded but functional" semantics. Easy to add
  on top of the current design if needed; not implementing speculatively.
- **`@next` / temporal evolution.** Whether and when to introduce
  transaction-level time. The Dedalus / Bloom roadmap is well-charted;
  the question is whether the use cases warrant the implementation cost.
- **Cross-replica answer caching.** Once recursive evaluation is in
  place, caching converged answers across queries (and invalidating on
  EDB change) is a substantial performance win. Architecture should not
  preclude it.

## Summary

The core architectural decision: **rules are stored in the database, can
be installed concurrently across replicas, and the rule set is a whole-
set property checked at query time rather than install time.** This is
the constraint that distinguishes our setting from textbook Datalog and
that drove design away from install-time rejection.

The recursive evaluator (Phase 2) will be a bottom-up seminaive fixpoint
with magic-set-style binding propagation, DBSP-shaped operators, and an
`AnswerTable` trait for bounded-memory storage. Stratification violations
are caught at query time; the system is deterministic given inputs,
though not CALM-monotonic in general. Aggregation (Phase 3) extends the
same stratification machinery with a separate rule kind, avoiding any
variable-scoping ambiguity in deductive bodies. Transaction-level
temporal evolution (Phase 4) is on the roadmap but not designed
speculatively.
