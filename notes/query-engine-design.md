# Query engine design

> Overview of dialog-db's query engine: how a rule is analyzed, planned, and evaluated, the type
> hierarchy that carries the guarantees, and the academic work it is based on. Companion to the
> sequencing record ([`operator-ir.md`](./operator-ir.md)), the planner design
> ([`planning-adornment-and-cost.md`](./planning-adornment-and-cost.md)), and the forward-looking
> [`incremental-subscriptions.md`](./incremental-subscriptions.md). User-facing optionality semantics
> are documented in [`rust/dialog-query/guide.md`](../rust/dialog-query/guide.md).

## The pipeline

A query rule moves through three stages, each producing a type with a stronger guarantee than the last:

```
DeductiveRuleDescriptor  ──analyze──▶  DeductiveRule  ──plan(scope)──▶  Conjunction  ──evaluate──▶ rows
   (parsed data,                       (analyzed:                       (concrete plan
    no guarantees)                      verified, plannable              for a scope)
                                        by construction)
```

- **`DeductiveRuleDescriptor`** — the serializable / wire form: a conclusion plus `when`/`unless`
  propositions. Just data.
- **`DeductiveRule`** — the *analyzed* rule. Analysis verifies every invariant (type inference,
  required-head-not-optional, Coalesce contracts, conclusion grounding) and proves the body is
  *plannable*. It holds the analysis: the premises (authored order), the inferred types, and the
  dependency graph (the SIPS — see below). Because analysis ran, a `DeductiveRule` is **plannable by
  construction**.
- **`Conjunction`** — the concrete execution plan for a *specific* scope, produced on demand by
  `DeductiveRule::plan(scope)`. The plan is what evaluates (`Conjunction::evaluate`). The rule analyzes
  and plans; the plan it returns evaluates.

`InductiveRule` is the assertion-shaped sibling (same pipeline; differs only at evaluation). The `Rule`
enum wraps either.

## The two questions, and where each lives

For each premise, planning asks two *separate* questions against the variables bound so far:

| | **Feasibility** (the gate) | **Cost** (the rank) |
|---|---|---|
| asks | can this run? what does it bind? | if it runs, how expensive? |
| function | `feasibility::feasible` / `categorize` | `Premise::estimate` |
| reads | the schema's `Requirement` + which slots are bound | which slots are bound (an access-path choice) — *never* `Requirement` |
| result | `Ok(binds)` or `Err(Infeasible::NeedsAll{…})` | a number |

Both depend on the bound set, but for different reasons. This separation is deliberate and is the key
design decision (see *Papers*): feasibility decides *which orderings are valid*; cost decides *which
valid ordering is cheapest*. The planner only ever asks `estimate` of a premise `feasible` has already
approved.

## Analysis vs. planning (cost-free vs. cost-driven)

- **Analysis is cost-free.** It builds the feasibility structure (the binding function + the dependency
  graph) from the premises alone, infers the rule-wide types once, and proves a valid total order
  exists from the empty scope (satisfiability). It never consults sizes or costs. Its output is the
  *space* of valid orderings. Planning consumes the analyzed types (`Planner::with_types`) and projects
  them onto a working copy of the premises per `plan(scope)` call — the stored premises stay in their
  authored, un-narrowed form so the rule's serialized descriptor round-trips unchanged.
- **Planning selects by cost.** `plan(scope)` greedily picks, at each step, the cheapest *feasible*
  premise under the variables bound so far; its binds extend the bound set for the next step. The plan
  it emits is one chosen ordering. Cost lives only here, per scope.
- **Evaluation** runs the chosen plan, threading a binding stream through each step.

Because the feasible space is scope-independent, analysis runs once and planning re-runs per scope
(e.g. concept adornment: a rule used with different bound arguments re-plans, cheaply, against the same
analysis).

## The operator IR

The planned `Conjunction` is a sequence of compiled `Plan` operators, not the syntactic AST. `Plan` is
an enum — `Scan` (scalar attribute lookup), `Maybe` (the optional lookup: a left-join over a scalar
lookup, realizing `maybe` concept fields), `Formula`, `Constraint`, `Concept`, `Negate` — each carrying
the lowered query plus a small `Header` (cost / binds / env). `evaluate` dispatches on the variant; the
AST is reconstructable from the payload (`Plan::as_premise`) for analysis but is not stored separately.
This keeps execution off the AST and gives later work (incremental maintenance) a concrete structure to
attach to.

`Maybe` is also where the optionality contracts live structurally: its schema hard-requires the entity
slot ("absent for whom?") and set-widens its value/cause content types, so feasibility and inference
need no special cases; type narrowing demotes it to a plain `Scan` when a sibling premise proves the
value present. `Coalesce` declares its source as a hard requirement for the same reason — ordering
correctness is schema-borne, never a cost accident.

## What the papers contribute

The engine is built on the **magic-sets / sideways-information-passing (SIPS)** line of work, with the
**propagator** model informing the constraint side and **DBSP / DRed** informing the forward
(incremental) direction.

### Magic sets and the SIPS — Beeri & Ramakrishnan; Balbin et al. 1991; Alviano

A *SIPS* is the formal account of how bindings flow through a rule body. Alviano (Def. 3.1.3) defines a
SIPS for a rule and adornment as a pair **`(≺ᵅ, fᵅ)`**:

- **`≺`** — a strict partial order over the body atoms (head precedes body; the dependency structure of
  which atom feeds which). In dialog-db this is the **`DependencyGraph`** (per-premise binds/needs plus
  the `requires` edges). It is half the SIPS *by definition*, not an optional cache — it is the
  dependency index that says, given a binding, which premises it affects/unblocks. The forward-looking
  demand work consumes it.
- **`f`** — the **binding function**: given what is bound, the variables an atom makes bound after it
  runs. In dialog-db this is **`feasibility::feasible`** (built on `categorize`). There is exactly one
  such function; the planner orders by it.

Two facts the papers settle, both adopted here:

1. **Cost is *not* part of the SIPS.** Balbin et al. (§3.1): "the choice of one SIPS over another is
   guided by factors such as the current and expected size of the different relations and the indexing
   mechanism employed… we assume this choice has been made." So cost selects *among* feasible SIPS, at
   planning time — exactly the gate/rank split above. dialog-db's greedy planner *is* the SIPS-selection
   stage; a future cost redesign improves only the selector and never touches `f` or `≺`.
2. **Adornment is generated on demand and memoized, never enumerated.** Alviano's `Adorn`/`ProcessQuery`
   drive compilation from a worklist of adornments *seen so far*, adding each only when a (sub-)query
   demands it. dialog-db's concept-rule planning derives the adornment from the first match and caches
   the resulting plan per adornment — the same demand-driven, memoized strategy, which also avoids any
   global adornment table to keep in sync across peers.

dialog-db's premises are *richer* than Datalog atoms: formulas and constraints have genuine input
*requirements* (a formula can't run until its input is bound), so feasibility is not merely an
adornment pattern but a real "can it run yet" predicate. This is why `f` carries a `NeedsAll` error
naming the still-required variables, and why the optional lookup (`OptionalAttributeQuery`) hard-requires its
entity bound rather than binding it.

### Negation as demand — Tekle & Liu, *Extended Magic for Negation* (arXiv:1909.08246)

The `n.p` complement-predicate construction: a negated literal becomes a query that *excludes* from the
positive set, made sound by stratification + demand. dialog-db already treats `Negation` as a filter
that consumes bindings without producing them; this is the basis for the planned demand-driven negation
in the incremental work.

### Propagators — Radul & Sussman, *The Art of the Propagator* (MIT-CSAIL-TR-2009-002); Radul, *Propagation Networks* (TR-2009-053)

A multidirectional constraint (e.g. `sum(x, y, total)`) is built from unidirectional propagators sharing
cells; whichever has enough inputs fires. This is the model for dialog-db's bidirectional
constraints/formulas: rather than one node with a k-of-n feasibility test, decompose into directional
sub-premises, each a trivial one-output case the planner runs when feasible. Cells accumulate *partial
information* and combine by `merge`; dialog-db's three-state binding (`unbound` / `Present` / `Absent`)
is a (currently equality-only) instance of that lattice, and `Coalesce` is a small propagator over it.

### Forward / incremental direction — DBSP; DRed/FBF

For the planned incremental-subscription work (not yet built): DBSP gives the precise algebra of *what*
each incremental operator needs (Z-sets, the chain/bilinear rules); DRed/FBF give over-delete →
re-derive → insert for retraction with multiple derivations. The architecture is demand-driven (magic
sets / pull) rather than DBSP's world-driven push, because dialog-db holds partial replicas — see
[`incremental-subscriptions.md`](./incremental-subscriptions.md) and [`dbsp.md`](./dbsp.md).

## Pointers (code)

- `rust/dialog-query/src/rule.rs` — the `Compile` trait (analyze → verify → plannable rule).
- `rust/dialog-query/src/rule/analyzer.rs` — `analyze`, `AnalyzedRule`, `DependencyGraph` (the SIPS `≺`).
- `rust/dialog-query/src/rule/types.rs` — `TypeEnv::infer` (inference + narrowing inputs).
- `rust/dialog-query/src/planner.rs` — the greedy SIPS-selection planner.
- `rust/dialog-query/src/planner/feasibility.rs` — `feasible` / `categorize` (the binding function `f`)
  and `Infeasible`.
- `rust/dialog-query/src/planner/plan.rs` — the `Plan` operator IR, type projection (`apply_types`),
  and `Conjunction` evaluation.
- `rust/dialog-query/src/optional.rs` — `OptionalAttributeQuery`, the optional lookup (left-join) operator.
- `rust/dialog-query/src/schema.rs` — `Requirement` / `Group` (feasibility input) and the cost
  constants (`Cardinality::estimate`, the SIPS-selection cost model).

## Pointers (papers)

- Beeri & Ramakrishnan, *On the power of magic* — magic sets.
- Balbin, Port, Ramamohanarao, Meenakshi, 1991, *Efficient bottom-up computation of queries on
  stratified databases* — SIPS, adornment, the cost-bracketing quote.
- Alviano, *Dynamic Magic Sets* (thesis) — SIPS formalization (Def. 3.1.3), demand-driven memoized
  adornment.
- Tekle & Liu, *Extended Magic for Negation: Efficient Demand-Driven Evaluation…* (arXiv:1909.08246) —
  the `n.p` negation construction + FBF.
- Radul & Sussman, *The Art of the Propagator* (MIT-CSAIL-TR-2009-002); Radul, *Propagation Networks*
  (MIT-CSAIL-TR-2009-053) — multidirectional constraints, cells, merge.
- Budiu et al., *DBSP: Automatic Incremental View Maintenance for Rich Query Languages* (VLDB 2023) +
  the DBSP spec — the incremental algebra (forward direction).
- Gupta, Mumick, Subrahmanian 1993 (DRed); Tekle & Liu (FBF) — incremental maintenance with retraction.

## Types are checked, not advisory

Rule-level inference (one pass, at analysis) is enforced at evaluation: typed scan slots filter facts
whose value falls outside the term's kind (`Type::admits`), `Match::bind` validates kinds as a
last-resort contract check, and `Equality` propagates `Absent` only into terms that explicitly admit
`Nothing`. An `Absent` binding matches nothing in any scalar slot, in both polarities — under negation
this makes "has no nickname" pass "unless the nickname is banned" instead of matching every banned
value. Narrowing is positive-polarity-only; negated subqueries are typed in their own context (see
[`polarity-and-negation.md`](./polarity-and-negation.md)).
