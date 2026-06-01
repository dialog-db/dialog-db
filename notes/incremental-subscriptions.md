# Demand-driven incremental query subscriptions

> Design note. Companion to [`dbsp.md`](./dbsp.md). Describes the target architecture for incrementally
> maintained, demand-driven query subscriptions, what the codebase already provides toward it, and the
> path to build it.

## Goal

Queries today are evaluated from scratch on each request. The target is **standing subscriptions that
are incrementally maintained**: a query is registered once, and as the underlying data changes the
subscriber receives the *delta* to the result rather than a recomputed result. Maintenance is
**demand-driven** — only the data a subscription actually touches is fetched and recomputed, never the
whole database.

## Replication model

The replica is a single local content-addressed tree; "peers" exist only at the merge boundary, never
during query evaluation.

1. **Merge.** Pull subtrees from the remotes that represent peers and reconcile overlapping subtrees
   into one logical tree. Reconciliation is deterministic at the tree layer (`Tree::integrate`,
   higher-hash-wins / LWW). The result is one authoritative root.
2. **Local evaluation.** Every query runs locally against that single tree. There is no cross-peer
   query and no "which peer" decision.
3. **On-demand replication via tree access.** Every tree operation is parameterized by a
   `ContentAddressedStorage` and resolves nodes by hash through it. Touching a key range that is not
   materialized locally fetches the covering subtree(s) from the (remote-capable) backing storage. So
   replication is the tree's **lazy-load**, driven by which subtrees a query reads — transparent to the
   query, not orchestrated by the planner.

Consequences this model gives for free, that an across-peers query model would not:
- **Demand = the query's subtree-access pattern.** What gets replicated is exactly what the query
  touches; the planner's existing access pattern *is* the demand.
- **Negation correctness is a tree-layer property, not a distributed one.** "Is `p(a)` absent?" is
  answered against the materialized subtree covering `a`. The only obligation is that the covering
  subtree is **fully materialized** before reading absence — and content-addressing makes that
  checkable (a subtree's hash commits to its full contents).

## Approach: demand-driven incremental evaluation (magic sets), not DBSP

Two evaluation polarities exist for incremental views:

- **Push / world-driven** (differential dataflow, DBSP): deltas arrive at the leaves and propagate
  forward; stateful operators (join, dist, aggregate) **retain their full integrated input** to be
  ready for any delta. That retained state is the cost center and is in direct tension with a partial
  replica — it re-materializes locally exactly the data the replica is designed not to hold.
- **Pull / query-driven** (magic sets / demand transformation): the query determines which data is
  relevant; only that is fetched and computed.

This design is pull-driven. DBSP's algebra remains a precise account of *what* each incremental
operator must compute (see [`dbsp.md`](./dbsp.md)); the *architecture* here is demand transformation
over the existing top-down engine.

## Technique stack

| Concern | Technique | Reference |
|---|---|---|
| Demand: only touch what the query needs | **Magic sets / SIPS** (sideways information passing) | Beeri–Ramakrishnan; Alviano Ch.3 |
| Demand cone that **grows with data** (recursion, activated negation) | **Dynamic Magic Sets** — magic atoms maintained during evaluation; sound/complete for stratified | Alviano, *Dynamic Magic Sets* |
| Demand **through stratified negation** | **`n.p` complement predicate** + stratified evaluation order; optimal (only query-relevant facts, O(1) per firing) | Tekle–Liu, *Extended Magic for Negation* (arXiv:1909.08246); Balbin et al. 1991 |
| **Incremental** maintenance with retraction | **DRed** (over-delete → re-derive → insert); **FBF** for facts with multiple derivations | Gupta–Mumick–Subrahmanian 1993; Tekle–Liu |
| Signed, range-scopable **delta** | prolly-tree `differentiate(range)` (`Add`/`Remove`, lazy, hash-skipping) | `dialog-prolly-tree` |

### Negation via `n.p`

A negated body literal `not p(args)` is rewritten to a fresh complement predicate `n.p(args)` plus one
rule `n.p(x…) ← not p(x…)`; everything else is demand-transformed positively. `n.p` is computed as "p,
take what's not there" — negation is a query that excludes from the positive set. (This matches how the
engine already treats `Negation`: a filter that consumes bindings rather than producing them; Balbin
1991 frames a negative literal as the existential query `∃Y ¬s(Y)` that "removes/restricts information,
does not generate it.")

Soundness requires that p be **complete for the queried args** before its absence is read. Two
properties supply this:
- **Stratification** — p sits in a lower stratum, evaluated to fixpoint before any `not p` is consulted,
  so "p not derived for `a`" is final rather than premature. (Naively demanding the negated predicate
  makes the demand program non-stratified; the `n.p` rewrite + stratified evaluation order is the fix —
  Tekle–Liu Lemma 1, Balbin §6.)
- **Demand** — `n.p(args)` is only needed for the demanded args, so p is computed restricted to those
  args, not in full.

On this replica, "complete for the queried args" is the tree-layer materialization invariant:
`d_n.p_s(args)` names the range of p that must be lazy-loaded; once the covering subtree is fully
materialized and p is evaluated to fixpoint over it, absence is sound.

### Retraction via DRed

On a deletion: **over-delete** (remove everything reachable forward — a superset, since a fact may have
other derivations), **re-derive** (for each over-deleted fact, evaluate rules backward — head→body as a
query — to find a surviving derivation and restore it), **insert** (propagate additions forward to
fixpoint). The re-derive step is a backward query against the surviving facts, bounded by the
over-deleted set. Cardinality-one winner-selection is an instance: retracting the current winner for
`(the,of)` re-derives by querying the next-highest-cause fact for that key.

## What the codebase already provides

- **The planner already computes a SIPS** — the conceptual core of magic sets. A SIPS is a partial
  order over body atoms plus a function recording which variables each atom binds for later ones, driven
  by bound/free adornments. That is precisely `Candidate`/`Planner::plan` threading
  `env`/`binds`/`requires`, with `TryFrom<&AttributeQueryAll> for ArtifactSelector` restricting fetches
  to bound slots. The engine is already a top-down, SIPS-driven demand evaluator; what's absent is
  *reifying* that SIPS into a demand program.
- **`dialog-prolly-tree::differentiate(other)`** yields `Stream<Change = Add | Remove>` — a signed delta
  (`Remove` = negative weight / retraction), lazy and hash-skipping (`O(changed subtrees)`), and
  range-scopable via `expand(range)`. This is the delta source. The EAV/AEV/VAE indexes are three sort
  orders, giving three prefix-scopable diff views.
- **`Tree::integrate`** with deterministic conflict resolution provides the merge/reconcile step of the
  replication model; tree access through `ContentAddressedStorage` provides the on-demand subtree fetch.
- **The planned history index** retains evicted/retracted facts, supplying the prior-state a backward
  (re-derive / `n.p`) query reads. Retraction physically evicts from the current EAV/AEV/VAE indexes, so
  the current indexes are the live positive set and history is the signed change log — the weighted
  model is native, not imposed.

## Path to build it (dependency order)

1. **AST → operator IR.** `evaluate` currently lives on the syntactic AST across four pass-through
   layers; there is no compiled plan to attach demand or incremental evaluation to. Splitting the
   syntactic AST from a compiled operator IR is the prerequisite and stands alone — it also removes a
   present defect (optionality encoded on a term kind, the `entity_known` guard, and constructible
   nonsense optional queries; see the optional-as-outer-join discussion). Ready to implement.
2. **Reify demand.** Emit `d_p_s` demand predicates and the `n.p` complement construction from the SIPS
   the planner already computes, so demand becomes first-class data that scopes which subtrees an
   evaluation touches.
3. **Incremental / subscription layer.** Standing subscriptions, result-delta emission, DRed/FBF
   re-derive. (`datalogui/datalog` demonstrates the subscription surface — a `.view()` handle over
   incremental results — though it is differential dataflow and materializes all relations, which the
   partial-replica model excludes; the subscription shape transfers, the demand-gated evaluation is
   specific here.)
4. **Dynamic demand maintenance.** Negation makes a subscription depend on absence; recursion makes its
   demand cone grow with data. The demand/magic predicates must be **maintained incrementally** (Dynamic
   Magic Sets) so the set of subtrees a subscription reads expands correctly as data changes — and so
   the materialization invariant for negation continues to hold as the cone grows.

## Pointers

- This repo: `notes/dbsp.md` (IVM / selective-pull exploration + DBSP formalism), 
  `dialog-prolly-tree/src/differential.rs` (delta + merge primitives), `dialog-query/src/planner/`
  (the implicit SIPS).
- Papers: Tekle–Liu *Extended Magic for Negation* (arXiv:1909.08246) — `n.p`, optimality, FBF; Alviano
  *Dynamic Magic Sets* — SIPS formalization + dynamic maintenance; Balbin et al. 1991 *Efficient
  Bottom-Up Computation … Stratified Databases* — stratified magic sets + completeness; DBSP VLDB'23/'25
  + spec + Lean proof — the incremental algebra.
- Related systems: `RhizomeDB/rs-rhizome` (semi-naïve RAM VM, content-addressed); `datalogui/datalog`
  (differential-dataflow subscriptions in JS).
