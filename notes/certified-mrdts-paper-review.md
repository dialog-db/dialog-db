# Paper Review: Certified Mergeable Replicated Data Types

Review of Soundarapandian, Kamath, Nagar, Sivaramakrishnan, *Certified
Mergeable Replicated Data Types* (PLDI '22, [arXiv:2203.14518]), and an
assessment of which of its ideas are worth incorporating into Dialog.

[arXiv:2203.14518]: https://arxiv.org/abs/2203.14518

## What the paper does

The paper presents **Peepul**, an F* library for building *mergeable
replicated data types* (MRDTs) — the branch-and-merge alternative to
CRDTs pioneered around Irmin. Its system model is a Git-like store:
branches evolve independently, and reconciliation is a **three-way merge**
`merge(lca, a, b)` where the store supplies the lowest common ancestor.
The store also supplies unique, causally-monotone timestamps (Lamport
clock + branch id), so the data type itself never manages causal
metadata — that is the middleware's job.

On top of that model the paper contributes:

1. **Declarative specifications over abstract states.** A data type is
   specified by a function `F(op, I)` giving the return value of `op`
   against an abstract state `I` = (events, oper, rval, time, vis) — a
   set of operation events plus a visibility (causality) relation.
   E.g. the OR-set spec: *read returns every `a` for which some
   `add(a)` event is not visible to any `remove(a)` event*.

2. **Replication-aware simulation relations** (adapted from Burckhardt
   et al., POPL '14) relating the efficient concrete state to the
   abstract state. Four local proof obligations — Φ_do (operations
   preserve the relation), Φ_merge (merge preserves it), Φ_spec
   (return values match the spec), Φ_con (same abstract state ⇒
   observationally equivalent concrete states) — are together
   sufficient for functional correctness *and* convergence. The store's
   own guarantees are factored out as assumptions the data-type proof
   may rely on: Ψ_ts (causally ordered events have increasing, unique
   timestamps) and Ψ_lca (the LCA's events are exactly the shared
   ones, its visibility agrees with both branches, and every LCA event
   is visible to every post-divergence event).

3. **Convergence modulo observable behaviour** — a deliberately
   weakened convergence notion: replicas that have seen the same
   events may hold *structurally different* states so long as every
   operation returns the same result on them (e.g. differently
   balanced BSTs holding the same elements).

4. **Efficient tombstone-free implementations**, mechanically verified:
   9 MRDTs including a space-efficient OR-set and an Okasaki two-list
   queue with O(1) enqueue/dequeue, O(n) merge, and at-least-once
   dequeue semantics. The OR-set's key subtlety: a *duplicate* add of
   an element already present must still bump that element's timestamp
   — silently dropping the duplicate would let a concurrent remove
   delete it, violating add-wins.

5. **Compositionality**: a generic `α-map` MRDT whose proof is
   parametric in the proof of its value type, demonstrated by building
   a verified IRC-style chat from `map` + `mergeable log` without
   re-proving either.

Proofs are discharged largely automatically by F*'s SMT backend and the
verified code extracts to OCaml running on Irmin.

## How this maps onto Dialog

The overlap in system model is close to total — Dialog *is* an MRDT
store in this paper's sense, independently arriving at the same
architecture:

| Paper | Dialog |
| --- | --- |
| Git-like branches, explicit merge | `dialog-repository` branches, push/pull/merge |
| Store-supplied LCA (`σ_lca`) | Sync base / divergence model (`notes/version-control.md`) |
| Store-supplied unique causal timestamps (Ψ_ts) | `Version = (Origin, Edition)` derived from the revision DAG (`dialog-artifacts/src/history.rs`) |
| Tombstone-free OR-set via per-add timestamps | Observed-remove screening R1–R3, no tombstones (`dialog-artifacts/src/merge.rs`) |
| Duplicate add must record its effect | Re-assert mints a new `Version`; history pass lands before data pass so a re-assert survives the retraction it supersedes |

So the paper's *mechanisms* offer little that Dialog lacks: its central
data type (the OR-set) is a special case of Dialog's merge screening,
and its causal-context management is a simpler version of Dialog's
revision-DAG-derived versions. What the paper has and Dialog does not
is a **specification and verification methodology** for exactly this
kind of system. That is where the incorporation opportunities are.

## Ideas worth incorporating

### 1. Write down the declarative merge specification

Dialog's convergence rules exist today as implementation rules (R1–R3
in `merge.rs`) plus prose in `notes/version-control.md`. Both describe
*how* the screen works, not *what* it must achieve. The paper's
abstract-state style gives a compact way to state the intent, roughly:

> A datum is live at a head iff its causal chain contains an assert
> that is not covered — via `cause`/`supersedes` — by any record in
> that head's ancestry; and two heads whose revision ancestries contain
> the same records have byte-identical trees.

Having this as the normative spec (a page in `notes/`, in the paper's
event/visibility vocabulary) separates "screening bug" from "spec
change" in future merge work, and it is the prerequisite for the next
item. Cheap, high value.

### 2. Model-based convergence testing with the paper's conditions as oracles

The most actionable transfer. The paper's Φ/Ψ conditions translate
directly into a property-test harness, no F* required:

- Maintain a *naive abstract replica* alongside the real store: a set
  of (claim, version) events plus a visibility relation — the paper's
  `I`, and precisely the `do#`/`merge#`/`lca#` bookkeeping of §3
  (merge = union of events, LCA = intersection).
- Generate random executions: fork branches, interleave asserts /
  retracts / re-asserts, merge in random topologies (not just
  two-party).
- Oracles: reads on the concrete store match the declarative spec
  evaluated on the abstract state (Φ_spec), and any two branches with
  equal abstract event sets have **equal root hashes** (Φ_con — see
  below, Dialog gets to use a stronger form).

Today the only convergence test is a single hand-written two-party
scenario (`dialog-repository/src/repository/branch/integration_tests.rs`,
`it_two_party_convergence`). The ordering subtleties `merge.rs` already
documents — history pass before data pass, re-assert racing its
superseded retraction, deletions reaching a replica whose sync base
never covered the fact — are exactly the class of bug this style of
testing finds and hand-written scenarios miss. The paper's own tricky
cases (duplicate add concurrent with remove; the same element added
concurrently on both branches) should be seeded as deterministic
regression tests regardless.

If we ever want more than testing, a model checker over the same
abstract model (e.g. `stateright`) is the natural middle ground; full
mechanized proof à la F* is not proportionate for us (the paper spends
1123 lines of proof on a 32-line queue, and our merge is entangled
with tree encoding and async I/O).

### 3. Make the store-side assumptions (Ψ_ts, Ψ_lca) explicit contract

The paper is careful that the data-type proof *assumes* store
guarantees it does not itself establish. Dialog has the same split:
`merge.rs` screening is only correct if `dialog-repository` and the
history layer deliver, among others:

- `Version`s are unique, and `Edition` strictly increases along any
  causal chain (Ψ_ts);
- the sync base used for a pull is a true common ancestor: every
  record in it is in both parties' ancestries, and anything outside it
  is on exactly one side (Ψ_lca);
- history-region integration happens before data-region integration.

These are currently implicit in the pipeline. Naming them once —
as documented invariants where `merge.rs` states its preconditions,
plus `debug_assert!`s where cheap — makes the dependency auditable and
gives the test harness of item 2 its store-property oracles.

### 4. Use "convergence modulo observable behaviour" where it actually fits

For tree state the notion is the *opposite* of what Dialog wants and
must not be adopted: history-independent, byte-identical trees are
load-bearing (root-hash equality is our state-equality and sync-dedup
primitive). Dialog targets the paper's *strong* convergence, and item
2's oracle should assert hash equality, not observational equivalence.

But the weakened notion is exactly the right correctness statement for
one thing we do have: a hitchhiker tree with pending novelty buffers
versus its canonicalized form. `canonicalize()`'s contract — flushing
yields the same bytes as a sequential build, and lookups through
buffers agree with lookups after flushing — is "observational
equivalence to the canonical representative" in the paper's terms. If
we ever property-test the novelty-buffer path independently, that is
the property to state: any interleaving of edits and partial flushes
is observationally equivalent to the canonical tree, and full
canonicalization is byte-identical.

## Ideas noted but not applicable now

- **The verified queue / ordered collections.** Dialog's data model is
  unordered triples; there is no queue-shaped merge today. If ordered
  collections (mergeable lists, text) land on top of Dialog, the
  paper's queue is a useful reference — both the merge (longest common
  contiguous subsequence with timestamp-sorted suffixes) and the
  honest concession that exact once-only dequeue is impossible without
  coordination, so the spec relaxes to at-least-once. That
  spec-relaxation move is worth remembering whenever we specify a
  datatype whose sequential contract cannot survive replication.
- **α-map composition.** Dialog's composition story runs through the
  query layer (concepts/rules) rather than nested MRDTs, so the
  parametric-proof machinery has no direct target. The general lesson
  — make datatype-level proofs/tests parametric so compound structures
  inherit them — applies to the test harness of item 2 if it grows.
- **F*/SMT mechanization itself.** Not proportionate; see item 2.

## Recommendation

The paper independently corroborates Dialog's core design choices —
branch-consistent MRDT model over a Git-like store, store-managed
causality, tombstone-free observed-remove merge — which is itself
useful signal. Adopt its methodology, not its mechanisms, in this
order:

1. seed the paper's adversarial merge scenarios as deterministic
   regression tests (hours of work, immediate value);
2. write the declarative merge spec note (item 1);
3. build the abstract-replica property harness with spec-match and
   root-hash-equality oracles (items 2–3);
4. keep "convergence modulo observable behaviour" in the vocabulary
   for buffered-vs-canonical tree reasoning only (item 4).
