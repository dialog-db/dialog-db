# Paper Review: Mergeable Replicated Data Types (Quark)

Review of Kaki, Priya, Sivaramakrishnan, Jagannathan, *Mergeable
Replicated Data Types* (OOPSLA '19, [Quark]), and an assessment of what
Dialog should take from it. Companion to
[`certified-mrdts-paper-review.md`](./certified-mrdts-paper-review.md),
which covers the PLDI '22 follow-up (Peepul) that builds on and
critiques this work.

[Quark]: https://www.cs.purdue.edu/homes/suresh/papers/oopsla19-mrdt.pdf

## What the paper does

Quark is the paper that established the MRDT programming model: take an
ordinary purely functional data type, run it over a Git-like versioned
store (branches, LCA, explicit three-way merge), and *derive* the merge
function automatically instead of hand-writing it. The derivation rests
on two ideas:

1. **Merge by difference against the LCA.** The primitive intuition,
   shown on an integer counter: `merge l v1 v2 = l + (v1 - l) + (v2 - l)`
   — compute each branch's difference from the common ancestor, compose
   the differences, apply to the ancestor. Not linearizable (concurrent
   `mult 2` and `mult 3` on 5 yields 25, not 30), but convergent and
   effect-preserving.

2. **Invertible relational specifications.** Differences don't
   generalize syntactically over inductive types, but every data type
   can be mapped losslessly into *characteristic relations* — sets and
   relations over its contents (list ↦ membership `Rmem` + occurs-before
   `Rob`; tree ↦ `Rmem` + labelled tree-order `Rto`; map ↦ key-value
   `Rkv`; BST ↦ just `Rmem`, since shape is not observable). In the
   relational domain, merge is *uniform* set algebra:

   ```
   A ⋄ B ⋄ C = (A ∩ B ∩ C) ∪ (B − A) ∪ (C − A)
   ```

   applied per relation, with ordering relations underspecified (`⊇`)
   and completed by a deterministic arbitration order (topological sort
   with tie-breaking, their `γ_ord`). A pair of functions α (abstract to
   relations) and γ (concretize back) makes the derivation automatic:
   `merge l v1 v2 = γ(α(l) ⋄ α(v1) ⋄ α(v2))`. Composition rules
   (Rel-Merge) handle containers of mergeable values by merging
   per-key with the LCA's value, zero-extending missing keys.

The runtime (Quark store) is a Git-like content-addressed block store
with a mutable ref store: branches are refs, versions are commits, the
LCA comes from the commit DAG, and structural sharing keeps diffs
proportional to change. Evaluated on lists, queues, ropes, red-black
maps, graphs, TPC-C/TPC-E.

Known limitations (established by the Peepul follow-up and visible in
the paper itself): merges reify the *entire state* into relations and
back on every merge — for ordering relations that is O(n²) tuples
(178 s to merge a 5000-op queue vs. Peepul's sub-millisecond direct
merge) — and the framework guarantees only convergence, not functional
correctness of the derived semantics.

## How this maps onto Dialog

Where Peepul's overlap with Dialog was the verification methodology,
Quark's overlap is the *thesis itself*. Quark's central claim — data
types are diverse, but the relational domain is a universal merge
domain where three-way merge is standard set algebra guided by the LCA
— is, read from Dialog's side, the justification for Dialog's core
design decision: **store the data as relations natively and never leave
the relational domain.**

| Quark | Dialog |
| --- | --- |
| α/γ: reify state into relations at merge time, concretize back | No α/γ — triples *are* the characteristic relation; the "abstraction cost" is zero by construction |
| `A ⋄ B ⋄ C` set merge per relation | Observed-remove screening of a tree differential against the sync base (`dialog-artifacts/src/merge.rs`) |
| LCA from the commit DAG | Sync base from the revision DAG (`notes/version-control.md`) |
| Git-like content-addressed block store + ref store, sharing across versions | Prolly-tree CAS + signed compare-and-swap branch heads — same shape, but structural sharing and diffs are at tree-node granularity and history-independent |
| Deterministic arbitration order to complete underspecified merges | The tree's deterministic hash race for concurrent same-slot byte-variants |
| Full-state merge, O(state) with O(n²) ordering relations | Differential merge, O(changed keys) |

Dialog is, in effect, the system Quark points at but cannot reach from
OCaml data types: by making the relational representation the *storage*
representation, the reify/concretize cycle that dominates Quark's merge
cost (and that Peepul demolished in its evaluation) disappears, and the
set-algebraic merge runs over a scoped tree diff instead of the whole
state. Note that Dialog's screening is deliberately *not* plain
`A ⋄ B ⋄ C`: naive set merge resurrects deletions on re-sync, which is
exactly what merge rules R1–R3 exist to prevent while staying
tombstone-free.

One semantic contrast worth noticing: Quark's queue merge is
*remove-wins for consumption* (an element popped in either branch never
reappears — clause 2 of their queue spec), while Dialog's screening is
*add-wins* in the OR-set tradition (a concurrent re-assert with a fresh
causal version survives a concurrent retract of the old version). Both
are defensible policies for their domains; the point is that the policy
is per-datatype, chosen, and belongs in Dialog's declarative merge spec
(item 1 of the Peepul review) as a stated decision rather than an
emergent property of the screening code.

## Ideas worth incorporating

### 1. Claim the lineage in the architecture notes

`notes/architecture overview.md` cites Datomic, Merkle-CRDTs, prolly
trees, RhizomeDB. The MRDT line — Farinier et al.'s mergeable
persistent data structures, Irmin, Quark (this paper), Peepul — is at
least as close a relative: it is the branch-consistent, LCA-mediated,
content-addressed-store model Dialog actually implements, and Quark
specifically articulates *why* relational state is the right merge
substrate. Recording this (a short prior-art paragraph pointing at
these two reviews) both credits the lineage and gives future
contributors the "why relations" argument in citable form.

### 2. Per-attribute mergeable value semantics (future direction)

Quark's Rel-Merge rule — containers merge per-key, delegating to the
value type's own three-way merge with the LCA value, zero-extended for
missing keys — describes something Dialog does not have and could
plausibly want: **attribute-level merge policies for mergeable value
types**. Today a concurrent update to the same `(entity, attribute)`
slot with different bytes falls to conflict detection or the hash race;
there is no way to say "this attribute is a counter — merge concurrent
updates as `l + (a − l) + (b − l)`". Dialog has everything the rule
needs: the slot is the key, the sync base supplies the LCA value, and
the merge screen already visits exactly the changed slots. Automerge's
counters are the same move in CRDT-land.

This is deliberately *not* a recommendation to build it now: a
value-level merge must be deterministic and associativity-robust across
merge topologies or it breaks byte-identical convergence, it interacts
with causal screening (a merged value has a synthetic causal identity),
and the query layer would need to surface it. But it is the natural
answer if users ask for counters/quantities that survive concurrent
edits, and Quark's derivation rule plus Peepul's verification
conditions are the right design references when that day comes. A
design sketch belongs in `notes/` before any implementation.

### 3. The order-merge recipe, as specification only

Quark's treatment of ordered structures is the cleanest statement
around of what merging an ordered collection *means*:

- preserve order agreed by all three versions;
- preserve each branch's order among its own surviving/new elements;
- restrict to surviving members;
- leave cross-branch order *underspecified* (`⊇`), completed by a
  deterministic arbitration order.

If Dialog ever grows ordered collections over triples (lists, text),
this is the right *spec shape* — order-merge as an underspecified
relation plus deterministic arbitration, which is structurally the same
move as Dialog's hash-race tie-breaking. The *representation* must not
follow the paper: materializing occurs-before as triples is O(n²) in
both space and merge cost (Peepul's headline result). Sequence-CRDT
representations (RGA-style per-element origin edges, i.e. the O(n)
"inserted-after" relation rather than the O(n²) closure) achieve the
same spec with linear state.

### 4. A cheap invariant from the counter example

Quark's counter analysis makes explicit that difference-composition
merges are convergent but non-linearizable, and states the resulting
correctness bar: *the effect of every operation is preserved in the
final state*. That effect-preservation phrasing is a good top-level
sentence for Dialog's declarative merge spec — every claim asserted or
retracted on any branch has its effect represented in the merged head
(as liveness, coverage, or a causally-resolved contest) — and it is
checkable by the property harness proposed in the Peepul review.

## Ideas noted but not applicable

- **Automatic merge derivation (α/γ + `@@deriving merge`).** The
  machinery exists to bridge from arbitrary in-memory structures to
  relations. Dialog has no such gap to bridge; adopting it would mean
  re-introducing the reify cost Dialog's design eliminates.
- **Full-state merges.** Strictly dominated by Dialog's differential
  screening; nothing to take.
- **Multiset encoding of numerics** (a natural number as a multiset of
  ones) — an artifact of forcing everything into set semantics; the
  per-attribute merge policy of item 2 is the better-typed version of
  the same goal.

## Recommendation

Quark is upstream prior art that independently validates Dialog's
deepest design bet — relations as the universal merge substrate over a
content-addressed, LCA-mediated versioned store — while its weaknesses
(reify-at-merge cost, convergence-without-correctness) are precisely
the two things Dialog's native-relational storage and the
Peepul-derived verification plan address. Concretely:

1. add the MRDT lineage to the architecture notes' prior art (item 1);
2. fold the effect-preservation sentence and the add-wins-vs-
   remove-wins policy statement into the declarative merge spec note
   proposed in the Peepul review (items 4 and the semantic contrast
   above);
3. keep per-attribute mergeable values (item 2) and the order-merge
   spec shape (item 3) on file as design references for future
   features, not current work.
