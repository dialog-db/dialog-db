# Porting the novelty buffer onto version control

Status notes for landing the hitchhiker tree (`feat/novelty-buffer`) underneath the version-control stack. The core is rebased and green; what follows is the integration contract the artifact and repository layers must honor.

## Status: the differential now reads novelty

The rule below was written when the differential was novelty-blind. That is fixed: expansion routes a node's pending ops to the children covering them (as a flush would), a node with pending ops is never pruned as shared, and the entry walk resolves each key to its winning op merged over the stored entries. An oracle pins that diffing buffered trees equals diffing their canonicalized forms, over randomized streams with mismatched buffer capacities.

So **canonicalize is now an optimization, not a correctness requirement**, and the constraint below softens accordingly: a buffered tree can be diffed and reconciled directly. What still holds is that a *published* root should be canonical, because root equality is what makes fast-forward detection and subtree adoption cheap: two replicas with identical content but different flush histories have different roots, so they would fail to recognize each other as equal and would do merge work where none was needed. That is a cost-and-convergence property now, not a soundness one.

## The one rule (historical: soundness, now cost)

**A root that leaves the commit path must be canonical.** Everything the version-control design does with roots assumes a root hash is a complete, history-independent identity of the content beneath it:

- Fast-forward adoption takes an upstream tree *by root*, reading nothing.
- The graft merge prunes diffs *by node hash* and adopts whole subtrees unread.
- `divergent_bounds` derives change spans from *pruned* diff frontiers.
- Push transfers the node closure of a root.

A buffered node's hash is not that identity: `TransientIndex::novelty` is part of the node's bytes, so two replicas holding the same facts hash differently if their flush histories differ. Worse, `TreeDifference` reads `index.links` and leaf entries only, never `index.novelty`, so a key that lives in a buffer is *invisible to the differential*. Reconciling two buffered trees directly can therefore drop a buffered insert, or let one side's flushed delete lose to the other side's still-buffered key, which resurrects a deleted fact. That is precisely the failure the observed-remove design exists to prevent.

So the buffer is a **commit-path staging area**, never a published representation. `HitchhikerTree::canonicalize` is the only exit: it drains every buffer through the canonical edit path and returns a `PersistentTree`. Because our published type (`ArtifactTree`) *is* a `PersistentTree`, this rule is enforceable by the type system rather than by convention: code that never sees a buffered tree cannot publish one.

## What a buffered write must still read

The buffer cannot be a blind append log, because two of the three instructions are read-modify-write against the current state:

- **Replace** range-scans the `(entity, attribute)` slot for priors, deletes the different-valued ones, and records their versions in the new claim's `cause`. A scan blind to buffered priors would supersede nothing, leaving two live values at a cardinality-one slot and a claim whose lineage skips the value it actually replaced.
- **Retract** reads the withdrawn datum to learn the version its record must cite. Blind to the buffer, it would emit a genesis retraction (`cause` empty), which covers nothing, so the deletion would not survive a merge with a peer still holding the fact.
- **Assert** is purely additive and needs no read.

`HitchhikerTree` today exposes a novelty-aware `get` but **no novelty-aware range scan**, so `Replace` cannot be served correctly by the buffered path as it stands. Options, in preference order:

1. Add a novelty-merging `stream_range` to the core (mirrors `get`: merge each covering node buffer over the stored leaves in key order). Makes the buffered path a drop-in for `apply_versioned`.
2. Flush before instructions that need a slot scan. Correct but forfeits the amortization exactly where writes are hottest.

## Retracted facts: dropped, not retained

Decided 2026-07-18. The hitchhiker buffer represents a delete as a `NoveltyOp::Retract` tombstone op, and the observed-remove design keeps *no* tombstone in the data regions (a retract deletes the fact's keys outright, and the signed retract record plus the causal watermark is what makes the deletion stick).

These reconcile because the buffer tombstone is **transient**: it is a pending delete, not a representation of a deleted fact. `canonicalize` applies it to the leaves and it ceases to exist. Since no root is published without canonicalizing, no buffer tombstone can ever reach a published tree, a differential, or a peer. The durable carrier of a deletion remains the history record, unchanged.

This also means the buffer never needs to retain the retracted value: the record already carries what the merge screens need (the covered versions), and the coverage region mirrors it without value bytes.

## Measured

`write_path_comparison` in the read-amplification harness (release, in-memory backend, 100 single-fact batches onto a tree of the given depth, every batch's nodes imported as the commit path does):

```text
depth    canonical    buffered, flush per batch    buffered, one flush at the end
 1000    76ms         43ms                          1ms
10000    374ms        200ms                         3ms
```

Per single-fact batch at depth 10000: **3.74ms canonical, 2.00ms buffered-with-flush, 0.03ms buffered-deferred**.

Two results, and the second is the one that matters:

1. **Buffering wins even when charged a flush every batch** (~1.9x). This was not expected: the flush does the same reshaping the canonical path does. The reason is that `canonicalize` replays the drained ops onto the in-memory spine through `TransientTree::from_loaded`, with no serialization round-trip, whereas the canonical path serializes on each `persist`. So the win here is really "one in-memory reshape per batch" versus "one serializing reshape per batch", not buffering per se.
2. **Deferring the flush is worth ~100x.** This is the regime the buffer is actually built for, and it is where the SQLite-class commit latency lives: 0.03ms per batch.

The catch, and it is the whole design question: a commit publishes a signed head naming a canonical root, so today every commit must flush, which puts us in column two. Reaching column three requires the head to be publishable without a canonical tree for every commit, which is a version-control change (a head that names base + novelty, canonicalized at sync/publish points), not a tree change. That is exactly the "flush cadence" question the original design note left open, and it is now quantified: it is worth about two orders of magnitude on interactive commit latency.

## Sync cost across flush regimes

`sync_flush_regimes` (dialog-search-tree, release, 10000-entry base, two replicas writing *disjoint* keys):

```text
divergence     canonical   buffered   staged
appended    1     10           6         6
appended   16     26          14        14
appended  256    164          84       164
scattered   1      6           4         4
scattered  16     74          38        38
scattered 256    164          84       164
```

**Buffering roughly halves sync reads.** The win holds at every divergence and for both key shapes (appended keys, which cluster; and scattered keys interleaved through the base).

The staged column tracks buffered while ops stay at the root and falls back to canonical once cascading pushes them into separate subtrees, which is the expected shape: it is the depth of the ops, not the flush policy per se, that decides whether the roots can answer the difference.

## Syncing a root-buffered fact costs two reads

The property the write buffer exists for, now pinned by `it_syncs_a_root_buffered_fact_without_descending`: a new fact buffered at the root syncs in **2 reads (one root per side), independent of tree size** (measured flat across bases of 500, 5000, and 20000).

It works because two nodes with identical child links hold identical stored content, so their entire difference is the difference of their two op sets. `SparseTree::settle_buffered` detects that and resolves the pair without descending; the walk then emits the ops as entries and reads nothing beneath.

The one hard limit, worth recording because it bounds how far this generalizes: a `Link` carries only an upper bound and a hash, so from a parent you can tell which child *would* hold a key but never whether it actually *does*. Emitting a change needs that, since a key already present resolves to `Remove(old)` + `Add(new)` and only the leaf knows `old`. So the fast path applies to keys sorting past the rightmost upper bound, which are provably absent from every shared subtree, and declines otherwise (including for all retracts). Widening it would need membership information in the links, e.g. a per-child filter.

## Bug found by benchmarking## Bug found by benchmarking

Asserting that all three regimes report the *same* change count caught a real defect: expansion routed pending ops to the child whose range covered them, but the last child's range was closed at its upper bound, so an op sorting past every existing key matched no child and was silently dropped (one key per side, at 256 divergence). A flush routes such an op to the rightmost child; the walk now does the same. The randomized oracle never generated keys past the rightmost bound, so it passed throughout. Pinned by `it_sees_a_buffered_op_past_the_rightmost_key`.

## Version control on the hitchhiker tree

The full `read_amplification` harness with the buffered tree underneath, against the pre-port baseline:

```text
scenario (depth 10000)        before    after
initial pull (adopt all)      0 reads   0 reads
no-op tick                    0 reads   0 reads
fast-forward (1 commit)       0 reads   0 reads
merge (both sides moved)     32 reads  37 reads
triangle: adopt alice        22 reads  22 reads
triangle: tracked bob after  22 reads  24 reads
tree shape            60000 entries in 234-249 nodes
```

Every frugal path is untouched: adoption, idle ticks, and fast-forwards still read zero blocks, and the graft still adopts foreign bulk without walking it. Merge counts move by a few blocks in both directions across runs, which is within the run-to-run variance already seen on this harness (the same rows read 19/24/32 and 20/25/35 on repeat runs of the *unmodified* baseline).

**Net: version control paired with the hitchhiker tree costs nothing on sync and wins on writes.** The write win is 1.9x when a canonical root is published every commit, and ~100x when the flush is deferred, which remains gated on the head format carrying base + novelty.

## Equivalence

`it_lands_on_the_same_root_as_the_direct_path` pins that the buffered path produces the **byte-identical canonical root** as the direct path for the same instruction stream, across asserts, replaces, and retracts. That is what makes buffering safe to put under the commit path at all: the root a peer adopts by hash and diffs against is unchanged, so every frugal pull scenario keeps working untouched.

## Expected performance shape

The commit-path win is real: buffered writes append to a node buffer instead of rebuilding and re-hashing the touched large leaves (~240 entries each in the measured tree), which is where the ~3ms per single-fact commit goes.

The sync numbers in `rust/dialog-search-tree/benches/SYNC_FINDINGS.md` do **not** transfer to this stack. They measure buffered-vs-buffered differentials where divergence hides in root novelty, so the walk prunes everything and reports a flat 2 round-trips. That is the novelty-blind path the same document's caveats declare unsafe. Under canonicalize-before-publish the flush happens first and sync costs return to roughly the canonical figures. Net expectation: **large commit win, no sync win, flush cost relocated to the canonicalize point.** Where flush lands (every commit, on a threshold, or at publish/sync) is the remaining tuning knob, and it trades commit latency against canonicalize latency.
