# Novelty Buffer (Hitchhiker Tree)

## Goal

Amortize write cost across the whole tree, and keep a deterministic canonical
form available on demand.

Today every commit rebuilds the search tree along the touched path, re-hashing
the spine to the root, so a stream of small commits re-hashes the upper levels
over and over. We want writes to be buffered so the expensive rebuild is
amortized, while still being able to produce the exact canonical
(history-independent) root whenever we need it for sync, publishing, or
comparison.

The model is a **hitchhiker tree**: every inner node carries a buffer of pending
ops, and a full buffer cascades one level down toward the leaves. Buffered writes
are cheap (append to a node's buffer); the canonical rebuild happens lazily and
incrementally as buffers overflow, instead of eagerly on every write.

This supersedes an earlier "buffer only at the root" sketch. That sketch kept the
base node hashes stable by never embedding buffers in nodes, but it gave up the
per-level amortization a real hitchhiker tree provides. We take the hitchhiker
tradeoff (see below) in exchange for that amortization, and recover the canonical
form explicitly via [`canonicalize`](#canonicalize).

---

## Hitchhiker mechanics

Each inner node holds, alongside its child links, a bounded buffer of pending ops
keyed by tree key. Operations:

- **Write**: append the op to the root node's buffer. An insert and a delete are
  the same mechanism: both are ops in the buffer, distinguished by a tag; a
  delete is a tombstone op, not a physical removal. Within a key, the last op
  wins.
- **Flush one level** (`flush_node`): when a node's buffer is full, partition its
  ops by which child's key range they fall into, and move them into those
  children's buffers. The buffer is consumed in key order so the partition is one
  pass. At the **leaf** level a flush applies the ops to the segment, which is a
  `TransientTree` batch-apply (the copy-on-write edit primitive we already have).
- **Read**: descend from the root; at each node, the buffered ops on the path
  that cover the queried key are merged over what the children hold, so a read
  sees pending writes before they reach the leaves. A buffered tombstone hides a
  base fact; a buffered assert shadows or adds one.

This is the standard fractal/Bε buffering: writes touch only the top buffer most
of the time, and work cascades down amortized over many writes.

### The hash tradeoff (accepted)

A buffer embedded in a node is part of that node's bytes, so a buffered write
changes that node's hash, and the change propagates up the spine. Consequences,
accepted deliberately:

- While buffers are non-empty, node hashes are **not** the canonical ones; the
  buffered tree's identity differs from the fully-flushed tree's.
- The byte-exact, history-independent canonical root exists only **after**
  [`canonicalize`](#canonicalize).
- Sync and dedup by subtree hash have full pruning power on the canonical
  (flushed) form. Recent novelty concentrates in the upper buffers, so a sync
  that exchanges the upper levels still carries most recent divergence.

This is the opposite of the rejected root-only sketch, which prized base-hash
stability. We give up always-canonical node hashes to gain per-level write
amortization, and make the canonical form an explicit operation.

---

## canonicalize

`canonicalize()` flushes every buffer all the way to the leaves, leaving all
buffers empty. The result is the deterministic, history-independent canonical
tree: the same fact set always produces the same root, byte for byte, regardless
of write/flush history.

Take it whenever a stable identity is required:

- a sync checkpoint, where two replicas must agree bitwise;
- a content-addressed publish, where the root hash names the data;
- a comparison or test that needs the order-free canonical form.

Between canonicalizations the tree is a faster, buffered, non-canonical
representation of the same logical fact set.

---

## Three modes as flush policies

One core mechanism, three behaviors selected by how far a flush cascades:

- **OneLevel (true hitchhiker)**: an overflowing buffer flushes one level down.
  Write-optimal; work is amortized across levels. The default.
- **Full**: a flush pushes ops straight through to the leaves rather than one
  level at a time. This reproduces the earlier "drain the buffer into the
  canonical tree" behavior: the tree stays close to canonical, with shallower
  buffering. Equivalent to canonicalizing on each flush trigger.
- **Never (passthrough)**: never buffer (or never flush); every write goes
  straight to the canonical tree, i.e. today's unbuffered behavior. Mostly a
  benchmark baseline and the trivial degenerate policy.

These are thin wrappers over the core's `flush_node` / `canonicalize` primitives,
choosing the cascade depth. The core does not know which policy is in force; it
just exposes the flush primitives.

---

## Layering

The core hitchhiker mechanism is generic over the base tree
(`dialog-search-tree`): node buffers, the per-node flush primitive, and
`canonicalize` know about keys and ordering but not about artifacts.

The artifact specifics (one logical fact derives EAV/AEV/VAE index keys via
`FromKey`; an `Instruction` becomes buffered ops; the read merge yields
`Artifact`s; cardinality-one / `Replace` supersession) live in the
`dialog-artifacts` consumer, exactly as `ArtifactTreeExt::apply` / `scan` do
today. The consumer drives the core's flush primitives and supplies the
index-derivation and merge semantics.

---

## Open questions (to resolve while building the core)

- **Node format**: the per-node op buffer is a `novelty` field on the
  content-addressed `Index` node: `PersistentIndex { links, novelty:
  Vec<NoveltyEntry<Key, Value>> }`. Only `Index` nodes carry novelty; leaf
  `Segment`s do not (ops flush into segment entries at the leaf). A node with
  empty `novelty` is the canonical form of that node: deterministic and
  order-free, so `canonicalize` (which empties all novelty) yields the
  history-independent root. (Adding the field does change the node's serialized
  bytes versus the pre-novelty format, so this is not wire-compatible with trees
  written before the change; it is a format revision, not an additive one. No
  stored data depends on the old layout.) This realizes the accepted
  node-hash-moves tradeoff.
- **Flush primitive shape**: `flush_node` at inner levels moves ops from a node's
  buffer into its children's buffers (new); at the leaf it is a `TransientTree`
  batch-apply (exists). How these compose, and how a flush interacts with the
  COW edit path and the caller-owned `Delta`.
- **Buffer capacity** per node (the write-amplification knob); hitchhiker runs
  buffers several times the fan-out (e.g. ~900-1000 against fan-out 100-200). For
  our base fan-out Q=254, pick a starting value and make it tunable.
- **Read merge** down a buffered path: descending the spine merging each node's
  covering buffer ops, with tombstones, in key order. Reuse the existing
  `merge_grouped` / `SortKey` / tombstone approach at the artifact layer.
- **`canonicalize` cost and when callers invoke it** (every commit, on a
  threshold, only at sync/publish), and how it relates to the Full policy.
- **Sync reconcile** over buffered trees: same-base union vs different-base merge,
  and whether reconciliation operates on canonicalized forms or on buffered
  forms with buffer union.
