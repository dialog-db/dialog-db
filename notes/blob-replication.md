# Blob Storage & Replication — Design + Spike

Status: spike / design record
Author: spike on branch `claude/blob-storage-replication-spike`

## Problem

We want to store binary blobs in the database and have them **replicate** to a
remote the same way the rest of the data does. Two of these requirements are in
tension and worth stating precisely:

1. Blobs can be large, so they should not be inlined into the triple
   (`the/of/is`) index — a multi-megabyte value in a single prolly-tree leaf
   wrecks the node-size distribution and forces a reader to pull the whole blob
   just to traverse that leaf.
2. Replication must be able to answer **"which blobs does the remote not yet
   have?"** without keeping a side-channel manifest in sync. Today the only
   thing that crosses the wire on `push` is the set of *novel tree nodes*
   computed by the differential; anything that is not a tree node is invisible
   to it.

The crate `dialog-blobs` already gives us local content-addressed blob storage
(Blake3, sharded directories, streaming `put`/`get`), but it is an island: no
capability surface, no participation in the differential, no replication. So a
peer that pulls the index gets triples that *reference* blob hashes it cannot
resolve.

## The core idea (refined)

The user's proposal — add a **blob index** keyed by `blob_tag / blob_hash →
bytes` so the *existing differential identifies the new segments and uploads
them with the other blocks* — is the right shape. The whole appeal is that
replication needs **zero new machinery**: a blob becomes ordinary tree content,
and `TreeDifference::novel_nodes` already computes "exactly the blocks the
holder of the source tree is missing" (see
`dialog-search-tree/src/differential.rs:915`). Push already streams those novel
nodes to the remote archive (`dialog-repository/src/repository/branch/push.rs:135`).
Put a blob's bytes *inside* the tree and it rides that path for free.

Two refinements make it actually work in practice.

### Refinement 1 — chunk the blob, and content-address the chunks

`blob_hash → bytes` as a *single* entry reintroduces problem (1) one layer down:
the entry's value is the whole blob, so the leaf holding it is enormous and any
diff that touches it ships the entire blob even for a one-byte change. So split
the blob into bounded chunks.

But **how the chunks are keyed decides whether dedup works**, and this is the
non-obvious part the spike surfaced. The tempting layout —

```
key = BLOB_TAG ‖ blob_hash ‖ chunk_index   ✗ no cross-blob dedup
```

— embeds the *blob* hash in every chunk key, so two blobs that share byte-for-byte
chunks still land in disjoint key ranges and **never share a tree leaf**.
Content-addressed dedup silently does not happen. (The spike's first iteration
used exactly this layout and its dedup assertion failed: a chunk-sharing blob
uploaded just as many novel nodes as a fully disjoint one.)

The correct layout content-addresses the **chunk**, with a separate manifest
mapping a blob to its ordered chunk list:

```
chunk    key = CHUNK_TAG ‖ chunk_hash (32)  →  chunk bytes (≤ CHUNK_SIZE)
manifest key = BLOB_TAG  ‖ blob_hash  (32)  →  length ‖ [chunk_hash; n]
```

Now identical chunks across blobs (and blob versions) are the *same entry* in the
same leaf, so the differential ships each distinct chunk exactly once. Reads
follow the manifest's chunk-hash list (no range scan needed). Both namespaces
share one fixed-width 33-byte key, distinguished by the tag byte.

`CHUNK_SIZE` is a tuning knob. Fixed-size chunking is enough for the spike;
content-defined chunking (rolling hash) is the upgrade that makes dedup survive
*insertions* (not just appends/replacements), since a CDC boundary set re-aligns
chunks after a shift instead of rewriting every following chunk.

### Refinement 2 — keep blobs in their own tree (own catalog), hydrated lazily

There are two viable placements:

| | Same tree as triples (tag-namespaced key) | Separate blob tree (own root/catalog) |
|---|---|---|
| Replication change | none — literally the user's plan | reuse `TreeDifference` on a 2nd root |
| Revision shape | unchanged | gains a 2nd root reference |
| Key layout | must fit the fixed 162-byte artifact key | free to use a compact 37-byte key |
| Lazy pull | no — pulling the index pulls blob bytes | **yes** — blob nodes fetched on demand |
| Blast radius | blob churn rewrites triple-index spine | isolated to the blob tree |

The same-tree variant is the smallest possible change and matches the proposal
literally. But it couples the two concerns the problem statement separates:
because the artifact key is a fixed 162-byte `(tag, entity, attribute,
value-type, value-ref)` layout (`dialog-artifacts/src/key.rs:75`), blob chunks
would have to be shoehorned into it, and — more importantly — pulling the index
would drag every blob byte along with it, defeating requirement (1) at sync
time.

**Recommendation: a dedicated blob tree** (`PersistentTree<[u8;37], Vec<u8>>`)
persisted under a new archive catalog `"blobs"`, with its root carried in the
`Revision` next to the existing triple-tree root. It keeps the exact
differential-replication property the proposal is built on, while also giving:

- **Lazy hydration for free.** `NetworkedIndex`
  (`dialog-repository/src/repository/archive/networked.rs`) already falls back to
  the remote on a local cache miss and caches what it fetches. Point a
  `NetworkedIndex` at the `"blobs"` catalog and a peer downloads blob chunks only
  when it actually reads a blob — the triple index stays small and pullable on
  its own.
- A compact key and node-size tuning independent of the triple index.
- Isolation: blob writes don't churn the triple-index spine.

The cost is one extra root reference in `Revision` and one extra
`TreeDifference` pass in `push`/`pull` — both mechanical, since the index tree
already does exactly this.

## How it maps onto capabilities

No new capability *kind* is needed; blobs are just another archive catalog. The
existing hierarchy (`dialog-effects/src/archive.rs`) already supports it:

```
Subject (repository DID)
  └── Archive                       // /archive
        └── Catalog { catalog: "blobs" }
              ├── Get   { digest }            → Option<Bytes>
              ├── Put   { block }             → ()
              └── Import { blocks }           → ()
```

A blob chunk is a content-addressed block exactly like a tree node, so `Put`,
`Import`, and `Get` carry it without change, and the `Attenuate` projections
(digest + sha256 checksum in the signed invocation) already cover integrity
across the wire. Read-only delegation = delegate `Catalog{"blobs"}` with `Get`;
write = with `Put`/`Import`. The blob *tree's* nodes live in this catalog; the
blob *content addresses* referenced from triples are the manifest hashes.

So a triple keeps referencing a blob by a single 32-byte hash (today
`Value::Bytes`/`Value::Record` inline the bytes — `value.rs:25`; the blob-aware
path would store the **blob hash** as the reference and move the bytes into the
blob tree). The blob tree resolves that hash to chunks.

## Garbage collection

Content-addressed blobs accumulate; nothing today reclaims them
(`dialog-blobs` has no GC). Because blob chunks are now *tree entries*,
reachability becomes a tree question: a blob is live iff some triple references
its manifest hash. A mark-and-sweep can walk the triple index, collect
referenced blob hashes, and drop blob-tree entries (and their backing chunks)
outside that set. Out of scope for the spike but unblocked by putting blobs in
the tree — the index *is* the reachability root set.

## What the spike proves

`rust/dialog-blobs/tests/blob_index_spike.rs` is a self-contained, runnable
demonstration (dev-dependencies only — no change to the production crate graph).
It implements the dedicated-blob-tree design against the *real*
`dialog-search-tree` differential and asserts the property the whole plan rests
on:

1. **Content-addressed chunk index.** `put_blob` splits bytes into `CHUNK_SIZE`
   chunks, stores each under `CHUNK_TAG ‖ chunk_hash → bytes`, and writes a
   `BLOB_TAG ‖ blob_hash → length ‖ [chunk_hash…]` manifest. `read_blob`
   reconstructs the bytes by following the manifest's chunk list.
2. **Replication rides the differential.** Inserting a blob and diffing the tree
   against its previous version yields novel nodes; streaming those novel nodes
   to a fresh "remote" store and rebuilding the tree from its root lets the
   remote `read_blob` the identical bytes — i.e. the differential *did* identify
   exactly the blocks needed to replicate the blob.
3. **Chunk-level dedup (and the keying pitfall).** A blob that shares most of its
   chunks with an already-replicated blob uploads strictly fewer novel nodes than
   a fully-disjoint blob of the same size. This test only passes because chunks
   are keyed by chunk hash; it is the guard that caught the `blob_hash ‖ index`
   mis-keying described above.

Run it:

```
cargo test -p dialog-blobs --test blob_index_spike
```

## Suggested path to production (post-spike)

1. Promote the blob-index key/chunk helpers into `dialog-blobs` as a real module
   (`BlobIndex`), generic over a `dialog-search-tree` storage, backed by the
   existing `dialog-blobs` VFS for the raw chunk bytes if we want large chunks
   off-heap.
2. Add a `"blobs"` catalog wiring in `dialog-repository` (a `NetworkedIndex`
   over the blobs catalog) and a second root in `Revision`.
3. Extend `commit`/`push`/`pull` to diff and import the blob tree alongside the
   index tree (the loops already exist; they get a sibling).
4. Switch large `Value` variants to store a blob-manifest hash instead of inline
   bytes, with a size threshold deciding inline-vs-blob.
5. Content-defined chunking + mark-and-sweep GC.
