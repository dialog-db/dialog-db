# Operational log architecture: reaching the SQLite/LMDB profile

Status: design proposal from the 2026-07 performance campaign (see
`notes/sqlite-baseline-results.md` for all numbers). Next step: group-6
spike validating the cost model on the existing harness.

## The structural diagnosis

After groups 1-4 removed the constant-factor waste (real commits -65%,
point gets -39%, batch -55%), the remaining 10-40x gap vs SQLite is
structural: the content-addressed prolly tree is both our REPLICATION
format and our OPERATIONAL format, so every commit pays blake3 over
rebuilt ~64 KiB nodes, rkyv encoding, and effect dispatch, and every read
pays decode/validation — costs SQLite (in-place pages + WAL) and LMDB
(mmap'd pages, zero serialization) simply do not have. SQLite itself does
not operate on its B-tree: it appends to a WAL and checkpoints.

## The proposal

Split the two roles. The prolly tree remains exactly what replicates;
it stops being what every operation touches.

1. Commit = DCAA log append (+ optional fdatasync) + in-memory index
   update. Recent facts live in a fast mutable structure — the hitchhiker
   novelty buffers already are this. Cost shape: tens of us per durable
   commit (SQLite WAL shape). The DCAA spike measured the append path at
   1.8x FASTER than file-per-block at equal durability.
2. Tree materialization becomes a background checkpoint: canonical nodes
   are built on sync boundaries or idle, amortizing all hashing and
   serialization across hundreds of commits. Enabled by the owner's
   policy decisions already landed: commits are non-canonical, sync ships
   novelty-near-root (fewer block exchanges), import canonicalizes.
3. Reads = memtable + zero-copy base: recent facts from the in-memory
   buffer, older facts from the mmap'd DCAA file via trust-once rkyv
   access (group 1 built the machinery). LMDB-style pointer reads.
4. Recovery = replay the log from the last checkpoint. Prolly-tree
   determinism guarantees replay converges to the identical root.
5. Replication unchanged: the tree still syncs; it is just built off the
   commit critical path.

## The Be-tree budget (why this can afford dialog's semantics)

The hitchhiker tree is a Be-tree: writes buffer in nodes and migrate down
in batches, amortizing one leaf rewrite across every buffered op destined
for it. With fanout ~256 and 256-op buffers the advantage over a classic
B+ tree (SQLite) is one to two orders of magnitude fewer node rewrites
per logical write. That factor is the budget that pays for what dialog
does extra per write: content hashing, three orderings, history records,
signatures. Evidence it works when collected: group 3 measured -73%
instructions on 500 sequential commits once commits stopped
canonicalizing. Where it still leaks: enqueue costs like a tree edit
rather than an append (group 4's enqueue-batching attempt measured +11%
from shifted overflow cascades — the buffer mechanics carry constant
drag); flushes re-enter per-node canonical rebuild rather than
bulk-applying a whole buffer.

Cost model to validate in the spike:
  dialog_commit ~= (per-op constant) + (semantic extras) / (Be batching factor)
  vs sqlite_commit ~= page write + WAL append + fsync/checkpoint share
Target after this architecture: 20-100 us durable commits, 1-3 us point
reads; irreducible floor above SQLite = signatures (batch-sign approved,
deferred) + history + triple ordering, amortized by the Be factor.

## Group-6 spike plan

Memtable-over-DCAA with checkpoint materialization, behind the same
archive-capability surface, measured on the existing dialog-baseline
harness (SE replay + synthetics, SQLite controls, callgrind where wall
clock is noisy):
- commit latency (durable and relaxed) vs sqlite_disk WAL+NORMAL
- read latency against a store with a deep unflushed memtable (worst case)
- checkpoint cost and its amortization curve (commits per checkpoint)
- crash-replay correctness: kill mid-log, replay, compare roots
- O(1)-ish enqueue and bulk per-node flush application as part of the
  spike, since the Be dividend depends on both

## Prerequisites already landed

Non-canonical commits + explicit canonicalize (group 3); trust-once
validation (group 1); DCAA v1 with delta-index chain, relaxed-fsync mode,
tested recovery (group 5 spike); deferred: batch-signing, ed25519 SIMD
backend flag, compaction (dead blocks dominate bytes in both stores).
