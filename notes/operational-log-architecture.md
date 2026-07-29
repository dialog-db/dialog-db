# Commit-cost architecture: reaching the SQLite/LMDB profile

Status: REVISED after owner review (2026-07-29). The original draft of
this note proposed splitting the operational format from the replication
format (commit = log append + memtable update, tree materialized in
background checkpoints). The owner rejected that split on contract
grounds, and the numbers support a better path inside the existing
design. This note records the decision and the revised plan; see
`notes/sqlite-baseline-results.md` for all measurements.

## Why the log/memtable split was rejected

1. **Every commit needs a stable content address.** Branch heads,
   history records, and replication all reference a revision hash; a log
   position is not a reference another replica can resolve. Deferring
   tree materialization to checkpoints leaves commits with no stable
   identity in between.
2. **Push happens after every commit**, so the sync point is effectively
   the commit. The thing a push ships must exist at commit time, and the
   hitchhiker design already makes it small: novelty accumulates near
   the root, so a per-commit push ships one small root-region block
   rather than rewritten leaves.
3. **The root-novelty write already IS the log entry.** A commit that
   appends its ops to the root's novelty buffer and rewrites only the
   root node has the same shape as a WAL append — one small write —
   except it also yields the stable content address and the pushable
   block. The architecture does not need a second log beside the tree;
   it needs the buffered commit to actually cost what that shape
   implies.

## The measured gap, restated

After groups 1-4, the 500-real-txn SE replay (same-run):

| config | per commit |
|---|---|
| sqlite_mem | 22 us |
| sqlite_disk (WAL + NORMAL, durable) | 180 us |
| sqlite_disk_nosync | 41 us |
| dialog_mem (raw store: no signing, no disk) | 286 us |
| repo_mem (+ signing, history, head publish) | 817 us |
| repo_dcaa_nosync (disk, no fsync) | 1.9 ms |
| repo_dcaa (disk, fsync per commit) | 5.5 ms |

`dialog_mem` is the configuration with signing and disk ALREADY at
zero, and it still trails durable SQLite. So removing signing and disk
I/O is necessary but not sufficient: the in-memory buffered commit
itself is the remaining wall, and that — not a new operational format —
is the target.

## The floor the design implies

If a buffered commit truly costs "append ops to the root novelty buffer,
re-encode + re-hash the root node, one backend set, amortized overflow
cascade", the irreducible terms are roughly:

- blake3 over a ~64 KiB root frame: single-digit us (SIMD)
- rkyv re-encode of the root node: memcpy-bound, order 10 us
- one memory-backend set + head/IndexRoot bookkeeping: low us
- overflow cascade every ~buffer-capacity/ops-per-commit commits,
  rewriting a handful of nodes: low single-digit us amortized

That is a ~20-50 us floor — sqlite_disk territory — while keeping a
stable per-commit root hash and the per-commit push contract. The
measured 286 us is 6-14x above that floor; the difference is
implementation constants, not architecture.

Known leak candidates from earlier groups: enqueue costs like a full
tree edit (per-commit clone/rebuild of the transient root — group 4
measured the clone drag; owner: "cloning seems like a waste that
probably needs to be removed"); frame election/merge machinery
re-running over entries the commit did not touch (group 2 territory);
per-commit artifacts-layer constants (effect dispatch, IndexRoot block,
head write). If re-encoding turns out to be the gap, alternative
encodings to rkyv are on the table; LMDB's approach (zero serialization
— page layout IS the format, updated in place via COW) is the reference
point for how cheap the write path of a COW tree can be.

## Revised group-6 plan

Same measurement discipline as groups 1-4; no change to fundamental
behavior (commits keep producing a stable root hash and a pushable
novelty-near-root block):

1. **Pin the floor empirically**: microbenchmark the irreducible terms
   against the real root node (rkyv encode, blake3, backend set), and
   callgrind-decompose one post-group-3 SE commit into named terms so
   the budget and the leaks are both quantified.
2. **Attack the leaks in measured order** — expected: O(1)-ish enqueue
   via structure sharing instead of per-commit root clone/rebuild;
   avoiding re-election/re-merge over untouched entries; trimming
   per-commit artifacts-layer constants. Consider encoding changes only
   if the decomposition shows rkyv itself is the gap.
3. **Success criteria**: dialog_mem SE commit < 100 us first milestone
   (beats durable SQLite while keeping per-commit identity and push);
   stretch ~50 us. Repo layer target = that + batch-signed signature
   cost (approved, deferred) over DCAA for disk (validated: 1.8x faster
   than file-per-block at equal durability).

## What carries over from the original proposal

- DCAA as the disk archive and the group-commit fsync option: unchanged.
- Trust-once reads and the group-4 read-path work: unchanged.
- The Be-tree budget framing stands: fanout ~256 with ~256-op buffers
  gives one to two orders of magnitude fewer node rewrites per logical
  write than a classic B+ tree; that amortization is what pays for
  hashing, three orderings, history, and signatures. The revised plan
  collects that dividend inside the tree instead of beside it.
