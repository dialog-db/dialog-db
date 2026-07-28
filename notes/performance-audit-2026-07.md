# Performance audit: closing the gap with SQLite while keeping partial on-demand replication

Date: 2026-07-28. Scope: full read of `dialog-search-tree`, `dialog-storage`,
`dialog-query`, `dialog-artifacts`, `dialog-repository`, the remote drivers
(`dialog-remote-fs`/`-s3`/`-ucan-s3`), and `dialog-common`/`dialog-encoding`.
Every finding cites real code; the top findings were independently
re-verified against source.

The constraint honored throughout: nothing here proposes abandoning
content-addressed prolly trees, hash-pruned diffs, or blob-store-only
persistence — those are what make partial on-demand replication work. Almost
every finding is a constant-factor or scheduling fix that leaves canonical
node bytes (and therefore root hashes) unchanged. The few format-touching
items are flagged explicitly.

## Executive summary

The architecture is sound and several hard things are already right: the tree
diff is genuinely hash-pruned (reads proportional to the difference, verified
by in-tree counting tests), key-range pushdown from Datalog selectors to tree
key ranges is real and thorough, plans are cached by `(rule, adornment)`,
recursion is semi-naive, node flush is batched and 16-wide concurrent, and
`Buffer` memoizes hashes and key decodes. The read-path `O(fanout)` walker
fix documented in `benches/DISTRIBUTION_FINDINGS.md` shows the team already
hunts this class of problem.

What separates the current system from SQLite is not the data structure — it
is five recurring habits:

1. **Re-validating trusted immutable data on every touch.** Every node access
   re-runs full rkyv bytecheck over the whole 64 KiB buffer; every storage
   read and store re-hashes blake3; every `Entity` reconstructed from a key
   this crate wrote re-runs `url::Url::parse` plus a blake3 hash — per
   scanned row.
2. **Sequential awaits where concurrency is safe.** Tree-diff expansion loads
   one node per fixed-point pass; remote fetches, blob shipments, and
   spilled-value I/O are one-at-a-time awaits. Pull latency is
   `differing_nodes × RTT` when it could be `÷ 16`.
3. **Per-operation work that should be per-batch.** A fresh
   `reqwest::Client` (new TLS/connection pool) per HTTP request; a UCAN
   access-service round trip + Ed25519 signature per block despite 1-hour
   permit validity; a full storage-pipeline setup per query input row; a
   root-path re-encode + re-hash per insert when a batch API already exists;
   an IndexedDB transaction per get.
4. **Copy-heavy hot-path representations.** Query rows are
   `HashMap<String, Binding>` deep-cloned per step per row; cache hits memcpy
   whole blocks; every read `into_owned`s values that rkyv could serve
   zero-copy.
5. **Concurrency ceilings and missing durability policy.** The
   `Arc<Mutex<Storage>>` wiring holds one async mutex across backend I/O —
   all reads serialize. The native store writes one file per 64 KiB node
   (≥ 4 syscalls, flat directory) with **no fsync**, i.e. file-per-blob write
   amplification without the crash-durability it should buy.

Fixing themes 1–4 requires no format changes at all. Theme 5's fix (a
pack-file or embedded-KV local node store with per-commit fsync) changes only
the *physical* local layout — the logical content-addressed model, and
therefore replication, is untouched.

## Top findings across all subsystems

Ordered by expected impact on the SQLite-comparison workloads (interactive
point reads/writes, flat joins, small frequent commits, cold sync).

| # | Finding | Where | Impact | Format |
|---|---------|-------|--------|--------|
| 1 | Full rkyv bytecheck on every `body()` call, many times per op | `dialog-search-tree/src/node/persistent.rs:142-145` | High: dominant constant on every read and write | none |
| 2 | Point lookup in a leaf is a linear front-coded decode; the existing key memo (`memoized_keys`) is used by scans but never by `get` | `node/archive.rs:425-438`, `tree.rs:190` | High for point reads | none (memo route) |
| 3 | Query engine: full storage pipeline (selector convert, branch resolve, stream merge, lock, tree clone) rebuilt **per input row** of every join step | `dialog-query/src/attribute/query/all.rs:291-316`, `dialog-repository/src/repository/branch/session.rs:397-428` | High: dominates every multi-premise query | none |
| 4 | Query rows are `HashMap<String, Binding>`, deep-cloned per emitted row per step; claims map populated even when nothing reads it | `dialog-query/src/selection/match.rs:113-124`, `all.rs:311` | High: per-row constant everywhere | none |
| 5 | `Entity` reconstruction runs `Url::parse` + blake3 per scanned row; fills a documented-legacy 64-byte buffer no live path reads | `dialog-artifacts/src/artifacts/artifact.rs:199-204`, `uri.rs:61-130` | High on scans | none |
| 6 | Commit encodes/hashes the same value 3–5× per instruction (payload, spill check, spill store, history record, coverage key) | `dialog-artifacts/src/key.rs:103-167`, `tree.rs:141-154,1110-1397` | High for versioned/spill workloads | none |
| 7 | `Artifacts::commit` uses the canonical rebuild path; the amortizing hitchhiker/buffered path exists (`buffered.rs`) but only `dialog-repository` uses it | `dialog-artifacts/src/artifacts.rs:419-422`, `buffered.rs` | High for many small commits | none (canonicalized roots identical) |
| 8 | Write path re-encodes/re-hashes the root path per op; the batch entry point (`HitchhikerTree::write`) is private; docs bless persist-per-op | `dialog-search-tree/src/hitchhiker.rs:250-298`, `tree/transient.rs:851-1387` | High for insert throughput | none |
| 9 | All diff/hydration fetches sequential; diff fixed-point expands **one node per pass** | `dialog-search-tree/src/differential.rs:1043-1155,217-225,709-770` | High: pull = differing-nodes × RTT | none |
| 10 | `NetworkedIndex` has no multi-get, no in-flight dedup, no negative cache | `dialog-repository/src/repository/archive/networked.rs:66-101` | High (multiplies with #9) | none |
| 11 | Fresh `reqwest::Client` per HTTP request (DNS+TCP+TLS per block); same in the UCAN site | `dialog-remote-s3/src/s3/permit.rs:56-75`, `dialog-remote-ucan-s3/src/site.rs:33` | High, ~10-line fix | none |
| 12 | UCAN path: one access-service POST + Ed25519 sign per block op; permits valid 3600 s but never reused | `dialog-remote-ucan-s3/src/provider/archive.rs:13-40`, `request.rs:42` | High on replication | protocol (not data) |
| 13 | `Arc<Mutex<Storage>>` blanket impls hold the lock across backend I/O — storage concurrency capped at 1 | `dialog-storage/src/storage/content_addressed.rs:90-105`, `backend.rs:158-205`, `dialog-artifacts/src/web.rs:220-229` | High wherever wired (all of dialog-artifacts, wasm) | none |
| 14 | Native store: file-per-block, blocking `exists()` stat, flat dir, unbounded parallel import, **no fsync** on block writes | `dialog-storage/src/storage/provider/fs/archive.rs:29-99`, `fs/native.rs:242-268,325-330` | High: the main local-write gap vs SQLite WAL, plus a durability hole | physical layout only |
| 15 | Redundant blake3 + copies at the tree/storage boundary: store re-hashes bytes whose hash is memoized, retrieve re-hashes every read, flush copies each node into a fresh `Vec`, reads realign into `AlignedVec` | `dialog-search-tree/src/storage.rs:37-69`, `accessor.rs:67`, `dialog-common/src/buffer.rs:148-154` | Med-High | none |
| 16 | `Cardinality::One`: per-candidate second scan to find the winner; per-input-row re-scan with fresh pipeline | `dialog-query/src/attribute/query/only.rs:48-94,227-279` | High for cardinality-one-heavy schemas | none (index-assisted winner would be format) |
| 17 | Fixpoint: re-plans per derived row, clones the answer table per round, caller join is O(inputs × table), no magic-set seeding | `dialog-query/src/concept/query/fixpoint.rs:140-152,386-395`, `concept/query.rs:301-307` | High for recursive queries | none |
| 18 | Commit holds the index write-lock across all storage I/O (readers block for the whole commit) | `dialog-artifacts/src/artifacts.rs:410-462` | Med-High for concurrent read/write | none |
| 19 | Blob + spilled-value shipment strictly sequential on push; single-part uploads; no remote-has probe for multi-MB blobs | `dialog-repository/src/repository/branch/push.rs:227-283` | Med-High for blob workloads | none |
| 20 | Linear O(fanout) child routing in transient/hitchhiker descents (archived path already binary-searches) | `tree/transient.rs:3313-3329`, `hitchhiker.rs:996-1013` | Med | none |

## Cross-cutting fixes (each resolves several findings)

### A. Trust-once validation and hashing (findings 1, 2, 5, 15)

`Buffer` is immutable, `Arc`-shared, and already memoizes its blake3 hash in
a `OnceLock`. Extend the same pattern:

- Memoize bytecheck: validate on first `body()` per buffer, then
  `rkyv::access_unchecked`. Keep the checked path for first-touch bytes from
  untrusted peers.
- `get` should consult `memoized_keys()` + binary search on warm leaves
  (`buffer.should_memoize()` already encodes the second-touch heuristic);
  memoize resolved dictionary tables per buffer too.
- A `store_buffer(&Buffer)` path that trusts the memoized hash (pointer
  identity — the hash was computed from these bytes), and a trusted-backend
  mode for `retrieve` from local storage. Verification stays mandatory at
  the *replication ingress* — which today is, backwards, the one place that
  does **not** verify (`networked.rs:88-98` caches remote bytes by
  re-hashing rather than checking the requested digest; fix as part of this).
- An internal trusted `Entity`/`Attribute` constructor for values decoded
  from keys we wrote; make the legacy 64-byte companion buffer lazy or
  delete it (only `#[cfg(test)]` code reads it).

### B. Batch the write path end to end (findings 6, 7, 8, 18, 20)

- Compute `ValuePayload` once per instruction and thread it through key
  building, spill store, history record, and coverage key. Short-circuit the
  spill decision on `raw.len() > inline_n` (an encoded value is always
  longer than raw).
- Make `HitchhikerTree::write(Vec<NoveltyEntry>)` public and route
  `Artifacts::commit` through the buffered path (`buffered.rs` pins root
  equality with the direct path by test). Document persist-per-op as an
  anti-pattern; persist per commit.
- Clone the tree under a short lock, run apply + flush outside it, re-acquire
  to swap the root.
- `partition_point` binary search in `child_for`/`child_index`.
- Sort assert-only bulk imports by key so the edit cursor makes one
  left-to-right pass instead of ping-ponging across EAV/AEV/VAE regions
  three times per fact.

### C. Concurrency where content addressing already makes it safe (findings 9, 10, 11, 12, 13, 19)

Content-addressed blocks make speculative and concurrent fetches trivially
safe — the codebase just doesn't exploit it yet:

- Diff: expand the whole frontier per fixed-point pass (gather every
  position that must expand, fetch concurrently, then re-prune) instead of
  one node per pass; fetch index-node children with bounded concurrency in
  `SparseTree::stream`; prefetch next sibling leaves in walker range scans.
- `NetworkedIndex`: in-flight dedup map (`Shared` futures keyed by digest),
  small negative-result TTL cache, and a `get_many` the prefetchers call.
- One process-wide `reqwest::Client` (`OnceLock`) — ~10 lines, removes a TLS
  handshake per block.
- Cache UCAN permits keyed by (subject, catalog, method) until near expiry;
  longer term, session-scoped grants instead of per-digest.
- Ship blobs/spilled values with the same `buffer_unordered(16)` the node
  upload already uses; probe remote-has (one HEAD) before shipping multi-MB
  blobs; implement the multipart import from `notes/blob-replication.md`.
- Storage traits: `get` should take `&self`; replace the
  `Arc<Mutex<…>>`-across-I/O blanket impls with read-locks or internally
  synchronized backends.

### D. Query engine per-row constants (findings 3, 4, 16, 17)

- Hoist per-step invariants out of the row loop: open the scan
  cursor/branch-stream/tree handle once per plan step and hand it to the row
  loop.
- Compile variable names to slot indices at plan time; make a row a small
  vector (or Arc-shared) instead of `HashMap<String, Binding>`; populate the
  claims map only when a plan step actually reads `source`/`cause`.
- Add a merge-join operator: two AEV scans of different attributes are both
  entity-sorted — a `name ⋈ age` join should be one linear zip, not one EAV
  probe per entity. Batch remaining probes (sort inputs, coalesce adjacent
  key ranges).
- `Cardinality::One`: batch winner verification per entity group from the
  already-sorted stream; cache winners per `(the, of)` within a step.
- Fixpoint: plan once per (rule, delta-position) via the existing
  `PlanCache`; `Arc` the answer-table rows; index the table by `this`; seed
  with caller bindings (magic sets) when the adornment binds `this`.
- Evaluate plans by reference/`Arc` so concept boundaries and negation stop
  cloning the whole plan tree per input row; bound the disjunction
  fork channels (currently unbounded — memory blowup risk on slow
  alternatives).

### E. Local persistence architecture (finding 14) — the one real architectural item

File-per-64 KiB-node cannot match SQLite's WAL for small-commit throughput,
and today it doesn't even buy durability (temp+rename with no `sync_all`; a
crash can leave a published head referencing missing block bytes). Options,
in increasing ambition:

1. Immediate hygiene: drop redundant `exists()` pre-checks (open and handle
   `NotFound`), bound import concurrency, shard the archive directory by
   hash prefix, and pick an explicit durability policy (fsync batch + dir
   fsync before publishing a head, or documented non-durability).
2. Pack small nodes into an append-only log/pack file with an in-memory (or
   embedded-KV) digest→offset index, one fsync per commit batch; keep
   file-per-blob only for large blobs. The logical model — blake3-addressed
   immutable blocks — is unchanged, so replication and the wasm/OPFS parity
   promise are unaffected as long as the layout is versioned (note
   `fs/web.rs:13-15` promises byte parity between web and native today).

### F. Planner statistics (finding, `dialog-query/src/schema.rs:85-168`)

Costs are a static constant ladder; the planner cannot tell a 10-row
attribute from a 10M-row one. The prolly tree can answer range-size
estimates in O(log n) cached node reads (top-level fanout × depth), and
subtree scale capture just landed (#400) — feed `estimate_rows(selector)`
into planning as the tie-breaker the design notes already propose, and
multiply (not add) downstream step costs by upstream cardinality.

## Smaller items worth a pass

- `Delta::get` takes the write lock for a read
  (`dialog-search-tree/src/delta.rs:66-69`) — one-word fix.
- `StorageCache`: single tokio `Mutex` + full-block memcpy per hit +
  count-based bound; switch to the `ShardedSieveCache` + `Arc<[u8]>` pattern
  `dialog-search-tree/src/cache.rs` already uses, and byte-weight both
  caches. Consider shrinking the encoded-level cache where a node-level
  cache sits above it (same bytes cached 2–3×).
- `SpillCache` clones the full block on every hit and insert
  (`dialog-artifacts/src/tree.rs:190-236`) — store `Arc<Vec<u8>>`/`Bytes`.
- Scan-side spilled-value fetches are serial — pipeline with
  `try_buffered(k)` (`tree.rs:960-967`); commit-side spilled writes should
  join the existing 16-wide flush and skip blocks the backend already has.
- Range-bound construction does full parse-and-rebuild chains with ~800-byte
  max sentinels per bound (`dialog-artifacts/src/key/entity.rs:77-111`,
  `varkey.rs:171-189`); build bounds from `KeyParts` directly as
  `artifact_index_keys` already does.
- `CompressedStorage` has a truncation bug (`write` vs `write_all`,
  `dialog-storage/src/storage/compress.rs:48`), is unused, and blocks carry
  no compressed/raw tag — decide compression's place in the block format
  (and hash-before-or-after-compression) **before** more data exists; the
  brotli dep in `dialog-encoding` is dead.
- IndexedDB: transaction per get, two copies per JS boundary crossing;
  bulk import is already one transaction — add the read-side equivalent.
  OPFS: 4–6 promise round trips per read, no directory-handle caching,
  copy-based rename.
- `merge_grouped` computes a `SortKey` per element with a linear peek-min
  (`dialog-repository/src/repository/branch/layer.rs:76`) — fine for few
  branches, worth a heap if branch counts grow.
- Value reads always `into_owned` (full rkyv deserialize + alloc) — offer a
  zero-copy archived-value handle (`(Buffer, offset)`) for callers that
  only inspect.
- Remote fetch re-publishes the snapshot cell even when the ETag didn't move
  (`remote/branch/fetch.rs:35-41`).

## What is already good — do not regress

- Hash-pruned diff with zero-read fast paths (context-inclusion skip,
  fast-forward adoption by root); read set proportional to the difference.
- Key-range pushdown from selectors to tree ranges, including prefix and
  numeric value-range bounds; plan caching by content-addressed rule +
  adornment; semi-naive recursion with stratification.
- Sealed novelty buffers pass through persist verbatim; untouched children
  re-emit links without re-hash — the core write-amplification win the
  batching work should build on.
- `Buffer`'s memoized hash / decode memo / touch-count heuristic; the
  walker's zero-copy search path (the `O(fanout)` fix in
  `benches/DISTRIBUTION_FINDINGS.md`).
- Node flush and upload bounded at 16-wide; IndexedDB bulk import in one
  transaction; commit batches novel blocks into one `Import`.
- The 64 KiB / high-fanout geometry is right for replication (Datomic-style
  few-round-trips); fix the per-node constants rather than shrinking nodes.
- `Scale` deliberately avoids exact counts so ordinary edits don't dirty the
  root path — statistics work (F) must not reintroduce that.

## Suggested sequencing

1. **Days, pure constant-factor, no behavior change:** memoized bytecheck
   (#1), memoized-keys binary search in `get` (#2), shared `reqwest::Client`
   (#11), storage `get(&self)` / drop the cross-I/O mutex (#13),
   `Delta::get` read lock, drop `exists()` pre-checks, trusted
   `Entity` constructor (#5), single value encode per instruction (#6),
   verify remote fetches by requested digest.
2. **Weeks, throughput and latency:** public batch write + buffered commit
   path (#7/#8), commit I/O outside the index lock (#18), per-step query
   pipeline hoisting (#3), slot-indexed rows (#4), concurrent diff
   expansion + `NetworkedIndex` dedup/multi-get (#9/#10), permit caching
   (#12), concurrent blob shipment (#19), cache Arc-values and
   byte-weighting.
3. **Architectural, sequenced behind benchmarks:** pack-file/embedded-KV
   local node store with per-commit fsync (E), merge joins and
   `Cardinality::One` index-assisted winners (D), planner statistics from
   subtree scale (F), fixpoint magic sets (#17), compression-in-format
   decision.

Phases 1–2 change no persisted bytes and can be validated root-for-root
against the existing test suite; the criterion benches
(`dialog-search-tree/benches`, `dialog-query/benches`) plus the Stack
Exchange dataset (`notes/benchmark-dataset.md`) are the right harness to
gate each step — especially the block-read-count benches, which will catch
any accidental regression in the replication-frugality properties this
audit was careful to preserve.
