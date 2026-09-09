# Sync performance: the cost of joining and reading over a real network

Status: audit record, 2026-09-01. Measurements from the `dialog-soak`
harness (`rust/dialog-soak`, `scripts/soak.sh`); baselines under
`soak/baseline`; the nightly `soak:sync` arm gates regressions.

## Why this audit exists

Joining a space (tonk's partial-replication join) feels slow on real
networks while being instant locally. Localhost hides the two quantities
that dominate a real join: **round trips** and **per-request authorization
overhead**. This note records what the replication path actually costs,
what the measurements say, and where the leverage is.

## The protocol shape today

A fresh join is cheap *at pull time* and pays its cost on first read:

1. `Branch::pull` resolves the upstream head (one `memory::Resolve` round
   trip) and, for a fresh replica, adopts the tree **by root hash** —
   zero block reads (`pull.rs`, the fast-forward-adoption arm). This part
   is excellent and must stay O(1); the soak gates it.
2. Every read after that hydrates blocks on demand through
   `NetworkedIndex::get` — **one block per request, no batching**
   (`dialog-repository/src/repository/archive/networked.rs`). The read
   side has no `GetMany`/pack effect at all; `archive::Import` exists but
   is write-only.
3. Behind a UCAN remote, each cold block costs **two sequential HTTP
   round trips plus an ed25519 signature**: redeem the invocation at the
   access service for a presigned permit, then GET the object
   (`dialog-remote-ucan-s3/src/provider.rs`). The permit cache is keyed
   per object path (`permit_cache.rs`), so its hit rate on a first
   replication is **0%** by construction — every block is a new path.
4. Tree descent is sequential: `TreeWalker::search` fetches
   root → child → leaf one at a time, so every cold point read pays
   `depth x (redeem + GET)` before its first byte. Scans prefetch
   siblings 16-wide once the descent reaches the leaf level
   (`walker.rs`); full-tree walks run level-order 16-wide with a barrier
   between levels (`traversal.rs`).

## What the soak measures

Scenario: a 4 000-entity / 24 000-fact space (18.4 MiB, ~400 blocks of
~47 KiB) seeded and pushed to an `Fs` remote; a fresh client joins over a
simulated link (per-request auth delay modeling the redeem, RTT, shared
bandwidth). Phases mirror tonk's join: pull, probe (validation point
reads), roster (membership selects), claim (commit + push), render
(first content query), entity (open one row), requery (warm), download
(full materialization). Modeled time is measured under tokio's paused
clock, so results are machine-independent.

Representative numbers (defaults: `max_segment` 64 KiB, fanout 2^8):

| link | lazy join (pull..requery) | full download |
|---|---|---|
| localhost (0.4 ms RTT) | ~0.03 s | ~0.16 s |
| broadband (30 ms RTT, 50 ms auth, 100 Mbit/s) | ~0.9 s | ~2.5 s |
| mobile (80 ms RTT, 120 ms auth, 20 Mbit/s) | ~2.5-2.9 s | ~8.5 s |
| intercontinental (250 ms RTT, 250 ms auth) | ~5.7-6.2 s | ~13.9 s |

The lazy join is ~30 requests; ~1.3 MiB moves to render a 4 000-row list
(read amplification: whole leaves for narrow queries). This harness
*undercounts* tonk's real join, which runs the same reads twice (its
staging pool is discarded; see the tonk-side audit) and adds more
validation selects.

## Findings

### 1. The single biggest lever is the per-object redeem

Setting the modeled auth delay to zero (everything else unchanged) cuts
the mobile lazy join from ~2.7 s to ~1.4 s — **nearly 2x**. That is what
a batched or scope-wide permit would buy: one redeem covering a
subject's whole read space (or a bearer token with a TTL) instead of one
per object. This needs an access-service change plus a
`dialog-remote-ucan-s3` change, and it composes with everything below.

The new `dialog::remote::ucan` tracing event splits `redeem_ms` from
`storage_ms` per effect with the cache-hit flag, so the same conclusion
is now verifiable in production.

### 2. The branching factor is no longer what sets block size

`DIALOG_TREE_FANOUT_N=5` (expected fanout 32) vs the default 8 (fanout
256) produces **the same ~400 blocks and identical join costs** for this
dataset. Since the distribution rebalance (#399), leaf size is paced by
`max_segment` (64 KiB weighted), and the fanout parameter shapes index
levels only — which barely exist at this scale (~400 leaves under 1-2
index nodes). The "revert the branching factor" hypothesis is dead:
nothing would change.

The knob that *does* trade round trips against bytes is `max_segment`:

| `max_segment` | blocks | mobile lazy join | mobile download |
|---|---|---|---|
| 16 KiB | 1 569 | 3.4 s (79 reqs) | 21.2 s |
| 64 KiB (default) | 393 | 2.7 s (29 reqs) | 8.4 s |
| 256 KiB | 101 | 2.5 s (10 reqs) | 8.3 s |

Small blocks are strictly worse on a network (more round trips *and*
more download waves). Going past 64 KiB helps the request count but the
gain flattens: per-request time becomes bandwidth-dominated (a 256 KiB
block costs ~100 ms of transfer at 20 Mbit/s), and bigger leaves raise
the read amplification of narrow queries and the write amplification of
commits. 64 KiB is a defensible default; the interesting move is not a
bigger block but **fewer sequential requests** (below).

### 3. Sequential descents and sequential phases dominate the lazy join

Each cold select descends root → leaf serially (2-3 levels here), and an
application join strings several selects back to back. On mobile every
sequential request costs ~200 ms before any bytes. The soak's probe +
roster + claim phases are mostly *serial* round trips. Mitigations, in
increasing order of work:

- Run independent selects concurrently (application-side; tonk's roster
  reads now do this).
- Speculative/batched descent: fetch the root's relevant children
  alongside the root once a query's bound is known, halving descent
  round trips for shallow trees.
- A read-side batch effect (`GetMany`) so one round trip carries many
  blocks; the 16-wide fetch windows then become 16-block request
  payloads. This is the protocol fix and pairs naturally with the permit
  work in finding 1.

### 4. What the soak gates now

- `pull` staying O(1) requests (adopt-by-root must never regress into a
  block walk).
- `requery` staying at 0 requests (warm reads must stay local).
- Lazy-join and download totals (requests, bytes, modeled ms) within
  10% of `soak/baseline` per network profile.

Run-to-run leaf-boundary wobble (randomized identities, commit
timestamps) moves a block or two between phases, so the sweep keeps the
median of 3 runs per configuration and the gate compares phase request
counts loosely and run totals tightly (`scripts/soak-compare.py`).

## Other hazards on the read/merge path (unmeasured here, real)

- The differential (`differential.rs::expand_at`) expands one node at a
  time through `ContentAddressedStorage::retrieve`, which bypasses the
  node cache and re-hashes every buffer; every merge arm inherits this.
- `Cache::get_or_fetch` deliberately does not single-flight concurrent
  misses (documented in `cache.rs`), so fan-out over a cold subtree can
  fetch the same block more than once. **Measured**: the soak's lazy
  join makes a deterministic 25 requests with zero link delay, but
  26-33 once delays are injected — the extra requests are concurrent
  misses re-fetching blocks already in flight, and the duplication
  grows with latency, i.e. exactly on the links that can least afford
  it. With concurrent application selects (finding 3) this compounds;
  worth revisiting with a wasm-compatible in-flight map.
- Hydrated blocks are written back one `put` at a time
  (`networked.rs`); an `Import` batch per fetch window would cut local
  write overhead on IndexedDB targets.
- Spilled values are fetched serially on scans (`tree.rs::scan`) and
  exports (`export.rs`); a spill-heavy space would feel this badly. The
  join scenario carries no spills — a follow-up scenario should.
- Fixed concurrency of 16 in six places; on high-bandwidth-delay links a
  higher window materially shortens downloads.

## Follow-ups, ranked by leverage per unit work

1. **Batch or scope the permit** (access service + ucan-s3): ~2x on
   every cold-read path. The tracing added in this change measures the
   win in production.
2. **`GetMany` read effect** (effects + s3 + repository): collapses
   fetch windows into single round trips; makes downloads latency-flat.
3. **Concurrent selects in application joins** (done in tonk for the
   roster reads) and speculative descent in the walker.
4. **Single-flight the node cache**, batch hydration write-backs.
5. Add a spill-heavy and a blob-carrying soak scenario, and a "staged
   join" mode that mirrors tonk's discard-and-refetch shape.
