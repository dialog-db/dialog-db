# Parallelizing query-driven replication (issue #492)

Query evaluation is a nested-loop join that awaits one complete `Select` per outer row (`rust/dialog-query/src/attribute/query/all.rs:322-333`). On a cold replica every block along every probe descent is a sequential network roundtrip, which makes first load of a tonk application very slow. All existing concurrency lives below the join: the walker's 16-way sibling read-ahead within a single scan (`walker.rs:465-506`), the level-parallel frontier walk in `traversal.rs:113-131` (whole-tree only, not query scans), and `Flight` single-flight dedup of identical remote GETs (#491). Nothing at the join level ever has two scans in flight, and there is no preload API at any layer.

## The crux

Reducing the number of sequential block reads matters more than reducing the number of block reads. Reading more blocks in fewer rounds beats reading fewer blocks in more rounds whenever fetch latency dominates, which is exactly the cold-replica case. The prior merge-join work was rejected on block-count grounds (a selective scan reads far fewer blocks than a parallel N-way merge); that argument dissolves once replication is decoupled from execution strategy: the engine can keep selective sequential semantics while the replication layer warms blocks in parallel. Wasted speculative blocks cost bandwidth, not latency.

Separately, the merge-join rejection itself is being reversed: the spike measured 4.5-5.9x CPU wins, the industry consensus favors it, our three sorted orderings mean the classic WCOJ adoption blocker (index availability) is already paid for, and the roundtrip reframing removes the remaining objection. See `notes/set-at-a-time-joins.md` on the `docs/set-at-a-time-joins` branch for the full history, and `spike/merge-join-planner` for the built spike (N-way concept merge with an `Estimate`-guarded fold fallback).

## What the WCOJ paper contributes

From `papers/A Gentle(-ish) Introduction to Worst-Case Optimal Joins.pdf.pdf`:

- The Generic Join appendix binds one variable at a time by intersecting sorted cursors. A concept query is the single-variable case (all premises share `this`), so the spike's `multi_merge_join` is the first rung of worst-case-optimal execution, not a detour.
- Variable ordering affects only constants, never the worst-case bound. Locality-aware cursor ordering is therefore free of asymptotic risk.
- The paper's chief adoption objection ("they require a lot of indexes") does not apply: every fact is already written to EAV, AEV, and VAE.
- The AGM bound consumes log relation sizes; `Scale` (#400) stores log sizes. Aligned by construction.
- Leapfrog's seeks are data-dependent: an intersection may skip large leaf stretches, so only the spine of a participating range is predictably needed. Leaf prefetch is speculative in proportion to the intersection's selectivity. This grounds the rank 2 / rank 3 split below. A committed full-range lockstep merge does consume all leaves, so commitment promotes a range's leaves from rank 3 to rank 2.

## The fetch scheduler

A priority work queue living entirely in the replication/storage layer.

**Ranks** (lower value = more urgent):

1. Blocking read: the query is blocked on this block now.
2. Likely-needed prefetch: spine blocks of ranges the query will consume.
3. Maybe-needed prefetch: leaf frontiers, and ranges the evaluator may not choose.
4. Permit-only: obtain the read permit (UCAN redeem) without fetching the block.

**The evaluator speaks ranges; the queue speaks blocks.** The evaluator API is `preload(selector, priority) -> PreloadHandle`. The tree layer resolves a range preload progressively: each fetched index node reveals the next spine level via `children_spanning` (enqueued at rank 2), and the leaf frontier under the range is enqueued at rank 3, budget-capped by `range_scale`. A range preload is a self-expanding job, and roundtrips per range come out proportional to tree depth because each spine level is one parallel wave.

**`PreloadHandle` carries abort and reprioritize.** Concept flow: block on the first scan (rank 1), preload sibling attribute ranges at rank 3, and at the decide-once point either abort the handles (entity-probe path chosen) or promote them to rank 2 including leaves (merge path chosen). Abort is advisory: blocks already in flight complete into the cache; only the unspawned tail is dropped.

**Priority inheritance through `Flight`.** A rank 1 demand read for a block already in flight at rank 3 joins the flight and promotes its priority. This makes wrong prefetch guesses safe: the worst case is joining work already started, never queueing behind it.

**Query priority composes as a tuple**, not a scalar. v1 treats all queries equally (rank only); the composite key `(query priority, rank)` versus `(rank, query priority)` is a later knob, deliberately left open.

**Permits never leak into the query engine.** Rank 4 is internal to the remote provider, where a block fetch is really permit-redeem then GET. The scheduler can degrade an over-budget rank 3 leaf prefetch to permit-only. The engine only ever said `preload(range, low)`.

**Liveness is the hard constraint.** On wasm nothing runs unless something polls it, and the standing rule from #466/#468 is that a reader never waits on a fetch it cannot drive. The scheduler must be driven by whoever awaits a demand read (the `while_warming` pattern generalized: while blocked on rank 1, make progress on lower ranks), plus opportunistically while the evaluator is CPU-busy. This constrains the design more than anything else; the scheduler gets a reviewed design note before implementation, because this same machinery took three PRs of liveness fixes to get right last time.

## Locality-aware ordering

A second input to the evaluator's decide-once point alongside `Estimate`: a residency probe answering how much of a range's spine (and optionally leaves) is already cached. Estimate is one root read; residency is cache lookups along the known spine. Scan and cursor ordering then prefers resident ranges. Per the paper, this reordering cannot affect worst-case optimality.

## Milestones

- **M0: latency-bearing benchmark.** Per-fetch latency injection, cold cache, reporting wall clock, block count, and longest sequential fetch chain (the roundtrip metric). Concept-query shapes from the spike. Everything below is judged against it.
- **M1: fetch scheduler.** Design note first (ranks, `PreloadHandle`, priority inheritance through `Flight`, permit staging, the wasm driving story), then implementation.
- **M2: range preload as a self-expanding job.** Spine resolution via `children_spanning`, leaf frontier budgeted by `range_scale`. First consumer of both landed primitives.
- **M3: evaluator hooks for concepts.** Demand the first scan, preload siblings at rank 3, abort or promote at the decide-once point. This is #492's headline case.
- **M4: revive the merge-join stack.** Rebase `feat/merge-join-operator` and the N-way concept merge onto the scheduler; a committed merge promotes its ranges to rank 2 including leaves. Re-tune the `Estimate` guard against M0 latency numbers rather than block counts. Resolve the spike's two flagged gaps: optional (left-join) concept fields, and cascade versus lockstep under skew.
- **M5: locality probe + ordering.** Residency input to the decide-once point and to cursor lead selection.
- **M6: binding-aware speculation for rule joins.** The general non-concept case: preload unbound attribute spans at rank 3.

## Decisions settled

- Warm hook lives evaluator-side (knows bindings; M6 needs it anyway).
- Prioritization is a real ranked queue, not just Flight dedup plus a budget.
- Range preload and block fetch are distinct notions; translation happens in the tree layer.
- Permits stay below the preload API.
- Merge-join is back on the roadmap as M4, no longer a separate deferred decision.
