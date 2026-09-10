# M1: the fetch scheduler (bead dialog-db-75)

The priority work queue from `notes/parallel-fetch-scheduler.md`, designed against the machinery as it stands after PR #495. The goal: a query evaluator can express "I will likely need these blocks" without changing its own semantics, demand reads are never slowed by speculation, and no fetched byte is thrown away by a cancellation the way the pre-#495 walker threw them away.

Revised after review: the first draft's detached driver owned an env clone, which violates the load-bearing invariant that **the env/operator is never owned by anything** — it is borrowed by `.perform` for a job's duration and released after. This version has no spawned driver and no owned env anywhere.

## What the investigation fixed the requirements into

1. **`Flight` gives priority inheritance for free, at the transport.** A `Shared` future is polled by every joiner, so when a rank-1 demand read reaches the transport while a preload's GET for the same object is in flight, joining it *is* promotion: the demand caller co-drives at its own pace. `Flight`'s stored futures are `'static`, which is exactly why it lives at the transport layer, where futures are built from owned pieces (client + URL) and never borrow the env. It stays there.

2. **The seam for preload state is the session read path.** Every query read funnels through `select_from_source` → `NetworkedIndex::get` (local-first, remote-hydrate, write-back) over the line's shared `node_cache`. A preload is precisely "run that path for a block nobody demands yet": local archive checked, remote fetched, block written back and landed in the node cache, so the later demand read is local. No new fetch path exists; preloads reuse the hydrating one.

3. **Nothing may drive work it does not have the env for.** The #466/#468 rule (a reader never waits on a fetch it cannot drive) plus the env-ownership invariant pin the driving model: deferred work is *described* as data and *executed* only by whoever currently holds a borrow of the env — the running query itself.

## Shape

Two pieces: a passive plan and an active driver, split so that ownership of the state is the caller's choice (the same lesson as the caches: placement decoupled, not hardwired).

**`FetchPlan`** — pure data, no futures, no env. A ranked queue of work items:

```text
FetchPlan
├── pending: rank 2 (likely: spine) and rank 3 (maybe: leaves, unchosen ranges)
│            items: (line, hash) or a range-expansion step, tagged by handle
└── handles: PreloadHandle bookkeeping (abort, promote, budget)
```

`preload(hash, rank) -> PreloadHandle` and `preload_range(range, budget) -> PreloadHandle` enqueue descriptions. `abort()` removes a handle's still-pending items; `promote(rank)` moves them between ranks. Item ordering is `(rank, sequence)`; the composite `(query priority, rank)` key stays a later knob. Because items are data, an abandoned plan holds no resources and costs nothing.

**`drive`** — a combinator that makes the query's own evaluation the driver:

```text
plan.drive(stream, env, store..., budget) -> impl Stream
```

It forwards the inner stream (the query's selection) and, on every poll where capacity allows, materializes pending items into fetch futures *borrowing the same env the stream already borrows*, holding them in a `FuturesUnordered` bounded by the budget. The futures live exactly as long as the evaluation: when the query's stream completes or is dropped, its preload work is dropped with it — which is correct, because the preloads existed for that query. This is the walker's `while_warming` pattern lifted one layer up, and it is the whole wasm story too: progress happens whenever the consumer polls the query stream, on any executor, with nothing detached.

Two consequences worth stating plainly:

- **Demand reads never enter the plan and never wait on it.** They keep today's exact path. If a preload's GET for the same object is in flight, the transport `Flight` joins them (co-driving); if a matching item is merely *queued*, the demand read just fetches — and hydration makes the queued item a no-op when the driver later reaches it (local hit). No cross-layer coordination needed.
- **Dropping mid-flight is an explicit scope now, not an accident.** A preload fetch cancelled by the query ending mirrors the pre-#495 walker loss in mechanics, but the scope is the query rather than a single probe stream: within one conjunction the work survives across all premises (the drive wrapper sits around the whole evaluation, not per-probe), and what is lost at query end is work for a query that no longer exists. The write-back lives inside each fetch future, so every fetch that completes is persisted even if its result is never decoded.

## Where the state lives, and the API surface

Decoupled, per review: `FetchPlan` is constructed by whoever wants preloading and passed by reference into `drive`; the primitive does not choose its owner. The surfaced form is a query-builder step (settled in review):

```text
branch.query().select(query).preload(FetchBudget::default()).perform(&env)
// or, with a caller-owned plan carrying its own budget:
branch.query().select(query).preload(plan.clone()).perform(&env)
```

`preload` takes `impl Into<FetchPlan>`: a bare `FetchBudget` stages a fresh plan (for the evaluator's own hints), and a caller-owned plan is passed by clone. The budget lives on the plan (`FetchPlan::with_budget`), settled in review. `preload` stores the plan on the select builder; `perform` hands the plan into the `QueryEnv` and wraps the output stream in `drive`. The stream being driven is the query's selection — the outermost stream `perform` returns — so the drive scope covers the whole evaluation for as long as the consumer pulls rows, and dropping the stream drops the plan (abort-on-drop for free). Without `.preload` there is no plan, no wrapper, and no cost.

The evaluator reaches the plan capability-style: `QueryEnv` implements `Provider<Preload>` by enqueueing into its plan (a no-op when absent), the same threading motion the merge-join spike used for `Provider<Estimate>`; test/bench envs implement it trivially. M3's concept hooks call it; in M1 the plumbing lands with nothing enqueueing yet, keeping M1 independently landable.

An embedder wanting a longer-lived, cross-query plan passes a clone of one it keeps; an env-wrapper form (`Prefetching<'a, Env>`) was considered and set aside — coherence forbids a blanket capability delegation alongside a native `Provider<Preload>` impl, so it degenerates into per-capability boilerplate, and it still cannot wrap the output stream on its own. It remains buildable later as sugar over the same primitives.

## Dedup, and what closes the `NetworkedIndex` single-flight gap

Concurrent identical fetches dedupe at the transport `Flight` (S3/UCAN-S3 today, keyed by presigned URL; permit redeems already single-flighted). Two gaps and their treatment:

- **The `Fs` transport has none**, which makes the soak unrepresentative of production dedup. M1 generalizes `Flight` into `dialog-common` (next to `r#async`) and the `Fs` archive `Get` provider adopts it, `dialog-remote-s3` switching to the shared implementation in the same PR.
- **`NetworkedIndex` itself still races on the local-check-then-fetch window** across independently borrowed envs. The drive loop dedupes its own items against in-flight work by hash, so a single query never double-fetches; cross-query races remain possible and rare (the transport flight still collapses the actual GET). A session-lifetime `Flight<'env>` inside `QueryEnv` is the refinement if the M0 benchmark ever shows this mattering; deliberately out of v1.

## Range preload (the M2 consumer, designed now so the API fits)

A `preload_range` item expands in the driver: fetching an index node yields its separators; `children_spanning` bounds the next level; spine children re-enqueue at rank 2, leaf children at rank 3; `range_scale` against the handle's remaining budget decides how deep the rank-3 frontier goes. A huge range warms only its spine — round trips proportional to depth times ranges, the #492 sketch. Expansion is part of the item's fetch future (decode what was just cached, enqueue, return), so it borrows the same env session and stops wherever the query's lifetime ends.

## Budgets

Configurable, not constants: a `FetchBudget { likely: usize, maybe: usize }` on the drive call (defaults proposed 16/8, matching the walker's and traversal's 16, tuned against the M0 soak before landing). Handles additionally carry the `range_scale` block budget for rank-3 expansion.

## Permits (rank 4)

Stay inside the remote providers, invisible to this API, as settled in the campaign note. `dialog-remote-ucan-s3` already single-flights permit redeems; a later refinement can degrade an over-budget rank-3 item to permit-only inside the provider. Nothing in M1 mentions permits.

## What M1 explicitly does not do

- No evaluator behavior changes (M3 wires `preload` calls into concept evaluation) and no merge-join (M4). M1 lands: `Flight` in `dialog-common` + `Fs` transport adoption, `FetchPlan`/`drive`, and the session-level plumbing for M2/M3 to call.
- No negative-result caching (the ledger measured zero empty lookups).
- No spawned tasks anywhere.

## Testing

- Unit pins: a demand read is never blocked by a full plan (budget zero leaves every demand read working); a queued item already satisfied by a demand read's hydration completes as a local no-op; abort drops pending items while an in-flight fetch that a demand read co-joined still completes and hydrates; rank 2 drains before rank 3; dropping the driven stream drops the work (no leaked fetches, asserted via the observing backend).
- Transport: the `Fs` provider's `Flight` adoption gets the same concurrent-join pins as #491 gave S3, and the soak's shaped profiles should show concurrent identical GETs collapsing once M3 makes them concurrent.
- The soak gate keeps duplicates pinned at zero; `concept`'s `rounds` remains the number M3 moves.

## Settled in review

- `Flight` is generalized into `dialog-common`; `dialog-remote-s3` switches to it in the same PR.
- State placement is the caller's choice by construction; `QueryEnv` is the v1 owner.
- Budgets are configurable.
- No component ever owns the env; deferred work is data until an env borrower drives it.
