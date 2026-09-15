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

## M3 addendum: probe pipelining, measured (2026-09-09)

The first consumer of the plan is probe pipelining (bead dialog-db-77): a premise's upstream selection is wrapped (`attribute/query.rs::pipelined`) so rows buffer up to `PROBE_LOOKAHEAD` ahead and each buffered row's would-be probe is offered as a `Likely` hint. A row entering an empty window is never hinted (it is the next demand, so its hint could overlap with nothing), which also makes a single-row seed hint-free. Hinting stops permanently on the env's first refusal, so un-staged queries pay one refused call.

Measured on the soak's cold concept join (broadband, 4,000 entities), from the pre-M3 105 rounds / 8.4s:

| lookahead / budget | rounds | modeled time |
|---|---|---|
| 64 / 16 (shipping default) | 87 | 6.9s |
| 64 / 64 | 51 | 4.1s |
| 128 / 128 | 33 | 2.6s |
| 256 / 256 | 21 | 1.7s |
| 512 / 512 | 14 | 1.2s |

Two findings behind the numbers:

- **Budget, not window, is the binding constraint** past small sizes: hint jobs queue behind the per-rank concurrency cap, and a queued cold-leaf hint that starts late completes late. Row-count lead gives little *time* lead (warm rows process in ~zero modeled time), so what matters is how many cold fetches are in flight when the chain stalls.
- **Raising the budget past ~16 currently re-fetches blocks** (bead dialog-db-81): a post-flight hydration race in which a reader passes its local check before a peer's hydration lands and reaches the transport after the peer's flight closed. Mitigations landed (a local re-check before the remote fork in `NetworkedIndex`; the walker now drives its remaining range-bounded warms home at scan end instead of dropping them), but the full fix is a hydration-inclusive single-flight at the `NetworkedIndex` layer, which needs its own design pass against the env-ownership rule. Until then the default budget stays 16 and the soak's unshaped profile runs without preload, pinning the engine's deterministic demand shape (110 requests, zero duplicates).

The ~6x still on the table behind dialog-db-81 is a constant-factor ceiling of row-granular hints; the structural next steps remain M2 (range-granular expansion: spine + leaf frontier, `range_scale`-budgeted) and M4 (the merge path consuming contiguous AEV ranges preloads align with), which the region analysis in this note's campaign predicted and these measurements confirm.

## dialog-db-81 resolved: hydration-inclusive single-flight (2026-09-09)

The race is closed by sharing the WORK, not a notification: `ScopedFlight<'f, K, V>` generalizes `Flight` over the lifetime its stored futures may borrow (`Flight` stays the `'static` alias the transports use), and the driven preload jobs share a `HydrationFlight<'env>` whose futures carry fetch AND local write-back. A joiner polls the shared work itself, so nothing waits on progress it cannot drive.

Two designs died on the way, both worth remembering:

- **A notification-channel flight (leader announces, waiters await a `'static` receiver) deadlocks.** A leader living in some other scan's read-ahead set is unreachable — nested generators poll only their current await chain — so its waiters park forever. This is the #466 lesson resurfacing one layer up: co-driving is the only sound sharing here.
- **`QueryEnv` cannot hold the flight.** Interior mutability over `'env`-borrowing futures makes the holder invariant, and `QueryEnv`'s covariance in its lifetime is load-bearing for the `Provider<Select>` lifetime unification (the strict-impl dance its comments document). The flight therefore hangs off `Driven`, whose lifetime nothing shrinks. Demand reads stay outside it, protected by the local re-check and the transport flight; the measured residual is ~2 duplicate fetches per cold join, versus 449 at budget 512 before.

Post-fix sweep (broadband, 4k entities): budget 512 gives 1.18s / 14.8 rounds / zero duplicates; the shipping default moves to 256/16 (1.65s / 20.6 rounds), at which the cold lazy join beats eagerly downloading the entire space (2.4s / 30 rounds) while transferring 3.5x less — the campaign's thesis, landed: from 8.4s / 105 rounds pre-M3, a 5x cut at defaults and 7x at full throttle, with M2 (range-granular preload) and M4 (merge over AEV ranges) still ahead.

## M2 landed: range-granular job executor (2026-09-10)

Preload jobs now execute as scoped level-parallel traversals (`warm_source` in `repository/fetch.rs`): the selector's exact key range (`selector_range` under the tree's manifest) scopes `traverse_available_within`, so each depth's whole frontier fetches concurrently and a cold range costs tree-depth round trips, not block-count round trips. Reads run through the line's shared node cache in front of the hydrating networked index (`CacheThrough`), so jobs share spines with one another and with the demand reads that follow, and every fetched block still hydrates the local archive under the hydration flight. Compared to the select-and-drain executor this drops per-row parsing and spilled-value fetches for rows nobody reads.

One deliberate scope trim against the original sketch: no spine-versus-leaf rank split and no `range_scale` block budget inside the job. Point probes (today's only hints) make both moot, and M4's merge-path ranges are wanted in full once promoted; the budget returns with M6's speculative unbounded ranges if measurements ask for it.

Measured: rounds unchanged at the defaults (20.6), as predicted — the executor is substrate; moving rounds further belongs to range-shaped hints, which arrive with M4's merge over AEV ranges.

## M4 landed: concept joins evaluate as an N-way merge (2026-09-10)

The merge-join stack from `notes/set-at-a-time-joins.md` (branches `feat/merge-join-operator` + `spike/merge-join-planner`), ported onto current main and the preload substrate. Four pieces:

- **The operator** (`merge_join`, `multi_merge_join` in `dialog-query/src/merge_join.rs`): sorted-cursor intersection generic over an `Ord` key with a caller-supplied extractor (index encoding stays in the scan layer), plus `Match::combine`/`value_of`. Ported near-verbatim; the oracle suite (nested-loop equivalence over 350 random shapes, N-way included) passes unchanged on current `Match` internals.
- **The `Estimate` capability**: one root read summing per-child `Scale`s over a selector's range (`PersistentTree::range_estimate` → `ArtifactTreeExt::estimate` → `Select::estimate` → `Provider<Estimate>` on `QueryEnv`, lines summed). The spike's wide bound-threading collapsed to one addition on `Scope` — the alias absorbed the whole motion, macros included.
- **The planner**: a conjunction whose every step is a positive attribute scan sorted on one shared variable (`sort_order_of`, now cardinality-independent) is structurally merge-eligible; `scans_balanced_for` resolves each scan against the first row and takes the merge only when the widest range estimate is within 3x of the narrowest, so a pinned selective value keeps the nested-loop fold (pinned by a test against real tree estimates). Optional (left-join) fields are excluded structurally — an `OptionalScan` step disqualifies the merge, so set-widening semantics never route through the inner intersection; only the lockstep N-way was ported (the spike's cascade variant measured identical and was dropped).
- **The preload wiring**: on the merge path every input range is committed work, so each scan's resolved selector is hinted `Likely` before the streams open; the driven plan warms the ranges level-parallel (M2's executor) while the merge consumes them.

Measured (cold 5-attribute concept join, 4k entities; campaign start 8.4s / 105 rounds / 5.5MB):

| profile | rounds | modeled time | blocks |
|---|---|---|---|
| broadband | **6.2** | **498ms** | 69 / 3.0MB |
| mobile | 9.3 | 1.86s | 69 |

**17x faster than the campaign start on broadband, 15x on mobile, 5x faster than downloading the entire space — and strictly fewer blocks than the fold (69 vs 110), because the merge consumes the three AEV ranges and never touches the EAV probe region.** The original rejection (merge reads more blocks) is fully dissolved for the balanced case: fewer rounds AND fewer blocks. `filtered` (status pinned) correctly folds at the 3x balance threshold and keeps the pipelined-fold profile (~20 rounds); tuning that guard against latency-weighted cost rather than block estimates is future work, as is bead 79 (locality) and 80 (rule-join speculation).
