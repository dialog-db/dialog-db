# Paging

> Design note. What "paging" can mean against this engine, which of those meanings the current
> architecture can serve cheaply, which it cannot serve at all, and the order to build them in.
> Companion to [`incremental-subscriptions.md`](./incremental-subscriptions.md) (the demand cover and
> the poll loop this design leans on) and [`query-engine-design.md`](./query-engine-design.md) (the
> planner whose freedom to reorder premises is what makes result order unstable today).

## Three asks wear the same word

"Paging" conflates three requirements with very different costs. Separating them is most of the design.

1. **Bounded work.** "Give me 20 rows and don't compute the other 40 000." A property of *evaluation*:
   stop early, read less, replicate less.
2. **Stable continuation.** "Give me the next 20 after those." A property of *position*: some notion of
   where the last page ended that survives concurrent writes.
3. **Random access.** "Give me page 47." A property of *rank*: the ability to name the 940th row without
   visiting the first 939.

They are ordered by difficulty against this codebase. (1) is nearly free today. (2) is a modest amount
of new machinery and, notably, makes subscriptions *cheaper* rather than harder. (3) is in direct
conflict with a deliberate design decision in the tree and should not be offered as an exact operation.

All three presuppose a fourth thing the engine does not have: **a defined result order**.

## What exists today

Verified against the tree at `534bf48`.

**Evaluation is already lazy and streaming.** `Selection` and `Output` are async streams
(`rust/dialog-query/src/selection.rs:23`, `rust/dialog-query/src/query/output.rs:12`), and a conjunction
is a nested-loop pipeline threading a binding stream through each `Plan` step
(`rust/dialog-query/src/planner/plan.rs:55`). Dropping the output stream stops the work. Nothing forces
full materialization except the callers that ask for it (`try_vec`).

**Scans come out in index order, and that order is well defined across sources.** A select merges each
branch's tree scan with the session overlay through `merge_grouped`
(`rust/dialog-repository/src/layer.rs:76`), which is a k-way merge on `SortKey`
(`rust/dialog-artifacts/src/artifacts/update.rs:267`) — the unique total order whose restriction
reproduces the residual order of all three index layouts. So a select's output order is exactly "what a
single physical tree containing every source would scan", for any selector. This is a stronger property
than it looks and it is the foundation for cursor paging.

**Ranges are first-class.** `selector_range` (`rust/dialog-artifacts/src/tree.rs:1008`) turns a
constrained selector into an inclusive key range in one of the three orders, and the selector already
carries prefix and open/closed value bounds (`is_at_least`, `is_less_than`, `is_starting_with`,
`rust/dialog-artifacts/src/artifacts/selector.rs:267`).

**Subscriptions are already gated by key ranges.** `Demand` is a merged set of disjoint key ranges
recorded at the `Select` boundary (`rust/dialog-repository/src/repository/branch/subscription.rs:100`,
recorded in `QueryEnv::execute`, `.../branch/session.rs:411`); a poll diffs the pinned root against the
new one *scoped to that cover* (`PersistentTree::differentiate_within`,
`rust/dialog-search-tree/src/tree.rs:275`), so subtrees outside the cover are never loaded.

**Subtree size estimates exist.** `Scale` (`rust/dialog-search-tree/src/scale.rs`) gives a one-byte
count estimate per node: exact up to 64, logarithmic above, monotonic, and an upper bound. Its module
doc is explicit that it is advisory — "never use it to skip work: not to decide a range is empty, not to
terminate a scan early".

What does **not** exist: any `limit`, `offset`, `order by`, or cursor concept anywhere in
`dialog-query`; any aggregation at all (no count, sum, min, max, group-by — the grep is empty); and any
notion of a partial result in the subscription layer, which retains the entire result set as a `Vec`
(`.../branch/subscription.rs:247`) and diffs it by full scan on each maintenance pass.

**There is no stable result order.** Two reasons. Within a rule body the planner greedily picks the
cheapest *feasible* premise at each step, so which premise drives the nested loop — and therefore which
index order dominates the output — is a cost decision that can change as the data changes (now more so,
since #400 feeds real subtree scales into the estimates). For recursive concepts, rows come out of a
fixpoint answer table keyed by a `HashMap<Entity, BTreeMap<..>>`
(`rust/dialog-query/src/concept/query/fixpoint.rs:99`), so there is no meaningful order across concepts
at all. Any `limit` shipped before an explicit order would silently return an arbitrary subset that
changes under the user's feet.

## Does Datomic support it?

Partially — and *where* it supports it is the instructive part: **Datomic pages the index, not the
query.**

- `d/q` returns a realized **set**. No order is defined, so no order is guaranteed. The client API's
  arg-map does accept `:limit` and `:offset` ([client
  API](https://docs.datomic.com/client-api/datomic.client.api.html)), but they are applied to the result
  of the query, not pushed into it: they bound what you receive, not what the engine computes, and
  because the result is a set the "page" they cut is not stable between calls.
- `d/qseq` ([executing queries](https://docs.datomic.com/query/query-executing.html)) is a lazy variant,
  but the laziness is in *tuple realization* — pull and `:xform` are deferred. The returned seq supports
  `count` efficiently, which tells you the join work already happened. It reduces peak memory and
  time-to-first-row; it does not make the query do less work.
- The real answer is the index API. `d/index-pull`
  ([docs](https://docs.datomic.com/indexes/index-pull.html)) walks `:avet` or `:aevt` in index order and
  returns a lazy seq, with `:start` (a position, "at least `:a` must be specified"), `:offset`,
  `:limit`, and `:reverse`. On the peer side the equivalents are `d/datoms`, `d/seek-datoms` and
  `d/index-range`. This is keyset paging over a physical index: ordered, lazy, resumable, and it reads
  proportional to what you consume.

So Datomic's answer to "sorted, paged access" is: leave the Datalog engine, walk an index with a start
position, pull the entities you land on. Sorting or paging by anything that isn't an index order is the
application's problem — the standard advice is to pull the (bounded) result into memory and sort it
there.

Two things Datomic never had to answer, which we do:

- It has no standing queries and no incremental view maintenance. `tx-report-queue` hands you raw
  transaction data and you re-run whatever you like against `d/with`; nothing maintains a page for you.
  There is no prior art there to copy for "a page that stays correct".
- It assumes the whole index is locally reachable. Here a page is also a *replication* decision: what
  the page's range covers is what gets fetched.

The precedent worth copying is the shape of `index-pull`: **an order, a start position, a count.** The
precedent worth *not* copying is `:offset` on `q`.

## Is paging possible with subscriptions and differential updates?

Yes — but only for one of the two paging semantics, and the one that works is the one that makes
subscriptions strictly cheaper.

### Offset paging is the semantics that breaks

An offset page is defined relative to the *whole* result: rows `[k, k+n)` of an ordered set. Three
independent problems, each fatal on its own:

- **It needs rank.** Landing on row 940 without visiting 939 requires a count per subtree, i.e. an
  order-statistic tree. The tree deliberately does not carry exact counts: `scale.rs` explains that an
  exact count changes on every insert, dirtying the whole root path and re-hashing every ancestor on
  each commit, which works directly against the novelty buffer and against structural sharing for sync.
  `Scale` is what we chose instead, and it is an upper-bound *hint* that must not gate work. So exact
  offsets cost O(offset) reads, always.
- **Its demand cover is the prefix, not the page.** A subscription on rows `[940, 960)` is affected by
  *any* insertion or deletion below row 940, because those shift the window. The honest cover for an
  offset page is `[start-of-order, end-of-page]` — the entire prefix. Every write into that prefix
  re-triggers, and on a partial replica it also forces the prefix to be materialized. This defeats the
  whole point of the cover.
- **It is not stable even without subscriptions.** The classic skip/duplicate problem: a row inserted
  before the window between two page fetches shifts everything down one, so page 2 re-shows a row from
  page 1.

### Keyset (cursor) paging is the semantics that fits — unusually well

A cursor page is defined relative to a *position in an order*: "the first `n` rows at or after key `k`".
The page is then literally a key range, and every piece of the subscription machinery is already stated
in terms of key ranges:

| Concept | Already exists as |
|---|---|
| the page's extent | an inclusive `Key` range |
| what the evaluation read | `Demand`, a merged set of `Key` ranges |
| what changed that matters | `differentiate_within(scope)` — a *range-scoped* diff |
| what to fetch on a partial replica | the covering subtrees of that range |

A paged subscription's cover is the page, so a write anywhere else in the relation is free and never
even loads a subtree — where today a subscription over an attribute is invalidated by *every* write to
that attribute. **Paging does not fight incremental maintenance here; it is the smallest possible
demand cover.** The interesting consequence: a UI showing a 20-row window over a million-row relation
holds a subscription whose poll cost is proportional to changes inside those 20 rows.

The maintenance rules for a page `(order, after: k, n)` retaining rows `r₀..r_{n-1}` spanning keys
`[k₀, k_{n-1}]`:

| Change | Effect | Cover |
|---|---|---|
| insert `< k₀` | none on the page's contents (a cursor is a position, not an ordinal) | unchanged |
| insert inside `[k₀, k_{n-1}]` | row enters at its sorted position; the last row is evicted | shrinks to the new last key |
| delete inside the window | row leaves; the page is short, refill by scanning forward past `k_{n-1}` | grows to the new last key |
| insert/delete `> k_{n-1}` | none | unchanged |

Every case is a bounded amount of work: at worst one forward scan of the page's own length. This is the
same shape as the existing `maintain` path but keyed on *position* rather than on subject entity — the
current per-entity DRed (`Application::restrict`, `.../branch/subscription.rs:605`) is the wrong unit
for a page and a page-aware maintenance variant is needed alongside it.

Two API consequences fall out:

- **`Delta` is not expressive enough.** Today it is a set delta (`{asserted, retracted}`,
  `.../branch/subscription.rs:212`). A page is an ordered list; a consumer needs to know where a row
  entered and what fell off the end, otherwise it re-renders the whole window on every keystroke of
  someone else's edit. A page delta wants position, plus an explicit "the window's tail moved" signal.
- **Insertions before the cursor are invisible by construction** — which is correct for a stable window
  and wrong for a UI that wants to say "3 new items above". That indicator is a *count over the prefix*,
  which is an aggregate, not a page. It should be an opt-in second subscription, priced as such, and it
  is genuinely expensive for the same reason offset paging is.

Cursors should be **exclusive bounds on a key, not references to a row**: encode the index tag, the key
bytes, and the revision they were minted at. Then a cursor whose row was retracted is still perfectly
valid, and the revision lets a server tell a client its cursor predates something it cares about, rather
than silently returning a page from a different world.

## The hard part: order that isn't index order

Cursor paging is exact and cheap precisely when **the requested order is an index order and the page is
a range on a single scan**. Three cases where it is not:

**Sort by value within one attribute is not contiguous today.** The VAE index is *value*-major:
`build_key` writes the value slot, then attribute, then entity
(`rust/dialog-artifacts/src/key/varkey.rs:342`), and `selector_range` picks the VAE branch as soon as
any value bound is set, so an attribute-and-value-bounded selector produces a range over the value
dimension with the attribute as a post-filter (`rust/dialog-artifacts/src/tree.rs:1025`). "Ten highest
scores" therefore scans every fact of every attribute whose value falls in that band. It is still
ordered, still streaming, and cursors still work — but the read amplification is proportional to
unrelated attributes' values, which is the wrong bill. The fix is a fourth ordering, AVE
(attribute-major, value-second, entity-third): the key layout is tagged and self-delimiting and the
history index shows adding an ordering is routine (`HISTORY_KEY_TAG` in
`rust/dialog-artifacts/src/key.rs`). This should be decided on evidence — it costs write amplification
and index size on every commit — but "page a list sorted by a field" is the single most common paging
request a UI makes, so it likely earns its place.

**Order by a joined or derived column is a blocking sort.** If the sort key comes from a premise other
than the driver, rows cannot be emitted in order without collecting them all first. That destroys
bounded work and streaming simultaneously. There is no clever fix; the honest options are to refuse it,
or to serve it through top-K (below), which bounds *memory* but still reads the whole input.

**Recursive concepts have no order.** Fixpoint results come from a hash-keyed answer table. A paged
recursive concept must either sort at the end (blocking) or be refused.

This suggests the surface should be explicit rather than accommodating: an order the engine can honor by
scan is accepted and pinned; an order it cannot is a planning **error** with a message naming why, not a
silent full sort. Note this pins the planner: if the page's order must be honored, the ordered premise
must lead, which removes a degree of freedom from cost-based selection. That is a real trade
(`plan(scope)` may then choose a worse join order) and should be recorded as a deliberate one — order is
a correctness constraint, cost is a preference, and constraints win.

## Could aggregation offer a way?

Yes, in one precise sense, plus two smaller ones.

**`ORDER BY … LIMIT n` *is* an aggregate — top-K — and top-K is the one paging primitive that is
incrementally maintainable with bounded state.** This is the standard result from the IVM line the
subscription work already draws on: differential-dataflow systems compile `ORDER BY … LIMIT` into a
hierarchical top-K reduce, whose retained state is O(K) per group rather than O(group size). The
maintenance rules are exactly the ones tabulated above, and crucially the deletion case — "the Kth row
was retracted, admit the K+1th" — is a *re-derivation*, which is precisely the step DRed already
performs here (`.../branch/subscription.rs:592`). So top-K is not a foreign body: it is the existing
delete/re-derive/insert cycle applied to a ranked window instead of to an entity's rows.

What top-K buys over cursor paging: it works when the order is *not* an index order, because the K
retained rows are an explicit materialized window rather than a range of the tree. What it costs: it
reads the whole input on first evaluation (no bounded work — only bounded memory and bounded *output*),
and its demand cover is the whole input range, so it gives up the cheap-poll property that makes cursor
paging attractive. It is the fallback for derived orders, not the primary mechanism.

**Counts.** Two useful and honest things are available without touching the tree:

- *Approximate totals for free.* `Scale` on the nodes covering a range gives "about 4 300 results" in
  O(depth) reads, monotone and upper-bounded — enough for "showing 1–20 of ~4 300" and for a scrollbar.
  It must be labelled approximate in the API, because it is: it excludes pending novelty and rounds up.
- *Exact totals as a maintained aggregate.* A count over a range is exactly `initial scan + (adds −
  removes) within the cover` on each poll — the diff the subscription already computes. Exact, cheap to
  maintain, expensive only to initialize. That is the right way to offer an exact count without adding
  exact per-subtree counts to the tree and paying the churn `scale.rs` rejects.

**`min`/`max` are top-1** and are the smallest useful instance of the whole aggregation story. Shipping
them first exercises the grouped-reduce plumbing (group key, ordered accumulator, retraction
re-derivation) against a case whose correctness is easy to see, and they are independently wanted.

What none of this changes: aggregation does not rescue **offset**. A maintained count tells you how many
rows exist; it does not let you jump to the 940th without walking. Offset over an aggregate is still
O(offset).

## Recommendation

Cursor-first. Offer offset only as an explicitly-priced convenience over a bounded range (or not at
all), and never inside a subscription.

Build order, each step independently useful:

1. **Order as a first-class query property.** An `Order` (index + direction) attached to a query;
   accepted only where the plan can honor it by scan order, a named error otherwise. Pin the ordered
   premise as the driver. Nothing else on this list is meaningful before this exists.
2. **Bounded output on one-shot queries.** `limit` on the `Output` stream. The streams are already lazy,
   so this is small — but it is only *correct* after (1), which is why it is second and not first.
3. **Cursors.** Opaque, encoded `(index tag, key bytes, revision)`, exclusive bound on key. Round-trip
   through the wire and the wasm/JS surface.
4. **Paged one-shot query.** `select(q).order(…).after(cursor).limit(n)`, returning rows plus the next
   cursor. At this point the engine matches Datomic's `index-pull`, over a partial replica.
5. **Paged subscription.** Cover = the page's range; page-aware maintenance (the table above); an
   ordered page delta. This is the step that only this codebase can do, and the one with the most
   design risk — do it after 1–4 are settled and tested.
6. **AVE ordering**, if sorting by a field's value within one attribute is a requirement (it probably
   is). Independent of 1–5; slots in under the same `Order` surface.
7. **Aggregation**, starting with `min`/`max` (top-1) → `count` → top-K → group-by. Unlocks paging over
   derived orders (via top-K) and honest totals, and is wanted for its own sake.

Steps 1–5 are the paging feature. Step 7 is a different feature that happens to subsume the hardest
corner of paging; it should not be on the critical path for a first page.

## Open questions

- **Does a paged subscription report activity above the window?** ("3 new items above.") It is a prefix
  count, i.e. an aggregate with a prefix-sized cover — the expensive thing cursor paging exists to
  avoid. Opt-in, separately priced, or refused?
- **Which order is the default** when a query is paged without an explicit one? Refusing is defensible
  and probably right; entity order is the cheap alternative but is meaningless to a user.
- **Do cursors survive a merge?** A merge can insert rows before the cursor; a keyset cursor is a key,
  so it stays valid and the window's contents are unchanged. Confirm this against `Tree::integrate`'s
  conflict resolution — specifically whether a losing-side value replacement can move a row's key out
  from under a cursor that points at it.
- **How does a page interact with the overlay?** Overlay facts merge into the ordered stream through
  `merge_grouped`, so they land in the page correctly, but the overlay is off-tree and an epoch move
  currently forces a full recompute (`.../branch/subscription.rs:402`). For a page that recompute is
  cheap (it is one bounded scan), so this may simply be fine — worth confirming rather than assuming.
- **Cardinality-many and pages.** A page counted in *facts* and a page counted in *entities* differ when
  an entity has several values for the ordered attribute. Which does a user mean, and does the answer
  change between an attribute query and a concept query?
- **Wire and JS surface.** `useQuery` (`typescript/dialog-experimental/src/react.ts`) hands the whole
  result set to `setState` on every poll; a paged hook wants the window plus a cursor plus an explicit
  "load more", and the ordered page delta from (5) is what makes it not re-render the world.
