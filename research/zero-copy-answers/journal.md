# Zero-copy answers: journal

## Where things stand

**Status: Evaluating.** Built: late materialization, a merge join keyed
on stored bytes, optional fields left-merged, the `dialog-answer` frame
with Rust and JS readers, and the `answers` harness (main built from its
worktree with `--cfg dialog_main`). The two settings the measurements
were taken with (`DIALOG_LAZY_BINDINGS`, `DIALOG_LEFT_MERGE`) are
removed; see 2026-11-18.

Headline, tonk's issue concept at 3,000 rows against main: engine
instructions −62% and allocations −84% per row; query to receiver 2.75×
fewer instructions with the frame. `tonk_bench` at 10×: warm `v2/issue`
288 → 82 ms, subscription polls 939 → 23 ms and 1,398 → 46 ms.

Open: `issue-detail` cold 3× slower with the left merge, cause not
settled; `subscribe-repoll` +2.9% and `read-scan` bytes +8.4% on the perf
gate; commands within noise but not separated. The report is in the
topic file.

## Datasets

Generated from fixed seeds by the `answers` harness, nothing on disk:
`stuff` at 2,000 and 20,000 rows (two text fields), `issue` at 300 and
3,000 (eight fields, 56-byte `did:key` entities). See
[evaluation.md](evaluation.md). `tonk_bench` replays tonk-team-shaped
datasets generated from the checked-in schema
(`rust/dialog-baseline/fixtures/tonk-team-schema.jsonl`): `tonk-1x.jsonl`
(dataset `40f769d902274835`) and `tonk-10x.jsonl` (`8aaf9d973a0a1ba6`).

## 2026-11-16 — where does a concept read's time go?

**Question.** Is materializing and copying values a large part of a
concept read?

**Method.** `perf sweep` under callgrind, `read-scan` and `query-join`
(2,000 entities, main at `04965aa`), inclusive costs.

**Results.** `query-join` is 80 M instructions, 40 K a row. The tree scan
that yields borrowed rows (`FactView`) is 15% of it; the allocator, 30%;
`to_owned`/`reconstruct` 10%; the rest is `Match` combining, cloning and
dropping, `merge_parameters` and `Value` clones. `read-scan` makes 16
allocations and 3 KB of allocation per row.

**Decision.** Worth trying: most of the work is after the scan.

## 2026-11-16 — late materialization alone

**Question.** Binding scanned rows in place, materializing only on read:
how much does it take off?

**Method.** `Match` slots over a shared scanned row; `answers` harness
under callgrind, engine phase only, both binding modes of one build.

**Results.** Engine, per row (main's eager mode of the same build ≈ main):

| dataset | eager instr | lazy instr | eager allocs | lazy allocs |
|---|---|---|---|---|
| stuff 2,000 | 36,804 | 34,898 | 51.2 | 36.2 |
| issue 300 | 198,503 | 181,020 | 161.5 | 116.5 |

Allocations −29%, instructions −5 to −9%: under the bar on instructions.
The profile says why: the merge join's key was
`value_of(&this).map(encode_value_owned)`, which materialized the entity
(a URI string, an `Arc` and a Blake3 hash of the URI for its key bytes)
and encoded it into a fresh `Vec` for every row of every input, eight
times per issue; and `cross_combine` cloned the growing row once per
input. The lazy row was materialized by the join anyway.

**Decision.** Not enough alone; the join had to stop materializing.

## 2026-11-16 — the join on key bytes

**Question.** Keyed on the scanned entity's bytes, with groups of one row
combined in place, does the engine clear the bar?

**Method.** `JoinKey` (a share of the scanned row; bytes compare as the
order-preserving encoding would, a terminator apart), `cross_combine`
moving a lone row instead of cloning the partial, `combine` reserving,
and a table in `is_provably_canonical`. Engine phase under callgrind;
main built from the main worktree with `--cfg dialog_main`.

**Results.** Engine, per row:

| dataset | variant | instructions | allocations | bytes |
|---|---|---|---|---|
| stuff 2,000 | main | 36,145 | 51.2 | 9,043 |
| | eager | 35,464 | 48.2 | 7,909 |
| | lazy | 31,216 (−14%) | 22.2 (−57%) | 8,069 |
| issue 300 | main | 194,675 | 161.5 | 42,427 |
| | eager | 168,923 | 142.5 | 31,231 |
| | lazy | 139,068 (−29%) | 54.5 (−66%) | 29,455 |
| issue 3,000 | main | 181,961 | 159.8 | 41,682 |
| | eager | 156,279 | 140.8 | 30,477 |
| | lazy | 125,931 (−31%) | 52.8 (−67%) | 28,688 |

The eight-field read clears both engine bars (allocations halved or
better, instructions −25% or better); the two-field read clears
allocations and not instructions: it has less to copy per row.

What is left on the eight-field read (lazy, issue 3,000): the scan 21%,
`checked` 14%, `merge_scanned` 14%, `combine` 12%, `memcpy` 11% (rows and
generator states moved), `merge_parameters` 8%, key compares 5%.

**Decision.** Keep. The engine still spends about four times the scan
turning scanned rows into joined rows.

## 2026-11-16 — the columnar floor

**Question.** What does the same read cost with no rows at all between
the scan and the frame?

**Method.** The `columnar` phase: one AEV scan per field, competing rows
elected with `FactView::elect`, the cursors aligned on the entity's
stored bytes, cells written from the winner's key. Checked against the
JSON row for row. Wall time, median of five.

**Results.**

| dataset | engine + frame encode | columnar |
|---|---|---|
| stuff 2,000 | 6.9 + 0.8 ms, 25 allocs/row | 2.8 ms, 6 allocs/row |
| issue 3,000 | 42.2 + 9.8 ms, 56 allocs/row | 21.1 ms, 13 allocs/row |

**Decision.** Not decided. It answers only required, cardinality-one,
stored fields (no rules, no optional fields): what tonk's list views read
most. Whether that is worth an operator of its own depends on how often
the planner could pick it, which `tonk_bench`'s views will say.

## 2026-11-16 — tonk's workload, with late materialization

**Question.** Does the engine's gain show on tonk's own views,
subscriptions and commands?

**Method.** `tonk_bench` over `tonk-1x.jsonl` (3 rounds) and
`tonk-10x.jsonl` (2 rounds), `fs` store, main, the branch eager and the
branch lazy, interleaved round by round. Medians of every round's
samples, milliseconds.

**Results.** Within noise almost everywhere: warm `v2/issue` at 10× is
263 ms on main and 232 ms lazy (−12%), warm `v2/activity` 501 and 474;
the subscription polls and commands do not move.

**Decision.** The harness's concept was not tonk's. Warm `v2/issue` at
10× is 87 µs a row, the harness's eight-field concept 14 µs: `v2/issue`
has thirteen fields, ten of them optional, and `v2/activity` five
optional of eight. A conjunction with an optional field was never
merged (`merge_variable` refused every `OptionalScan`), so a list read
folded: one scan, then every other field probed once per entity. That,
not copying, is where a list view's time goes.

## 2026-11-16 — left-merging optional fields

**Question.** Read every optional field once, in entity order, beside
the required scans (an entity a field lacks bound Absent), instead of
probing it once per entity: what does it take off?

**Method.** `left_merge_join` (`merge_join.rs`): each optional side a
cursor advanced to the left row's key; a side standing on it extends the
row, one that does not binds its value and cause Absent. A conjunction
merges its optional fields when their scans sort on the merge variable
and some required scan names the entities; a row that pins an optional
field's value or cause, or a required scan pinned to a value or an
entity, keeps the fold. An optional range is weighed against the
required scans' size, not as one of them: the first cut weighed it as
one, and a field present on 10% of issues made every list read look
unbalanced and fold. `DIALOG_LEFT_MERGE=0` turns it off. Tests: an exact
case (present, absent, many-valued sides) and 200 random shapes against
the per-row probe it replaces; every `dialog-query` test in every mode.

The `answers` harness gained `tonk-issue`, `v2/issue`'s shape: three
required fields, ten optional at the fixture's rates. Every variant's
answer hashes the same.

**Results.** `answers`, `tonk-issue`, engine phase, wall time (median of
seven) and allocations per row:

| rows | main | lazy, no left merge | lazy, left merge |
|---|---|---|---|
| 300 | 20.6 ms, 445 | 15.3 ms, 384 | 7.8 ms, 74 |
| 3,000 | 165 ms, 446 | 182 ms, 385 | 66 ms, 71 |

`tonk_bench`, 10×, main / branch without the left merge / with it:

| measure | main | no left merge | left merge |
|---|---|---|---|
| query/v2/issue/warm | 246 | 224 | 59 |
| query/v2/issue/cold | 527 | 554 | 135 |
| query/v2/activity/warm | 493 | 397 | 115 |
| subscription/v2/issue/first | 300 | 271 | 60 |
| subscription/v2/issue/poll | 845 | 806 | 21 |
| subscription/v2/activity/poll | 1,238 | 1,248 | 41 |
| query/issue-detail/cold | 2.9 | 3.4 | 12.8 |
| steady/all (commands) | 4.1 | 4.4 | 4.6 |

Every build answers every view with the same number of rows, at the
start and at the end of the week.

**Decision.** Keep, with one fix: `issue-detail` (the activity of one
issue, `issue` pinned to a value) regressed 4×. The conjunction was
newly eligible, and deciding it estimated every required range, which on
a cold replica walks each range's edges; the decision was the fold all
along. A required scan pinned to a value or an entity now folds without
estimating (`66f04f6`); the rerun is in the next entry. The polls'
30–40× is more than the query's 4×: a poll's delta joins and
re-derivations run with the changed entity pinned, every scan a point
read, which now merges instead of probing ten fields per entity per
premise. Not yet profiled apart.

## 2026-11-16 — the perf gate's scenarios

**Question.** What do the gated `dialog-perf` scenarios say, main
against the branch?

**Method.** `perf sweep` of the read, query, rule and subscribe
scenarios under callgrind, main's `perf` built in its worktree. The
baselines are not touched.

**Results.** Instructions: `query-join` −16.9%, `rule-scan` −15.2%,
`read-scan` −12.3%, `query-member` −10.4%, `first-query` −4.0%, point
reads and `subscribe-fanout`/`-pulled` within 1%, `subscribe-repoll`
**+3.5%**. Allocation calls fall up to 54%; `read-scan`'s allocated bytes
rise 8.4%, and a few scenarios' realloc bytes 2–4%: the gate would fail
those.

**Decision.** Two regressions to fix before anything lands:

- `subscribe-repoll` +3.5%: subscription maintenance compares retained
  conclusions by entity, so the lazy `ConceptConclusion::this` is always
  materialized there, through a `OnceLock` and a slot lookup it did not
  pay for before. `this` should be lazy only on the path that hands
  answers on.
- `read-scan` bytes +8.4%: one shared row per scanned fact
  (`Arc<Scanned>`: the checked row, its pieces, four `OnceLock`s) is
  bigger than the small allocations it replaces. Fewer calls, more
  bytes; a leaner `Scanned` (one cell, the part known per slot) would
  close it.

## 2026-11-16 — the rerun, and `issue-detail`

**Question.** Does folding a pinned join without estimating it close
`issue-detail`'s regression, and does the rest hold?

**Method.** `tonk_bench`, main and the branch (lazy, left merge),
interleaved, 3 rounds at 1×, 2 at 10×, each store removed once its report
is copied (the first attempt filled the session's disk). Then
`issue-detail` alone (`DIALOG_BENCH_WEEK=0`), 3 rounds of main, the
branch with and without the left merge and eager with it.

**Results.** 10×, ms:

| measure | main | branch |
|---|---|---|
| query/v2/issue/warm | 288 | 82 |
| query/v2/issue/cold | 701 | 172 |
| query/v2/activity/warm | 486 | 106 |
| subscription/v2/issue/poll | 939 | 23 |
| subscription/v2/activity/poll | 1,398 | 46 |
| query/issue-detail/cold | 2.4 | 8.4 |
| command/all | 6.1 | 6.9 |

`issue-detail` cold, alone, 3 rounds: main 2.9–3.3 ms, the branch without
the left merge 2.8–3.2, with it 7.0–11.1 in either binding mode; warm
the same everywhere. What it is not: the decision (instrumented: 2.8 µs,
and it folds, as intended), the OS page cache (dropped before the cold
read: main 4.1–4.5, the left merge 7.1–10.8), lingering background
replication from the merged queries before it (a 2 s pause changes
nothing).

**Decision.** Not resolved. The decision is right and the fold is the
same code either way, so what differs is the state the queries before it
leave. The likeliest: the cold step releases the repository's caches,
and something it does not release keeps the EAV leaves the earlier full
`v2/activity` read touched when it probed every entity; with the left
merge that read scans AEV instead and leaves nothing `issue-detail`
reads. If so main's "cold" is partly warm, and the regression is in what
the previous query leaves behind, not in `issue-detail`. To settle it:
run `issue-detail` cold in a fresh process, or count the blocks it
reads. `command/all` is 13–16% slower, with `steady/all` up at 1× and
down at 10×: within what these runs separate, not decided.

## 2026-11-16 — the page side

**Question.** In the page, what does reading an answer cost: `JSON.parse`
of tonk's JSON, against the JS frame reader?

**Method.** `bench/run.mjs`: headless Chromium (V8, as the page has),
over the exact bytes the engine produced (`answers run --dump`), median
of 30 runs. `JSON.parse` of the SSE text, then reading two fields of
every row (what a template showing two does), against accepting the
frame and reading two fields, or every field. The page today also runs
`serde_wasm_bindgen` over the parsed graph back into Rust structs; that
is not counted, which flatters JSON. `performance.now()` is coarse in a
page that is not cross-origin isolated (0.1 ms).

**Results.** ms:

| answer | JSON bytes | frame bytes | JSON.parse | JSON, two | frame, two | frame, all |
|---|---|---|---|---|---|---|
| stuff 2,000 | 184,705 | 85,926 | 0.4 | 0.5 | 0.6 | 1.1 |
| stuff 20,000 | 1,906,705 | 897,926 | 4.1 | 5.3 | 5.5 | 12.2 |
| issue 3,000 | 1,410,435 | 397,482 | 1.8 | 2.0 | 0.2 | 2.9 |
| tonk-issue 3,000 | 1,421,621 | 436,031 | 2.2 | 3.7 | 0.2 | 3.0 |

**Decision.** The frame wins where rows are wide and the page reads few
fields: tonk's views are thirteen and eight fields wide, and a list
template shows two or three (10–18× here). Reading every field it does
not beat V8's native parser, and on narrow rows of unique strings it
loses: the reader calls `TextDecoder` once per token, and a two-field row
of unique strings is all tokens. The fix the numbers point at: write a
frame's text tokens as one run, decode the run once per frame into one
JS string, and record UTF-16 offsets, so a cell is a `slice`. Not built.

## 2026-11-16 — closing the regressions

**Question.** Of what measured slower, what is real, and what can be
fixed?

**Method.** The full `dialog-perf` sweep (30 scenarios) under callgrind,
main's `perf` from its worktree, compared with the gate's own
`perf compare`; for `issue-detail`, a cold read by a member that has run
no query (`DIALOG_FRESH_DETAIL=1` in `tonk_bench`), 3 rounds each.

**Results and changes.**

- *`subscribe-repoll` (+2.9%).* Subscription maintenance compares every
  retained row by entity, so a lazy `this` was always built, in the
  measured poll instead of the unmeasured initial query, with a
  `OnceLock` on top. `ConceptConclusion::this` is eager again (main's
  layout; the answer path reads `this` with `view`): −0.5%. The lazy
  `this` had also flattered `read-scan` and `query-join`, which collect
  conclusions and never read their entity: with it `read-scan` was −12.7%,
  without it −4.1%, before the changes below.
- *`read-scan` bytes (+12%, then +8.4%).* The shared row was 512 bytes,
  carrying the whole scanned `FactView` (its 160-byte payload among it).
  `CheckedRow` now keeps the key, the cause and where each part sits (128
  bytes, the standing taken once by the scan), the attribute's cache is
  boxed on demand, and `Scanned` is 256 bytes: +3.2%.
- *`read-scan` outside the allocator (+6.7%).* The key was walked four
  times per row and every slot's kind rechecked by decoding the value.
  `checked` walks it once; `place` decides from the type the key records
  (decoding only the number band); a slot whose kind is settled is not
  rechecked: −4.6%.
- *`number-range-*` (+1.5% to +3.0%, 8–10% more allocation calls).*
  `place` built the scan's kinds for every row, and a range's kind clones
  a boxed interval. The kinds are decided once per scan (`Placement`):
  −0.2% to +0.6%.
- *Commands.* Every commit scenario is within ±0.9% of main: the
  `tonk_bench` command numbers were noise.
- *`issue-detail` cold (3.5×).* A fresh member reads it cold in 7.9–8.8 ms
  on main and 7.4–8.8 ms on the branch. Main's 1.6–1.9 ms through the
  member that ran the views first is not cold: its earlier queries probed
  every entity and left the pages `issue-detail` reads in caches the
  bench's release does not drop. Not a regression.

The full sweep against main now: instructions `query-join` −19.9%,
`rule-scan` −18.0%, `query-member` −12.9%, `read-scan` −12.4%,
`rule-ranked` −6.9%, `first-query` −4.6%, everything else within ±2%. The
gate still flags:

| scenario | measure | main | branch |
|---|---|---|---|
| number-scan | bytes allocated | 5,934,794 | 6,187,194 (+4.3%) |
| read-scan | bytes allocated | 6,008,113 | 6,202,733 (+3.2%) |
| rule-ranked | bytes allocated | 15,277,135 | 15,569,453 (+1.9%) |
| number-range-narrow | bytes allocated | 1,431,205 | 1,445,765 (+1.0%) |
| number-scan | instructions outside the allocator | 20,773,393 | 21,140,922 (+1.8%) |
| rule-recursive | instructions outside the allocator | 582,854,364 | 590,409,424 (+1.3%) |
| rule-recursive | allocation calls | 839,087 | 848,688 (+1.1%) |

**Decision.** The byte rows are the trade the shared row makes: one
256-byte allocation per scanned fact in place of several smaller ones,
30–50% fewer allocation calls for 1–4% more bytes. A cached binding is 48
bytes because `Value` holds a `u128` (16-byte alignment), so closing the
last bytes means changing how a binding is stored; the maintainer's
call. `rule-recursive` is laziness where every value is read anyway (a
recursive rule's body joins on all of them): `Match::find` pays the slot
and its cache, and slots are bigger to move. The fix is a decision the
plan can make, not the engine at large: bind a scan's variable in place
only when no later premise reads it, eagerly otherwise. Not built.

## 2026-11-16 — what is hydrated, and whether it has to be

**Question.** Deferring hydration moved it around (into a poll, into
`project`). Where is a value actually hydrated, by which read, and does
that read need an owned value?

**Method.** A `trace-hydration` feature on `dialog-query` counts every
value made out of a scanned row (a binding, a claim, an eager fact), by
the source line whose read asked for it (`#[track_caller]` on the read
paths). The `perf` scenarios print what their measured phase hydrated;
`answers` reports it per phase.

**Results.** Before the fixes below, per scenario:

| scenario | hydrated | asked for by | needed? |
|---|---|---|---|
| read-scan, query-join, rule-scan | 1 entity a row | `realize`, for `ConceptConclusion::this` | no: the conclusion held an owned `Entity` |
| query-member | 250 entities, 249 claims | a lookup's selector (`term.rs`), `challenged` | yes, by today's `Selector` (owned entity and attribute) |
| rule-recursive | 6,000 entities, 480 values, 5,520 claims | the fixpoint's row store, `challenge` | yes, by the fixpoint's design (owned rows) |
| rule-ranked | 2,500 entities | the ranked election's memo rows | yes, by the election's design |
| tonk-issue engine | 0.5 facts a row | the eager path | only the value: a natural read as `float:` |
| every list read, JSON | every field | tonk's `project` | no |
| the absence checks | the value | `absent_blocked`, an optional lookup | no: a value bound in place is never `Absent` |

**Changes.**

- `Match::absent` answers from the slot; `absent_blocked` and the
  optional lookup use it. Entering a concept's scope moves slots, as
  leaving it already did.
- A value the scan's kind reads through a view (a natural as a float) is
  hydrated alone, into the row's value cell, and the row stays in place:
  no entity, attribute or claim is built for it.
- `ConceptConclusion` holds its entity, or the scanned row that holds it
  (`This`); `entity()` builds it through the row's cache, and
  `Conclusion::this_uri` reads the stored URI. Subscription maintenance
  compared every retained row's entity per entity touched; it now takes
  the touched rows out in one pass, by URI. The first cut compared bytes
  through the row's slot lookup per comparison and made
  `subscribe-repoll` 39% slower; the direct handle and the one pass make
  it 1% faster than main.
- `answers` writes tonk's snapshot JSON from `ConceptConclusion::view`
  (`json_view_encode`), checked byte for byte against `project` +
  `serde_json` every run.

After: `read-scan`, `query-join`, `rule-scan`, `subscribe-repoll`,
`read-point` and `number-scan` hydrate nothing in their measured phase;
tonk-issue's engine hydrates 0.5 values a row (the float views). What is
left is what an owned value is needed for today: selectors, the
fixpoint's and the election's row stores.

The JSON, per row (callgrind):

| dataset | project + serde_json | JSON from views | frame |
|---|---|---|---|
| stuff 2,000 | 10,072 instr, 17 allocs | 2,424, 0 | 3,544, 3 |
| issue 3,000 | 29,870, 43 | 9,503, 0 | 10,288, 4 |
| tonk-issue 3,000 | 37,277, 60.5 | 11,806, 0 | 12,874, 4.1 |

The `perf` scenarios against main: `query-join` −22.3%, `rule-scan`
−20.0%, `read-scan` −18.9% instructions and −0.1% bytes (below main),
`number-scan` −9.1%, `subscribe-repoll` −1.0%. The larger conclusion
(16 bytes of `this` for 8) costs 2–7% more bytes reallocated growing
result vectors.

**Decision.** Tonk's encode cost is hydration: written from views, its
JSON costs a third to a quarter as much, with no allocation, and the
wire format does not change. That is a change tonk can make on its own,
ahead of any frame. The frame's case is then the bytes (2–3.6× smaller,
deltas 7–13×) and the receiver reading in place, not the encoding. The
remaining hydration is a question about three designs (the selector
API, the fixpoint's rows, the election's rows), not about the engine's
read path.

## 2026-11-16 — binding in place only where nothing reads the value

**Question.** `rule-recursive` was +3.1% against main (+1.1% with lazy
bindings off). Do rules need every value, and can the plan defer only
what nothing reads?

**Method.** `rule-recursive` profiled main, eager and lazy; then the
changes below, each remeasured; then the full `dialog-perf` sweep.

**Results.** The fixpoint keeps every derived row as owned values
(`Row`) to deduplicate it and feed it back, and projects every head
operand into it: not because a rule needs owned values (it compares
them), but because its row store is owned. So every value its scans
bind is hydrated, and binding in place only added to it. Of the +3.1%,
two thirds was that; the rest showed with lazy bindings off too: an
out-of-line slot read on every `Match::find`, and a scan's placement
built on every invocation, which a recursive probe makes many times
for few rows.

- The slot read for a bound value is inlined, and a scan decides its
  placement on its first row: eager `rule-recursive` +1.1% → +0.5%.
- *Plan-level deferral, first cut:* a binding mode on the scans of the
  plans the fixpoint evaluates. +3.0% → +2.7%: most of a recursive
  rule's scans are in the concepts its premises read, which evaluate
  their own cached plans, so the flag did not reach them.
- *The demand travels with the rows instead:* a row whose consumer
  reads every value says so, the fixpoint seeds its rows that way,
  derived rows and the rows of a concept evaluated for them inherit
  it, and a scan extending such a row binds eagerly. On `Match` the
  flag grew every row from 88 to 96 bytes (+3–6% bytes in the rule
  scenarios); it lives on the row's shared frame instead, and every
  eager seed shares one empty frame (a frame per seed was +2.1%
  allocation calls).

The full sweep against main, instructions / allocation calls:
`query-join` −22.3% / −54.1%, `rule-scan` −19.9% / −50.2%, `read-scan`
−18.8% / −42.7%, `query-member` −14.2% / −41.4%, `number-scan` −9.0% /
−29.9%, `rule-ranked` −6.1% / −19.6%; `rule-recursive` +0.8% / +0.7%,
`number-range-narrow` +0.4% / +1.0%; every other scenario within ±2%,
most below main. The gate flags only bytes: 1–2% more allocated in
`rule-ranked`, `subscribe-repoll` and `number-range-narrow`, and 2–7.5%
more reallocated growing result vectors of the larger conclusion.

**Decision.** The bytes are the trade (taken). The fixpoint's owned row
store is the next place hydration could go, if a recursive rule's rows
held references and compared bytes; not now.

## 2026-11-18 — the switches are removed

`DIALOG_LAZY_BINDINGS` and `DIALOG_LEFT_MERGE` let one build measure
the branch against itself with each change off. Every number above that
compares eager and lazy, or probed and merged optional fields, was taken
with them. With the measurements taken they cost more than they gave: a
process-wide setting read from the environment by a library, read once,
so a test could not turn it off, and a second path through every scan
that CI never ran.

Both are removed. A scan binds in place whenever its kinds allow it
(the eager path remains for a refined attribute, a value pinned to a
constant or read through a refined kind, and the rows of a recursive
rule), and an optional field sorted on the merge variable is always
left-merged. The `answers` harness now runs this build against main's;
its variant keeps the `lazy` label so its reports compare with the ones
recorded on 2026-11-16. Reproducing the eager column needs a build of
this branch from before the switches were removed.

## How to reproduce

```sh
# the answers harness, one store
cargo run --release -p dialog-perf --bin answers -- run --dataset issue --rows 3000

# every variant: wall time interleaved, then callgrind per phase
cargo run --release -p dialog-perf --bin answers -- sweep --out-dir target/answers

# main's build of the harness: a worktree of main with rust/dialog-answer
# and rust/dialog-perf/src/bin/answers.rs copied in
RUSTFLAGS="--cfg dialog_main" cargo build --release -p dialog-perf --bin answers

# what each read hydrates, by the line that asked
cargo run --release -p dialog-perf --features trace-hydration --bin perf -- run rule-recursive
cargo run --release -p dialog-perf --features trace-hydration --bin answers -- \
  run --dataset tonk-issue --rows 3000 --repeat 1

# tonk's workload
cargo run --release -p dialog-baseline --example tonk_dataset -- \
  generate rust/dialog-baseline/fixtures/tonk-team-schema.jsonl 1 tonk-1x.jsonl
cargo run --release -p dialog-baseline --example tonk_bench -- \
  tonk-1x.jsonl fs path/to/scratch
```
