# Zero-copy answers: journal

## Where things stand

**Status: Exploring.** Built on this branch:

- late materialization in the engine (`Match` slots that share the scanned
  row, `CheckedRow`, `ConceptConclusion::view`), behind
  `DIALOG_LAZY_BINDINGS` (on by default, `0` for main's eager path);
- a merge join keyed on the scanned entity's bytes, rows combined in
  place, and a table for the canonical-URI check (these help eager too);
- `dialog-answer`, a binary columnar answer frame with a session token
  dictionary, its Rust reader and a JS reader;
- the `answers` harness (`rust/dialog-perf/src/bin/answers.rs`), which
  measures every phase from the query to the receiver, on main (built
  with `--cfg dialog_main`) and the branch.

Headline, tonk's issue list shape at 3,000 rows, per row, against main:

- engine: instructions −31%, allocations −67%;
- wire: the frame is 3.5× smaller than tonk's JSON, a delta 13× smaller;
  writing it costs 40% of `project` + `serde_json`, reading it back 4%
  (every field) to 9%;
- a columnar operator (no rows between the scan and the frame) answers
  the same read in 40% of the time the engine and the frame encoder take.

Next: wall clock on tonk's own workload (`tonk_bench`), the page side in
Chromium, then whether the columnar read is worth building as an
operator.

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

## How to reproduce

```sh
# the answers harness, one store
cargo run --release -p dialog-perf --bin answers -- run --dataset issue --rows 3000

# every variant: wall time interleaved, then callgrind per phase
cargo run --release -p dialog-perf --bin answers -- sweep --out-dir target/answers

# main's build of the harness: a worktree of main with rust/dialog-answer
# and rust/dialog-perf/src/bin/answers.rs copied in
RUSTFLAGS="--cfg dialog_main" cargo build --release -p dialog-perf --bin answers

# tonk's workload
cargo run --release -p dialog-baseline --example tonk_dataset -- \
  generate rust/dialog-baseline/fixtures/tonk-team-schema.jsonl 1 tonk-1x.jsonl
DIALOG_LAZY_BINDINGS=1 cargo run --release -p dialog-baseline --example tonk_bench -- \
  tonk-1x.jsonl fs path/to/scratch
```
