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
