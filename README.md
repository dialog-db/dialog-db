# Research data for dialog

What research topics were measured with and what the measurements
produced, kept off `main`: `main` keeps each topic's journal and report,
with their tables and charts, and this branch keeps the tools (harnesses,
bench and report scripts) and the raw runs. It shares no history with
`main`. Paths are the ones the runs wrote, under `research/<topic>/`.

## `research/single-file-format/bench/`

The bench, report and chart scripts of the single-file format
(`research/single-file-format` on `main`), as the journal and the
evaluation name them (`bench/<script>`). Run them from a dialog
checkout, with this branch's `research/` beside it (`git worktree add
../research-data research-data`), as they read and write paths relative
to the repository root. `file-archive-bench.yml` is the GitHub workflow
that ran `ci.sh` on every push to the branch of
[#733](https://github.com/dialog-db/dialog-db/pull/733), comparing the
file archive with main's store; it is kept here as it was, not run.

## `research/single-file-format/browser/`

The browser research's harness, run and report scripts; its `README.md` is the
runbook.

## `research/single-file-format/browser/results/`

Every run of the browser research (`research/single-file-format/browser` on `main`): one JSON
file per run, named `<date>T<time>-chromium-<major>[-quick][-qN|-roundN].json`,
as `run.js`, `round3.js` and `round4.js` write them. The journal,
`research/single-file-format/browser/journal.md`, cites them by name. They were moved
here from `worker/pensive-edison-fu3bx5` at `7dfb9f2c`.

To print a run's tables, from a dialog checkout with this branch beside it:

```sh
git fetch origin research-data
git worktree add ../research-data research-data
cd ../research-data/research/single-file-format/browser
node report.js results/<file>.json          # rounds 1 and 2
node round3-report.js results/<file>.json   # round 3
node round4-report.js results/<file>.json   # round 4
```

## `research/zero-copy-answers/`

The tools and raw runs of the zero-copy answers research
(`research/zero-copy-answers.md` and its journal on `main`; the engine
change landed through
[#825](https://github.com/dialog-db/dialog-db/pull/825), the answer frame
is [#831](https://github.com/dialog-db/dialog-db/pull/831)).

- `design.md`, `evaluation.md`: what the prototype did and the axes it was
  measured on, as they stood when the report was written.
- `harness/answers.rs`: the `answers` harness, a `dialog-perf` binary
  (`rust/dialog-perf/src/bin/answers.rs`) with every phase: the engine,
  tonk's JSON both ways, JSON written from rows in place, the answer frame
  written and read, the columnar lower bound, and a delta encoded both
  ways. It needs #825's engine (`ConceptConclusion::view`) and #831's
  crate; copy it into `rust/dialog-perf/src/bin/` of such a checkout and
  add `dialog-answer` and `ipld-core` to `dialog-perf`'s dependencies.
  Main's build of it is `--cfg dialog_main`.
- `bench/`: the page-side measurement (`frame.js`, the JS frame reader;
  `bench.html` and `run.mjs`, which time it against `JSON.parse` in
  headless Chromium over the harness's `run --dump` output) and
  `charts.py`, which draws the report's charts from a sweep and
  `tonk_bench` reports.
- `results/2026-11-16/`: every report behind the journal's numbers:
  `answers/` (one sweep, each dataset and variant), `tonk-bench/`,
  `browser.json`, and the charts and `charts.md` drawn from them.

## `research/benchmark-dataset/`

`se-transform.py`, which turns an extracted Stack Exchange site dump into
the transaction-ordered fact log the SE replay benchmarks read
(`txn,at,the,of,as,is`, rows sharing a `txn` one commit; the format is
`notes/benchmark-dataset.md` on `main`). It was `scripts/se-transform.py`
on `main` (added in #715, last in the tree of `v0.5.0`), moved here
unchanged:

```sh
python3 research/benchmark-dataset/se-transform.py <extracted-dump-dir> <output.csv> [--limit N]
```

## `design/macros/` and `rust/dialog-notation/examples/`

What the notation's proof of concept (`design/macros` on `main`, #773)
was explored and measured with, taken off the pull request before it
landed, at the paths they had on `feat/notation-poc` at `79979af`:

- `design/macros/model/`: the model of the kernel and the core forms
  in Python (`model.py`, `checks.py`) and the prelude and to-do it ran.
- `design/macros/rounds/`: the reader rounds (`k01`, `k02`) and the
  output of every cost run the journal cites (`cost-*.txt`).
- `design/macros/sketches/`: the design's first sketches of tonk's core
  and notebook. The crate's own, which its tests run, are on `main`
  under `rust/dialog-notation/tests/fixtures/`.
- `design/macros/*.md`: the status reports, plans and comparisons the
  journal and the ledger cite: `alignment.md`, `synthesis.md`,
  `poc.md`, `decisions-overnight.md`, `where-things-stand.md`, the
  `tonk*.md` readings, the composite and definitions plans. Every
  decision of the maintainer's they held is a dated row of
  `design/macros/claims.md` on `main`.
- `rust/dialog-notation/examples/`: the crate's examples, kept as files
  for the record: the cost harness that produced the `cost-*.txt` runs
  (`cost.rs`), the notebook bar (`bar.rs`, `notebook/bar.rs`), the
  shapes count (`shapes.rs`) and the runner (`notebook.rs`,
  `notebook/session.rs`, `todo.clj`, `done.clj`). They are not built
  here and are not expected to build against a later `main`; check out
  `79979af` of `feat/notation-poc`, or the branch
  `archive/notation-poc-2026-11-24`, to run them as they were.
