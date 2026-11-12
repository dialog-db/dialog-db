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
to the repository root.

## `research/web-spike/`

The web spike's harness, run and report scripts; its `README.md` is the
runbook.

## `research/web-spike/results/`

Every run of the web spike (`research/web-spike` on `main`): one JSON
file per run, named `<date>T<time>-chromium-<major>[-quick][-qN|-roundN].json`,
as `run.js`, `round3.js` and `round4.js` write them. The journal,
`research/web-spike/journal.md`, cites them by name. They were moved
here from `worker/pensive-edison-fu3bx5` at `7dfb9f2c`.

To print a run's tables, from a dialog checkout with this branch beside it:

```sh
git fetch origin research-data
git worktree add ../research-data research-data
cd ../research-data/research/web-spike
node report.js results/<file>.json          # rounds 1 and 2
node round3-report.js results/<file>.json   # round 3
node round4-report.js results/<file>.json   # round 4
```
