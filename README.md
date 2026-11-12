# Research data for dialog

The raw output of research runs, kept off `main`: `main` keeps each
topic's journal and reports, with their tables and charts, and this
branch keeps what those were computed from. It shares no history with
`main`. Paths are the ones the runs wrote, under `research/<topic>/`.

## `research/web-spike/results/`

Every run of the web spike (`research/web-spike` on `main`): one JSON
file per run, named `<date>T<time>-chromium-<major>[-quick][-qN|-roundN].json`,
as `run.js`, `round3.js` and `round4.js` write them. The journal,
`research/web-spike/journal.md`, cites them by name. They were moved
here from `worker/pensive-edison-fu3bx5` at `7dfb9f2c`.

To print a run's tables, from a dialog checkout:

```sh
git fetch origin research-data
git show origin/research-data:research/web-spike/results/<file>.json > /tmp/run.json
node research/web-spike/report.js /tmp/run.json          # rounds 1 and 2
node research/web-spike/round3-report.js /tmp/run.json   # round 3
node research/web-spike/round4-report.js /tmp/run.json   # round 4
```
