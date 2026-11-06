---
name: research
description: Run a research topic under research/ - turn an idea into a hypothesis with success and failure criteria, measure it against main on one fixed dataset, keep a dated journal, chart the numbers, and record decisions and status. Use when starting or continuing research, a spike, an evaluation or a measurement that decides a design, or when asked to journal, chart or report findings.
---

# Doing research

Read `research/README.md` first: it lays out the folder and lists every
topic with its status. A topic is one file, `research/<topic>.md`: the
idea, its status, and its report once the research is done. While the
research runs, its working material sits beside it in `research/<topic>/`.
The single-file format
([dialog-db/dialog-db#733](https://github.com/dialog-db/dialog-db/pull/733))
is the worked example: read its topic file, journal and evaluation on that
branch before starting one.

## The files

| file | holds | changes |
|---|---|---|
| `<topic>.md` | the idea (the question, the hypothesis, what success and failure look like), the status, and the report when it is done | the idea rarely, a change to its bars journaled; the status as it moves |
| `<topic>/journal.md` | what was tried, measured and decided, newest last | every finding |
| `<topic>/spike.md` | the first round of measurements in full, if there was one | once |
| `<topic>/design.md` | the current decision and the measurements behind it | when a decision moves |
| `<topic>/evaluation.md` | the axes a decision is measured on, and where each stands | as runs come in |
| `<topic>/results/` | a report per sitting of runs, with its tables and charts | each sitting |

A new topic starts as its file alone, **Proposed**. Picking it up adds the
folder and the journal. Finishing it, landed or shelved, writes the report
into the topic's file (section 7).

## Statuses

One of **Proposed** (written down, not picked up), **Exploring** (is it
worth building), **Building** (decided, being built, open choices being
measured), **Evaluating** (built, measured against what it would replace),
**Landed** (merged; the topic's file ends with its report), **Shelved** (not
worth it; the report says why). The status is in `research/README.md`, at
the top of the topic's file and in the journal's "Where things stand".
Change them together.

## 1. The idea: a hypothesis and its bars

Before measuring anything, write down in the topic's file:

- **The question**, in a sentence, and the **hypothesis**: what you expect
  and why.
- **What it is measured against.** The baseline is `main` as it is, not a
  copy of it on the branch. When a branch changes more than the thing under
  test, measure the branch with main's component too, so each change shows
  apart (main, branch with main's component, branch with the new one).
- **Success**, as numbers: the gate it must hold, the ratio it must reach.
- **Failure**, as numbers: below what it is not worth it. A no is an
  answer; say in advance what it looks like.
- **The middle**, and who judges it: usually a ratio of cost to gain the
  maintainer weighs on the numbers.

Do not move the bars after seeing results. If one was wrong, say so in a
journal entry, with why, and then change the topic's file.

## 2. The evaluation: axes, not one number

A single measure (size, a point lookup) rarely decides a design. List the
axes the decision turns on in `evaluation.md`, each with:

- **Workload:** what runs, as close to real use as possible (real queries,
  real commands, their rules included; not point lookups);
- **Reports:** what is measured, cold and warm where caching matters;
- **Harness:** the code that runs it;
- **Status:** not run, or where the numbers are.

Compare every variant, not only the expected winners: a layout that lost
on one axis can win another. Make variants settings of one build where
they are, so each costs a setting, not a build.

## 3. Datasets: one, fixed, not committed

- Every store, variant and build replays the **same dataset file**.
  Generate or export it once (from a seed, if synthetic), outside the
  repository, and name it in every result by its file's hash.
- A synthetic dataset follows a real one's shape: counts, and how records
  link to each other (who, what, which group), measured from the real
  data and written in the evaluation.
- Scale it (1×, 10×, 100×) when growth is a question.
- **Never commit** a dataset, a replica, or anything exported from a
  private space. A replica may hold credentials; read it from a copy.
- Derive every identity from one seed, so every store writes the same
  blocks, and refuse to report stores that disagree on the result.

## 4. Measuring honestly

Before trusting a number, check that it measures what it claims:

- **A test of a fix fails without the fix.** Revert it and run it; a test
  that passes either way proves nothing (two processes that never overlap
  do not test locking).
- **The input is not flattering the result.** A size that is a power of a
  merge factor, a dataset that fits a cache, one lookup point: vary them.
- **A profile counts one thing.** Subtract the copy, the setup, the
  allocation that belongs to something else.
- **Report the spread or the run count**, and rerun anything that moved
  more than the difference it claims. One run decides nothing: three is
  the least, and a difference inside the spread between runs is noise.
- **Interleave the variants and compare within one sitting.** Run A, B,
  C, A, B, C rather than A, A, A, B, B, B, so drift on the machine falls
  on every variant alike; numbers from different sittings drift, so a
  report compares only runs it holds.
- **Nothing else runs while a timed run does**: no build, no other
  benchmark.
- The `dialog-perf` and `dialog-soak` baselines are the maintainer's to
  move (`.claude/rules/perf-baselines.md`).

## 5. The journal

`journal.md` opens with **Where things stand** (status, what is built,
the headline numbers, what is next), kept current, then **Datasets**, then
an entry per finding, newest last, then **How to reproduce** (the exact
commands). An entry:

```markdown
## 2026-11-06 — <the question, as a short title>

**Question.** What this tried to find out.

**Method.** What ran, on which dataset, with which harness.

**Results.** The numbers, in a table when there are more than three.

**Decision.** What it settled, or "not decided" and what would decide it.

Commits: `abc1234`.
```

Write the entry when the result is in, not at the end. A result that
contradicts an earlier entry gets its own entry saying so; do not rewrite
the earlier one. Keep the PR description's summary in step with "Where
things stand".

## 6. Charts

Every chart's numbers are in a table under it, so nothing is read off a
picture.

- **One series: a Mermaid bar chart**, which GitHub draws. Mermaid's
  `xychart-beta` draws no legend, so a chart carries one series: the
  variants along the x axis, one measure per chart.

  ```mermaid
  xychart-beta
    title "Cold query, ms"
    x-axis ["main/fs", "branch/fs", "file/memory"]
    y-axis "ms" 0 --> 10
    bar [4.1, 4.3, 3.9]
  ```

- **Several series on one chart** (a growth curve per store): an SVG the
  harness writes, with a legend, committed with its run under
  `research/<topic>/results/`. Keep it to polylines, axes and labels, with
  no plotting dependency.
- Name the dataset (and its hash) and the build in the chart's title or
  caption.
- **Show results as pictures in the conversation, not only as tables.**
  A conversation or terminal draws neither Mermaid nor SVG: render the
  charts to PNG (a headless browser screenshot of the SVG does it; the
  single-file topic's `bench/render.js` is one) and send the images with
  every result you report, the table beside them for the exact numbers.
  A table alone leaves the reader to draw the chart in their head.

## 7. The report

When the research is done, landed or shelved, its report goes into the
topic's file, after the idea: what was decided, the numbers and charts
that decided it, every variant side by side on every axis, and what was
left open. The journal and results stay in `research/<topic>/` as the
record of how it got there. How to compile one will grow here as the
first topics finish.
