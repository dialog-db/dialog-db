# Single-file format: evaluation

The scripts named `bench/…` below are on the [`research-data`](https://github.com/dialog-db/dialog-db/tree/research-data/research/single-file-format/bench) branch, under `research/single-file-format/bench/`.

What we measure to decide the format's open choices, and against what.
Size alone does not decide them: an index layout that saves a megabyte can
cost every query a page, or every sync a rewrite. Each axis below says the
workload it runs, the harness that runs it, what it reports, and where it
stands. Results go in the [journal](journal.md) as they come in.

## What is compared

Every axis runs the same stores, each a space that differs only in its
archive, plus main as it is:

| store | what it is |
|---|---|
| `main/fs` | **the baseline:** dialog as `main` is, its archive a file per block |
| `branch/fs` | this branch with main's archive: what the branch's other changes (the tree's read path among them) cost on their own |
| `file/memory` | the single file as built today: its index loaded into memory at open |
| `file/tree` | the index in the file as a search tree, children named by position, every node written whole on change (path-copied) |
| `file/tree-delta` | the same, each node written against its last version, whole every 32 |
| `file/hitchhiker` | the same tree with buffers of 256 riding its links, only the root written against its last version, whole every 32 |
| `file/hitchhiker-delta` | the hitchhiker tree with every node written against its last version |
| `file/runs` | the index in the file as sorted runs merged 4 at a time, the merges spread over appends |

Every permutation the [replay](design.md#where-the-block-index-lives-layouts-replayed)
looked at is built, not only its two finalists: the replay measured size
and pages a lookup reads, and a layout that lost there can still win on
writes or sync. The four tree layouts are one tree with two settings
(buffers, and which nodes are written as deltas), so they cost one build.

`main/fs` against `branch/fs` isolates the branch's changes outside the
archive; `branch/fs` against the `file/*` stores isolates the archive. The
harness uses only the API main and the branch share (repository, branch,
transaction, query), and is built twice: in this branch, and in a worktree
of `origin/main`. Every store reads through the tree in place where it can
(the archive answers the tree's `nodes::Read` with nodes read in place; main reads blocks). Every identity is derived
from one seed, so every store writes the same blocks and ends on the same
root, and a run refuses to report stores that disagree.

## Datasets

Every store and every run reads the same dataset: each is generated or
exported once, to a file outside the repository, and replayed from it.
The report names each dataset by its file's hash, so a run against a
different one shows. None is committed.

- **Stack Exchange:** 4096 commits of the `retrocomputing` dump.
- **tonk-team:** the team space's history, as the replica tonk keeps of
  it. Main and `branch/fs` open a copy of it as it is; the `file/*` stores
  open it moved with `dialog-migrate`. Tonk itself does not run: the space
  carries its own schema (concepts, and the rules behind its commands, as
  `dialog.rule/*` facts), so dialog alone can query it and run its
  commands. The copy holds an identity's credentials, so nothing from it
  is committed.
- **tonk-shaped, synthetic, at 1×, 10× and 100×:** the space's own schema
  (read from the copy, not committed) over generated records, cross-linked
  as tonk-team's are:

  | record | at 1× | links |
  |---|---|---|
  | person | 6 | an account each |
  | label | 11 | |
  | project | 22 | an initiative each |
  | issue | 294 | a creator and an assignee (people), 0.6 labels, 0.4 projects |
  | activity | 168 | on an issue, by a person; comments and changes, replies by parent |
  | memo | 9 | on a doc, an issue or another memo, 0.9 mentions of people |

  Each record is made by the space's own command where it has one
  (`v2/issue-create`, `v2/issue-add-label`, `v2/issue-set-project`,
  `v2/issue-set-assignee`, `v2/issue-post-comment`, `v2/label-create`,
  `memo/post`), so the history is commits of commands, as a user's is. The
  generator runs once from a seed and writes the commands it made, in
  order; every store replays that file.

## 1. Queries

**Workload.** Real queries, not point lookups:

- the queries of `dialog-perf`'s read and rule scenarios: scans, joins,
  rule-derived concepts;
- on tonk-team and the tonk-shaped datasets, the queries behind the
  space's views, as dialog concept queries built from the descriptors the
  space stores, joins across its links included:
  - the schema read every view starts from (`concept:`, about 300
    concepts; `rule:`, 116 rules);
  - the issue list as the issues page reads it (`v2/issue`, 301 issues in
    the replica), with `v2/issue-labels`, `v2/label`, `v2/project` and
    `v2/person` beside it;
  - the issues of one label, of one project, and assigned to one person;
  - an issue's detail: the issue, its activity (`v2/activity`) with each
    author's name, and the replies under each comment;
  - the memos (`memo/full`, with `memo/byline`), on a doc and mentioning
    a person.

  These are the store's share of a page, not a page's time: tonk's
  templating and formulas are left out, and they do not touch the
  archive.

**Reports.** Time, cold (a reopened store, nothing cached) and warm; pages
read from the file; rows returned. Cold matters most for the in-file
indexes, whose first lookup reads the root.

**Harness.** `dialog-perf`'s scenarios, with the store chosen per run (its
environment is volatile today), and the tonk-team harness.

**Status.** tonk-shaped 1×, `fs` and `file/memory`, three runs each: [results](results/2026-11-06-tonk-1x.md). Main, tonk-team's own history, the scales and the index layouts are not run yet.

## 2. Writes

**Workload.** What an application does:

- `dialog-perf`'s commit and transaction scenarios, inductive rules
  included;
- on tonk-team, the space's own commands: creating an issue
  (`v2/issue-create`) and posting a memo (`memo/post`). A command is
  event facts (`dom.event.detail/*`) of a transient concept, dispatched
  (`Transaction::dispatch`, as tonk sends one) under the session's
  profile (`dialog.session/profile`); at commit, the space's inductive
  rules turn them into the issue's or memo's facts (`squash.bug/*`,
  `xyz.tonk.memo/*`, the author's name), and the event itself is never
  stored. So the harness dispatches what tonk's page would and commits;
  dialog runs the rules. A run of a few
  hundred of each, as a busy week would.

**Reports.** Time per command and per commit, the archive's share of it
(splitting nodes into rows, verifying frames, the index's own writes),
bytes appended per commit, and the largest single append (the merge spike
the sorted runs carry).

**Harness.** `dialog-perf`, and the tonk-team harness.

**Status.** tonk-shaped 1×, `fs` and `file/memory`, three runs each: [results](results/2026-11-06-tonk-1x.md). Main, tonk-team's own history, the scales and the index layouts are not run yet.

## 3. Sync

**Workload.** Pulling another replica's changes. A pull merges and moves
the head; it does not run inductive rules. They catch up over the
pulled-in facts at the next commit, or at an explicit `Branch::induce`
(the "post-pull instant"), each running them over everything since their
watermark. Tonk calls neither after a pull, so its rules catch up on the
first command after one. Both halves are measured:

- `dialog-soak`'s join and catch-up scenarios, the client's space in each
  store;
- on tonk-team, a replica that is behind pulling what another wrote (the
  week of issues and memos above), then its first command after, which
  catches the rules up.

**Reports.** For the pull: time, modeled network cost (the soak's shaped
link), bytes appended to the receiving file, and the index's writes. For
the catch-up: the time of the first commit after the pull against an
ordinary one, and what it appends.

**Harness.** `dialog-soak`, and the tonk-team harness.

**Status.** tonk-shaped 1×, `fs` and `file/memory`, three runs each: [results](results/2026-11-06-tonk-1x.md). Main, tonk-team's own history, the scales and the index layouts are not run yet.

## 4. Subscriptions

**Workload.** The views' queries above held as standing queries
(`Branch::subscribe`), polled after each write, as a page kept open is.
A poll diffs the tree between the root it last read and the new one, and
re-evaluates only where the diff meets the key ranges the query read, so
its cost is tree nodes read, which the archive serves. Three cases, each
after a commit:

- a write the query does not read (a memo, under the issue list): the
  poll only diffs and advances;
- a write it reads (an issue's label, under the issue list): the poll
  maintains its result incrementally;
- a rule installed or changed: the poll re-evaluates in full.

And after a pull, every subscription polled once over the pulled-in
history.

**Reports.** Time per poll, by case; nodes read per poll; the result's
delta size, to check every store sees the same delta.

**Harness.** `dialog-perf`'s subscribe scenario, and the tonk-team
harness.

**Status.** tonk-shaped 1×, `fs` and `file/memory`, three runs each: [results](results/2026-11-06-tonk-1x.md). Main, tonk-team's own history, the scales and the index layouts are not run yet.

## 5. Memory

**Workload.** The phases above, for a reader (a process that only queries)
and a writer (one that commits and pulls), each in a process of its own.

**Reports.** Heap at open, after the cold and warm queries, after a run of
writes and after a pull (a counting allocator); the node cache's charge;
resident memory, which counts the file's mapped pages the OS keeps.

**Harness.** All of the above, with a counting allocator, one process per
role.

**Status.** Not run.

## 6. Storage

**Workload.** Every history replayed append by append.

**Reports.** The file's size after each append, split into frames, tokens,
whole blocks and the index (live and superseded); the same for `fs`; the
growth curve across the scaled datasets.

**Harness.** `single_file_census` and `file_archive_bench`.

**Status.** Sizes and index sizes measured once (see the journal); the
growth curves not yet.

## What it takes

1. **The index layouts, for real.** The archive's index behind one
   interface, with every `file/*` store above behind it, each passing the
   archive's tests and the both-ways node comparison.
2. **A store per run in `dialog-perf` and `dialog-soak`.** Both run on a
   volatile peer today; each takes the store as a setting. On main the
   setting offers `fs` only; it is a small change of its own, which can
   land on main first so the baseline needs no patch.
3. **The tonk-shaped generator** (built: `tonk_dataset`). Reads the space's schema from the copy,
   generates the records above from a seed through the space's commands,
   and writes them to a dataset file, once.
4. **A tonk-team harness, in dialog** (built: `tonk_bench`, six members on a shared remote). No tonk build: it opens a store,
   replays a dataset file into it (or opens the replica as it is), and
   runs the queries, commands, subscriptions and pulls above through
   dialog's own API, the API main shares. What it cannot see is tonk's own
   cost on top (rendering, the worker's handlers), which no archive
   changes.
5. **A main worktree** (built: `bench/main-baseline.sh`). `origin/main` checked out beside the branch, the
   harness built there for `main/fs`, against the same dataset files.
6. **One report** (`bench/report.py`, tables and Mermaid bars; the growth curves to come) that puts the six axes side by side for every store,
   with charts (below).

## Charts

The report is Markdown with its charts in it, so it reads on GitHub and
in the journal as it is:

- **A bar chart per measure** (Mermaid `xychart-beta`, which GitHub draws):
  the stores along the axis, one bar each. Mermaid draws no legend, so a
  chart carries one series, and each axis's measures come one chart each:
  query time cold and warm, time per command, time per poll, bytes per commit, pull time,
  heap per role.
- **Growth curves as SVG**, written by the report itself (a few polylines
  and a legend, no plotting dependency): size against appends for every
  store on one chart, and the same at 10× and 100×. These need several
  series on one chart, which Mermaid cannot label.

The numbers each chart draws are in a table under it, so nothing is read
off a picture.
