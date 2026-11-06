# Single-file format: evaluation

What we measure to decide the format's open choices, and against what.
Size alone does not decide them: an index layout that saves a megabyte can
cost every query a page, or every sync a rewrite. Each axis below says the
workload it runs, the harness that runs it, what it reports, and where it
stands. Results go in the [journal](journal.md) as they come in.

## What is compared

Every axis runs the same stores, each a space that differs only in its
archive:

| store | what it is |
|---|---|
| `fs` | main's archive: a file per block |
| `file/memory` | the single file as built today: its index loaded into memory at open |
| `file/hitchhiker` | the single file with its block index in the file as a hitchhiker tree (32 a node, buffers of 256, the root written against its last version, whole every 32) |
| `file/runs` | the single file with its block index in the file as sorted runs merged 4 at a time, the merges spread over appends |

The two in-file layouts are the finalists of the
[replay](design.md#where-the-block-index-lives-layouts-replayed); the
path-copied and per-node-delta trees lost there on size or on cold reads,
and are not built. Every store reads through the tree in place where it
can (`NodeSource`). Every identity is derived from one seed, so every store
writes the same blocks and ends on the same root, and a run refuses to
report stores that disagree.

## Datasets

- **Stack Exchange:** 4096 commits of the `retrocomputing` dump.
- **tonk-team:** the team space's history, as the replica tonk keeps of
  it, moved into each store with `dialog-migrate`. Tonk itself does not
  run: the space carries its own schema (concepts, and the rules behind
  its commands, as `dialog.rule/*` facts), so dialog alone can query it
  and run its commands. A copy of the replica is used, and nothing from it
  is committed: it holds an identity's credentials.
- **Synthetic, scaled:** the same shapes at 10× and 100×, for growth.

## 1. Queries

**Workload.** Real queries, not point lookups:

- the queries of `dialog-perf`'s read and rule scenarios: scans, joins,
  rule-derived concepts;
- on tonk-team, the queries behind the space's views, as dialog concept
  queries built from the descriptors the space stores: the issue list
  (`bug`, 294 issues), an issue's detail (`bug` and its
  `issue-activity`), the memos on a doc (`memo`; the space holds 9, so this query is
  measured again after the writes below have added a few hundred), and the schema read
  (`concept:`, about 300 concepts, and `rule:`, about 120 rules), which
  every view starts from. These are the store's share of a page, not a
  page's time: tonk's templating and formulas are left out, and they do
  not touch the archive.

**Reports.** Time, cold (a reopened store, nothing cached) and warm; pages
read from the file; rows returned. Cold matters most for the in-file
indexes, whose first lookup reads the root.

**Harness.** `dialog-perf`'s scenarios, with the store chosen per run (its
environment is volatile today), and the tonk-team harness.

**Status.** Not run.

## 2. Writes

**Workload.** What an application does:

- `dialog-perf`'s commit and transaction scenarios, inductive rules
  included;
- on tonk-team, the space's own commands: creating an issue
  (`v2/issue-create`) and posting a memo (`memo/post`). A command is
  event facts (`dom.event.detail/*`, a transient concept) asserted under
  a session profile (`dialog.session/profile`); at commit, the space's
  inductive rules turn them into the issue's or memo's facts (`squash.bug/*`,
  `xyz.tonk.memo/*`, the author's name). So the harness asserts what
  tonk's page would and commits; dialog runs the rules. A run of a few
  hundred of each, as a busy week would.

**Reports.** Time per command and per commit, the archive's share of it
(splitting nodes into rows, verifying frames, the index's own writes),
bytes appended per commit, and the largest single append (the merge spike
the sorted runs carry).

**Harness.** `dialog-perf`, and the tonk-team harness.

**Status.** Not run.

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

**Status.** Not run.

## 4. Memory

**Workload.** The phases above, for a reader (a process that only queries)
and a writer (one that commits and pulls), each in a process of its own.

**Reports.** Heap at open, after the cold and warm queries, after a run of
writes and after a pull (a counting allocator); the node cache's charge;
resident memory, which counts the file's mapped pages the OS keeps.

**Harness.** All of the above, with a counting allocator, one process per
role.

**Status.** Not run.

## 5. Storage

**Workload.** Every history replayed append by append.

**Reports.** The file's size after each append, split into frames, tokens,
whole blocks and the index (live and superseded); the same for `fs`; the
growth curve across the scaled datasets.

**Harness.** `single_file_census` and `file_archive_bench`.

**Status.** Sizes and index sizes measured once (see the journal); the
growth curves not yet.

## What it takes

1. **The index layouts, for real.** The archive's index behind one
   interface, with `file/memory` (today's), `file/hitchhiker` and
   `file/runs` behind it, each passing the archive's tests and the
   both-ways node comparison.
2. **A store per run in `dialog-perf` and `dialog-soak`.** Both run on a
   volatile peer today; each takes the store as a setting.
3. **A tonk-team harness, in dialog.** No tonk build: it opens a copy of
   the migrated replica in each store with `dialog-repository`, reads the
   concepts and rules the space stores, and runs the queries, commands
   and pulls above through dialog's own API. What it cannot see is tonk's
   own cost on top (rendering, the worker's handlers), which no archive
   changes.
4. **One report** that puts the five axes side by side for every store,
   with charts (below).

## Charts

The report is Markdown with its charts in it, so it reads on GitHub and
in the journal as it is:

- **A bar chart per measure** (Mermaid `xychart-beta`, which GitHub draws):
  the stores along the axis, one bar each. Mermaid draws no legend, so a
  chart carries one series, and each axis's measures come one chart each:
  query time cold and warm, time per command, bytes per commit, pull time,
  heap per role.
- **Growth curves as SVG**, written by the report itself (a few polylines
  and a legend, no plotting dependency): size against appends for every
  store on one chart, and the same at 10× and 100×. These need several
  series on one chart, which Mermaid cannot label.

The numbers each chart draws are in a table under it, so nothing is read
off a picture.
