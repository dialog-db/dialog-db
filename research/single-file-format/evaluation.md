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
  it, moved into each store with `dialog-migrate`.
- **Synthetic, scaled:** the same shapes at 10× and 100×, for growth.

## 1. Queries

**Workload.** Real queries, not point lookups:

- the queries of `dialog-perf`'s read and rule scenarios: scans, joins,
  rule-derived concepts;
- on tonk-team, the queries tonk renders a space with: its concepts, an
  issue list, a memo list and an issue's detail, and the outline of the
  space's views.

**Reports.** Time, cold (a reopened store, nothing cached) and warm; pages
read from the file; rows returned. Cold matters most for the in-file
indexes, whose first lookup reads the root.

**Harness.** `dialog-perf`'s scenarios, with the store chosen per run (its
environment is volatile today), and a tonk harness for tonk's queries.

**Status.** Not run.

## 2. Writes

**Workload.** What an application does:

- `dialog-perf`'s commit and transaction scenarios, inductive rules
  included;
- on tonk-team, tonk's own commands: creating an issue and creating a
  memo, each through tonk's evaluator, so the rules they trigger
  (inductive rules among them) run as they do for a user. A run of a few
  hundred of each, as a busy week would.

**Reports.** Time per command and per commit, the archive's share of it
(splitting nodes into rows, verifying frames, the index's own writes),
bytes appended per commit, and the largest single append (the merge spike
the sorted runs carry).

**Harness.** `dialog-perf`, and the tonk harness.

**Status.** Not run.

## 3. Sync

**Workload.** Pulling another replica's changes, which merges and
re-derives what inductive rules produce:

- `dialog-soak`'s join and catch-up scenarios, the client's space in each
  store;
- on tonk-team, a replica that is behind pulling what another wrote: the
  week of issues and memos above.

**Reports.** Time, modeled network cost (the soak's shaped link), bytes
appended to the receiving file, and the index's writes during the pull.

**Harness.** `dialog-soak`, and the tonk harness.

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
3. **A tonk harness.** Tonk pins dialog `v0.4.0-rc.3`, and this branch is
   past several breaking changes, so tonk is patched to build against the
   branch (on tonk's branch of the same name, never pinned to it in a
   release). The harness opens a copy of a tonk-team replica in each
   store and drives tonk's commands, queries and pulls through tonk's own
   code.
4. **One report** that puts the five axes side by side for every store.
