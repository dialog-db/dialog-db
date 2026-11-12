# A replica that holds every block still pays to warm it

Phase: **Designing.** See the [design index](README.md) for what the
phases mean, and the [journal](warm-walk-on-local-tree/journal.md) for
where the topic stands.

## Description

Every evaluation runs inside `Driven`
(`rust/dialog-repository/src/repository/fetch.rs`), which pops the
prefetch hints evaluations enqueue and runs `warm_source` for each: a walk
of the hinted key range from the head's root, so that what a later read
needs is local by then. The walk's only guard is `routes.fetches(subject)`,
true as soon as any open branch of the repository tracks a remote.

On the `query-member` perf scenario (2,000 entities recorded by an
inductive rule, a member branch that adopted the result and tracks a
remote it never reaches, a full join then a filtered join) the phase runs
503 walks over 1,020 nodes, all from the node cache, fetching nothing.
After #767 removed the buffer decoding that made them 40% of the phase,
they are about 8% of it (`notes/fetch-scheduler.md`, "A warm walk on a
local member"). The maintainer reports a tonk profile of about 18.5K walks
per query on tonk's member branches.

Two remedies were examined and rejected before this topic. #767 rejected
a memo of completed (root, range) walks: hints are point probes that
rarely repeat. It named a record that a root is wholly local as the honest
fix, and noted that nothing records one: a download completes without
leaving a marker, and a commit on a partial replica makes a head that is
partly by reference. #770 rejected running speculation only while demand
is pending: measured, it starves a partial replica's prefetch and breaks
the catchup soak's contract that the first poll's hints drain.

## Problem

Warming exists to fetch, ahead of demand, blocks a replica does not hold.
A walk over a tree whose every block is local can fetch nothing, and is
run anyway for every hint, because the walk asks whether the repository
has somewhere to fetch from and never whether this tree has anything left
to fetch. Nothing in dialog can answer the second question: no operation
that leaves a tree wholly local records that it did. So a query on a
replica that holds its whole tree pays for speculation that cannot return
anything, once per probed row and merged range, and a guard that cannot
tell such a tree from a partial one either keeps that cost or withholds
the prefetch a partial replica depends on.

## Approach

Decided 2026-11-12, column D of the [matrix](warm-walk-on-local-tree/direction.md),
by the agent from the code and the brief, for the maintainer to read cold
(see the journal: the maintainer was not in this sitting). Persistence
across a restart is a separate decision,
[design-persistence.md](warm-walk-on-local-tree/design-persistence.md),
decided "not yet".

- **What is recorded.** A set of tree hashes known wholly local in one
  repository's archive. Keyed by tree hash, not by head or branch: a head
  adopted by reference names a tree this replica did not record, so it
  cannot inherit the record, and a head whose tree hash is recorded names
  a tree that is local, whoever adopted it.
- **Where it lives.** Held by the environment beside the routes, per
  repository. It is knowledge about the archive, not a copy of content,
  so releasing or clearing the caches does not drop it. Bounded to the
  most recent few trees per repository; an entry evicted costs a walk.
- **Who writes it.** Two writers, nothing else:
  - a commit whose instructions were applied to an empty tree or to a
    recorded tree, once its nodes are persisted, records the new tree;
  - a `Download::Everything` of a revision that drained without error
    records that revision's tree.

  A pull that fast-forwards to a recorded tree needs no writer: the lookup
  is by hash. Grafts, replays, resets, rewrites and scoped downloads write
  nothing, so their heads walk as today.
- **Who reads it.** `warm_source` alone, after the routes check and before
  the walk's span: a recorded root returns at once.
- **The invariant that keeps it safe.** Only advisory code reads the
  record. A wrong entry then skips a walk, and the demand read that needed
  the range fetches it, as it does with speculation switched off. No
  download, pull or read may consult the record: there a wrong entry
  would make a replica believe it holds what it does not.

Not taken on: surviving a restart (a restarted member walks as today
until a download of everything records its tree; its commits cannot,
since their base is not recorded); a record of a scoped download, (tree, scope); recording what
a pull's replay arm produces. Each is a later step that adds a writer,
not a change to the reader.

Open implementation decisions, for the Design phase: the one place a
commit knows both its base tree and that its nodes persisted; the bound;
how a test observes a skip, given the perf gate allows no new `span.*`
growth without a re-recorded baseline.
