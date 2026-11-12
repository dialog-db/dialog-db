# A query or a click waits on blocks the replica does not hold

Phase: **Diagnosing.** See the [design index](README.md) for what the
phases mean, and the [journal](partial-replicas/journal.md) for where the
topic stands. The title is the symptom; it changes when the problem is
stated.

## Description

The maintainer reports that tonk sometimes feels unresponsive. Two shapes
are named: a page load that stalls, which is said to be tolerable, and a
button that seems not to react when it is clicked, which is said to be the
bad one. The maintainer says that when the device is offline the same
operations do not work at all.

The maintainer attributes both to dialog: a query or a transaction reaches
a tree block the replica does not hold, and waits for that block to be
replicated before it goes on. For the click, the maintainer says the wait
is replication done "in order to be able to update the tree where the write
is supposed to go", and, asked again, says which read is meant: a write
under a pick such as `last` reads the cell's current value so as to retract
it and assert the new one, and that read halts or fails when the segment
holding the current value is not on the replica.

The request arrived as two features. One is a mode in which a query over a
replica that lacks blocks returns what it can and leaves out what it cannot
read. The other is to create transactions against the incomplete tree
rather than stall or fail, and reconcile them later. The maintainer also
asked whether doing anything useful with such a replica is flawed as an
idea.

tonk's own plan for the same symptoms,
[`plan/steady-interface.md`](https://github.com/tonk-labs/tonk/blob/main/plan/steady-interface.md),
lists reads that fetch from the remote mid-query as one of five things it
says make a page fast one time and slow the next. The other four are a
worker-wide lock held across network round trips, subscription polls that
run one after another on a branch, a worker boot that waits on a remote,
and what happens while a deploy is installed. That plan recommends that a
space's head move only once what reads need is local, and, for data the
device has never held, a read mode that answers from what is local and
says the answer is incomplete.

What was checked in the code on 2026-11-12 (dialog `a31b144`, tonk
`bfb2d72`, which pins dialog `v0.4.0-rc.7`) is in the
[claims ledger](partial-replicas/claims.md). In short:

- A pull that fast-forwards adopts the upstream head by its root and reads
  nothing; every block of that tree the replica did not already hold is
  then fetched by the first read that reaches it. tonk pulls its spaces
  this way and downloads only its account and profile branches whole.
- A read that misses with the remote unreachable fails naming the remote;
  with the connection stalled it does not return, since the S3 transport
  sets no timeout. tonk checks `navigator.onLine` before a sync and not
  before a query or a commit.
- The read the maintainer names exists and is as described: a write under
  a pick that elects scans its cell in the entity ordering, and on a
  replica that holds that leaf by reference the scan fetches it. It is the
  one read an instruction makes; its writes are appended to buffers in the
  root node, the attribute and value orderings and history included,
  unread. A commit also reads the children of a node whose buffer
  overflowed, whatever the bodies of the inductive rules it triggers read,
  and its own bookkeeping records.
- A list reads the attribute ordering and a commit reads the entity
  ordering, and all three orderings store the same datum for a claim. So a
  value can be on the screen while the block the commit wants is not on
  the replica.
- Between a click in tonk and the page changing there are, in order: a
  mount that can pull, the commit, every command provider the commit
  fired, and a poll of every subscription on the branch, one after
  another. Nothing counts how long each takes or how many blocks each
  fetched.

## Problem

Not stated yet. The request names two remedies, and the cause they answer
is an attribution that has not been measured: see the journal's agenda.

## Approach

Not decided.
