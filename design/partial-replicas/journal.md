# A query or a click waits on blocks the replica does not hold: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* How a replica comes to hold a tree by reference (a lazy pull adopts by root), what a read does on a miss (fetch; fail naming the remote; or not return, there being no timeout), what a commit on main reads (one entity-index path per instruction, overflow cascades, induction, bookkeeping; the other writes are blind appends at the root), and the steps between a click in tonk and the page changing. That a write which did not observe a cell already has a defined meaning (a concurrent peer's), and that a read with holes has none in the engine. That stale and partial are two different states. | *Where are we at?* Diagnosing. The description is written. The cause the request assumes for the click (the commit replicating the tree where the write goes) is an attribution: nine places the time could be are in the ledger, none measured. |
| **Agenda** | *What do we need to know?* (1) From the maintainer, the next question, asked and not yet answered: on the clicks that stall, is the person changing something the page was showing, or writing something the page never read (a new entity, a field not on screen)? Still open from the first sitting: when a device holds an older revision completely and a newer one by reference, what should a person looking at the page see, the older answer or the newer one with holes? (2) Where a stalled click's time goes: the result template below. (3) Whether the replica was complete for its reads before the pull that made it partial. (4) Whether a cell reads the same from the attribute ordering as from the entity ordering at every root. (5) How many of tonk's queries and rules read absence (a negation, an aggregate, a pick over a cell, an existence check before a write). | *Where are we going?* Stay in Diagnosing until (2) is filled in; then Delimiting. The measurement is tonk's to run and may become a research topic; link it from the ledger row. For Direction, later: what others do with a tree held by reference (git's partial clone and sparse index; Datomic peers; Zero's `unknown`/`complete` results; certain answers over incomplete databases), to be read, not yet read. |

### Result template for agenda (2)

If this table were filled in for twenty clicks that felt stalled and twenty
that did not, on a shaped link (tonk's `bench/trace` stall scenario), we
would know which of the hypotheses in the ledger holds:

```
| click | mount ms | commit ms | of which induce ms | blocks fetched in commit, by region (entity / attribute / value / history) | providers ms | wait for poll slot ms | own poll ms | blocks fetched in polls | lock wait ms | click to frame ms |
```

## 2026-11-12, Describing to Diagnosing: what the request assumes

**Asked.** The maintainer asked for a plan for working with partial
replicas: a query mode that returns what it can, and transactions against
the incomplete tree that are reconciled later; and asked for the
consequences, and whether the idea has merit or is flawed. No question was
put back to the maintainer during the sitting; the first one is in the
agenda.

**Learned.**

- *That is a feature, not a problem.* Both halves of the request are
  remedies. The complaint under them is a person clicking and seeing
  nothing happen, and a person offline being unable to use what the device
  already has.
- The cause given for the click does not match how a commit works on main.
  A commit no longer rewrites the paths its writes land on: since the
  write buffers, three of an instruction's four writes are appended at the
  root unread, and the commit reads one path of the entity index.
  `notes/version-control.md` says so in as many words, in the paragraph on
  why the push invariants were written down. tonk pins `v0.4.0-rc.7`, nine
  commits behind main; the buffered path predates the pin.
- The click's wait has at least nine candidate places, five of them in
  the commit (the entity-index read, the same for a new entity, an
  overflow cascade, induction, the writer lock behind a pull) and four
  outside it (the mount, command providers, the queue of subscription
  polls, the worker lock). tonk's own plan lists the last two as inferred,
  not measured. Nothing counts fetches per phase.
- "Offline it does not work at all" is two behaviours: an error that names
  the remote, or a read that never returns because the transport has no
  timeout. tonk asks `navigator.onLine` before a sync only.
- *What does that word mean?* "Partial replica" was standing for two
  states. A replica behind its upstream that holds everything it reads is
  stale. A replica at the newest head that holds part of it by reference
  is partial. The lazy pull turns the first into the second, and it is the
  second that waits. The glossary now separates them. "Reconcile" was
  standing for three things (a merge, a later covering record, running
  rules again) and is left undefined until one is meant.
- A write that did not read its cell is not new to the design: it is what
  a concurrent peer's write is, and the merge's screens converge whatever
  the writer had read. What it loses is not safety but intent: the write
  supersedes less than the person meant. Under `last` the new value still
  reads as the cell's value; under `max`, `min` or `top` it may not. A
  retract cannot be blind at all unless the version it withdraws is
  carried from the row the person was looking at.
- The read a blind write skipped is a read of an immutable revision, so
  its answer is fixed when the write is minted and could be recorded by a
  later revision. That is a hypothesis with a stated test. It does not
  carry over to induction without contradicting what
  [query isolation](../query-isolation.md) decided: a transaction reads
  and commits against one revision.
- A read with holes has no representation. A held parent names the key
  range of each absent child, so a scan could say exactly what it could
  not read, and a subscription's demand cover is the same kind of thing (a
  set of key ranges), so "does a hole matter to this query" is an
  intersection. But no result type carries it, a subscription is woken
  only by a root that changed, and `notes/incremental-subscriptions.md`
  rests negation on the covering subtree being fully materialized.
- *Those two cannot both be true:* tonk's plan recommends that a space's
  head move only once what reads need is local, and also a local-only read
  mode for partial answers. If the first holds for a space, the second is
  never exercised for it. They are answers to different cases (data the
  device held; data it never held), and the request did not say which case
  the stalls are.

**Decided.** Not decided, and nothing to decide yet. What would decide
whether either remedy is worth building: the result template, and the
maintainer's answer to the agenda's first question. Three opinions of the
agent's are in the ledger, marked as such; the second rests on an open
hypothesis (that the replica was complete for its reads before the pull).

## 2026-11-12, Diagnosing: the read a write under a pick makes

**Asked.** The maintainer said the first sitting misunderstood the
attribution: a commit that writes a fact under `last` will try to read the
current value to retract it and assert the new one, and that read will
halt or fail because the segment where the current value should be is not
on this replica.

**Learned.**

- The maintainer is right about the read, and the ledger already held it
  as a fact; the summary given in the conversation ("a commit does not
  replicate the tree where the write goes") read as a denial of it. What
  that sentence was about is the size of the read: one cell, in one
  ordering, not the path of every write. The attribution is now two rows:
  that the read exists and fetches (a fact), and that it is where the
  stalled clicks spend their time (still a hypothesis; the other places in
  the ledger are not ruled out by the read being real).
- What the scan is for, read from the code: to elect the claim to
  retract, to learn that claim's value so its keys can be erased in all
  three orderings, to cite its versions as the new claim's cause, and to
  write nothing when the cell already reads as the written value. A write
  without the scan cannot erase the old value, because the keys to erase
  are made of the value it did not read.
- The cell is stored three times with the same datum, and a query that
  lists by attribute reads it from a different ordering than the commit
  does. So "the person was looking at the value" does not mean the
  replica holds the leaf the commit scans. That gives the maintainer's
  account a mechanism for the commonest click, changing something on
  screen.
- *Think of a second one.* The request's answer to this read is to write
  without it and reconcile later. Between that and today there are two
  others, neither of which changes what a commit means: answer the scan
  from the attribute ordering when that is the one held; and answer it
  from what the transaction itself read, since a transaction's queries
  read the revision its commit builds on. Both are hypotheses with tests
  in the ledger. The blind write is then what is left for a cell held in
  neither ordering and not read by the transaction: a new entity, a field
  never shown, a device that is cold.

**Decided.** Not decided. The agenda's first question changed: which kind
of cell the stalled clicks write decides whether the two narrower answers
cover the complaint or the blind write is needed.
