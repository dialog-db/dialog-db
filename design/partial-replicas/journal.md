# A query or a click waits on blocks the replica does not hold: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* A replica holds what its reads reached and never what the next interaction will read; a space may be too large to hold whole (the maintainer). An evaluation that reads only held blocks is exact, and whether an operation is such an evaluation cannot be known without running it. Nothing tells a caller that an operation needs the network: a local read exists only at the tree, and its miss is a string error. What a commit reads (the cell scan in the entity ordering above all), and that the same cell is held in the attribute ordering. That a write which did not observe its cell has a defined meaning and a read with holes has none. | *Where are we at?* Delimiting. The maintainer named the cause: nothing recognizes what can run on a partial replica. A problem statement is drafted in the top story, not accepted. |
| **Agenda** | *What do we need to know?* (1) From the maintainer, asked and not yet answered: if the aim is that a screen's footprint is replicated so its controls work, are answers with holes and writes made without their read now outside the problem, so that a control whose footprint is not held, with no network, is disabled and nothing else? (The earlier form of this question, disabled, held or written blind, was not answered directly.) (1b) How much larger a tonk screen's footprint is than what its queries already fetched: the number that says to what degree this is viable. (2) The same for a query: is a list that shows the rows it holds, marked as not all, a degraded answer or a wrong one? (3) Whether the wait or the silence is the complaint online. (4) Whether the draft problem statement is accepted. Carried over, now for Direction and not blocking: where a stalled click's time goes (the template below); whether a cell reads the same from the attribute ordering; how many of tonk's queries and rules read absence. | *Where are we going?* Accept or cut the problem statement, then Direction: use cases (what a person and an application could do were the verdict known), then approaches. What others do with a tree held by reference (git's partial clone and sparse index, Datomic peers, Zero's `unknown`/`complete` results, certain answers over incomplete databases) is still to be read. |

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

## 2026-11-12, Diagnosing to Delimiting: what can run on what is held

**Asked.** The maintainer corrected a hypothesis of the first sitting: the
replica was never complete. A space may hold huge amounts of data; a load
runs queries and replicates only the segments they touched, and the next
interaction may need more of the tree. The maintainer then offered a
problem: "we have no good way of recognizing what functionality is
available and what isn't in a partial replica because any action or query
may require part not yet available", and what is needed is "a way to
operate what can be operated in that mode and gracefully degrade or
disable functions that can not be executed on the partial replica".

**Learned.**

- The hypothesis as written was about reads already made, not the whole
  space, but the correction stands against what it was used for. "Stale
  and complete" exists only for reading again what was read; the click's
  read is a first read. The opinion built on it is narrowed in the ledger,
  and holding the head back until blocks are local is not an answer to
  this topic's complaint.
- *That is a feature, not a problem*, half of it. "No way of recognizing
  what is available" is a cause. "A way to operate what can be operated"
  is a remedy, and it hides the word that needs a definition. An operation
  that reads only held blocks is not operating in a lesser mode: its
  answer is exact. So "what can be operated" has a plain meaning (every
  block it reads is held), and the trouble is that this is not known
  before an operation runs, since what it reads depends on what it reads
  first, and is not said while it runs.
- What exists toward a verdict: a read can be made local at the tree and
  then misses as `None`. What does not: no query, subscription or commit
  can be made local, and a local miss surfaces as a string error with no
  range in it. A caller gets an answer, a late answer, an error or
  nothing.
- *Those two cannot both be true:* the request arrived asking for answers
  that leave out what is not held and for writes made against the
  incomplete tree. The problem as now stated asks for recognizing what
  cannot run and disabling it. The first runs the operation anyway and
  accepts a different result; the second refuses to. "Degrade" sits
  between them and is not defined. The agenda's first two questions ask
  which is meant, once for an action and once for a query.
- "Disable" and "degrade" ask for the verdict at different times: before
  the person acts, for everything on screen and again whenever the
  replica changes; or when the operation is attempted.
- The measurement of where a click's time goes matters less to this
  statement than to the first: the statement does not depend on which
  read waits. It moves to Direction, where it decides between approaches.

**Decided.** Not decided. The phase moved to Delimiting on the
maintainer's naming of the cause; the problem statement is a draft.

**Backtracked.** No. The first sitting's opinion that holding the head
back would spare the warm case partial answers is narrowed, not reversed.

## 2026-11-12, Delimiting: the screen as the unit

**Asked.** The maintainer, in place of an answer to the agenda's first
question: "what we actually need is ability to know ranges that need
replicating to offer a full functionality on specific screen. Not sure how
and to what degree that is viable but that seems like ideally what one
would want."

**Learned.**

- *That is a feature, not a problem*, and the third remedy of the day
  (answers with holes and blind writes; recognizing and disabling; knowing
  the ranges to replicate). What stays the same under all three is an
  objective: a person looking at a screen expects every control on it to
  work. The unit is the screen, which is the application's word. Dialog
  can speak of a query's and a would-be transaction's footprint; the
  glossary defines footprint and frontier.
- For a screen's queries the ranges are already recorded (`Demand`), and
  are held by the time they are known. The whole question is the controls.
- A control's reads are of three kinds. The cells it would write have
  ranges computable when it is rendered, with nothing read. What the rules
  it triggers read is found only by evaluating them against the batch,
  which could be done ahead by staging the would-be transaction when its
  values are fixed, and cannot when they are typed later or an entity is
  minted at the click. What tonk's command providers do is outside
  dialog's sight.
- "Know the ranges" has a limit that follows from reads depending on
  data: what can be known is the frontier now, perhaps with a size, and
  when it is empty. Not the whole set before fetching any of it.
- The cost has a shape worth measuring before anything else: one
  entity-ordering leaf for each actionable row. Answering a commit's scan
  from the attribute ordering would put the cell of a shown attribute
  inside the list's own footprint, so the narrow change from the second
  sitting and this remedy are not alternatives; one shrinks the other.
- Pieces that exist: the cover of a query, preloading a range below
  demand, staging without publishing, the diff-against-cover check after a
  pull, a size on each link. Pieces that do not: a cover for a commit, a
  commit or query made local, a typed miss, and any way for an application
  to say which transactions a screen's controls would make.
- Where this works, nothing incomplete is ever used as an answer, so the
  two remedies the request arrived with would have nothing left to do
  except offline with a frontier that is not empty. Whether they are
  wanted there is the scope question now first in the agenda.

**Decided.** Not decided. The draft problem statement stands unaccepted
and does not yet name the screen.
