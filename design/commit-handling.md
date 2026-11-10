# A branch commit is refused when the head has moved

Phase: **Diagnosing.** See the [design index](README.md) for what the
phases mean, and the [journal](commit-handling/journal.md) for where the
topic stands.

The title is a symptom. The topic arrived as a remedy, "a Software
Transactional Memory inspired approach to handling commits"; the title
changes again when the problem is stated.

## Description

On main (`4fd67af`), a transaction on a branch collects assertions,
retractions and dispatched commands. Committing it runs commit-time
induction, applies the writes to the tree at the head the branch handle
holds, and mints and signs a revision. Publishing replaces the branch's
head cell with that revision by compare-and-set against the head the
revision was built on. If the head cell changed in between, the publish
fails with `PublishError::VersionMismatch`, the staged revisions are
stale (their editions belong to whatever moved the head), and the code
says recovery is to run the transaction again on the fresh head. Dialog
does not do that itself; the caller does.

Within one environment, commits and pulls of one branch take turns under a
writer lock. A commit can ask to `merge()`: when it loses the race, the
revision it minted is kept and a merge of it with the head that won is
published, the way a pull merges two peers' changes.

Tonk (pinned to dialog `v0.4.0-rc.7`) handles the refusal in its callers.
Its reactor refreshes the branch and runs the commit again, up to four
times, recognising the refusal by the text of the error. Its worker's
evaluate route does the same, unless the request named the revision it
expected, in which case it does not run again and answers with a revision
conflict. A comment in the reactor says the only thing that races a commit
there is sync, and that one retry settles it in practice.

Tonk's command line pulls the upstream before a write, evaluates the
document (its queries and its writes) in one process, and publishes
without `merge()` and without running again; a refusal is reported as
"commit failed" with the error's text. After the write it pushes, and a
push the upstream refuses because it moved is answered by pulling, which
merges both sides' facts, and pushing again, up to five times. A query an
agent ran in an earlier invocation, and decided from, is not known to the
invocation that writes.

The maintainer says a commit on a branch can be refused if the branch head
has moved, and that this gives a UI application no good strategy for
handling it. As the maintainer reports it:

- In the web app, a refused commit shows as an error in the console, or
  somewhere in the interface if the application's author handled the
  error.
- Two agents working on the same branch through the command line often
  run into conflicts and are confused by them. By the time one has gone
  back and tried again the head has often moved again, and it is refused
  again.
- The agents do not necessarily consider that the query they are working
  from may be stale.

The maintainer asks for an approach inspired by Software Transactional
Memory, and describes the ideal as: a commit never fails; instead it
receives an operation that dialog can run again on the latest state, and
that considers the latest state.

## Problem

Not stated yet.

## Approach

Not decided.
