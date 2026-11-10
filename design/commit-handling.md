# A branch commit is refused when the head has moved

Phase: **Describing.** See the [design index](README.md) for what the
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

The maintainer says a commit on a branch can be refused if the branch head
has moved, and that this gives a UI application no good strategy for
handling it. The maintainer asks for an approach inspired by Software
Transactional Memory.

## Problem

Not stated yet.

## Approach

Not decided.
