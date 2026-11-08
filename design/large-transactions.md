# Large transactions

**Phase:** Dropped

## Description

Replaying 4.47 million MusicBrainz facts as 2,000 commits of 50 entities
(about 2,234 facts each) through a repository wrote 2.88 million node
versions, about 1,440 a commit, and took 86 to 114 minutes on the
single-file archive, most of it in the tree and in the archive splitting
each new node into rows
([research/single-file-format](../research/single-file-format/journal.md),
"the index in the file, measured"). The tree those commits leave is a small
fraction of what they wrote (53,201 nodes when the same replay was stopped at
about 1,250 commits). The same data loaded through `branch.import` is one
commit, and a commit applies its whole instruction stream before it persists
anything. Irakli says the replay is a pathological case for the tree
implementation, and that a transaction should be able to write its nodes to
disk as it goes and commit at the end.

## Problem

Not stated: no one is known to write transactions this large (Irakli,
2026-11-08). The question that started it was how large a database would
be without much commit history; large commits were found on the way.

## Approach

Dropped, with what was learned kept in the journal and the claims ledger,
for whoever meets a large transaction first: a commit far larger than a
node's buffer seals by replaying its ops one canonical edit each, which ran
for hours at 447,000 facts; one commit of 4.47 M facts did not fit in 15
GiB; commits of about 45,000 facts wrote the replay's data fastest. Reopen
when an importer, a migration or a sync path writes commits of tens of
thousands of facts.
