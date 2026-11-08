# Large transactions

**Phase:** Describing

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

Not stated yet.

## Approach

Not chosen yet.
