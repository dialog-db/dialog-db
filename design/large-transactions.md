# Importing a large dataset into dialog

**Phase:** Delimiting

## Description

Replaying 4.47 million MusicBrainz facts (100,000 entities, 1.8 GiB of
JSON, 488 MiB as a fact log) through a repository, on the single-file
archive of [dialog-db/dialog-db#733](https://github.com/dialog-db/dialog-db/pull/733):

- as 2,000 commits of about 2,234 facts each: 2.88 million node versions
  written, about 1,440 a commit, for a final tree of 77,808 nodes three
  levels deep; 6,562 to 7,808 seconds of commits; a 1,003 MiB file;
- as 100 commits of about 44,700 facts: 1.01 million node versions, 2,616
  to 2,791 seconds, 530 MiB, the commits holding up to 4.9 GiB;
- as 10 commits of about 447,000 facts: the first, into an empty tree,
  finished; the second ran for over two hours at full CPU and was stopped;
- as one commit: killed by the kernel at 13.6 GiB on a 15 GiB machine, the
  commit holding over 8 GiB.

The final tree alone, frames written whole and the tokens they name, is
about 250 MiB. Irakli says no one is known to write transactions this
large today, that the replay is a pathological case for the tree
implementation, and that a tool writing just a replica's current state
would be useful.

## Problem

Someone importing a large existing dataset into dialog cannot. As one
commit, the import holds every new node in memory until the commit seals,
and a commit into a tree that is not empty applies the ops its buffers
send toward the leaves one canonical edit at a time: 4.47 million facts
did not fit in 15 GiB, and a commit of 447,000 facts ran for over two
hours. As many smaller commits, each commit rewrites the nodes its ops
reach, so the same nodes are written again and again: 2,000 commits wrote
37 node versions for every node of the tree they left.

*How large a dataset, within what memory and time, is not stated yet.*

## Approach

Not chosen yet.
