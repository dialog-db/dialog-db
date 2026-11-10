# Writers on separate branches wait on one another in the file archive

**Phase: Delimiting.**

## Description

A space keeps its blocks in one file
([research/single-file-format](../research/single-file-format.md)), and
every writer to the space takes its turn on that file's exclusive lock,
whatever branch it writes to. Branch heads and the space's other memory
cells are not in the file: under the `fs` provider each cell is a file of
its own, updated compare-and-swap under a lock file of its own.

Irakli says that branches contending on one file does not feel right, and
keeps returning to an idea: each session (a connection to the database),
or each branch, writes to a file of its own without waiting on anyone, and
an asynchronous job integrates those writes into the space's file; readers
look at both. He would also like branch heads, or memory cells generally,
kept in the same file archive, perhaps as a tree of their own built with
the same techniques as the blocks, or as something more specific.

The situation Irakli has in mind: two processes advance two branches of
one space independently and at the same time. On main they share nothing a
write waits on: each block is a content-addressed file of its own, written
atomically and skipped if it exists, and each head is a cell of its own
under its own lock. With the single-file archive, both processes append to
one file and take turns on its lock.

The single-file design already names the integrator as an option and leaves
it unstarted, and keeps branch heads and credentials out of the file
([design.md, "Not in this design"](../research/single-file-format/design.md)).
No measurement so far has two branches of one replica writing at once.

## Problem

Many processes work on separate branches of one space at once. On main
they never wait on one another to write: a block is a file of its own and a
head is a cell of its own. The single-file archive makes every write to a
space take the same file's one writer turn, so processes on separate
branches serialize behind each other, and the archive cannot be used where
such processes run.

Not yet settled: whether processes on the *same* branch, which already
coordinate on that branch's head, must also write without waiting on each
other; and whether branch heads, or memory cells generally, belong in the
file, a second question with its own problem not yet stated.

## Approach

Not chosen yet.
