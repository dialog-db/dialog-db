# Writers on separate branches wait on one another in the file archive

**Phase: Building.**

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

And a space should be one file, as a SQLite database is: blocks and
branch heads together. A head kept apart from its blocks is a second thing
to copy, back up, and keep consistent with them.

Two processes on the same branch already meet at that branch's head; they
need not meet anywhere else.

## Approach

One file, an extent per writer, on native platforms (the web keeps
IndexedDB). A writer claims an extent of the space's file by a byte-range
lock and appends its records there without waiting on anyone; claiming an
extent takes a short lock once per extent filled. There are two points of
coordination:

1. **Integration into the archive.** Whoever next gets the file's turn
   indexes the extents written since; the records are already in place, so
   nothing is copied. A writer that left records unindexed does it after
   its commit, off the commit.
2. **Updating a branch.** Under the branch's own byte-range lock, a writer
   compares the head with the one it started from. If another writer moved
   it, a check of what the transaction read decides: nothing changed, and
   its writes go onto the latest head as they are; something changed, and
   it replays its operation over the latest head. Either way it tries
   again.

How a branch update becomes visible before integration:
[design-branch-heads.md](file-writers/design-branch-heads.md). The
approaches weighed: [direction.md](file-writers/direction.md).

## Build order

Each stage is tested and pushed on its own, on the single-file branch.

0. **Merge main** (#768: `dialog-artifacts` becomes `dialog-corpus`), so
   the work starts from what main is.
1. **Extents.** A writer claims a run of pages under a short lock, holds a
   byte-range lock on it, and appends whole blocks and a record per commit
   of their hashes and offsets, publishing the extent's end. A reader keeps
   the records it has not seen integrated in memory, finds blocks through
   them, and holds a shared lock on each extent it reads. A writer that
   gets the file's turn without waiting writes as today.
2. **Integration.** After its commit, a writer that left blocks in an
   extent takes the turn, off the commit, and converts them into frames
   and tokens with today's append; once that is durable, the extent's pages
   go on the free list, from which new extents are claimed. Compact output
   is appended at the end at first; written into free pages later.
3. **Cells.** The archive keeps memory cells: an entry per cell in the
   index, a slot of two halves at the offset it names, capacity the
   value's size at creation plus 20%, a byte-range lock per slot for a
   compare-and-swap, a spill past capacity. A space keeps its cells in its
   file.
4. **Retry.** A commit that finds its branch's head moved applies the same
   commit with updated versions over the latest head, keeping its
   instructions until it publishes.
5. **The last close** truncates the file past what is integrated.

Tests at each stage: two processes on two branches of one space, neither
waiting on the other; a crash at each step of integration and of a spill;
readers during integration.
