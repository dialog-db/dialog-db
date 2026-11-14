# Design: memory cells in the file, and how an update is visible before integration

**Decision:** where a space's memory cells (branch heads among them) live
in its file, and how a reader finds a cell and the nodes it names, so that
the next query or transaction in any process sees an update without waiting
for integration.

**For:** writers on separate branches never wait on one another, a space
is one file, and a commit that loses its branch's race retries
([the problem](../file-writers.md#problem); the direction: one file, an
extent per writer).

Two things must be findable without the file's index: the latest head of a
branch, and the nodes it names, which sit in a writer's extent until
integration indexes them.

| criterion \ approach | Head records only: a head is a record in the writer's extent; a reader finds the newest one for a branch by scanning every extent not yet indexed | A slot per cell (Irakli): an index of cells maps each cell's name to the byte offset of its slot; the slot is written over in place on every update, under a byte-range lock on the slot itself; the index changes only when a cell is created or deleted, as two tables swapped in turn; a cell created since the index was written is found in the extents | Heads stay cells: a file per branch head, as main keeps them |
|---|---|---|---|
| what a reader does to find a cell | 🔴 scans the head records of every extent not yet indexed, newest first | 🟢 looks the cell up in the index once, then reads its slot from the mapping on every read, with no lock; only a cell created since the index was written sends it to the extents | opens the cell's file |
| what a reader does to find a node not yet indexed | the same scan | looks in an in-memory map of the per-commit node records not yet integrated, extended by reading only the records appended since it last looked | the same as the slot design |
| what publishing a head holds, and for how long | nothing beyond its extent | the byte-range lock on the cell's slot, for reading it, comparing and writing it over | the cell's lock file, for a compare-and-swap |
| a torn slot write | none to tear | ? a slot written over in place can tear, by a crash or under a reader; two halves per slot written in turn, each with a sequence and a checksum, as the file's own slots are, and the reader takes the newer whole one | the cell is replaced by rename |
| durability of what a head builds on | `fdatasync` of the one file covers every earlier append | 🟢 the same: making a head durable makes everything appended before it durable, another writer's commit included | 🔴 the head's file and every block file it names are synced apart |
| a space at rest is one file | 🟢 | 🟢 | 🔴 a file per branch |
| how many cells | no limit | 🟢 no fixed limit: the index names any number of slots, and grows when cells are created | no limit |
| what a slot holds | no slot: the head record holds the whole value | the value inline, in a slot whose capacity is the value's size at creation plus 20% (Irakli). The slot carries the value's hash, which is the cell's edition, so a compare-and-swap compares the slot without reading the value. Cells do not accrete: an update writes over the slot and keeps no history. An update that outgrows the capacity spills: the value is written as a block in the updating writer's extent, and the slot holds its hash, offset and length, so readers' cached offsets stay valid and the index does not change (see "Spills and integration" below) | the cell's file holds the whole value |
| what integration does to cells | indexes the head records | adds cells created since to the index, and removes deleted ones; never touches an existing slot | nothing |

## Spills and integration

Integration only indexes records already in the file: it never moves,
copies or writes over a record, and never touches a cell's slot. A spilled
value is a block in the updating writer's extent, and its slot names it by
offset, so integration has nothing to do with it beyond indexing it like
any block. What keeps a spill whole:

1. **Order.** The block is appended first, then the slot's half is written.
   A crash between them leaves the slot naming the previous value; the new
   block is never referenced.
2. **The hash.** The slot carries the value's hash. A reader checks the
   block against it, so a slot that reached the disk without its block
   (pages written back out of order) fails the check, and the reader takes
   the slot's other half, the previous value, which this update did not
   write over: the halves alternate.
3. **Published ends.** An integrator reads a writer's extent only up to the
   end that writer has published, after the records before it are whole.
4. **Two checks for a reader.** A reader takes the newer of the slot's
   halves, each with its own checksum, then checks the block against the
   hash; anything torn fails one of them, and it takes the other half.

Nothing in this design reclaims space in extents; only freed index pages
are reused, as today. Anything that later does (a compaction, as SQLite's
`VACUUM`) has to keep every block a slot names.

## When the head moved

A commit that finds its branch's head moved since it started applies the
same commit over the latest head and tries again, always: its instructions,
with the versions they carry minted afresh against that head (the rules run
again, as on any commit). What this needs is that ability, applying a
commit with updated versions; today a commit's instructions are a stream
the mint consumes, so after a failed publish there is nothing left to
apply, and the commit has to keep them until it publishes. Skipping the
replay when nothing it read changed is a topic of its own:
[replay-avoidance](../replay-avoidance.md).

## The cell index

How many cells a space has cannot be known: it is as many as the branches
its users create, one or two in every run measured, but without a bound
(Irakli). So the index has to cost the same at one cell and at millions.
A flat array written as two copies in turn rewrites itself whole on every
create, which does not.

The file already has an index built for that: its block index, a paged tree
in the file keyed by hash, with pages reused after a sync point and its root
published through two rolling slots and a durable one (format 10). It holds
blocks, tokens and lineages; a cell is a fourth kind of entry, the hash of
its name mapped to its slot's offset. Creating or deleting a cell is an
insert or a removal, made by integration under the turn as every index
change is. A reader looks a cell up once by the same path and keeps its
offset; a cell created since the last integration is found in the extents.
