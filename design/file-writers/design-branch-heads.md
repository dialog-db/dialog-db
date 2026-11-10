# Design: how a branch update becomes visible before integration

**Decision:** where a branch's head lives in the space's file, and how a
reader finds it and the nodes it names, so that the next query or
transaction in any process sees a commit without waiting for integration.

**For:** writers on separate branches never wait on one another, a space
is one file, and a commit that loses its branch's race retries
([the problem](../file-writers.md#problem); the direction: one file, an
extent per writer).

Two things must be findable without the file's index: the latest head of a
branch, and the nodes it names, which sit in a writer's extent until
integration indexes them.

| criterion \ approach | Head records only: a head is a record in the writer's extent; a reader finds the newest one for a branch by scanning every extent not yet indexed | A slot per branch: a table of branch slots in the file's header region, updated in place under the branch's byte-range lock, each pointing at the branch's newest head record; a record per commit maps its new nodes' hashes to their offsets | Heads stay cells: a file per branch head, as main keeps them |
|---|---|---|---|
| what a reader does to find a branch's head | 🔴 scans the head records of every extent not yet indexed, newest first | 🟢 reads the branch's slot from the mapping, with no lock: one read | opens the branch's cell file |
| what a reader does to find a node not yet indexed | the same scan | looks in an in-memory map of the per-commit node records not yet integrated, extended by reading only the records appended since it last looked | the same as the slot design |
| what publishing a head holds, and for how long | nothing beyond its extent | the branch's byte-range lock, for reading the slot, comparing, appending the head record and pointing the slot at it | the cell's lock file, for a compare-and-swap |
| a torn slot write | none to tear | two slots per branch, alternating, each with a sequence and a checksum, as the file's own slots are | the cell is replaced by rename |
| durability of what a head builds on | `fdatasync` of the one file covers every earlier append | 🟢 the same: making a head durable makes everything appended before it durable, another writer's commit included | 🔴 the head's file and every block file it names are synced apart |
| a space at rest is one file | 🟢 | 🟢 | 🔴 a file per branch |
| how many branches the table holds | no limit | ? a fixed table, with a branch hashed to its slot and a collision moving on; a space with more branches than slots needs a second level | no limit |
| what integration does to heads | indexes the head records | nothing: slots already point at head records, which stay where they were written | nothing |

## The read-set check

A commit records the ranges of the tree it read. When it takes the
branch's lock and finds a head other than the one it started from, it
computes the difference between the two trees restricted to those ranges
(`TreeDifference` reads only the paths that differ):

- empty: nothing it read changed, so its writes go onto the latest tree as
  they are, and it publishes with the new parent;
- not empty: it replays its operation over the latest head, and tries
  again.

Open: what a commit records as its read set (key ranges scanned, or the
entities and attributes queried), and what it costs to keep.
