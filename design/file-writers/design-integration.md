# Design: what a writer puts in its extent, and what integration does

**Decision:** whether a writer appends the file's final form (frames and
tokens) to its extent, so that integration only indexes, or whole blocks,
which integration converts into the final form.

**For:** writers never wait on one another, a space is one file, and a
cell past its capacity spills to a block its slot names
([the problem](../file-writers.md#problem)).

| criterion \ approach | Final form in the extent: the writer splits its nodes into frames and tokens before appending, as a commit does today; integration indexes and moves nothing | Whole blocks in the extent: the writer appends blocks as they are; integration splits them into frames and tokens, appends that, and frees the extent's space once nothing can still read it |
|---|---|---|
| what a commit pays | 🟡 splitting, about 60% of an append today | 🟢 writing whole blocks only |
| what integration does | indexes the extent's records | 🟡 splits every block, appends the final form, indexes it, frees the extent |
| records move | 🟢 never: an offset stays valid for good | 🔴 every record in an extent moves once |
| a spill its slot names by offset | 🟢 stays valid | 🔴 left pointing at freed space; the slot names a spill by hash instead, resolved through the index, or integration rewrites the slot under the cell's lock when it moves the block |
| reusing an extent's space | nothing to reuse: extents keep what was written | only after a sync point and once no reader can hold an old offset, as the file's freed index pages are reused today |
| a token two writers introduce at once | 🟡 written by both; the index names one of them | written once, by integration |
| frames against their predecessor | against a predecessor already indexed, or the writer's own earlier node | against any predecessor, integration seeing them all |
| what the file holds at rest | the final form, with tokens a few writers wrote twice | the final form, compact |
| what exists to build on | 🟢 today's append, unchanged, into an extent | 🟡 today's append, run by the integrator; moving records and reusing extents are new |

## Decided: whole blocks in the extent (Irakli, 2026-11-11)

Writers append whole blocks; integration converts them into frames and
tokens, and the file comes out compact. The file is laid out as the
integrated region, then the extents; a mark records where the extents not
yet integrated begin, and integration works from there.

What keeps it from losing or corrupting data:

1. **A block frees its space, once its integration is durable.**
   Integration writes no block's compact form larger than the block: one
   that would not be smaller is kept whole, as the file already keeps whole
   a node whose frame does not pay. So integrating a block frees the space
   it took, or breaks even (Irakli). That space may be written over only
   after the integration that converted it reaches a sync point: until
   then, the old bytes are the only copy the durable index names, and a
   crash after overwriting them would reopen to an index naming other
   bytes. So the compact form goes into space already free and durable,
   never over a block still being converted. Index entries go to the
   index's own pages, reused after a sync point as today.
2. **A spill moved left:** the new copy written, synced, then the slot
   written under the cell's lock; the old place is free once no reader can
   hold its offset.
3. **Truncation at the last close only.** A reader with the file mapped
   that touches a page past a new, shorter end gets `SIGBUS`. Every process
   holds a shared lock on one byte while it has the space open; one that
   takes it exclusively at close, without waiting, is the last and may
   truncate past what is integrated (SQLite cleans its WAL up on the last
   connection's close). While the file is open, freed space is reused
   instead.

Decided: a reader holds a shared byte-range lock on each extent it reads
from, and an integrator frees an extent only once it can take that lock
exclusively without waiting; the operating system releases a dead reader's
locks.

## Where integration's output goes: SQLite's answer

Integration needs somewhere to write the compact form. Sliding it left
into a gap behind the extents works only if the gap is there when needed;
appending it at the end instead means records keep moving (Irakli).
SQLite avoids the question:

- its database file is fixed-size pages, each with a home: a checkpoint
  copies each committed page from the WAL to its page's home, so output
  always fits ([fileformat2.html](https://www.sqlite.org/fileformat2.html),
  [wal.html](https://www.sqlite.org/wal.html));
- freed pages go on a free list in the file, which later writes take from
  before the file grows ([fileformat2.html](https://www.sqlite.org/fileformat2.html), "the freelist");
- the file grows in chunks (`SQLITE_FCNTL_CHUNK_SIZE`), and shrinks only
  on request (`auto_vacuum`, `incremental_vacuum`,
  [pragma.html](https://www.sqlite.org/pragma.html#pragma_auto_vacuum));
- once a checkpoint has copied everything and no reader uses the WAL, the
  next writer starts again at its beginning (the WAL reset).

Here, by the same rules: extents are runs of pages, claimed from the free
list or from the file's end, the file growing in chunks; integration
writes its compact output into free pages, and gives an extent's pages
back to the free list once the integration that converted them is
durable. No gap has to be kept, and nothing slides. The file's index pages
are already pages reused after a sync point (format 10).

And readers find a block not yet integrated as SQLite's readers find a
page in the WAL: SQLite keeps a hash table from page to its newest WAL
frame (`-shm`), updated as frames are appended; here, a record per commit
maps its blocks' hashes to their offsets, and each reader keeps those it
has not seen integrated in memory.

Open, to evaluate: the page size, the chunk the file grows by, and the run
a writer claims at once.
