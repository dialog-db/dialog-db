# Prior art: many writers, one store

How other stores let several writers proceed, and what they serialize
instead. From each system's own documentation, as linked; not measured here.

## One writer at a time

Most single-file stores take one writer at a time and make readers free.

- **SQLite, WAL mode** ([wal.html](https://www.sqlite.org/wal.html)): one
  writer at a time, on a lock; readers never block. Commits append to a
  second file, the WAL, and a checkpoint copies them into the database file
  later; a reader looks in both. The experimental `BEGIN CONCURRENT`
  ([begin_concurrent.md](https://www.sqlite.org/src/doc/begin-concurrent/doc/begin_concurrent.md))
  lets transactions run at once and serializes only their commits,
  refusing one whose pages another changed.
- **LMDB, BoltDB, Bitcask:** one writer, under a lock; readers see a
  snapshot.

The file archive is this design today.

## Data lands apart, only publishing is serialized

The stores that let writers run at once write their data somewhere no
other writer writes, and serialize, or compare-and-swap, only the small
step that makes it visible.

- **Git** ([gitrepository-layout](https://git-scm.com/docs/gitrepository-layout),
  [git-repack](https://git-scm.com/docs/git-repack)): a writer adds each
  object as a loose file named by its hash, with no lock (two writers of
  one object write the same bytes). `git gc` packs loose objects into a
  packfile later, in the background, and deletes them; a reader looks in
  the packs and among the loose objects. A ref is updated under a lock file
  of its own, `refs/heads/x.lock`, so two branches never wait on each
  other. This is main's `fs` archive plus the packing it lacks, and the
  proposal in this topic.
- **Git's reftable** ([reftable](https://git-scm.com/docs/reftable)): refs
  kept as a stack of immutable tables; a writer adds a table and swaps the
  stack's list under a short lock; compaction merges tables later. Refs in
  few files without one file per ref.
- **Delta Lake, Iceberg** ([Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)):
  writers write data files independently; a commit is one atomic put of the
  next log entry, and a writer that loses the race re-checks for conflicts
  and retries.
- **Jujutsu** ([concurrency](https://jj-vcs.github.io/jj/latest/technical/concurrency/)):
  each operation writes its own object and adds itself as an operation
  head, with no lock; concurrent operations leave several heads, which the
  next reader merges.

## One stream, written at once

- **PostgreSQL** (`src/backend/access/transam/xlog.c`, "WAL insertion
  locks"): backends reserve space in the one WAL stream by advancing a
  shared position, then copy their records in at the same time; only the
  reservation is serialized, and flushes are grouped.
- **RocksDB** ([WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log)):
  one process owns the database; concurrent writers in it join a group,
  and its leader writes the whole group to the log at once.

These keep one file but need memory shared by every writer: one process,
or shared memory across processes.

## What carries over

1. **A single file serializes its writers.** Every design that does not
   gives each writer a place of its own to land its data.
2. **Only publication is coordinated, and by the unit that conflicts:**
   git's lock is per ref, Delta's commit is per table. Writers to different
   branches never meet; writers to one branch meet only at the head.
3. **Integration is deferred and readers look in both:** SQLite's
   checkpoint, git's repack, compaction in LSM stores.
4. **The landing place is per writer, not per branch:** git's loose
   objects, Delta's data files and Jujutsu's operations belong to whoever
   wrote them. A per-branch landing file would still make two writers on
   one branch take turns on it.
