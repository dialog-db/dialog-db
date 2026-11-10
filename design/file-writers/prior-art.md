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

## Dolt: branches over a content-addressed tree

The closest product to dialog: SQL tables as probabilistic search trees
over content-addressed chunks, with branches and merges. Sources:
DoltHub's posts on the [chunk journal](https://dolthub.com/blog/2023-03-08-dolt-chunk-journal),
[ACID transactions](https://www.dolthub.com/blog/2023-01-04-acid-transactions/),
[conjoin](https://www.dolthub.com/blog/2025-07-23-conjoin-means-bugs),
[journal repair](https://www.dolthub.com/blog/2025-12-10-journal-data-loss-detection),
[concurrency](https://www.dolthub.com/blog/2026-02-17-dolt-concurrency) and
[CLI against a server](https://www.dolthub.com/blog/2023-06-12-cli-to-sql/).

- **One journal file a database.** Chunks and root records are appended to
  one file, `.dolt/noms/vvvv…`, fsynced at each transaction commit. A
  manifest names the root and the immutable table files; since the journal
  it is rarely written. Conjoin merges table files and their indexes into
  one, in the background, because a read has to look in each.
- **One writing process.** A `dolt` command run while a `dolt sql-server`
  is up is told the database is locked to writes; embedded use is
  single-writer, by a file lock (the second from third-party
  documentation, [beads](https://beads.gascity.com/architecture/dolt)).
- **Many sessions inside that process.** Concurrent writers are sessions
  of the one server. Writes to the commit graph (commits, branch creation
  and deletion, merges) take a lock that was a global one and is now one
  per branch; merge work is done outside it. A SQL transaction that
  conflicts with its branch's head on commit is rolled back and retried.
- **Not stated** in what was read: whether branch heads live inside the
  root value, and how the root is updated when two branches commit at once.

So Dolt does not let separate processes write one database: it puts every
writer in one process and serializes per branch inside it.

## What carries over

1. **A single file serializes its writers.** Every design that does not
   gives each writer a place of its own to land its data.
2. **Only publication is coordinated, and by the unit that conflicts:**
   git's lock is per ref, Delta's commit is per table. Writers to different
   branches never meet; writers to one branch meet only at the head.
3. **Integration is deferred and readers look in both:** SQLite's
   checkpoint, git's repack, compaction in LSM stores.
4. **Or one process owns the file.** Dolt answers concurrent writers with
   a server that every writer talks to, not with writers of their own:
   the file keeps one writer, and branches meet only on a lock per branch
   inside it.
5. **The landing place is per writer, not per branch:** git's loose
   objects, Delta's data files and Jujutsu's operations belong to whoever
   wrote them. A per-branch landing file would still make two writers on
   one branch take turns on it.
