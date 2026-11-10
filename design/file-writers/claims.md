# Claims

| claim | kind | backing | status |
|---|---|---|---|
| every writer to a space's file takes its turn on one exclusive file lock, whatever branch it writes | fact | `rust/dialog-archive-file/src/archive.rs` (`Exclusive::take` in the append path); research/single-file-format/journal.md, 2026-11-05, "writers take turns" | holds |
| under `fs`, each memory cell is a file of its own, published compare-and-swap under a lock file of its own, by a temporary file renamed into place | fact | `rust/dialog-storage/src/storage/provider/fs/memory.rs` | holds |
| the single-file design keeps branch heads and credentials outside the file, and lists one integrating process as an option, not started | fact | research/single-file-format/design.md, "Not in this design" | holds |
| no measurement so far has two branches of one replica writing at once: in the bench each member is a replica with a file of its own | fact | `rust/dialog-baseline/examples/tonk_bench.rs`; research/single-file-format/journal.md | holds |
| a file run writes 23,144 `flock` calls (0.75 s of a 53 s run) and 472 `fdatasync` (0.44 s), none of them waiting on another writer | fact | research/single-file-format/journal.md, 2026-11-10, "the push behind main is this disk's" | holds |
| on main, two processes advancing two branches share no lock: each block is a content-addressed file written atomically and skipped if present, each head a cell of its own | fact | `rust/dialog-storage/src/storage/provider/fs/archive.rs` (`Put`, `Import`); `fs/memory.rs` | holds |
| with the file archive, two processes advancing two branches of one space take turns on the file's writer lock | fact | `rust/dialog-archive-file/src/archive.rs` (`Exclusive::take` in the append path) | holds |
| branches of one space writing at once wait on each other's turn enough to matter | hypothesis | confirmed if a commit's latency with k writers on k branches of one file grows with k at the k a session really has; refuted if it stays flat there. Not measured | superseded: the requirement forbids the wait whatever it costs |
| branches contending on one file does not feel right | opinion | Irakli, 2026-11-10 | - |
| processes on separate branches must not wait on one another to write; serializing them disqualifies the file archive from those uses | opinion | Irakli, 2026-11-10: a requirement, and it decides the topic's scope | - |
| branch heads, or memory cells generally, belong in the space's file | opinion | Irakli, 2026-11-10 (what it would buy is not yet said) | - |
| the single-file stores surveyed (SQLite WAL, LMDB, BoltDB, Bitcask) take one writer at a time | fact | [prior-art.md](prior-art.md), each store's documentation | holds |
| the stores surveyed that let writers run at once (git, Delta Lake, Iceberg, Jujutsu) land each writer's data where no other writer writes, and coordinate only publication, per ref or per table | fact | [prior-art.md](prior-art.md) | holds |
| git's object store is main's `fs` archive plus background packing: loose objects written without a lock, packed later, read from both | fact | [prior-art.md](prior-art.md); gitrepository-layout, git-repack | holds |
| a landing file per writer (process or session) is what the surveyed designs use; one per branch would make same-branch writers take turns on it | hypothesis | follows from the survey; refuted if a surveyed store lands data per branch without serializing that branch's writers | open |

