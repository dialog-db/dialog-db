# Claims: importing a large dataset into dialog

The measurements were made on the single-file archive of
[dialog-db/dialog-db#733](https://github.com/dialog-db/dialog-db/pull/733)
(format 10), with its `file_archive_bench` replaying the MusicBrainz fact
log (`DIALOG_BENCH_GROUP=k` commits every `k` of its 2,000 transactions as
one); the numbers and their method are in that branch's
`research/single-file-format/journal.md`.

| claim | kind | backing | status |
|---|---|---|---|
| The MusicBrainz sample is 100,000 entities: 1.8 GiB of JSON, 4.47 M facts, 488 MiB as a fact log | fact | the fetch and conversion scripts in #733's `research/single-file-format/bench/` | holds |
| As 2,000 commits (about 2,234 facts each) the replay wrote 2,883,182 node versions, about 1,440 a commit, a 1,003 MiB file, in 6,562 to 7,808 s of commits | fact | `breakdown` and the bench's timers, 2026-11-08 | holds |
| The tree such a replay leaves is 77,808 nodes in three levels: a root of 169 children, 169 index nodes of about 459, 77,638 leaves of about 224 entries; written whole with the tokens it names, about 250 MiB | fact | `breakdown::live` on the replay's file, 2026-11-08 | holds |
| As 100 commits (about 44,700 facts each): 1,013,154 node versions, 2,616 to 2,791 s of commits, 530 MiB, the commits holding up to 4.9 GiB of heap | fact | the bench with `DIALOG_BENCH_GROUP=20`, two runs, 2026-11-08 | holds |
| As 10 commits (about 447,000 facts each): the first, into an empty tree, finished; the second ran for over two hours at full CPU on one thread, holding 6.8 GB, and was stopped | fact | the bench with `DIALOG_BENCH_GROUP=200`, 2026-11-08 | holds |
| As one commit: killed by the kernel after 116 s at 13.6 GiB of anonymous memory on a 15 GiB machine; the bench holds about 5.7 GiB itself, so the commit held over 8 GiB, still growing | fact | the bench with `DIALOG_BENCH_GROUP=2000`, 2026-11-08 | holds; how much a commit streamed through `branch.import` holds is not measured |
| A node's buffer flushes past 256 ops (the manifest's branch factor) or 64 KiB of op weight (its `max_segment`) | fact | `op_buf_size`, `op_buf_bytes` in `rust/dialog-search-tree/src/hitchhiker.rs` | holds |
| A flush lifts the child it descends into to a transient node once per commit, edits it in place, and encodes it once, at persist; children nothing descends into pass through without a decode | fact | `enqueue`, `lift_child` in `rust/dialog-search-tree/src/hitchhiker.rs` | holds |
| A commit applies its whole instruction stream to the buffered tree before persisting anything | fact | `rust/dialog-repository/src/repository/branch/commit.rs`, the comment before `ArchiveDelta::zero()` | holds |
| Sealing a commit applies the ops its buffers sent toward leaves one canonical edit at a time (`replay_ops`: an insert or delete each, descending, editing the leaf, re-summarizing its piece and reshaping the path), except into an empty tree, which is built bottom-up in one pass (`plant`) | fact | `replay_ops` in `rust/dialog-search-tree/src/hitchhiker.rs` | holds |
| In the 10-commit run's second commit, five stack samples were all in `BufferedBatch::seal` → `HitchhikerTree::settle` → `replay_ops` → `TransientTree::insert` → `Edit::apply` (`PieceSummary::build`, `weight_paced_cut`, `reshape_path`) | fact | gdb, 2026-11-08 | holds |
| Over a 2,000-commit replay's 6,562 s of commits, the archive took about 4,125 s (appending 2,975, of which splitting nodes into rows 2,507; rebuilding blocks for reads 1,150, which a store serving nodes in place skips) and the tree's seal and the repository about 2,437 s | fact | the bench's archive timers, 2026-11-08 | holds |
| `branch.import` makes an artifact stream one commit; `dialog-csv`'s `CsvImporter` is such a stream | fact | `rust/dialog-repository/src/repository/branch/import.rs`, `rust/dialog-csv/src/importer.rs` | holds |
| A canonicalized commit gives the history-independent tree, whatever the order its facts arrived in | fact | the doc of `Commit::canonicalize`, `rust/dialog-repository/src/repository/branch/commit.rs` | holds |
| One canonical edit costs about the size of the piece it re-summarizes, so a commit whose ops land many to a leaf costs about the square of them per leaf | hypothesis | confirmed if the time to replay n ops into one leaf's range grows as n², against linear for the same ops planted; refuted if it grows linearly | open |
| With buffers about as large as a node's fan-out (256 against about 459 children a level below the root), a flush moves one or two ops into each child it rewrites | hypothesis | ops moved per child rewrite, counted over the replay; the `FlushTrigger::Capacity` doc says the same of scattered keys | open |
| A tool that writes a replica's current state into a new file writes a whole tree at once, so it is a large transaction | hypothesis | confirmed if such a tool goes through the commit path; refuted if it copies nodes the heads reach without committing them | open |
| No one is known to write transactions this large today | opinion | Irakli, 2026-11-08 | - |
| Importing a large dataset into dialog is a problem to design a solution for, even though no one does it now | opinion | Irakli, 2026-11-08 | - |
| The replay is a pathological case for the tree implementation | opinion | Irakli, 2026-11-08 | - |
| A tool offering just a replica's state, without its history, would be useful | opinion | Irakli, 2026-11-08 | - |
| Writing a transaction's nodes to a side file and copying the ones still reachable at commit would handle large transactions | opinion | Irakli, 2026-11-08 | - |
| Building each ordering bottom-up from sorted runs would write only finished nodes, never lifting one twice | opinion | the agent, 2026-11-08; the tree does this already for a commit into an empty tree (`plant`) | - |
| Sealing could apply each leaf's ops as one sorted batch, cutting boundaries once, as `plant` does for an empty tree | opinion | the agent, 2026-11-08 | - |
