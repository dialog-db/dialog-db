# Claims: large transactions

| claim | kind | backing | status |
|---|---|---|---|
| The MusicBrainz replay (2,000 commits, 4.47 M facts) wrote 2,883,182 node versions, about 1,440 a commit | fact | `breakdown` of the replay's file, research/single-file-format journal, 2026-11-07 and 2026-11-08 | holds |
| The tree such a replay leaves is about 53,000 nodes (53,201 at ~1,250 commits) | fact | `live_breakdown`, research/single-file-format journal, "MusicBrainz against its source" | holds for that point of the replay |
| A node's buffer flushes past 256 ops (the manifest's branch factor) or 64 KiB of op weight (its `max_segment`) | fact | `op_buf_size`, `op_buf_bytes` in `rust/dialog-search-tree/src/hitchhiker.rs` | holds |
| A flush lifts the child it descends into to a transient node once per commit, edits it in place, and encodes it once, at persist; children nothing descends into pass through without a decode | fact | `enqueue`, `lift_child` in `rust/dialog-search-tree/src/hitchhiker.rs` | holds |
| A commit applies its whole instruction stream to the buffered tree before persisting anything | fact | `rust/dialog-repository/src/repository/branch/commit.rs`, the comment before `ArchiveDelta::zero()` | holds |
| `branch.import` makes an artifact stream one commit; `dialog-csv`'s `CsvImporter` is such a stream | fact | `rust/dialog-repository/src/repository/branch/import.rs`, `rust/dialog-csv/src/importer.rs` | holds |
| A canonicalized commit gives the history-independent tree, whatever the order its facts arrived in | fact | the doc of `Commit::canonicalize`, `rust/dialog-repository/src/repository/branch/commit.rs` | holds |
| A commit that fits in the root's buffer persists a handful of nodes; one larger than the buffers persists about one node per distinct leaf its ops reach | hypothesis | confirmed if nodes persisted per commit, on one base tree, stay near the tree's depth for commits of 10 and 100 facts and grow with the leaves touched for 1,000 and 10,000 | open |
| The same facts in fewer commits write fewer node versions and take less time; one commit writes about the final tree once | hypothesis | the MusicBrainz facts as 1, 10, 100 and 2,000 commits: node versions written, time, file size | open |
| A one-commit import of the MusicBrainz facts holds gigabytes at its peak (the final tree as blocks is about 53,000 × 71 KB, 3.7 GB) | hypothesis | peak anonymous memory of `branch.import(CsvImporter)` over those facts | open |
| With buffers about as large as a node's fan-out (256 against ~185 children a level below the root), a flush moves one or two ops into each child it rewrites | hypothesis | ops moved per child rewrite, counted over the replay; the `FlushTrigger::Capacity` doc says the same of scattered keys | open |
| The replay is a pathological case for the tree implementation | opinion | Irakli, 2026-11-08 | - |
| Writing a transaction's nodes to a side file and copying the ones still reachable at commit would handle large transactions | opinion | Irakli, 2026-11-08 | - |
| Building each ordering bottom-up from sorted runs would write only finished nodes, never lifting one twice | opinion | the agent, 2026-11-08 | - |
