# Claims

| claim | kind | backing | status |
|---|---|---|---|
| A write under an electing pick retracts the one claim the pick elects; every other claim of the cell stays. | fact | `Instruction::Assert` in `rust/dialog-artifacts/src/artifacts/instruction.rs`; `Update::associate` in `artifacts/update.rs`; `policy.elect(...)` in the write path of `rust/dialog-artifacts/src/tree.rs` | holds at `3b5835ad` |
| A test pins that behavior. | fact | `it_succeeds_the_claim_the_succession_elects` in `rust/dialog-artifacts/src/history/tests.rs`: 100 and 200 held, a `max` write of 150 leaves `[100, 150]` | holds |
| Reads elect by standing and value, and nothing checks whether one claim's cause names another. | fact | `ArtifactView::elect` in `artifacts/artifact.rs`; `Election::beats` in `rust/dialog-query/src/concept/query.rs` | holds at `3b5835ad` |
| The read's comment assumes two live claims in one cell are concurrent. | fact | doc comment on `ArtifactView::elect` | holds |
| Under `last`, a claim that succeeds another always wins the read. | fact | a successor has a deeper edition, and `elect` compares edition first; `notes/version-control.md` | holds |
| Every election is deterministic: ties fall to standing, then the hash of the value. | fact | `Pick::prefers`, `value_beats` in `artifacts/update.rs` (#752) | holds |
| A cell is keyed by relation and entity, not by pick, on purpose: the pick is the reader's refinement, and one peer may write a cell under `all` while another reads it under `max`. | opinion | Irakli, 2026-11-08 | decides |
| Retiring must not happen on merge, because replication is partial: a replica can take part of a tree, and it cannot know what it has not received. | opinion | Irakli, 2026-11-08 | decides |
| A write should retire the claims concurrent with the elected claim, not claims written before it. | opinion | Irakli, 2026-11-08 | decides |
| Partial sync can bring a replica a claim after the claim that succeeded it, with three replicas or more; with two it cannot. | hypothesis | Irakli's scenario, 2026-11-08: A writes, B writes having seen it, A writes again succeeding B's; C syncs with A, then with B, and receives B's write after A's. A test of that sequence is still to write. | confirmed by the scenario; test open |
| `history::causality` judges "seen" by claim lineage, so an `all` value from another origin counts as `Concurrent` with a claim whose cause chain does not reach it, even when the writer held that value. | fact | `causality` in `rust/dialog-artifacts/src/history/causality.rs`: tier 0, tier 1 cause check, tier 2 cause traversal | holds |
| Revision causality, from revision parents, tells "written before" from "concurrent" where claim lineage cannot. | hypothesis | `History::revision_record` gives parents and `common_ancestor` walks them. Confirmed by a test: an `all` value from another origin, held when the max was written, is "before" by revision and "concurrent" by lineage. | open |
| A partial replica may not hold the revision records a causality check needs. | hypothesis | `causality` already returns `IncompleteHistory` when a claim it traverses is missing. Confirmed if a partial replica in a sync test hits it while writing. | open |
| Checking at read whether one live claim's cause names another costs little, since causes are read off the rows. | hypothesis | would be measured as read latency on a cell with two to four live claims, before and after the filter; research topic if it is not obviously small | open |
| Whatever a write does when it cannot decide concurrency must converge. | opinion | Irakli, 2026-11-08 | decides |
| A write should be able to decide while offline. | opinion | Irakli, 2026-11-08 | decides |
| A write makes a decision durable only through the cause it records; retiring a claim from its own index changes nothing on other replicas. | fact | the read filter in column D reads causes; the operational index is local | holds if the read filter lands |
| Writing a value as concurrent with an undecided claim converges: every replica elects between the two by the pick's order. | hypothesis | confirmed by applying the same records in every order on three replicas and comparing reads; refuted if any order reads differently | open |
| Fetching revision records on the write path costs little. | hypothesis | a measurement of fetch latency and size per missing revision; a research topic if it decides between D and E | open |
