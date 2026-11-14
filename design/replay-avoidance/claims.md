# Claims

| claim | kind | backing | status |
|---|---|---|---|
| a commit that lost its branch's race replays its operation over the latest head, always, until a read set lets it skip | fact | design/file-writers, 2026-11-10 | holds |
| a read set of matched triples misses a fact newly asserted into a scanned range that would now match (a phantom) | fact | the phantom problem; `Pick::Last` in `rust/dialog-repository/src/repository/branch/transaction/induce.rs` | holds |
| recording each scan's range and pattern, diffing over the ranges and testing changed facts against the patterns is precise and catches phantoms | hypothesis | proposed 2026-11-10; confirmed by tests of a value change, a phantom and an unrelated change in range | open |
| replays are frequent or costly enough to be worth a read set | hypothesis | measured by a run of k writers on one branch counting replays and their time; refuted if replays are rare or cheap there | open |
