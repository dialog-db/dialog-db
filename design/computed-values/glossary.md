# A value computed from many writers' claims either loses writes or keeps every one: glossary

Terms this topic needs. Dialog's own terms (claim, cell, pick, standing,
selective and computing aggregators) are as
[`notes/glossary.md`](../../notes/glossary.md) and
[`notes/aggregation.md`](../../notes/aggregation.md) define them.

| term | meaning |
|---|---|
| writer | whatever writes one claim after another with no concurrency of its own: a process on one branch, a replica's main when its writes take turns. Two processes on two branches of one replica are two writers |
| contribution | what one write adds to a computed value: one increment, one reading, one member added |
| owner | the identity a cell is keyed by, so that one writer, and only it, writes that cell. Whether an owner is a replica, a branch, an agent or a member is open |
| owned cell | a cell keyed by an entity, an attribute and an owner, written by its owner alone, under cardinality one |
| fold on read | a computing aggregator applied, when the value is read, over the owned cells of one entity and attribute; "mine" reads the reader's own cell and folds nothing |
| computed value | a value that is none of the claims it is computed from: a count, a sum, an average; as `notes/aggregation.md` uses "computing" |
