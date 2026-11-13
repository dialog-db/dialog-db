# What the order of matches may decide

Decision: what a rule writes when its matches come in another rule's order. For: a commit pays for a condition once for every rule that states it.

A rule's matches come in the order its premises are seeded and planned, which follows the order the rule lists them. A stored rule lists its premises sorted by their encoding under names its head takes part in choosing (`spell` in `rust/dialog-query/src/rule/canonical.rs`). So two rules of one body list it in two orders, and one rule's matches are the other's in another order.

A head's writes land in its cells, and a cell keeps its writes in the order they came (`Changes` in `rust/dialog-corpus/src/model/update.rs`). The order of two heads shows only where both write one cell, and they do not write it the same.

```
 two matches of one rule, one cell

   item:1  tag ◀ "red"      under last:  the later stands
   item:1  tag ◀ "green"    under all:   both are kept, in the order written
   item:1  by  ◀ user:0     the same value twice: the same either way
   item:1  by  ◀ user:0
```

| criterion \ approach | Share only rules that list the body in one order | Sort the matches before writing | Write from the shared matches unless the order decides, then evaluate the rule alone |
|---|---|---|---|
| what is written when the order decides | 🔴 the same only if the planner and every scan give one order under two namings, which nothing states or tests | 🔴 another order than the rule's own: `last` would elect another value than before | what the rule writes alone, since it is evaluated alone |
| which rules share | 🔴 none of those the notation writes, whose premises are sorted under each rule's own names | all | all; a rule whose matches write one cell two ways is evaluated a second time that round |
| what it costs | a comparison in order | a sort of every rule's matches | a pass over the heads of a rule before they are written, one table entry a cell |

Chosen: the last column. The agent's choice, not yet the maintainer's.

It rests on one hypothesis (claim 6): two rules of one body find the same set of matches, whatever order each is planned in. The differential check is what tests it.

The guard is not there for a case nobody has seen. With it turned off, a test in which eight rules state one body and list it in both orders fails the differential check: a rule that read the first rule's matches would have reported another command than it reports alone (claim 6a).
