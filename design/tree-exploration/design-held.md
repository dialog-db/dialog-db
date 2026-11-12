# How a query learns whether a block is held

Decision: how a consumer that can only query learns whether a node's
block is held by the replica, without fetching it, for: the problem in
the [top story](../tree-exploration.md), under the maintainer's
constraint that a tree read stays idempotent (see the
[glossary](glossary.md)).

Not decided. The last column was born from the contrast and is the
agent's proposal; its `?` cells are what has to be checked before it can
be chosen.

| criterion \ approach | Status quo: tonk's reader probes the local archive while answering | A resolver that takes a time (the maintainer's) | A scan that probes the archive when the query runs (the agent's earlier) | Recorded answers: a command probes, the answer is a fact in the branch's ephemeral line |
|---|---|---|---|---|
| the same query over the same inputs answers the same | 🔴 the answer changes when a block arrives and nothing the query names changed | 🟡 holds only if the archive can say when each block arrived; it keeps no such record today | 🔴 two reads of one node inside one evaluation can disagree if the block arrives between them | 🟢 the query reads a fact; the line it reads is pinned for the evaluation, and its revision is already part of what a query's caches are keyed by |
| asked again as of an earlier moment | 🔴 answers for now | 🟡 needs the arrival record above | 🔴 answers for now | the fact as it stood, for as long as the line's ring of 1024 instants reaches; past that a reader recomputes at the present, as it does for every session fact |
| a subscriber is told when a block arrives | 🔴 no; tonk's reads are one-shot | 🟡 something must change the time and something must read it into every rule | 🔴 nothing in a poll looks at the archive: a poll re-runs when the head or the ephemeral line moved | 🟢 the arrival replaces the fact, which mints an instant; a poll already reads the instants since its pin and maintains per touched entity |
| a block is fetched without being asked for | does not fetch: reads with `Load::locally` | does not fetch | does not fetch | does not fetch: the probe is a local read, and the fetch is a second, separate command |
| what a consumer does to see a node's children as held or not | one request | reads the time, passes it to the resolver | one query | issues the probe for the children, then queries; a node never probed has no fact, which reads as not known |
| what has to be built | exists, in the embedder | a clock fact, commands that move it, an arrival record per block, the resolver | a new kind of scan, and a way for a poll to notice the archive changed | a probe command; a hook where a hydrated block is written back; one reserved attribute |
| where the fact lives | - | - | - | ? the ephemeral line is per branch and a block belongs to a space's archive, shared by its branches; which lines a hydration writes to is not worked out |
| a replica drops a block | - | ? | ? | ? no effect that removes a block from an archive was found (`Get`, `Put`, `Import`, `Load`); if one exists it needs the same hook |
