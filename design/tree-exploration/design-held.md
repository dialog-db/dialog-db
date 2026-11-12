# How a query learns whether a block is held

Decision: how a consumer that can only query learns whether a node's
block is held by the replica, without fetching it, for: the problem in
the [top story](../tree-exploration.md), under the maintainer's
constraint that a tree read stays idempotent (see the
[glossary](glossary.md)).

Not decided. The fourth column was born from the contrast and is the
agent's; the fifth is the maintainer's simplification of it, which drops
the hook in the hydration path and with it two of the fourth's `?`
cells. One row was added for it, at the bottom: where a handler lives.

| criterion \ approach | Status quo: tonk's reader probes the local archive while answering | A resolver that takes a time (the maintainer's) | A scan that probes the archive when the query runs (the agent's earlier) | Recorded answers: a command probes, the answer is a fact in the branch's ephemeral line | Two commands whose handlers assert (the maintainer's): one asks whether a block is local and asserts the answer; one fetches a block and asserts it is local |
|---|---|---|---|---|---|
| the same query over the same inputs answers the same | 🔴 the answer changes when a block arrives and nothing the query names changed | 🟡 holds only if the archive can say when each block arrived; it keeps no such record today | 🔴 two reads of one node inside one evaluation can disagree if the block arrives between them | 🟢 the query reads a fact; the line it reads is pinned for the evaluation, and its revision is already part of what a query's caches are keyed by | 🟢 as the column to the left: the query reads a fact |
| asked again as of an earlier moment | 🔴 answers for now | 🟡 needs the arrival record above | 🔴 answers for now | the fact as it stood, for as long as the line's ring of 1024 instants reaches; past that a reader recomputes at the present, as it does for every session fact | as the column to the left |
| a subscriber is told when a block arrives | 🔴 no; tonk's reads are one-shot | 🟡 something must change the time and something must read it into every rule | 🔴 nothing in a poll looks at the archive: a poll re-runs when the head or the ephemeral line moved | 🟢 the arrival replaces the fact, which mints an instant; a poll already reads the instants since its pin and maintains per touched entity | told when the fetch command's handler asserts; 🟡 a block that lands another way (a tree read that fetched it, a pull) leaves "not local" standing until something asks again |
| a block is fetched without being asked for | does not fetch: reads with `Load::locally` | does not fetch | does not fetch | does not fetch: the probe is a local read, and the fetch is a second, separate command | does not fetch: only the fetch command does |
| what a consumer does to see a node's children as held or not | one request | reads the time, passes it to the resolver | one query | issues the probe for the children, then queries; a node never probed has no fact, which reads as not known | as the column to the left |
| what has to be built | exists, in the embedder | a clock fact, commands that move it, an arrival record per block, the resolver | a new kind of scan, and a way for a poll to notice the archive changed | a probe command; a hook where a hydrated block is written back; one reserved attribute | two commands and their handlers; nothing in the hydration path |
| where the fact lives | - | - | - | ? the ephemeral line is per branch and a block belongs to a space's archive, shared by its branches; which lines a hydration writes to is not worked out | 🟢 a command is issued against a branch, so its handler writes to that branch's line |
| a replica drops a block | - | ? | ? | ? no effect that removes a block from an archive was found (`Get`, `Put`, `Import`, `Load`); if one exists it needs the same hook | the fact stands until something asks again |
| where the probing and fetching code lives | 🔴 in the embedder, woven into its worker | in dialog | in dialog | ? a command and its handler are the embedder's mechanism in tonk (`impl Provider<C> for CommandEnv`); the hook is dialog's | ? the handlers are the embedder's unless dialog offers the two operations for a handler to call; every embedding writing its own is what the problem statement names |
