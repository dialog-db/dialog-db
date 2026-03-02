# Built-in Sync

Most databases treat synchronization as someone else's problem. You store data locally, and then build or adopt a separate system to move it between peers: WebSockets, CRDTs, operational transforms, REST polling. Dialog takes a different approach. Sync is part of the data model itself.

This chapter gives an overview of how Dialog's sync works. The implementation details are still evolving, so we'll focus on the architecture and the guarantees it provides rather than specific API calls.

## Content-addressed facts

Every fact in Dialog is content-addressed. Its identity is derived from its content: the attribute, entity, value, and causal reference. Two peers that independently assert the exact same fact will produce the same content address.

This property is what makes sync tractable. When two peers want to synchronize, the question reduces to: "which content addresses do you have that I don't, and vice versa?"

## Causal tracking

Each fact carries a `cause` field with three components:

- **origin**: identifies which peer asserted the fact (a DID)
- **period**: a coordinated counter that advances when peers sync
- **moment**: a local counter that advances between syncs

Together, these form a hybrid logical clock. The `period` gives you a rough global ordering (facts from after a sync have a higher period than facts from before). The `moment` gives you a local ordering within a single period of offline work.

This is similar in spirit to the logical timestamps used in systems like [Automerge](https://automerge.org/), where an actor ID plus a sequence number identify each change.

### What causal tracking enables

Causal references let Dialog answer questions like:

- "Was this fact asserted before or after that fact?" (partial ordering)
- "Were these two facts asserted concurrently?" (neither caused the other)
- "What did this peer know about when they made this assertion?" (causal context)

These questions matter for conflict resolution. When two peers concurrently set a cardinality-one attribute to different values, the causal references tell you that neither peer was aware of the other's change. The transactor can use this information to handle the situation appropriately rather than silently dropping one value.

## Prolly trees

Dialog stores its facts in a data structure called a [prolly tree](https://www.dolthub.com/blog/2024-03-08-prolly-trees/) (probabilistic B-tree). The key property of prolly trees is that two trees with the same content will have the same structure, regardless of the order in which items were inserted.

This structural determinism makes diffing efficient. To synchronize two peers, you compare their prolly tree roots. If the roots match, the trees are identical. If they differ, you walk down the tree, comparing nodes. Subtrees that match can be skipped entirely. The work is proportional to the *difference* between the two trees, not their total size.

This is the same idea behind content-addressed storage systems like git or IPFS, but applied at the level of individual facts rather than files or blocks.

## The sync protocol

At a high level, sync between two Dialog peers works like this:

1. **Compare roots**: Each peer shares the root hash of their prolly tree.
2. **Diff**: If the roots differ, the peers walk their trees together, identifying the ranges where they diverge.
3. **Exchange**: Each peer sends the facts that the other is missing.
4. **Merge**: Each peer incorporates the received facts into their local tree.

Because facts are immutable and content-addressed, there's no ambiguity about what to merge. A fact is either present or absent. The interesting part is what happens *after* merge, when the application's semantic layer interprets the combined facts.

## Conflict resolution

"Conflict" in Dialog means something specific: two peers concurrently asserted different values for a cardinality-one attribute. The facts themselves don't conflict. Both are valid facts that were asserted by their respective peers. The conflict arises at the semantic layer, when a concept expects at most one value.

Dialog's approach to conflict resolution is value-based. The transactor inspects the current set of claims and their causal references to decide what to do. The [design decision document](https://github.com/dialog-db/dialog-db/blob/main/notes/causal-information-design-decision.md) describes this in more detail, but the summary is:

- Default queries return plain domain types without provenance overhead. Most application code never needs to think about causality.
- The transactor handles cardinality enforcement using causal references internally.
- If an application needs explicit provenance (for compare-and-swap operations, for example), that can be made available as an opt-in query mode.

This is an active area of development. The goal is to make the common case simple (just read and write data, sync happens) while providing escape hatches for applications that need fine-grained control.

## What this means for your application

From the perspective of application code, sync is largely transparent. You open a session, read and write data, and Dialog handles replication in the background. A few things to keep in mind:

- **Design for convergence**: Since multiple peers can write concurrently, your data model should be resilient to that. Cardinality-many attributes naturally converge (all values are kept). Cardinality-one attributes need the transactor to resolve concurrent writes.

- **Facts, not state**: Dialog syncs facts, not snapshots. Two peers don't need to agree on the "current state" of the entire database. They exchange facts and each peer derives its own view.

- **Offline by default**: Since sync is based on exchanging content-addressed facts, there's no requirement for peers to be connected at the same time. A peer can work offline indefinitely and sync when a connection becomes available.

In the [Beyond Rust](./dialog-web.md) chapter, we'll look at how this plays out in practice for web applications running Dialog in a service worker.
