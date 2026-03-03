# Built-in Sync

In Dialog, sync is part of the data model itself — not a separate system bolted on top. This chapter covers the architecture and guarantees.

## Content-addressed claims

Every claim in Dialog is content-addressed. Its identity is derived from its content: the attribute, entity, value, and causal reference. Two peers that independently assert the exact same claim will produce the same content address.

This property is what makes sync tractable. When two peers want to synchronize, the question reduces to: "which content addresses do you have that I don't, and vice versa?"

## Causal tracking

Each claim carries a `cause` field with three components:

- **origin**: identifies which peer asserted the claim (a DID)
- **period**: a coordinated counter that advances when peers sync
- **moment**: a local counter that advances between syncs

Together, these form a hybrid logical clock. The `period` gives you a rough global ordering (claims from after a sync have a higher period than claims from before). The `moment` gives you a local ordering within a single period of offline work.

This is similar to [Automerge](https://automerge.org/)'s actor ID + sequence number approach.

Causal references answer: was this claim before or after that one? Were they concurrent? What did this peer know? This matters for conflict resolution — when two peers concurrently set a cardinality-one attribute to different values, the causal references reveal that neither was aware of the other's change.

## Prolly trees

Dialog stores its claims in a data structure called a [prolly tree](https://www.dolthub.com/blog/2024-03-08-prolly-trees/) (probabilistic B-tree). The key property of prolly trees is that two trees with the same content will have the same structure, regardless of the order in which items were inserted.

This structural determinism makes diffing efficient. To synchronize two peers, you compare their prolly tree roots. If the roots match, the trees are identical. If they differ, you walk down the tree, comparing nodes. Subtrees that match can be skipped entirely. The work is proportional to the *difference* between the two trees, not their total size.

This is the same idea behind content-addressed storage systems like git or IPFS, but applied at the level of individual claims rather than files or blocks.

## The sync protocol

At a high level, sync between two Dialog peers works like this:

1. **Compare roots**: Each peer shares the root hash of their prolly tree.
2. **Diff**: If the roots differ, the peers walk their trees together, identifying the ranges where they diverge.
3. **Exchange**: Each peer sends the claims that the other is missing.
4. **Merge**: Each peer incorporates the received claims into their local tree.

Because claims are immutable and content-addressed, there's no ambiguity about what to merge. A claim is either present or absent. The interesting part is what happens *after* merge, when the application's semantic layer interprets the combined claims.

## Conflict resolution

"Conflict" means two peers concurrently asserted different values for a cardinality-one attribute. The claims themselves are both valid — the conflict arises at the semantic layer.

Query through cardinality one: Dialog resolves via last-writer-wins. Query through cardinality many: you get all concurrent values. The application chooses which view it needs.

See the [design decision document](https://github.com/dialog-db/dialog-db/blob/main/notes/causal-information-design-decision.md) for details.

## What this means for your application

Sync is largely transparent. A few things to keep in mind:

- **Design for convergence**: Cardinality-many attributes naturally converge. Cardinality-one uses last-writer-wins or can be queried through cardinality-many for full visibility.
- **Claims, not state**: Dialog syncs claims, not snapshots. Peers exchange claims and each derives its own view.
- **Offline by default**: Peers can work offline indefinitely and sync when connected.
