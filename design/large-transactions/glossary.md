# Glossary: bringing a large dataset into a replica

| term | meaning |
|---|---|
| dataset | facts that exist before the replica does: records from another system, or a replica's current state |
| large | many times the ops a node's buffer holds (256 ops or 64 KiB): a commit whose ops reach the leaves of much of the tree |
| node version | a node as one commit persisted it; a node rewritten by 37 commits has 37 versions |
| seal | the end of a commit: the buffered tree's pending ops applied and its nodes persisted |
| canonical edit | one insert or delete applied to the tree with its shape restored after it, as `TransientTree::insert` does |
| plant | the tree's one-pass, bottom-up build of a tree from sorted entries, used when a commit lands in an empty tree |
