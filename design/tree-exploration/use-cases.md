# A consumer that can only query cannot draw a branch's search tree: use cases

Problem: a consumer that can only query cannot draw a branch's search
tree in a number of reads that does not grow with the tree, so the one
embedder that shows it reads blocks through machinery of its own (the
[top story](../tree-exploration.md)).

Drafted by the agent from what tonk's inspector shows and what the
maintainer has said; not yet read by the maintainer. `How` is filled in
the Design phase.

| # | What the consumer could accomplish | How |
|---|---|---|
| 1 | Start from a branch and reach its tree's root with nothing else given | |
| 2 | See what a node is: index or segment, its size, how many children or entries, how much is buffered on it, the format it was written under | |
| 3 | See a node's children in order, each with what the parent holds about it: where its key range starts and ends, the rank of its boundary, what is buffered for it | |
| 4 | See for each child whether its block is local, without that fetching it | |
| 5 | Have a block that is not local fetched, and see the tree change to show it | |
| 6 | See a segment's entries in order, each with its key in readable parts, the fact the key encodes, its version and cause, and the blob record that belongs to it | |
| 7 | Do each of the above for one node in a number of block reads that does not depend on the size of the tree | |
| 8 | Stay current: when the branch's head moves, what is shown follows without being asked again | |
| 9 | Do all of it as a component stored on the branch, as a command line, or as an agent, with no code in the embedder written for the tree | |
