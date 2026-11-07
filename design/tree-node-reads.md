# The tree reads nodes beside the capability system

Phase: **Describing.** See the [design index](README.md) for what the
phases mean, and the [journal](tree-node-reads/journal.md) for where the
topic stands.

## Description

A tree operation in `dialog-search-tree` reads its nodes through an
`Accessor`. On main (`20f0a45`), the accessor takes the node cache the
environment keeps for the space out of the environment's `Holds` (a map of
type-erased values under string keys, which the code that put a value in
takes back out and downcasts), answers the nodes it holds from it, and for
any other performs the `Load` command, which returns a block's bytes; the
tree then checks the bytes against the hash, validates their layout and
decodes them into a node, and puts the node in the cache.

The single-file archive (dialog-db/dialog-db#733) keeps nodes as rows in a
mapped file rather than as blocks, and reads them in place. To hand those
to the tree it added a second value kept through `Holds`, a `NodeSource`,
which the accessor asks before it loads. The lookup ran for every tree read
in every store, and measured as up to 16% more instructions on the
`dialog-perf` scenarios against main, with no store but the single file
gaining anything from it; it now runs only when the cache misses, which
brought every scenario but one within 3% of main.

The maintainer says the tree should not read nodes through side channels:
it should ask for what it needs through the capability system, and caching,
reading and decoding belong to the providers the environment is composed
of. The maintainer also expected main to have moved caching out of the tree
into the environment already, and asked for that to be checked: the cache's
ownership moved (the environment keeps it, one per space, and the peer
releases it), while the lookup and the answering from it stayed in the
tree.

`Holds` keeps other things on main besides the node cache: the query
engine's formula cache, the repository's caches and the network's routes.
