# The search tree's concerns are not expressed as capabilities

Phase: **Building.** See the [design index](README.md) for what the
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

## Problem

*Draft, not yet accepted.* The search tree was written before dialog's
capability model and was only partly moved onto it, so what the tree needs
from the world is not expressed as capabilities it invokes. It performs
`Load` for bytes, then does a provider's work itself: finds a cache through
a lookup that may come back empty, checks, decodes and keeps nodes, and
other code keeps them again its own way. Concerns that belong to whoever
answers a read (where a node is kept, in what form, whether it is
remembered) are decided in the tree or beside it. So an environment can
lack a cache without anything failing, a read path can skip the cache by
loading for itself, and a store that keeps nodes in a form of its own (the
single file's rows) can answer only through a side channel. Each new
environment, read path or store has reopened one of these, and fixing them
has kept falling to the maintainer.

*Proposed scope:* the search tree's concerns. The formula cache and the
repository's caches, kept through `Holds` the same way, are sibling topics.

## Approach

Decided by the maintainer, 2026-11-07, as the design the capability model
always called for: the tree talks in nodes, through capabilities it
performs on the environment, and every concern beside that belongs to the
providers the environment is composed of.

- **Layers.** A block layer reads and writes blocks as bytes (`fs`,
  remotes, the network). A translation layer turns blocks into nodes and
  nodes into blocks. The tree talks to a node layer. A store that keeps
  nodes in a form of its own (the single file) answers at the node layer
  directly, and still answers block requests by encoding; what it lacks
  arrives from a remote as a block, through translation.
- **Operations the tree performs:** read a node from anywhere; read a node
  locally (never hydrated); preload nodes; write the nodes a persist made.
  The memory-only reads go: one becomes a value the forced-run check
  returns, the other an ordinary read (a batch read is added only if that
  costs the write path).
- **Caching** is a provider layer answering node reads and keeping written
  nodes; a write that fails keeps nothing. No `Holds` lookup in the tree,
  no `NodeSource`.
- **A node's hash** is computed from its content and never supplied; a
  node read is named by the hash it was asked for, and the consumer
  chooses to trust or verify.
- **The commit** still orders its writes: spilled values, nodes, the head.

Built on main without #733; the [plan](tree-node-reads/plan.md) says what
landed where and what waits for #733's rebase (a node's name beside its
hash).

