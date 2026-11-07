# The tree reads nodes beside the capability system: glossary

**environment** (env): the value every operation is performed on. It
answers capabilities (`Provider<C>` for each command or effect it
supports) and keeps long-lived handles for the code running in it
(`Holds`). A peer's session is one; a test store is another.

**side channel**: a way for code to reach something through the
environment other than by performing a capability on it: today, a value
taken out of `Holds` under a string key and downcast. What it reaches is
invisible in the types, so an environment can lack it without anything
failing to compile.

**node cache**: checked, decoded tree nodes by content hash, bounded by
the bytes they hold (`NodeCache` in `dialog-search-tree`). The environment
keeps one; the tree consults it.

**cache**: *not yet defined for this topic.* The maintainer said "the
environment is a cache"; whether that means one cache or the home of every
cache is open (see the journal's agenda).

**hash invariant**: a node's content hash is the hash of its block's bytes.
On main the node type holds it (the hash is computed from the bytes the
node is made of). A node made from rows has no bytes until it encodes them.

**name** (of a node): the hash a node was requested by, which the
provider that served it vouches for. What tree code identifies a node by.

**hash** (of a node): the hash of the node's block, computed from its
content and never supplied. Asking for it may cost materializing the
block (a node read from rows); comparing it with the name is verifying.

**spine**: the nodes on the paths a write has opened, held in memory and
edited in place until `persist` seals them. The environment keeps a
branch's live spine between commits while a handle is open.

