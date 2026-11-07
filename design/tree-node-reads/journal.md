# The tree reads nodes beside the capability system: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* How a tree reads a node on main and on #733. The maintainer's objective: the environment is where every cache lives, so it can be passed, evicted and reused across sessions, and nothing caches or skips caching elsewhere. Main moved toward that in three steps (#730, #735, #742), but the cache is still optional (an environment that keeps nothing reads uncached, silently), and it is reached from two places (the tree's `Accessor`, the query session's `Load` provider) while `Load` itself is uncached. | *Where are we at?* Diagnosing: from the feature as asked to the problem underneath; the maintainer's account is backed by main's history. |
| **Agenda** | *What do we need to know?* What "the environment is a cache" means: one cache, or the home of every cache (nodes, blocks, spilled values, rules, formulas). Whether a cache that is optional is the failure, or caching beside the capability that loads. Whether in-place reads are worth an interface at all (open hypothesis). | *Where are we going?* Delimiting: a problem statement with no remedy in it. |

## 2026-11-07, Describing: what reading a node involves today

**Asked.** Is a `Load` that returns a buffer the right abstraction for the
tree, rather than asking for a node and leaving reading and decoding to the
provider? Answer as given: the maintainer wants a correct design with no
side channels; caching should not be the tree's, and was expected to have
moved into the environment on main already, to be verified.

**Learned.** Checked on main: the cache's ownership moved into the
environment, the cache's use did not; the tree still looks it up through
`Holds` and answers hits itself. The `NodeSource` on #733 copied that
pattern, and its per-read lookup was the branch's regression. `Holds` keeps
a formula cache, the repository's caches and network routes as well.

**Decided.** Not decided. The request arrived as a remedy ("read through
capabilities, no side channels"); the next sitting asks what problem it
answers.

## 2026-11-07, Diagnosing: what goes wrong

**Asked.** What goes wrong because the tree reaches into `Holds` for its
cache and node source rather than asking through a capability? Answer as
given: "we had too many code paths, ended up with their own different
caches, and I keep trying to fix that. From the get-go the plan was that
the environment's whole job is to be the cache and carry everything, so
you can pass it around, evict the cache, or reuse it across sessions.
When things have implicit dependencies in various places, that breaks
down: something bypasses the cache, or caches elsewhere, and an
environment, when introduced, doesn't have a cache. I don't want it.
There is the environment, it is a cache."

**Learned.** Main's history matches the account: #730 took the cache out
of trees, #735 put it on the environment and removed the node-level
`Nodes` trait with its `Cached` wrapper, #742 bounded every cache the
environment holds. `notes/tree-reads-as-effects.md` argues #735: no reader
can skip the cache because the only way to a node is `Load` on the env.
Read against the code that does not hold as stated: the cache is consulted
beside `Load`, by the tree's `Accessor`, and filled again by the query
session's `Load` provider; `Load` itself is uncached; push decodes
forwarded remote blocks uncached on purpose; and an environment that keeps
nothing silently has no cache. The failures the maintainer names are three:
caches outside the environment, reads that skip it, and environments
without one. The last two are still possible on main by construction.

**Decided.** Not decided. Next: what "the environment is a cache" means.
