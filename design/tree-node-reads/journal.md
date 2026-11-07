# The tree reads nodes beside the capability system: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* How a tree reads a node on main and on #733. The maintainer's objective: the environment is where every cache lives, so it can be passed, evicted and reused across sessions, and nothing caches or skips caching elsewhere. Main moved toward that in three steps (#730, #735, #742), but the cache is still optional (an environment that keeps nothing reads uncached, silently), and it is reached from two places (the tree's `Accessor`, the query session's `Load` provider) while `Load` itself is uncached. | *Where are we at?* Delimiting: the cause is named (the tree predates the capability model and its transition is partial, so its concerns are not expressed as capabilities); the problem statement is being cut to scope. |
| **Agenda** | *What do we need to know?* Which concerns are the tree's (to become capabilities it invokes) and which are a provider's; whether writes (persist) are in scope with reads; whether other partial transitions (formula cache, repository caches) are this topic or siblings. Open hypotheses: policies as requests, the per-read cost. | *Where are we going?* Delimiting, then Direction: use cases from the tree's concerns, and approaches to express them. |

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

## 2026-11-07, Diagnosing: one cache, or the home of every cache

**Asked.** What does "the environment is a cache" mean: one cache the
environment answers every read from, so callers never see it; or the
home of every cache, each still consulted by the code that uses it? Answer
as given: the maintainer asked for the trade-offs, leans to the first ("from
the tree's perspective it just needs to get a node; why would callers have
to think about a cache?"), allows there may be cases where seeing the cache
is useful without knowing what they are, and asked why sync would care.

**Learned.** Every place outside the cache that touches it on main is one
of three ways of asking for a node: made just now (persist), only from
memory (`cached`), ahead of need (`warm`, `keeps`). None needs the cache
itself; each is a policy a request could carry. Sync does not care about
the cache; it needs bytes rather than nodes, and push forwarding wants a
remote's block passed through, not kept. The open cost is per read: today a
cache hit is a direct call on a cache found once per operation; answered by
the environment, every node read is a capability performed (a hypothesis,
measurable).

**Decided.** Not decided. A problem statement is drafted in the top story
for the maintainer to accept or cut.

## 2026-11-07, Diagnosing to Delimiting: the cause, and the tree's concerns

**Asked.** Does the drafted problem statement state the problem, and is it
only about tree nodes? Answer as given: the whole design is an
object-capability model and an effect system, all I/O through effects, the
environment their provider. The original implementation predated the
capability model, and the transition to it was not finished, so some code
is designed for it and some around it. The point is separation of
concerns: what is the tree's concern should be expressed as capabilities
it invokes, and providers provide them.

**Learned.** The cause is the unfinished transition, not caching as such:
the cache problems are what a tree whose concerns are not expressed as
capabilities looks like. Caching, decoding and the single file's rows land
in the tree, or beside it, because the tree has no capability to ask for
what it needs. The same partial transition shows elsewhere (formula cache,
repository caches), which makes scope a question.

**Decided.** The problem statement is rewritten around separation of
concerns. Proposed scope, not yet accepted: this topic covers the search
tree's concerns; the formula cache and the repository's caches are sibling
topics that would follow its answer.

## 2026-11-07, Delimiting: is checking a node the tree's concern?

**Asked.** Which of the tree's nine dealings with the world are its
concerns, and which a provider's; in particular checking a node. Answer as
given: the check was meant to be (and was thought to be already) a type
invariant: a persistent node cannot be made that does not match its hash,
so the tree trusts no one; it only needs to receive that type.

**Learned.** True on main: the hash is computed from the node's bytes and
the block constructor validates the layout. A load still compares the
node's hash with the one requested, which needs no trust. False on #733: a
node built from rows carries the store's hash, held by the store's write-time
check behind an `unsafe` trait, not by the type. Holding it by type for
rows means encoding the block to hash it, which is the work reading in place
avoids: the maintainer's invariant and in-place reads are in tension.

**Decided.** Checking (2 in the list) is not a concern of anyone's at read
time beyond comparing hashes: the node type holds it. Decoding (3) and
caching (4) belong to providers; the rest not yet answered. Open: whether a
store may establish the invariant under a contract, or only the type may.

## 2026-11-07, Delimiting: the invariant held by the type, rows included

**Asked.** May a store establish the hash invariant under a contract, or
only the type? Answer as given: no decoding, not even for the single file;
it has pointers to the tokens a node is made of and can hash the node by
streaming its bytes as it reads them.

**Learned.** The bytes a node's hash is over are the tree's encoding of it,
not its tokens end to end, so streaming them is running the encoder into
the hasher: no decoding, no allocation, the encoder's CPU on a miss. Making
the hash cover tokens as stored would change node identity across
replicas, out of scope.

**Decided.** Only the type holds the hash invariant. A node made from rows
gets its hash by streaming its encoding into the hasher, once, when it is
made; the `unsafe` contract (`NodeRows` vouching for its hash) goes. This
rests on an open hypothesis, that the hashing is cheap next to a cold
read; the bench measures it.

