# The tree reads nodes beside the capability system: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* How a tree reads a node on main and on #733. The maintainer's objective: the environment is where every cache lives, so it can be passed, evicted and reused across sessions, and nothing caches or skips caching elsewhere. Main moved toward that in three steps (#730, #735, #742), but the cache is still optional (an environment that keeps nothing reads uncached, silently), and it is reached from two places (the tree's `Accessor`, the query session's `Load` provider) while `Load` itself is uncached. | *Where are we at?* Delimiting: the cause is named (the tree predates the capability model and its transition is partial, so its concerns are not expressed as capabilities); the problem statement is being cut to scope. |
| **Agenda** | *What do we need to know?* Whether the tree's remaining dealings are its concerns or a provider's: memory-only reads, read-ahead, reads without hydration (5-7), and persist's blocks and kept nodes (8-9); whether the scope (the tree only) is accepted. Open hypotheses: policies as requests, the per-read cost, the `hash()` inventory, hashing a rows node on demand. For Design: the requested name and the actual hash as one type or two. | *Where are we going?* Delimiting, then Direction: use cases from the tree's concerns, and approaches to express them. |

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

## 2026-11-07, Delimiting: the invariant moves to the provider (reverses the last entry)

**Asked.** No question asked; the maintainer reconsidered the last
decision once hashing a rows node was shown to mean running the encoder on
every miss: "we trust the provider, and then it's up to the provider to
verify that what they serve corresponds."

**Learned.** Two claims in the ledger now conflict: the type-only invariant
and trusting the provider. The maintainer chose the second; the first is
marked withdrawn.

**Decided.** Reverses the last entry. The node type offers a constructor
for providers that vouch for a node (today's `unsafe` `NodeRows`, kept);
the provider that makes a node is responsible for having verified it.
Proposed reading, to be confirmed by the maintainer: bytes from any store
or remote are still hashed by the provider that decodes them; only a
provider that keeps nodes in its own form (the single file, verifying at
write and checksumming records) vouches without hashing at read time; what
goes uncaught is the file changed on disk in a way its checksums miss.

## 2026-11-07, Delimiting: a truthful hash, and verifying as a choice

**Asked.** Is the proposed reading of "trust the provider" right? Answer
as given: somewhat. A persistent node must have a way to give its hash,
and it must not be possible to lie to the node about what its hash is;
asking for the hash means materializing the block, which is the cost of
asking. So the tree can either trust the provider or verify.

**Learned.** This replaces both earlier positions: the hash is always
truthful (computed from content, never supplied), and verification is the
consumer's choice and cost. A provider vouches only that the node it
served is the one asked for, which the consumer may check. The cache
already keys nodes by the requested hash, but the tree diff identifies
loaded nodes by calling `hash()`, which would hash every rows node it
loads: trusting stays cheap only if tree code uses the requested hash as a
node's name and `hash()` only to verify.

**Decided.** A node's hash is computed from its content, lazily for a rows
node, and cannot be supplied: `NodeRows` no longer carries a hash the node
believes. The tree trusts by default and verifies where it chooses. Rests
on an open hypothesis: the inventory of `hash()` calls on loaded nodes.

## 2026-11-07, Delimiting: the requested name and the actual hash

**Asked.** (Continuing the last entry.) The maintainer proposed a
distinction, perhaps as another type: the thing a node was requested by,
and a way to get what it actually is, so different consumers decide to
trust or verify.

**Learned.** It gives the last decision a shape the code can hold: a
node's name (the hash it was requested under) and its hash (computed from
its content) are two things. Two forms: one node type with both, or a
served node and a verified node, the second obtained only by checking
the first, so a function that needs verification says so in its types.

**Decided.** The distinction is adopted; its form is an implementation
decision for the Design phase (`design-node-identity.md`, when that phase
comes). The question on reads 5-7 is still open.

## 2026-11-07, Delimiting: what a node request can say

**Asked.** Should a node request be able to say "only if in memory",
"read ahead" and "without hydrating", or leave all three to providers?
Answer as given: we do want to express things: give me the node; preload
the node; and something like an optimistic read, which the maintainer did
not object to but asked what it is used for before deciding.

**Learned.** The memory-only read is used only by writes, to keep an edit
surgical: a neighbouring leaf's summary decides whether one leaf can be
rewritten without moving a chunk boundary, and without it the tree takes
the general path or loads later. It is an optimistic read with a fallback;
nothing in it needs the word memory. What it reads is a value derived from
the node (its summary), not only the node.

**Decided.** Not yet: the optimistic read waits on the maintainer now that
its use is known. "Without hydrating" (export, push's local read) is not
yet answered. Noted for Design: whether derived values (a piece summary)
ride with the node.

## 2026-11-07, Delimiting: three operations, and a fourth unaccounted for

**Asked.** Should the optimistic read be a third thing a node request can
say? Answer as given: the operations are read a local node, read a node
from anywhere, and preload: three distinct operations, all needed.

**Learned.** The maintainer's three cover hydrating reads, local reads and
read-ahead. The optimistic read is not among them and is not a local read:
a local read may wait on this replica's storage, the optimistic one must
not wait at all. The three as stated and the optimistic read cannot both
be the whole list: either it folds into one of them, is a fourth, or the
write path stops using it.

**Decided.** The three operations are adopted. The optimistic read is
open, put to the maintainer.

## 2026-11-07, Delimiting: why a write asks what is in memory

**Asked.** Why does the tree need to know, on a write, whether a node is
already in memory, and is that a need of the write or a shortcut?

**Learned.** A write in a forced run first checks whether the edit leaves
every boundary in place, to avoid re-chunking and re-encoding the whole
run. The check gathers each piece's summary in two passes: pass 1, unable
to await under the tree borrow, takes summaries from the cache or defers;
pass 2 reads the deferred pieces ordinarily and keeps their summaries on
the nodes. The memory-only reads are: one in pass 1 (a sync convenience
pass 2's read covers), and one after the check (carrying a summary pass 2
just computed and dropped, through the cache; with no cache it silently
takes the expensive path). Neither is a need of the write. The one real
use of caching here is the summary kept on a node across writes, which is
a provider's caching and needs no request.

**Decided.** Proposed, for the maintainer: the optimistic read is dropped;
the two sites become a returned value and an ordinary read. Rests on an
open hypothesis (verdicts unchanged, cost within the gate).

