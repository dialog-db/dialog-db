# The tree reads nodes beside the capability system: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* How a tree reads a node on main and on #733. The maintainer's objective: the environment is where every cache lives, so it can be passed, evicted and reused across sessions, and nothing caches or skips caching elsewhere. Main moved toward that in three steps (#730, #735, #742), but the cache is still optional (an environment that keeps nothing reads uncached, silently), and it is reached from two places (the tree's `Accessor`, the query session's `Load` provider) while `Load` itself is uncached. | *Where are we at?* Building (decided by the maintainer; see the last entry). Was Delimiting: the cause is named (the tree predates the capability model and its transition is partial, so its concerns are not expressed as capabilities); the problem statement is being cut to scope. |
| **Agenda** | *What do we need to know?* Whether the tree's remaining dealings are its concerns or a provider's: memory-only reads, read-ahead, reads without hydration (5-7), and persist's blocks and kept nodes (8-9); whether the scope (the tree only) is accepted. Open hypotheses: policies as requests, the per-read cost, the `hash()` inventory, hashing a rows node on demand. For Design: the requested name and the actual hash as one type or two. Whether a mapped file's page cache, which the environment cannot own or evict, is an accepted exception to "the environment is the cache". | *Where are we going?* Delimiting, then Direction: use cases from the tree's concerns, and approaches to express them. |

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

## 2026-11-07, Delimiting: what "in memory" means for a mapped file

**Asked.** With the single file memory-mapped, does a store answer "yes, I
have it" or "no" to a memory-only read?

**Learned.** Neither answer is grounded: rows are addressable at once, but
touching them may page-fault, a synchronous wait the operating system
decides and nothing reveals beforehand. "Yes for everything" makes the
optimistic read a local read that may block on disk; "yes for what I built
recently" makes it the store's bookkeeping. "In memory" is a property of a
provider's storage that not even a mapping store fully knows, which
supports dropping the request. And a mapping store keeps a cache the
environment does not own: the page cache, outside its budget and its
`release`, visible only in resident memory.

**Decided.** Nothing new beyond the proposal to drop the optimistic read,
which this strengthens. The page cache's place under "the environment is
the cache" is on the agenda.

## 2026-11-07, Delimiting: the writer's spine, and the rule for the optimistic read

**Asked.** (The maintainer, unsure what the writer does.) Something
remembered: as nodes are built, the writer can tell which path was updated,
and a node written again on the same path is replaced, so everything is
flushed at once. And the rule: the write path must not get worse; drop the
operation if that holds, reshape it if it does not.

**Learned.** The memory is right and is a separate mechanism: the edit
spine replaces nodes in place within a batch, `persist` adds only the
nodes the batch created to the delta, flushed once; the environment holds
a branch's live spine between commits; `persist` fills the node cache with
what it seals. The memory-only reads sit inside the forced-run check and
can go without touching the spine: one becomes a returned value (cheaper),
the other an ordinary read (cost to measure).

**Decided.** The optimistic read is dropped, conditionally: if the
ordinary read keeps the write path within the perf gate, nothing replaces
it; if not, a batch read over a run's pieces is the operation to add
instead. Persist's two dealings (the blocks it hands over, the nodes it
keeps) are next.

## 2026-11-07, Delimiting: what each operation would replace (a sketch)

**Asked.** The maintainer asked for exactly which operation replaces what,
since the shapes were being described without the code they replace.

**Learned.** A sketch, with placeholder names, each a command the tree
performs on the environment and providers answer (a caching layer, the
single file directly, or a decoding layer over `archive::Load` for stores
that keep bytes):

| today (main, and #733) | replaced by |
|---|---|
| `Accessor::load_node` / `get_node`: `NodeCache::of` through `Holds`; on a miss `archive::Load`, `checked`, `PersistentNode::try_from`, insert | `ReadNode { catalog, name }`, answered by providers |
| `Accessor::reading(Reading { local: true })` (`Load::locally`): push's local read, export | `ReadNode` with `local: true` |
| `Accessor::warm` and `keeps()`: the walker's read-ahead | `PreloadNode { catalog, names }`, returning nothing |
| `accessor.cached()` in `forced_run_quiet` | nothing: `compressed_run_quiet` returns its summary |
| `accessor.cached()` in `compressed_run_quiet`, pass 1 | the `ReadNode` pass 2 performs |
| `NodeSource::of`, `from_source` (#733) | nothing: the single file answers `ReadNode` |
| the query session's `ArchiveLoad` provider inserting into the cache | nothing: the caching layer answers `ReadNode` |
| `TransientTree::persist` / `HitchhikerTree::persist`: seal, add blocks to `Delta`, insert into `NodeCache::of` | `persist` seals and returns the nodes |
| `archive::persist` in dialog-repository: spilled values, blocks (`Import`), `forget` on failure | spilled values as today, then `WriteNodes { catalog, nodes }`; the provider stores and keeps them, forgetting on failure |

Unchanged: `archive::Get`/`Load` for bytes (sync, push, serving a peer),
spilled values' blob commands, and the commit's own order of writes
(spilled values, nodes, then the head). Gone: `NodeCache::of` and every
`Holds` lookup in the tree, the accessor's cache, the second cache filler,
`NodeSource`.

**Decided.** Not decided; this is the material for the next question
(does the mapping match the maintainer's expectation, and does
`WriteNodes` with the caller ordering writes answer the writer's concern).
It belongs to Direction once the problem is accepted.

## 2026-11-07, Delimiting: blocks, translation, nodes

**Asked.** (The maintainer, checking their understanding of `WriteNodes`.)
Is the point a batch write rather than single block writes? Answered: no,
main already imports a commit's blocks in one batch; the change is nodes
rather than bytes handed over, and caching moved out of the tree. The
maintainer then stated the structure underneath: a block layer that reads
and writes bytes, which `fs`, the network and the rest need and keep; a
layer above that translates (decodes, encodes); and a node layer the tree
talks to. The single file can skip the translation and be a node layer
itself.

**Learned.** The single file skips translation only for node reads and
writes. It still answers block requests (sync, push, serving a peer) by
encoding, and a node it lacks arrives from a remote as a block, through
translation, before it stores it as a node. So the layering holds for
both stores; what differs is which layer a store answers at.

**Decided.** The layering is the first candidate approach for Direction:
node requests answered by a caching layer, then by a store's node layer
where it has one (the single file) or by translation over the block layer
(`fs`, remotes). Not yet decided: the problem statement is still a draft,
and Direction starts from an accepted one.

## 2026-11-07, Building: decided by the maintainer

**Asked.** Whether the drafted problem statement and scope are accepted.
Answer as given: the maintainer does not want the full design process for
this: the design was always meant to be this way and did not end up so
only because an implementation did not follow instructions; stop
debating and build it as described.

**Learned.** Nothing new about the problem.

**Decided.** The approach in the top story, as the maintainer described
it, is decided on the maintainer's judgement, not on a completed matrix:
the problem statement stays a draft and no Direction matrix was made.
Building starts now. Open hypotheses carried into the build: the
`hash()` inventory, the ordinary read in the forced-run check (perf gate),
hashing a rows node on demand. The page cache of a mapped file is left
open.

## 2026-11-07, Building: built on main, and where the plan moved

**Asked.** The maintainer, reviewing the plan: refine it where the review
found flaws, then build it, unless it is the wrong approach. The flaws the
review named: the hot path could still go through `Holds`, per read this
time; making the requests effects contradicts why `Load` is a command; a
batch's reads between seal and flush would decode staged blocks; preload
had to say who drives it; step 2 puts the single file's representation
into the tree's node type; and the boundary read could be a local read.

**Learned.** Built on main (`9b98395`), without #733. What the build
settled:

- *How a node request reaches a store with its own node form* (the plan's
  open question): through the storage. `Storage` routes a request to the
  space by the catalog's subject, `Space` hands it to its archive, and the
  three block stores answer through the translation layer, all implemented
  in the tree crate (the orphan rule allows it, the request type being
  local). A store with its own form implements the three commands and the
  chain reaches it unchanged. The peer adds the cache and the hydration
  above the storage. The effect option was dropped.
- *The cache lookup.* Every environment finds its cache with
  `NodeCache::of` inside its own provider, per node read. It is the
  environment's lookup of itself, not a side channel, but it runs once per
  read where the accessor ran it once per operation. Measured below.
- *Seal to flush.* The delta a persist fills holds nodes, so `DeltaOverlay`
  answers a read of a staged node with the node the seal produced, hashes
  adopted and all; `persist` keeps nothing and takes no environment.
- *Preload.* The command's future is the work; the walker keeps it and
  polls it, as it kept its read-aheads, and an environment without a cache
  returns at once, so a keep-nothing environment reads each sibling once.
- *The boundary read* is a local `Read`. A scope's cache holds only what
  its archive produced or a successful write stored, so consulting it
  changes no answer. One test environment that hydrated every load had to
  learn to honor a local one.
- *Step 2* (a node's name beside its hash, the enum of block or rows) is
  #733's to do when it rebases: on main a node is its block and is checked
  against its name before it exists.
- *The integration's read-ahead* could no longer ask which nodes were in
  memory. It now opens a window once per lookahead batch and reads each
  path's next node as the previous one lands, deduplicating by hash. The
  widening heuristic that depended on counting misses went with it.
- *The forced-run check* hands its summaries back and reads a piece it did
  not summarize; pass 1 defers every stored piece to pass 2, which takes
  the summary a node already carries.

The `Nodes` bound replaced `Provider<Load> + Holds + ConditionalSync` at
75 sites in the tree crate; `Holds`, `Load` and `Get` are gone from every
tree bound. Clippy with every feature and target is clean, the touched
crates' tests pass, and the tree, storage, artifacts, repository, peer and
query crates build for wasm.

**Measured.** `dialog-perf` on main and on this branch, one toolchain
(1.99.1), callgrind instructions, selected scenarios:

| scenario | main | branch | Δ |
|---|--:|--:|--:|
| commit-batch | 322,565,708 | 316,971,704 | -1.7% |
| commit-hot-key | 197,194,160 | 196,056,570 | -0.6% |
| commit-rows | 199,917,663 | 198,886,359 | -0.5% |
| export-walk | 28,125,916 | 28,001,393 | -0.4% |
| first-query | 1,858,567 | 1,848,887 | -0.5% |
| number-point | 33,342,531 | 34,062,376 | +2.2% |
| query-join | 76,793,183 | 76,814,264 | +0.0% |
| read-point | 22,657,816 | 23,226,640 | +2.5% |
| read-scan | 30,374,954 | 30,350,055 | -0.1% |
| read-sibling | 30,098,475 | 30,780,435 | +2.3% |
| rule-overlay | 263,576,069 | 265,616,935 | +0.8% |
| rule-point | 52,296,615 | 54,325,634 | +3.9% |
| rule-ranked | 59,624,369 | 59,885,686 | +0.4% |
| subscribe-fanout | 39,533,736 | 39,857,365 | +0.8% |
| subscribe-pulled | 39,372,584 | 39,882,228 | +1.3% |
| transaction-reads | 303,657,543 | 303,404,155 | -0.1% |

Every counter (blocks read and written, engine steps) is unchanged. The
writes got cheaper: a seal no longer inserts each node into the cache and
adopts its hashes there, and the commit's one `Write` keeps them. The point
reads pay the per-read dispatch the plan flagged: one boxed future per node
read (the capability's `execute`) and one cache lookup per read where the
accessor made one per operation. Answering a hit before any loading
machinery is built took read-point from +3.6% to +2.5% and number-point
from +3.7% to +2.2%. What is left is the box and the lookup: about 350
instructions a read, with the lookup (`NodeCache::of`: the `Holds` map,
the scope) about a third of it. The rule-point figure moved by 2% between
two builds with identical malloc counts, the difference entirely inside
`_int_malloc` and `malloc_consolidate`: heap layout, not more work; the
gate's cross-CPU allowance for rule-point is 2.4%.

Two ways to buy the rest back, neither taken here: the query session, an
environment itself, could answer a hit from the cache the peer holds before
forwarding the read (one box instead of two for a query's reads, which is
what main did under another name); and the peer could hold its node cache
as a field found at construction instead of looking it up per read.

**Decided.** Built as above. Open for #733's rebase: step 2 and the
`hash()` inventory. Open for a later measurement: whether the peer should
hold its node cache as a field found at construction instead of looking it
up per read.

## 2026-11-07, Building: the dispatch cost, and what it was

**Asked.** The maintainer: why exactly did the point reads regress, and
can it be avoided?

**Learned.** Not from reading more: every counter was unchanged. From how
a read crossed the environment boundary. `read-point`, about 1,600 node
reads, under callgrind:

| per node read, roughly | instructions | cause |
|---|--:|---|
| allocate, move and free a boxed future | ~250 | `Provider::execute` is `async_trait`: every call heap-allocates a `Box<dyn Future>`, copies the state machine in and frees it. On a hit the future never suspends. A query's read crossed two such boundaries: the tree into `QueryEnv`, and `QueryEnv` into the erased `dyn Capabilities`. |
| find the cache | ~200 | `NodeCache::of` per read: the `Holdings` mutex, a 45-byte key hashed, the map probed, the `Arc` cloned, the scope computed. The accessor did it once per operation. |
| the dispatch layers | ~100 | `Metered`'s downcasts, the `through_*` nesting, drop glue. |

On main a hit was a direct `get_cached` inside the tree's own future.
The regression was the price of crossing an `async_trait` boundary per
node, not of the separation: the design says what crosses (a node
request), and `async_trait` plus `dyn` erasure decided how.

**Decided.** The maintainer chose the first of the three ways out: a
synchronous answer path on the request. `nodes::Ready` is what an
environment can answer at once (a node it holds in memory, or nothing),
and `Read::perform` asks it before performing the boxed command;
`Preload::perform` asks it and does nothing for a node already held.
The tree still asks one way and awaits either way, so this is not the
optimistic read that was dropped: a miss changes nothing the tree does.
Every environment answers: the peer, the walk, the metering and counting
harnesses and the test environments from the cache they hold
(`NodeCache::held_for`, which makes no cache); the query session for a
wildcard read from each line in turn; the batch overlay with a staged
node first; a store with nothing, a space with its archive's answer, a
storage with its space's. The pre-check inside the translation layer
stays for environments reached through `execute`.

**Measured.** Same toolchain, same main sweep, callgrind:

| scenario | main | branch | Δ |
|---|--:|--:|--:|
| commit-batch | 322,565,708 | 316,523,199 | -1.9% |
| commit-hot-key | 197,194,160 | 196,030,823 | -0.6% |
| commit-rows | 199,917,663 | 198,619,399 | -0.6% |
| export-walk | 28,125,916 | 28,168,338 | +0.2% |
| first-query | 1,858,567 | 1,904,851 | +2.5% |
| number-point | 33,342,531 | 33,406,400 | +0.2% |
| query-join | 76,793,183 | 76,828,338 | +0.0% |
| read-point | 22,657,816 | 22,752,469 | +0.4% |
| read-scan | 30,374,954 | 30,490,553 | +0.4% |
| read-sibling | 30,098,475 | 30,098,403 | -0.0% |
| rule-overlay | 263,576,069 | 263,750,430 | +0.1% |
| rule-point | 52,296,615 | 52,228,008 | -0.1% |
| rule-ranked | 59,624,369 | 59,889,737 | +0.4% |
| subscribe-fanout | 39,533,736 | 39,717,715 | +0.5% |
| subscribe-pulled | 39,372,584 | 39,324,790 | -0.1% |
| transaction-reads | 303,657,543 | 302,074,629 | -0.5% |

No regression against the gate. The point reads are within 0.4% of main,
rule-point and read-sibling level, the writes 0.6% to 1.9% cheaper.
`first-query` is 46k instructions over on a 1.9M phase, under the gate's
absolute slack, and moves by about that between builds. What a hit costs
now is the lookup of the cache the environment holds, about 200
instructions; holding the cache as a field of the peer would take most of
that and is still open.


## 2026-11-08, Building: the answer at hand belongs in the provider

**Asked.** The maintainer, on `nodes::Ready`: it "isn't exactly using
capability model", which "smells again like a layering issue or flaw in
capability system design". The intent was always an effect system in the
manner of effing-mad, gozala/actor and Unison abilities, where a handler
is sync or async without the caller caring, and `perform` stands in for
`yield`. Could the capability system be rethought so a provider may be
either?

**Learned.** `Ready` was a symptom. `Provider::execute` under
`async_trait` can only ever answer later, in a boxed future, so a
provider holding the answer had no way to say so inside the model, and
the fix went beside it: a second trait the request's `perform` asked
first. Every environment then had to answer twice, and nothing in the
types tied the two answers together. The cost it worked around was never
the tree's: every provider in the workspace paid a box per call, hit or
miss.

What the maintainer asked for is a handler that may finish without
suspending. In a coroutine-based effect system that is the handler not
yielding; in Rust without coroutines on stable, it is a return type that
is either the value or a future. So `Provider::execute` now returns
`Resume<'_, Output>`: `Resume::now(value)` or `Resume::later(future)`,
the latter boxed exactly as `async_trait` boxed before. `Resume` is a
`Future`, so a caller awaits it and cannot tell which it got; a provider
in front of another returns what it was handed, so a hit stays a hit
through the storage, the overlay and the query session. An attribute,
`#[dialog_capability::provider]`, writes the common case: an `async fn
execute` whose body becomes the future, so the 299 impls that always wait
changed one attribute pair for one attribute and nothing else. The
`derive(Provider)` forwarding stopped boxing too.

Three things were learned building it. `Resume` holds its output by
value, so it is `Send` only when the output is: `async_trait`'s box hid
that bound, and a generic provider that awaits another's answer now says
`C::Output: ConditionalSend`. A `Future` impl that needs `T: 'a` (which a
sentinel typed by `T` would) trips rustc's higher-ranked lifetime
limitation wherever the answer is awaited inside a generic `async fn`
(the fs remote's traffic shaping did), so `Resume` keeps the answer at
hand in an `Option` behind a private enum and polls without any bound on
`T`; `into_now` takes it apart for a provider that must look before it
forwards. And a storage that hands out its space by value cannot pass a
space's answer through unboxed: a read routed through `Storage` always
waits, which costs nothing today (the peer answers its hits above the
storage) and is the one place a native node store beneath `Storage`
would still pay a box per hit.

**Decided.** `nodes::Ready` is gone; `Nodes` is the two providers and
`ConditionalSync`. The translation layer's `through_*`, the repository's
`read_node_or_hydrate` and friends, and each request's `perform` return a
`Resume`, answering a cached node, an empty write, a preload with nowhere
to land or a staged node at once. The query session's wildcard read takes
the lines answered at once before it waits on any, which is sound because
a node is its hash whichever line holds it.

**Measured.** Same toolchain, same main sweep, callgrind:

| scenario | main | branch | Δ |
|---|--:|--:|--:|
| commit-batch | 322,565,708 | 316,906,545 | -1.8% |
| commit-hot-key | 197,194,160 | 194,967,085 | -1.1% |
| commit-rows | 199,917,663 | 197,596,530 | -1.2% |
| export-walk | 28,125,916 | 28,171,371 | +0.2% |
| first-query | 1,858,567 | 1,873,818 | +0.8% |
| number-point | 33,342,531 | 33,472,313 | +0.4% |
| query-join | 76,793,183 | 76,693,056 | -0.1% |
| read-point | 22,657,816 | 22,457,161 | -0.9% |
| read-scan | 30,374,954 | 30,322,064 | -0.2% |
| read-sibling | 30,098,475 | 30,436,711 | +1.1% |
| rule-overlay | 263,576,069 | 263,073,663 | -0.2% |
| rule-point | 52,296,615 | 51,540,637 | -1.4% |
| rule-ranked | 59,624,369 | 59,802,045 | +0.3% |
| subscribe-fanout | 39,533,736 | 39,153,689 | -1.0% |
| subscribe-pulled | 39,372,584 | 39,401,683 | +0.1% |
| transaction-reads | 303,657,543 | 301,928,820 | -0.6% |

Every counter is unchanged. The point reads are now under main: a hit
costs the cache lookup and a `Resume` by value, no box and no second
trait; `read-point` is 0.9% and `rule-point` 1.4% below, where the
`Ready` path had left them level. The writes are 1.1% to 1.8% cheaper
than main. `read-sibling` read 1.1% over in this sweep and 0.5% under in
the next build of the same code, with identical counters: heap layout
again, as with rule-point in the first entry. The sweep was taken before
the read-ahead window's widening was restored (below); re-measured after
it, the commit scenarios moved by at most 0.13% (`commit-batch`
316,505,639, `commit-hot-key` 195,064,078, `commit-rows` 197,633,615,
`transaction-reads` 301,956,679): a commit's integration opens one window
either way. The pull request's own Perf gate and Replication gate both
pass on this head.

**Also decided.** The replication gate on the first head of #755 failed
one row: `join-none`, phase `seeded`, 17 round trips to 37. The seeded
replay is dense in key order, so a fixed window of 512 changes names one
or two leaves and the integration read the remote tree leaf by leaf, the
chain main's widening heuristic (`OPEN_WIDTH`) existed to break. The
heuristic went with the cache peek it counted by. It is back, counting
the distinct stored nodes the window names (where the spine hands a path
to the environment), which needs no peek and is the same count on a cold
tree. On a warm tree it stops widening sooner than main did, since nodes
the environment holds count too; no gated scenario has that shape, and
the gate will say if one appears. Measured locally with the soak's `join`
scenario over the `none` network: the seeded phase is 16 round trips
against the baseline's 17 (every window is opened now, so the one the
first change's miss used to trigger goes out with the rest), with the
same 63 requests and bytes; every other phase matches the baseline row
for row.

## 2026-11-09, Building: the bounds say which providers, and the node is the corpus's

**Asked.** The maintainer, reviewing #755: `ArtifactNodes` and
`ArtifactWrites` were not what they expected; a bound should name
providers, `Provider<Get<ArtifactNode>>`-shaped. And the name "artifact"
should go: it made sense once and no longer says what the crate holds.
What should the crate be called, and what should the new names be now,
so they do not have to move again?

**Learned.** The alias traits existed because the requests were generic
over the tree's `Key` and `Value`, which made the honest bound long. Make
the request generic over the node instead (`Read<N>` for an `N:
nodes::Node`, sealed, with `PersistentNode<Key, Value>` its one
implementor) and the bound falls out at the node: a crate that fixes the
keys and values names its node once and writes `Provider<Read<ItsNode>>`.
The tree crate, which does not care what the keys and values are, kept
`Nodes<Key, Value>` as its shorthand at first; the maintainer's objection
was to the layer that does care hiding what it asks for, and later in the
day to the shorthand itself (see the entry below). `Get`/`Put` as the
verbs were set aside: those are the local block effects, and the node
read hydrates and carries a priority.

On the name: `IndexNode` collides with the tree's own index and segment
nodes; `FactNode` reads as a node of one fact. The maintainer proposed
"corpus", a body of facts, and `notes/notation.md` already says that an
assertion the transactor has *incorporated* is a claim, the stored unit:
the corpus is the body of claims, and incorporating is taking into it.
The word was unused in the repository.

**Decided.** The requests are `Read<N>`, `Preload<N>`, `Write<N>` over
`nodes::Node`. `dialog_artifacts::tree::CorpusNode` is the corpus's node;
the alias traits and the `ArtifactRead`/`Preload`/`Write`/`Node` aliases
are gone, and every bound above the tree crate names its providers at
`CorpusNode`. Names that predate this topic (`ArtifactTree`,
`ArtifactNodeCache`, `Artifact` itself) wait for the crate's rename, where
the storage side becomes the corpus (`Corpus`, `CorpusNode`,
`CorpusCache`) and the model side's `Artifact` becomes `Claim`, per the
notation; the glossary has the term. Whether that rename also splits the
crate into model and engine is a question for that pull request.

## 2026-11-09, Building: no shorthand in the tree crate either

**Asked.** The maintainer: does the tree still need `Provider<blob::Load>`,
and "I also do not want to have `Nodes<Key, Value>`! just spell it out
everywhere".

**Learned.** No tree operation bounds on `blob::Load`. In
`dialog-search-tree` the only library code that names `Load` is the
translation in `nodes.rs` (`Read::through_blocks`, `Preload::through_blocks`,
`Write::through_blocks`): the way an environment that has only blocks
answers a node request, which is what `Load` is for. Everything else that
implements `Provider<Load>` in that crate is a test backend. Above the
tree, `dialog-artifacts` bounds on `Provider<blob::Load>` wherever a value
may be spilled: `fetch_spilled` and `load_spill` read a `Spilled { hash }`
payload's bytes out of the archive's blobs, and `spilled_refs`,
`blob_index`, `merge` and `rekey` walk those blocks for shipment. That is
the corpus's requirement, not the tree's, and it is written in the block
layer's vocabulary (a blob by hash) rather than the corpus's (a spilled
value by reference); whether a corpus request should stand in front of it,
as `nodes::Read<CorpusNode>` now stands in front of the node's block, is a
question for the crate's rename, not this pull request.

**Decided.** The `Nodes<Key, Value>` alias trait is gone from
`dialog-search-tree`, with its `lib.rs` re-export. Every site that bounded
on it (seventy-two across `tree/transient.rs`, `hitchhiker.rs`, `tree.rs`,
`differential.rs`, `walker.rs`, `traversal.rs`, `helpers.rs`,
`accessor.rs`, `validate.rs`) writes
`Provider<Read<PersistentNode<Key, Value>>> +
Provider<Preload<PersistentNode<Key, Value>>> + ConditionalSync` itself.
The one thing the alias bought, a `&dyn Nodes<K, V>` to collapse
monomorphic copies, nothing used; a bound of three traits cannot be a
trait object, and no call site had asked for one.

## 2026-11-10, Building: the crate is the corpus, and it reads its world through providers

**Asked.** The maintainer: do the refactor of `dialog-artifacts` becoming
`dialog-corpus`, and take out all the storage stuff so only provider-based
implementations remain.

**Learned.** The crate's library code named a store in four ways, and
only one of them was a store. `dialog_storage::Blake3Hash` is the raw
`[u8; 32]`, not `dialog_common::Blake3Hash` (a newtype with its own rkyv
and serde derives), so the model's hash is not already the common one;
moving the model onto the newtype would change nothing stored but is a
far wider change than a rename, so the alias moved into the crate
instead. `CborEncoder` served one type, `IndexRoot`, that nothing in the
workspace used. `DialogStorageError` was named only to read an
authorization or a rejection out of the tree's error; the tree's error
can say that itself (`AuthorizationFailure`, `rejection()`). And the
test-only `Volatile` in the crate's doc example is what
`dialog_search_tree::helpers::Blocks` already wraps.

Of the `Provider<blob::Load>` bounds in the crate, twenty-two sat on
paths that never reach `load_spill`: `select_data`, `format_manifest`,
`record` and `estimate` on the tree; the blob index's reads and
`blob_changes`; a buffered batch's `record` and `seal`; `rekey_tree`;
the context and coverage region reads; `spilled_refs`; the history
screen. They read keys and payloads, and a spilled value's reference
is in its key. The thirty-one that stay reach a spilled value through `apply`
(succession reads the prior claim), `select_record`, the scans,
`observe_revisions` and `TreeHistory`; `content_size` and `asset_size`
reach it only by type, through `select_record` on a fact whose value is
a number and never spills.

On a corpus request in front of `blob::Load`, the question the second
2026-11-09 entry left for this rename: a request defined in
`dialog-corpus` cannot be answered by the stores in `dialog-storage`,
because the dependency runs the other way through the tree crate, and a
blanket impl over every `Provider<blob::Load>` is refused by the orphan
rule. The tree crate could write `reads_nodes_through_blocks!` for the
stores because it sits above them; the corpus crate cannot. So such a
request would be answered only by the wrapper environments (the peer,
the access and query environments, the overlay, every test
environment), each by hand, and would move about a hundred and ten
bounds in the repository, query and peer crates. What it would add
beyond its name is the hash check, already in `load_spill`, and later an
answer from a spill cache the environment holds, which is the move that
would earn it (`design-spilled-values.md`).

**Decided.** The crate is `dialog-corpus`; `ArtifactTree` is `Corpus`,
`ArtifactTreeExt` `CorpusExt`, `ArtifactNodeCache` `CorpusCache`,
`BufferedArtifactTree` `BufferedCorpus`, `ArtifactWriter` `CorpusWriter`,
`DialogArtifactsError` `DialogCorpusError`. The crate depends on no store
crate. `blob::Load` stays the spilled-value read, performed through
`load_spill` alone, and bounded only where a path reaches a spilled
value; a corpus request in front of it is one decision with the cache
move, deferred to the subject decision, and the matrix says why. The
overlay answers the corpus's requests (`Read`, `Preload`, `Write` of
`CorpusNode`, `blob::Load`) and nothing of the block layer:
`ArchiveDelta::block`, `flush_blocks`, and the overlay's `archive::Load`,
`Get` and `Holds` are gone, and nothing needed them. The one caller that
shipped a batch's nodes as blocks (the read-amplification bench) writes
them with `nodes::Write`, as a commit does.

The model-side rename (`Artifact` to `Claim`) is not in this change. Two
things argue against doing it blind: `history::Claim` already names the
claim as the history records it, with a version-set cause, so the rename
collides with a type in the same crate; and `Artifact`'s shape (`the`,
`of`, `is`, an optional causal reference) is the notation's *assertion*,
the thing proposed, as much as its claim, the thing incorporated,
which `Instruction::Assert(artifact)` shows. The maintainer decides
which name each gets.

Where this touches the `Accessor` follow-up: nowhere in code. The
overlay's `archive::Load`, `Get` and `Holds` were what an accessor over a
batch's staged nodes would have needed; none exists, and the compile
confirmed it. The `CatalogScope` every corpus operation carries is as it
was.
