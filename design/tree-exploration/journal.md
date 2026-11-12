# A consumer that can only query cannot draw a branch's search tree: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* The problem, accepted. What the maintainer means by idempotent: a tree read behaves as if the whole tree were local, fetching what it needs, so holding a block or not cannot be observed through it; and a resolver requires its node so that a query cannot download a whole tree by mistake. That the draft branch's listing breaks the second of those. Three ways the maintainer sees to tell what is held, and why each troubles them (fifth entry). That dialog already has one mutable thing reaching tree queries as a tracked fact, the root, and already has a replica as an entity with synthesized facts. | *Where are we at?* Directing. Approaches to "what is held" are being collected, not chosen. |
| **Agenda** | *What do we need to know?* Whether "what this replica holds" as a tracked fact of the replica, read by a scan and not by a resolver, is what the maintainer meant by capturing partiality (asked). Then whether such a scan can be built: answered per node by a local read, listed by a walk that never fetches, and re-run when a block arrives. Whether eviction exists and what it does to any of this. Open hypotheses in the ledger. | *Where are we going?* Use cases, then the strategy matrix. |

## 2026-11-12, Describing: what was tried and what was seen

**Asked.** The maintainer, after several rounds of fitting the resolvers
to the inspector: stop working with the resolvers we have and ask what
the right way is to expose an interface for exploring a search tree from
the query engine; from a clean slate.

**Learned.** The description in the top story: what the inspector needs,
what the worker's hand-written operations give it, what the resolvers
give instead, and where the two part. Each claim is in the ledger with
what backs it.

Two approaches were put on the table before any problem was stated, and
are parked in the ledger as opinions, not examined:

- the maintainer's: lower-level primitives, reading a block and then
  separate formulas that decode it as an index node or as a segment node;
- the agent's: a resolver yields each span whole, as one record value.

The maintainer asked that the first not bias the thinking. Neither is a
column yet; there is no matrix until there is a problem for it to serve.

**Decided.** Not decided. The draft branch stays as it is: what it
changed (entities for nodes and entries, a resolver that lists) was
decided while fitting the resolvers to the inspector, which is the
question this topic sets aside, so whether any of it is wanted is open
until a direction is chosen.

## 2026-11-12, Diagnosing: a problem statement is offered

**Asked.** When someone opens the tree for a branch, what are they
trying to find out? Not answered. The maintainer gave a problem
statement instead: "the dialog query engine cannot be used to inspect
the underlying tree, which causes the embedder to implement side
channels to do it."

**Learned.** The statement has no remedy in it, which the request it
replaces did. Two things in it do not yet hold up:

- *Those two cannot both be true.* The ledger has as a fact that a
  resolver heads a query and answers when given a node. So "cannot be
  used to inspect" is not true as worded; it is true of something
  narrower. What tonk's side channel does that a query did not, read off
  its code:
  1. starts from the branch with nothing given (the root is the default);
  2. returns each child of a node with its details together in one row;
  3. says whether a child's block is held locally, without fetching it;
  4. returns a key already split into parts, and the fact it encodes as
     entity, attribute, type and value;
  5. merges a blob record's fields into its entry's row.
- *"Side channel"* is used as if its cost were known. Tonk's reads
  blocks through the environment and decodes them with dialog's own
  public `inspect_*` functions. What goes wrong because of that is not
  written anywhere in the topic, and it is what the criteria will come
  from.

**Decided.** Not decided. The statement is recorded in the top story as
offered. The next question is what goes wrong because an embedder has a
side channel.

## 2026-11-12, Delimiting: what goes wrong with the side channel

**Asked.** What goes wrong because an embedder has a side channel? The
maintainer: "What exists today was a hastily put together bandaid that
we're struggling to detangle from the embedder code."

**Learned.** The harm is the braid, not the reading of blocks. What the
reader is woven into in tonk, each checked:

- the worker: `resolve_formula` answers `tree/*` by name;
- the query wire format: `Predicate::Formula(String)` exists for it, and
  the worker's `intent` operation now rides the same arm;
- the page bundle: `<tonk-tree>` is Rust registered by `tonk-guest`;
- display routing: `<tonk-display>` names `tonk-tree` among the elements
  it forwards a space's context to.

So the objective under the first statement is that showing the tree need
not be part of the embedder's host at all, which holds only if a
consumer that can do nothing but query can get what it draws. That gave
the draft in the top story. It keeps three of the five things tonk's
reader does (start from the branch, a child with its details, a key in
parts) and leaves two out pending the maintainer: local-or-remote, and
blob fields on an entry's row. "A number of reads that does not grow
with the tree" is there because a walk of the whole tree to show one
node's children was measured on the draft branch and is what a page
cannot afford on a tree held partly on a remote.

**Decided.** Not decided: the draft awaits the maintainer.

## 2026-11-12, Delimiting to Directing: the statement is accepted, and a constraint surfaces

**Asked.** Is the draft the problem, and what in it is wrong or missing?
The maintainer: yes to "a number of reads that does not grow with the
tree"; yes to consumers beyond the inspector, "any embedding should be
able to inspect the tree where now it's done through custom machinery";
the two things left out are needed. On the first of them: "I'm not sure
there is a clean way to accomplish this without undermining the
resolver's idempotence claim, because the same query may return a
different result depending on whether the node has been downloaded or
not. If you can help me find a way to do it without undermining a
property I'd be delighted. Otherwise I may have to let this property
go."

**Learned.** *What does that word mean?* Two different properties are
going by one name:

- **Same rows.** The same tree query yields the same rows whenever it is
  asked.
- **Permanent rows.** A row, once yielded, is never wrong.

The module that makes the claim argues only the second, and says what it
is for (`rust/dialog-corpus/src/inspect.rs`, "Why this is sound under
differential subscriptions"): rows derived through a `Load` "can become
unnecessary, never wrong, so no invalidation machinery is required for
them". It names a locality probe as the thing that would break it.

The first property does not hold today. A resolver whose block is
neither held nor reachable yields no rows (`let Some(block) = loaded
else { continue }` in `rust/dialog-query/src/resolver.rs`); asked again
once the block can be had, it yields them. Nothing false was said in
between, which is the second property holding while the first does not.

That suggests where a way through might be, entered in the ledger as a
hypothesis and not examined yet: "this block is not held" is a claim
that turns false when the block arrives, and is what breaks permanence;
"here are this block's rows", yielded by a read that does not fetch
(`Load::locally` exists), is permanent, and a consumer that sees no rows
for a child its parent names has learned the child is not held without
the engine having said so. What that leaves unanswered is how a
subscriber is told to look again when a block arrives, which is news of
more rows, not a withdrawal of any.

**Decided.** The problem statement is accepted as it now reads in the
top story. Phase moves to Directing. The constraint is not yet a
criterion: which of the two properties it is has been asked.

## 2026-11-12, Directing: what "idempotent" means here, and three ways to say what is held

**Asked.** Which property is to be kept: the same rows always, or a row
once yielded never wrong? The maintainer: permanence "may suffice,
although that seems far more confusing to me than thinking of
idempotence", and then what they had actually meant:

- A tree read fetches whatever blocks it needs, "meaning behavior will
  be unobservable from having those blocks vs not".
- "The reason we required node as opposed to allowing scan was precisely
  so that you would not download the whole tree by a mistake."

And three ways they see to tell what is held:

1. Capture partiality in what is queried, so the same revision says it
   has no node where the replica lacks one, and a command that fetches a
   block changes that, so asking again answers. "Very complicated and
   I'm not even sure possible at all."
2. Pretend the whole tree is local and be deliberate about not asking
   for what would download. Then nothing can be asked about locality.
3. A resolver that takes a time, and says whether a block was local at
   that time; commands update the time and a rule passes it in. "None of
   this is ideal."

**Learned.** The glossary now has the maintainer's meaning of
*idempotent*; the agent's split into "same rows" and "permanent rows"
was the agent's and is dropped as a framing.

*Those two cannot both be true.* On the draft branch a resolver whose
node is open walks from the root and loads every block, fetching what is
not held. That is the download by mistake that requiring the node was
there to prevent. The maintainer asked for listing ("No it should list")
before saying why the node was required; the two are now both on record
and one has to give.

Ways 1 and 3 are one idea: make what the answer depends on an input, so
the read stays a function of its inputs. They differ in the input. Time
moves when nothing was fetched, and needs commands to move it. What the
answer depends on is what the replica holds, and dialog already treats
one such thing this way: the module's argument says the only mutable
fact in the domain is the current root, "which reaches queries as an
ordinary tracked fact (`dialog.branch/tree`), never through this
command". What made way 1 look impossible is putting it in the
revision, which is shared and synced while holdings differ per replica.
Dialog has a place for what differs per replica: the `Replica` entity,
"this device's view of the repository", whose facts are synthesized when
a query runs and never stored.

That gives a fourth way, entered as an opinion of the agent's and not
examined: what a replica holds is a fact of the replica, read by a scan,
which the resolvers' own admission rule already says anything addressed
by mutable state must be. The resolvers stay as the maintainer means
them. A rule that asks whether a child is held before asking for its
details never fetches, and a scan that lists what is held, by walking
from the root through local reads only, could be the thing that makes a
rule over a resolver answerable with nothing given, without a resolver
ever listing.

Nothing in the fourth way has been checked against the code.

**Decided.** Not decided. The agent's "presence under a local read" from
the last entry is withdrawn: it has a tree read's rows depend on what is
held, which is what the maintainer means the resolvers not to do.
