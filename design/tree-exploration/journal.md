# A page cannot show a node's children with their details: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* What the tonk inspector shows and where its data comes from. What the seven resolvers and two formulas yield, and five things that held when a page's needs were tried against them. That a concept is read an attribute at a time and only `this` reaches a rule's body. What the draft branch changed and what it measured. The maintainer's statement of the problem: the query engine cannot be used to inspect the tree, so an embedder builds side channels. What tonk's side channel does that a query did not (five things, in the second entry). | *Where are we at?* Diagnosing. A problem statement was offered and is not yet accepted: it contradicts a fact in the ledger as worded, and its second half is a consequence whose cost is not written. |
| **Agenda** | *What do we need to know?* What goes wrong because an embedder has a side channel (asked, not yet answered). Then: which of the five things tonk's side channel does are the embedder's need and which are how it happened to be built. What "inspect" covers: who does it and what they are finding out (asked in the first sitting, not answered). Open hypotheses in the ledger: a record value reaching a page whole; why a supplied value cannot be pushed into a body. | *Where are we going?* Diagnosing, then Delimiting: the statement cut to unmet objectives and their cause. |

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
