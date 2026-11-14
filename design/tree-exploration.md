# A consumer that can only query cannot draw a branch's search tree

Phase: **Designing.** See the [design index](README.md) for what the
phases mean, and the [journal](tree-exploration/journal.md) for where the
topic stands.

## Description

Tonk has a tree inspector, `<tonk-tree>`: a page that shows the search
tree behind a branch as an outline of nodes, each row labelled by the
separator its parent holds for it, with the node's kind, size and count
beside it, and a pane listing a segment's entries with their keys split
into coloured parts. It is Rust compiled into tonk's page bundle, and it
gets its data from three operations the tonk worker answers by hand
(`tree/node`, `tree/child`, `tree/entry` in tonk's
`rust/dialog-reactor/src/formula.rs`), outside dialog's planner. Each
returns whole rows shaped for the page: `tree/child` returns one row per
child of a node carrying the child's position, its separator already
split into parts, whether its block is held locally, and, when it is, the
child's own kind, size and count.

The maintainer wants the inspector to be a component a space installs,
written as branch data like tonk's `<code-editor>`, reading through what
dialog's query engine offers instead of through those operations.

Dialog has seven resolvers over the tree (`tree/node`, `tree/span`,
`tree/key`, `tree/entry`, `tree/value`, `tree/blob`, `tree/manifest`) and
two formulas that split a key or a separator into parts. A resolver takes
a node and yields rows of plain values, one value per column. Tried
against a real space, these held:

- A resolver heads a query when its node is written as a constant.
- A tonk document of two expressions does not pass a value from one into
  a resolver in the next; the second yields nothing, with no error.
- A formula cannot head a tonk query, so the two key formulas are
  reachable only inside a rule.
- A rule joins resolvers, including recursively from the branch's root.
  A rule whose body needs its node given was refused at install.
- A rule's rows take the entity they are given as `this`; resolver rows
  had none, and 20 children of a root derived onto the branch's entity
  came back as one row whose fields were from different children.

A branch in dialog (`worker/affectionate-pascal-iey41u`) then changed
the resolvers: a node and an entry are entities, and a resolver whose
node is left open walks from the branch's root and yields every node, so
a rule over one installs. On that branch a rule keyed by a node reads
that node's block alone, and so does a rule that lists a node's children
as a many-valued field of the node. The same children as rows keyed by
the child, asked for by their parent, came back right and read every
block of the tree.

A concept is read one attribute at a time and joined on the entity, and
of what a caller supplies only `this` reaches a rule's body. So the
details a parent's block holds about each child (position, separator,
rank, buffered writes) come back either as separate sets with nothing
pairing them, or as rows that cost a walk of the tree.

The maintainer says the question being asked was the wrong one: not how
to make the resolvers serve the inspector, but what the query engine
should offer for exploring a search tree at all.

## Problem

Accepted by the maintainer, 2026-11-12, with the last two needs added at
their word:

A consumer that can only query (a page component stored as branch data,
a command line, an agent, any embedding) cannot draw a branch's search
tree. Starting from the branch, it cannot read a node's children
together with what their parent holds about each, nor whether each
child's block is held by the replica without fetching it, nor an entry's
key in readable parts with the blob record that belongs to it, in a
number of reads that does not grow with the tree. So the one embedder
that shows the tree reads blocks itself, through machinery of its own,
woven into its worker, its query wire format, its page bundle and its
display routing, where it cannot be changed or removed on its own.

A constraint the maintainer holds and would give up only if no way is
found to keep it: what a tree read yields stays permanent (see the
journal's fourth entry for what that word has to mean).

As the maintainer first gave it:

> The dialog query engine cannot be used to inspect the underlying tree,
> which causes the embedder to implement side channels to do it.

## Approach

Decided over 2026-11-12 to 2026-11-14; each decision's matrix is beside
this file.

**A node is an opaque thing with a handle, and accessors read parts of
it** ([design-resolvers](tree-exploration/design-resolvers.md)). The
handle is the node's entity, `node:<hash>`. An accessor is a query step
that takes the handle, asks the environment for the decoded node, and
yields one part of it as rows. The environment reads, checks and decodes
a node once and keeps it, as it already does for the tree; no accessor
decodes, and no block travels in a row unless a query asks for its
bytes. What a node points at in the blob store is an `asset:` entity.

**Five accessors expose everything a node holds**
([design-accessors](tree-exploration/design-accessors.md)): `tree/node`
(what it is and the format it was written under), `tree/span` (an
index's children with what the node holds about each), `tree/entry` (a
segment's entries whole), `tree/value` (a spilled value) and
`tree/block` (the raw block). Each requires its handle, so no query
reads a tree it was not pointed at.

**What a replica holds is learned and changed by commands, not by
queries** ([design-held](tree-exploration/design-held.md)). One command
asks whether a node's block is local; one fetches it. Dialog handles
both and answers each with a second command; a rule in the space decides
whether the answer is kept and where. A consumer asks before it reads,
so a read never fetches by surprise, and an accessor's answer never
depends on what happened to be local.

**A consumer asks the accessors directly, or through a concept; both
work** ([direction](tree-exploration/direction.md), column 7 beside
columns 2 and 3). The maintainer, 2026-11-14: the two are
complementary, and working through concepts does not depend on an
embedder learning to ask an accessor directly. A rule over an accessor
installs when another premise names the nodes, and the facts a space
keeps about what it holds can be that premise. Three things are open
for the concept path ([design-accessors](tree-exploration/design-accessors.md),
last section): what a span's and an entry's entity is made of, how a
row's details are reached by that entity, and the planner running an
accessor before the fact that would rule its node out.

Not taken on here:

- A way for rules to write to the overlay. Wanted by the maintainer;
  until it exists a rule that keeps a command's answer commits it, and
  the fact must name the replica it is true of.
- Goal-directed evaluation of recursive rules
  (`notes/goal-directed-fixpoint.md`). The approach does not need it.
- The embedder's side: a tonk page cannot send a query step that is not
  a concept, and tonk's tree reader is still woven into its worker, wire
  format, page bundle and display routing. Removing it is tonk's work,
  made possible by this.

What has to be built in dialog, for whoever builds it:

1. The `inspect` decoders take a decoded node instead of bytes.
2. The accessors ask `nodes::Read<CorpusNode>` and project; `tree/key`,
   `tree/blob` and `tree/manifest` go; names become kebab-case; a slot
   that does not apply is bound absent.
3. `dialog.branch/tree` and `tree/span`'s `node` are `node:` entities
   (on the draft branch); a spill and a blob hash are `asset:` entities.
4. A built-in reaction to a dispatched command that performs an effect
   and dispatches a command in answer; the probe and fetch commands on
   top of it.

Of the draft branch `worker/affectionate-pascal-iey41u`: the `node:`
entity stays; an entity for an entry stays in some form, to be decided
with a span's; listing when no node is given comes out.
