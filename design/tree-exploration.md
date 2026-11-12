# A page cannot show a node's children with their details through the tree resolvers

Phase: **Diagnosing.** See the [design index](README.md) for what the
phases mean, and the [journal](tree-exploration/journal.md) for where the
topic stands.

The title is a symptom and will change when the problem is stated.

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

As the maintainer gave it, 2026-11-12, not yet accepted as the statement:

> The dialog query engine cannot be used to inspect the underlying tree,
> which causes the embedder to implement side channels to do it.

Two things stand between this and a statement (see the journal): the
engine does answer tree queries today, so what it cannot do needs
saying; and what goes wrong because an embedder has a side channel is
not yet written.
