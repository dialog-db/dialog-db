# Subscribe to tree edits

The first poll seeds a tree. Each later poll describes the change to
that tree as an ordered batch of inserts, local value updates, moves
and removes. Both a record consumer and a view host can use these
edits. A renderer can map a node ID to a mounted instance and move
that instance and its children together.

For a notebook, run standing queries for its notebooks, cards and
tags. Project a changed answer to one or more local nodes. The
notebook contributes the root, a card contributes its title and its
placement under the notebook, and a tag contributes a leaf under
the card. An empty notebook or card still has a node of its own.

An ID names an occurrence in this result and is independent of its
payload, sort key and parent. A query which displays the same entity
twice assigns two IDs. Changing a position can then be expressed as
`Move { id, to: { parent, before } }` without resending a subtree.
The sort key is a producer input, with ID as the tiebreaker. The
receiver needs only the destination and an existing sibling anchor.

The producer indexes nodes and sibling order. It reference-counts
identical contributions and refuses conflicting definitions of an ID
or a batch that would leave an orphan or cycle. The input queries must
include their own membership conditions, so children of a disappearing
parent either leave or move in the same batch. A batch is calculated
from the final contributions after the feeds reach one stable stack
stamp. Inserts and moves are ordered so their parents and anchors
exist when applied. Removal of a subtree is one edit. The receiver
keeps sibling links, so the position of an unchanged sibling need
not be recomputed on insert or move.

Use `Branch::subscribe_tree` or `Stack::tree_subscription` to combine
sources of one query type, such as `ConceptQuery`. The generic local
payload can carry a scalar, the kind of container, or the attributes
of a renderable node. The existing nested fold remains useful for a
single assembled answer. The prototype starts from explicitly defined
standing queries and does not yet lower notation `select` to such
queries or define a wire encoding.

The comparison test uses the same 512-card notebook for a tree
subscription and a nested fold. A tag addition must project one tag
row, emit one insert, and give the same answer as the fold and a
fresh select. Reparenting the card emits one move and keeps its tags.
The native test also prints an alternating-order poll comparison for
16 tag additions and retractions.
