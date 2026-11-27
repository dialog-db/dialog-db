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
single assembled answer.

## Notation and HTML

`Live::standing_tree` lowers a keyed record select into separate
concept subscriptions for its required rows and distinct collections.
A nested call must return a record containing its entity key and link
to its parent through one named field. Only the selected roots and
their descendants are published. Results from independent feeds can
stay dormant until the parent appears. The output applies to an
`EditedTree<Value>`; `RecordNode::decode` describes the local value
as a record, a set, or a scalar. Unsupported forms are refused by
the tree API and can still use the ordinary select API.

`Live::watch_html` adapts the existing incremental view patches to
the same edit operations. A new chip inserts its element and text
children. `HtmlMount` applies edits to an `HtmlHost`; the WASM
`BrowserHost` applies them to the DOM and delegates command listener
binding to the embedder. The mount preserves a node handle on a
generic `Move`. The legacy view plan names instances by their parent
path, so moves across parents in that plan still appear as removals
and inserts. Only mount frames from a trusted notation producer.

## Binary frames

Keep one `EditEncoder` and `EditDecoder` per stream. Each versioned
frame has a sequence number and an operation count. Short strings
and IDs receive dense dictionary indices and can be referenced in
later frames, including strings inside local composites. The
dictionary stops growing at its bound; larger or unregistered
strings can travel inline. This uses corpus scalar encodings and
no JSON. A truncated frame or sequence gap leaves the decoder
unchanged. Reconnect by replaying the missing frames in order,
or restart both endpoints with an empty mount and dictionary.
A host failure during application calls for a new seed.

The comparison test uses the same 512-card notebook for a tree
subscription and a nested fold. A tag addition must project one tag
row, emit one insert, and give the same answer as the fold and a
fresh select. Reparenting the card emits one move and keeps its tags.
The native test also prints an alternating-order poll comparison for
16 tag additions and retractions.
