# The accessor set

Decision: which accessors there are over a node's handle and what a row
of each holds, for: a consumer that can only query cannot draw a
branch's search tree in a number of reads that does not grow with the
tree (the [top story](../tree-exploration.md)), under what is already
decided: a node is read through accessors over its `node:` entity, the
environment decoding it once ([design-resolvers](design-resolvers.md)).

**Decided by the maintainer, 2026-11-14**, on the agent's draft of
2026-11-13, with two changes: nothing a node holds is hidden ("it's
really a way to expose node info to the query engine"), and the manifest
is folded into `tree/node`. One `?` is left, a name.

## The set

Every accessor takes the handle as `of`, and `of` is required: an
accessor never lists.

| accessor | given | one row per | a row holds |
|---|---|---|---|
| `tree/node` | a node | the node | `kind` (`"index"` or `"segment"`), `size` (its block, in bytes), `count` (spans or entries), `scale`, `novelty`; and the format it was written under, every field of it: `version`, `fanout-n` ?, `max-separator`, `inline-n`, `spill-prefix`, `max-segment`, `frame-ceiling-factor`, `anchor-selector`, `entry-overhead`, `key-overhead`, `link-overhead` |
| `tree/span` | an index node | each child | `at`, `node` (the child, a `node:` entity), `separator`, `until`, `rank`, `scale`, `novelty` |
| `tree/entry` | a segment node | each entry | `at`, `key`, `rank`; its state and claim: `state`, `retraction`, `origin`, `edition`, `cause`, `collapsed`, `supersedes`; `spill` (an `asset:` entity); and of a blob record: `blob` (an `asset:` entity), `blob-size`, `blob-version`. A field that does not apply to an entry is absent, not zero or empty |
| `tree/value` | an `asset:` | the value | `size`, `bytes` |
| `tree/block` | a node | the node | `bytes`: the block as stored |

Where a handle comes from: `dialog.branch/tree` carries the root as a
`node:` entity, and `tree/span` yields each child as one.

Unchanged beside them: the formulas `dialog/key-part` and
`dialog/separator-part`, which split a key or a separator into parts,
each part with its kind, its text and its bytes.

Gone: `tree/key`, `tree/blob` and `tree/manifest`, whose rows are in
`tree/entry` and `tree/node`.

## What it changes from today, and why

| | today | the draft | why |
|---|---|---|---|
| a segment's entries | three resolvers (`tree/key`, `tree/entry`, `tree/blob`) paired by `at` | one | use case 6 asks for an entry whole; tonk's reader merges the three by hand |
| `of` | a base58 string or 32 raw bytes | a `node:` entity (an `asset:` for `tree/value`) | the handle is the entity; `node:<base58>` is as easy to type as the string was |
| a spill, a blob record's hash | a base58 string that is empty for "none"; raw bytes | `asset:` entities, absent when there is none | they name content in the blob store, which is what `asset:` means; a row can bind a slot absent (`Match::bind_absent`) |
| operand names | `fanout_n`, `max_separator`, `inline_n`, `spill_prefix`, `max_segment`, `frame_ceiling_factor`, `anchor_selector` | kebab-case, as every other name in a query | one spelling |
| decoding | each resolver, from bytes | the environment, once | decided in [design-resolvers](design-resolvers.md) |

## How the open points were settled

- `tree/block`: in. Nothing is hidden.
- The three manifest fields no resolver yielded: in, for the same
  reason.
- `tree/manifest`: folded into `tree/node`. The maintainer does not
  recall why it was apart; `notes/tree-relations.md` gives no reason and
  this clone's history does not reach the commit that added it. With the
  environment decoding once, a longer row costs nothing a query does not
  ask for: a slot left out is not bound.
- The fact a key encodes: `dialog/key-part` yields each part's bytes as
  well as its text, so nothing is hidden; a consumer that wants the
  value typed has the type and the bytes.
- A blob record's row: one row, with the claim fields absent.
- `fanout`: the node stores `n`, and what is stored is what is exposed.
  ? The name: `fanout-n` says it is the parameter and reads oddly;
  `fanout` would be taken for `2^n`.
- An entity per entry and per span: none. This is the agent's reading
  of the maintainer's "a way to expose node info to the query engine",
  not something the maintainer said: an accessor yields rows, and a
  space that wants a concept of nodes builds it with a rule and gives
  its rows whatever entity it chooses. The draft branch's `entry:`
  entity goes. To be struck if the maintainer reads it otherwise.
