# The accessor set

Decision: which accessors there are over a node's handle and what a row
of each holds, for: a consumer that can only query cannot draw a
branch's search tree in a number of reads that does not grow with the
tree (the [top story](../tree-exploration.md)), under what is already
decided: a node is read through accessors over its `node:` entity, the
environment decoding it once ([design-resolvers](design-resolvers.md)).

Not decided. A draft by the agent, 2026-11-13, for the maintainer to
mark up. A `?` is a point the draft could not settle.

## The draft

Every accessor takes the handle as `of`, and `of` is required: an
accessor never lists.

| accessor | given | one row per | a row holds |
|---|---|---|---|
| `tree/node` | a node | the node | `kind` (`"index"` or `"segment"`), `size` (its block, in bytes), `count` (spans or entries), `scale`, `novelty` |
| `tree/manifest` | a node | the node | the format it was written under: `version`, `fanout`, `max-separator`, `inline`, `spill-prefix`, `max-segment`, `frame-ceiling-factor`, `anchor-selector` |
| `tree/span` | an index node | each child | `at`, `node` (the child, a `node:` entity), `separator`, `until`, `rank`, `scale`, `novelty` |
| `tree/entry` | a segment node | each entry | `at`, `key`, `rank`; its state and claim: `state`, `retraction`, `origin`, `edition`, `cause`, `collapsed`, `supersedes`; `spill` (an `asset:` entity, absent when the value is inline); and when the entry is a blob record: `blob` (an `asset:` entity), `blob-size`, `blob-version` |
| `tree/value` | an `asset:` | the value | `size`, `bytes` |
| `tree/block` ? | a node | the node | `bytes`: the block as stored |

Where a handle comes from: `dialog.branch/tree` carries the root as a
`node:` entity, and `tree/span` yields each child as one.

Unchanged beside them: the formulas `dialog/key-part` and
`dialog/separator-part`, which split a key or a separator into parts.

Gone: `tree/key` and `tree/blob`, whose rows are in `tree/entry`.

## What it changes from today, and why

| | today | the draft | why |
|---|---|---|---|
| a segment's entries | three resolvers (`tree/key`, `tree/entry`, `tree/blob`) paired by `at` | one | use case 6 asks for an entry whole; tonk's reader merges the three by hand |
| `of` | a base58 string or 32 raw bytes | a `node:` entity (an `asset:` for `tree/value`) | the handle is the entity; `node:<base58>` is as easy to type as the string was |
| a spill, a blob record's hash | a base58 string that is empty for "none"; raw bytes | `asset:` entities, absent when there is none | they name content in the blob store, which is what `asset:` means; a row can bind a slot absent (`Match::bind_absent`) |
| operand names | `fanout_n`, `max_separator`, `inline_n`, `spill_prefix`, `max_segment`, `frame_ceiling_factor`, `anchor_selector` | kebab-case, as every other name in a query | one spelling |
| decoding | each resolver, from bytes | the environment, once | decided in [design-resolvers](design-resolvers.md) |

## Open points

- ? `tree/block`. The maintainer does not mind a query getting a whole
  node as bytes; nothing in the use cases asks for it. In or out.
- ? `fanout`. The node stores a parameter `n` and the fanout is `2^n`.
  Today's resolver yields `n` as `fanout_n`; tonk's reader shows `2^n`.
  Which one the accessor yields.
- ? The manifest has three more fields the resolver never yielded
  (`entry-overhead`, `key-overhead`, `link-overhead`,
  `ManifestSummary` in `rust/dialog-corpus/src/inspect.rs`). In or out.
- ? `tree/manifest` apart from `tree/node`, or folded into it. Apart
  keeps a node's row short; folding is one accessor fewer. Decoding no
  longer argues either way.
- ? The fact a key encodes (use case 6). `dialog/key-part` yields a
  key's parts as text: the entity, the attribute, the value's type, the
  value. Tonk's reader also yields the value typed. Whether text is
  enough for a consumer that only shows it.
- ? An entity per entry and per span. Needed only if a row has to be the
  subject of a concept, which is the question still open in
  [direction](direction.md); asked directly, a row needs none. The
  draft branch's `entry:` entity waits on that.
- ? `tree/entry` on a blob record. Today the claim fields of such a row
  are empty and its blob fields come from a second decode
  (`inspect_blob_records`); whether one row can hold both without the
  claim fields reading as a claim that is not there.
