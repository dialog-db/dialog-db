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
is folded into `tree/node`. **Reopened the same day in one respect:**
the accessors must serve a concept as well as a direct query, so a span
and an entry each carry an entity; what that needs is in the last
section, with three `?`.

## The set

Every accessor takes the handle as `of`, and `of` is required: an
accessor never lists.

| accessor | given | one row per | a row holds |
|---|---|---|---|
| `tree/node` | a node | the node | `kind` (`"index"` or `"segment"`), `size` (its block, in bytes), `count` (spans or entries), `scale`, `novelty`; and the format it was written under, every field of it: `version`, `fanout-bits` ?, `max-separator`, `inline-n`, `spill-prefix`, `max-segment`, `frame-ceiling-factor`, `anchor-selector`, `entry-overhead`, `key-overhead`, `link-overhead` |
| `tree/span` | an index node | each child | `span` (the row's own entity) ?, `at`, `node` (the child, a `node:` entity), `separator`, `until`, `rank`, `scale`, `novelty` |
| `tree/entry` | a segment node | each entry | `entry` (the row's own entity) ?, `at`, `key`, `rank`; its state and claim: `state`, `retraction`, `origin`, `edition`, `cause`, `collapsed`, `supersedes`; `spill` (an `asset:` entity); and of a blob record: `blob` (an `asset:` entity), `blob-size`, `blob-version`. A field that does not apply to an entry is absent, not zero or empty |
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
  ? The name. `fanout-n` reads oddly. The maintainer offered
  `fanout-factor`, 2026-11-14. *What does that word mean?* A tree's
  branching factor is its fanout, so `fanout-factor` would be taken for
  `2^n`, the confusion the name is meant to remove. `n` is how many bits
  of a key's hash decide a boundary, an exponent: `fanout-bits`, or
  `fanout-exponent`. The maintainer's to choose.
- An entity per entry and per span: **needed.** The agent had read the
  maintainer as not requiring a concept and wrote "none"; the maintainer
  struck that on 2026-11-14: "it should work through concepts, as in be
  complementary, independent of whether tonk-display is changed to view
  resolvers directly." A concept's row is identified by an entity, so
  `tree/span` yields one for each span and `tree/entry` one for each
  entry. ? What the entity is made of: the entry's key (the draft
  branch's `entry:`), or where the row sits (its node and position),
  which would let an accessor given the entity find the block.

## What working through concepts needs

Found by test on the draft branch, 2026-11-14
(`it_reads_only_the_nodes_a_fact_names`), and by what was learned on
2026-11-12:

- A rule over an accessor installs when another premise names the
  nodes. With a fact naming two nodes of a 21-node tree, the concept
  listed those two with two block reads and looked one up with one. So
  no accessor has to list, and the facts a space keeps about what it
  holds ([design-held](design-held.md)) are a natural namer: a concept
  of the nodes this replica is known to hold.
- 🔴 Asked by `this` for a node no fact names, the concept answered no
  row and read the node first: the planner runs the accessor (cost 200)
  before the fact that rules the node out. Where that fact means "held",
  the read is a fetch nobody asked for. The order has to be the other
  one. ? How: an accessor costed above any read of facts, or a rule the
  planner follows that a step which may fetch runs after every step
  that cannot.
- Only `this` reaches a rule's body. A node's own fields and the set of
  its spans are keyed by the node, one read. A span's or an entry's
  details keyed by the row's own entity need the block that row is in:
  ? from the entity itself, if it says where it sits, or from the nodes
  the facts name, which reads every named node (each decoded once, none
  fetched).
