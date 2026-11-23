# What the query engine exposes for reading a node

Decision: which query steps read a search-tree node and what each
yields, for: a consumer that can only query cannot draw a branch's
search tree in a number of reads that does not grow with the tree (the
[top story](../tree-exploration.md)).

**Decided by the maintainer, 2026-11-13: column 4.** A node's entity is
its handle; accessors are steps that ask the environment for the decoded
node and project one part. The scheme of the handle is decided below; the accessor
set is drafted in [design-accessors](design-accessors.md).

Opened because the maintainer said, 2026-11-13: "current
ones are no good. I think something that allows us to obtain a block as
record or bytes and have a formula to parse it is probably better than
what we have now." What is no good about them has not been said by the
maintainer; the first column's cells are what the agent found, each
with where it was seen. Column 3 is there because one alternative is
not a choice.

The maintainer's model, given 2026-11-13: nodes as opaque resources, in
the sense of the wasm component model, with accessors that read the
parts a query needs; a bad query getting a whole node as bytes is
acceptable; what is not wanted is every accessor decoding the bytes
again. The maintainer asked for a recommendation; it is column 4, and
the last row is the one they named.

Held-ness and fetching are out of this matrix; they are decided in
[design-held](design-held.md). Whether a consumer asks these steps
directly or through a concept is the strategy decision in
[direction](direction.md) and is not settled by this one.

| criterion \ approach | 1. Status quo: seven resolvers, each loads the block and decodes one part of it | 2. One step yields the block, formulas decode it (the maintainer's): the block as bytes or as a record; one formula reads it as an index node, another as a segment | 3. Three resolvers, one per thing a consumer asks: a node's header with its manifest; an index's spans; a segment's entries whole | 4. Accessors over a node handle (the maintainer's model, 2026-11-13, as the agent recommends building it): the node's entity is the handle; each accessor is a step that asks the environment for the decoded node and projects one part |
|---|---|---|---|---|
| reading and decoding are separate things | 🟡 braided: every resolver is "load, check the hash, decode one part, project" (`ResolverQuery::evaluate`); the decoders exist apart from it as functions (`dialog_corpus::inspect`) | 🟢 one step reads and checks; a formula is a function of bytes and reads nothing | 🟡 braided as today, in fewer places | 🟢 reading, checking and decoding are the environment's node layer, which already does them for the tree (`nodes::Read<CorpusNode>`); an accessor only projects |
| what a consumer asks to draw one node | three or four steps over one node (`tree/node`, `tree/manifest`, then `tree/span` or `tree/key` + `tree/entry` + `tree/blob`), each loading the block. ? whether the loads after the first are served from a cache was not measured | one read, then the formulas it wants over the bytes already in hand | two steps: the header, then spans or entries | the accessors it wants, as today; the node is read and decoded once however many are asked, by the environment's node cache |
| a segment's entry with its key, its metadata and its blob record | 🟡 three resolvers paired by position (`at`); tonk's reader merges them by hand (`segment_rows`, `blob_fields`) | whatever the segment formula's row is: ? one row per entry with all of it, or the same three-way split, is not said | 🟢 one row per entry | 🟢 one accessor, one row per entry with all of it |
| a key in readable parts | a formula per key (`dialog/key-part`), several rows per key | as today, or the segment formula yields them | as today, or the entries resolver yields them | a formula per key, as today: a key is small and is plain bytes |
| adding a field, or a new way to read a block | 🟡 a field on a resolver is six edits: the struct, the cells, `parameters`, `with_parameters`, the projection, the tests' constructors (seen adding `entry` on the draft branch, `6817bb6`) | a field is one more output on a formula; a new reading is a new formula, and the read step does not change | as the status quo | a new accessor is a new step; ? whether the six edits per field can be brought down is a matter of how accessors are declared, not decided here |
| bytes that are not a node, or not the kind asked for | no rows, no error: `tree/span` of a segment and `tree/key` of an index both answer nothing | the same, by the formula; a formula given any bytes at all has to hold up, since nothing says its input came from the read step | as the status quo | the node layer answers "no such node"; an accessor for the other kind of node answers no rows |
| a block travelling as a value | never: a row carries fields | 🟡 the block is a value in the row, up to the size of a node (tonk's notes record one of about 150 KB); it reaches a page if a query selects it, as an array of numbers in tonk's JSON today | never | never, unless an accessor for the raw bytes is offered, which the maintainer does not mind |
| a tree read stays idempotent, node required | 🟢 on main | 🟢 the read step is the one content-addressed read; ? whether the same step also reads a spilled value, which lives in the blob store | 🟢 | 🟢 the node layer's read is the tree's own; `tree/value` stays the one step that reads the blob store |
| names | 🟡 `tree/manifest` operands are snake_case (`fanout_n`); `spill` is an empty string for "none" | to be chosen | to be chosen | to be chosen |
| precedent in dialog | - | 🟢 a revision is one record-valued fact, and `dialog/revision` and `dialog/revision-parent` are formulas that project it, one row and one row per parent (`rust/dialog-query/src/formula/revision.rs`) | - | 🟢 the tree itself reads nodes this way since #755; the topic [tree-node-reads](../tree-node-reads.md) decided that caching and decoding belong to providers |
| what has to be built | exists | one read step, about four formulas over `inspect`'s decoders, and the removal of seven resolvers | three resolvers in place of seven | the decoders in `inspect` take a node instead of bytes; the steps ask `nodes::Read` instead of `archive::Load`; the accessor set is redrawn |
| who has to change | nobody | tonk's analyzer lists the resolvers by name and reads their operands (`resolver_registry.rs`), its guide documents them, and one CLI test uses `tree/node` | the same | the same |
| every accessor decodes the block again | 🔴 yes: each resolver loads bytes, copies them (`into_vec`) and each `inspect_*` builds a node from them (`CorpusNode::try_from(Buffer::from(bytes))`); tonk's reader does the same with clones | 🔴 yes, per formula, unless each is marked cached, and the cache is then keyed by hashing the whole block per call | 🔴 as the status quo | 🟢 once per node while the environment keeps it |

## The handle's scheme

The maintainer asked, 2026-11-13, whether a block can be read through an
`asset:` URL, and if so whether a node's entity should be
`asset:<hash>`.

What `asset:` is today: the entity of content kept in an archive's blob
store (`Entity::from_blob`, `Asset::entity`), read through the blob
lane (`blob::Read`, `blob::Load`), and carrying a fact that a branch
holds it (`Asset::fact`). A tree node's block is not there: it is kept
in the archive's index catalog (`ArchiveScope::index`) and read through
the node layer. So a node's hash asked for through the blob lane is not
found, and no asset fact names it.

| criterion \ scheme | `node:<hash>` (the draft branch) | `asset:<hash>` for nodes as well |
|---|---|---|
| what the handle says about where its bytes are | which store: `node:` is the index catalog, `asset:` the blob store | 🟡 nothing: an accessor given `asset:` must try both stores or be told which |
| what else is true of the entity | nothing is claimed | 🟡 an `asset:` entity has a size fact when a branch holds it, and is readable as a blob; neither holds of a node |
| one scheme for everything addressed by its hash | 🟡 two schemes | 🟢 one |
| things a node points at in the blob store (a spilled value, a blob record) | are `asset:`, as they are everywhere else in a branch | are `asset:` |

**Decided by the maintainer, 2026-11-13: `node:` for nodes**, and
`asset:` for what a node points at in the blob store, so `tree/value`
takes an `asset:` and a blob record's hash is yielded as one.
