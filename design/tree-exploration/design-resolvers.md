# What the query engine exposes for reading a node

Decision: which query steps read a search-tree node and what each
yields, for: a consumer that can only query cannot draw a branch's
search tree in a number of reads that does not grow with the tree (the
[top story](../tree-exploration.md)).

Not decided. Opened because the maintainer said, 2026-11-13: "current
ones are no good. I think something that allows us to obtain a block as
record or bytes and have a formula to parse it is probably better than
what we have now." What is no good about them has not been said by the
maintainer; the first column's cells are what the agent found, each
with where it was seen. Column 3 is there because one alternative is
not a choice.

Held-ness and fetching are out of this matrix; they are decided in
[design-held](design-held.md). Whether a consumer asks these steps
directly or through a concept is the strategy decision in
[direction](direction.md) and is not settled by this one.

| criterion \ approach | 1. Status quo: seven resolvers, each loads the block and decodes one part of it | 2. One step yields the block, formulas decode it (the maintainer's): the block as bytes or as a record; one formula reads it as an index node, another as a segment | 3. Three resolvers, one per thing a consumer asks: a node's header with its manifest; an index's spans; a segment's entries whole |
|---|---|---|---|
| reading and decoding are separate things | 🟡 braided: every resolver is "load, check the hash, decode one part, project" (`ResolverQuery::evaluate`); the decoders exist apart from it as functions (`dialog_corpus::inspect`) | 🟢 one step reads and checks; a formula is a function of bytes and reads nothing | 🟡 braided as today, in fewer places |
| what a consumer asks to draw one node | three or four steps over one node (`tree/node`, `tree/manifest`, then `tree/span` or `tree/key` + `tree/entry` + `tree/blob`), each loading the block. ? whether the loads after the first are served from a cache was not measured | one read, then the formulas it wants over the bytes already in hand | two steps: the header, then spans or entries |
| a segment's entry with its key, its metadata and its blob record | 🟡 three resolvers paired by position (`at`); tonk's reader merges them by hand (`segment_rows`, `blob_fields`) | whatever the segment formula's row is: ? one row per entry with all of it, or the same three-way split, is not said | 🟢 one row per entry |
| a key in readable parts | a formula per key (`dialog/key-part`), several rows per key | as today, or the segment formula yields them | as today, or the entries resolver yields them |
| adding a field, or a new way to read a block | 🟡 a field on a resolver is six edits: the struct, the cells, `parameters`, `with_parameters`, the projection, the tests' constructors (seen adding `entry` on the draft branch, `6817bb6`) | a field is one more output on a formula; a new reading is a new formula, and the read step does not change | as the status quo |
| bytes that are not a node, or not the kind asked for | no rows, no error: `tree/span` of a segment and `tree/key` of an index both answer nothing | the same, by the formula; a formula given any bytes at all has to hold up, since nothing says its input came from the read step | as the status quo |
| a block travelling as a value | never: a row carries fields | 🟡 the block is a value in the row, up to the size of a node (tonk's notes record one of about 150 KB); it reaches a page if a query selects it, as an array of numbers in tonk's JSON today | never |
| a tree read stays idempotent, node required | 🟢 on main | 🟢 the read step is the one content-addressed read; ? whether the same step also reads a spilled value, which lives in the blob store | 🟢 |
| names | 🟡 `tree/manifest` operands are snake_case (`fanout_n`); `spill` is an empty string for "none" | to be chosen | to be chosen |
| precedent in dialog | - | 🟢 a revision is one record-valued fact, and `dialog/revision` and `dialog/revision-parent` are formulas that project it, one row and one row per parent (`rust/dialog-query/src/formula/revision.rs`) | - |
| what has to be built | exists | one read step, about four formulas over `inspect`'s decoders, and the removal of seven resolvers | three resolvers in place of seven |
| who has to change | nobody | tonk's analyzer lists the resolvers by name and reads their operands (`resolver_registry.rs`), its guide documents them, and one CLI test uses `tree/node` | the same |
