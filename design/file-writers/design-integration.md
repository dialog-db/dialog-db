# Design: what a writer puts in its extent, and what integration does

**Decision:** whether a writer appends the file's final form (frames and
tokens) to its extent, so that integration only indexes, or whole blocks,
which integration converts into the final form.

**For:** writers never wait on one another, a space is one file, and a
cell past its capacity spills to a block its slot names
([the problem](../file-writers.md#problem)).

| criterion \ approach | Final form in the extent: the writer splits its nodes into frames and tokens before appending, as a commit does today; integration indexes and moves nothing | Whole blocks in the extent: the writer appends blocks as they are; integration splits them into frames and tokens, appends that, and frees the extent's space once nothing can still read it |
|---|---|---|
| what a commit pays | 🟡 splitting, about 60% of an append today | 🟢 writing whole blocks only |
| what integration does | indexes the extent's records | 🟡 splits every block, appends the final form, indexes it, frees the extent |
| records move | 🟢 never: an offset stays valid for good | 🔴 every record in an extent moves once |
| a spill its slot names by offset | 🟢 stays valid | 🔴 left pointing at freed space; the slot names a spill by hash instead, resolved through the index, or integration rewrites the slot under the cell's lock when it moves the block |
| reusing an extent's space | nothing to reuse: extents keep what was written | only after a sync point and once no reader can hold an old offset, as the file's freed index pages are reused today |
| a token two writers introduce at once | 🟡 written by both; the index names one of them | written once, by integration |
| frames against their predecessor | against a predecessor already indexed, or the writer's own earlier node | against any predecessor, integration seeing them all |
| what the file holds at rest | the final form, with tokens a few writers wrote twice | the final form, compact |
| what exists to build on | 🟢 today's append, unchanged, into an extent | 🟡 today's append, run by the integrator; moving records and reusing extents are new |
