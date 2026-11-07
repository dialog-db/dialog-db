# Glossary: collaborative text editing

- **conflict unit**: the smallest piece of text a concurrent write can
  lose. Today, one text field: with last writer wins, the whole field.
- **text CRDT library**: a library that merges concurrent text edits
  without coordination. Named generically on purpose.
- **write amplification**: bytes stored and replicated per edit, beyond
  the edit itself. Two parts: the **commit cost** (each commit writes at
  least a new root with its buffer; cascades rewrite more when a buffer
  overflows; old nodes are never reclaimed) and the **payload copy** (a
  value holding a whole document is copied on every change).
- **mutable resource**: a document Dialog can reference and read as text
  whose content changes in place by merging, unlike an asset, which is
  hash-addressed and immutable.
- **transport**: where a resource's changes travel: Dialog's facts,
  Dialog's sync engine outside the tree, or another sync engine. Replaces
  "side lane", which conflated the last two.
