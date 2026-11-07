# Glossary: collaborative text editing

- **conflict unit**: the smallest piece of text a concurrent write can
  lose. Today, one text field: with last writer wins, the whole field.
- **text CRDT library**: a library that merges concurrent text edits
  without coordination. Named generically on purpose.
- **write amplification**: bytes stored and replicated per edit, beyond
  the edit itself. Two parts: the **spine cost** (every commit rewrites a
  path of search tree nodes, never reclaimed) and the **payload copy**
  (a value holding a whole document is copied on every change).
- **side lane**: storage and replication for text that runs beside
  Dialog's index rather than through it.
