# A consumer that can only query cannot draw a branch's search tree: glossary

- **idempotent** (of a tree read): the read behaves as if the whole tree
  were local. It fetches what it needs, so whether the replica held a
  block beforehand cannot be observed through it. The maintainer's
  meaning, 2026-11-12. The module that makes the claim words it as rows
  being permanent, "never wrong"
  (`rust/dialog-corpus/src/inspect.rs`); the two agree wherever a block
  can be reached.
- **held**: a block is held when the replica has it and a read of it
  needs no fetch. The pull design uses the word the same way
  ([held-across-pull](../held-across-pull.md)).
- **resolver**: a query step that reads by content address, so what it
  yields depends only on the address it is given.
- **scan**: a query step that reads mutable state, and records what it
  read so a subscription is re-run when that changes.
