# A query or a click waits on blocks the replica does not hold: glossary

Terms this topic needs. Each is used one way throughout.

- **Held by reference.** A subtree the replica knows only by the hash in
  its parent's link, with none of its blocks in the local archive. The
  term is `notes/version-control.md`'s.
- **Partial replica.** A replica whose head names a tree some of whose
  blocks it holds only by reference. The request also says "incomplete
  tree" for the tree such a replica has; this topic says partial replica
  for the replica and names the tree by its revision.
- **Stale replica.** A replica whose head is an older revision than its
  upstream's. Not the same thing as partial, and the two are independent:
  a replica can hold every block of an old revision (stale, not partial),
  or hold the newest revision by reference (partial, not stale). A pull
  that adopts by root turns the first into the second.
- **Unknown range.** The key range of one index (entity, attribute or
  value order) that a subtree held by reference covers: from its link's
  separator to the next link's. What the replica can say about that range
  is that it cannot say.
- **Demand cover.** The set of index key ranges an evaluation of a query
  read, as a subscription records it (`branch/subscription.rs`). A scan
  that came back empty is in it.
- **Cell.** One `(entity, attribute)` pair and the claims stored for it.
  The term is the code's (`transaction/succession.rs`).
- **Blind write.** A write minted without reading the cell it writes to.
  Coined here. On main the writes to the attribute and value indexes and
  to history are already blind; the write to the entity index is not.
- **Reconcile.** Left undefined on purpose. The request uses it for what
  happens to a transaction made against a partial replica "later". It
  could mean the merge a pull already does, or minting a later record that
  supersedes what the blind write did not read, or running again the rules
  the transaction could not evaluate. These are three different things;
  the word is not used in this topic until one is meant.
