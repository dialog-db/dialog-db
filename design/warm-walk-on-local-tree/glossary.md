# A replica that holds every block still pays to warm it: glossary

- **Hint.** A selector and a likelihood an evaluation puts on the
  environment's preload queue, saying a range is worth having local soon.
  It is a description: no future, no environment.
- **Driver.** The `Driven` stream that wraps an evaluation and, while it
  is polled, pops hints and runs a warm walk for each.
- **Warm walk.** `warm_source`: a level-by-level traversal of a hinted
  key range from a head's root, whose loads hydrate what is missing. Its
  only product is blocks made local. It is counted by the `warm_walk`
  span, and each node it visits by `warm_node`.
- **Advisory.** Of code whose result no read depends on: if it does not
  run, or ends early, every read still returns the same rows, and a
  demand read fetches whatever it needed. The warm walk is advisory.
- **By reference.** Of a block a head names and the local archive does
  not hold: it is fetched when a read first asks for it.
- **Wholly local.** Of a tree, in one repository's archive: every block
  reachable from its root is in that archive. Spilled values and blobs are
  not part of it, since a warm walk does not touch them.
- **Partial replica.** A replica whose head is not wholly local: it
  adopted a head, or part of one, by reference.
- **Member.** In the perf scenario's sense: a branch that tracks a remote
  and whose head it may or may not hold. The word says nothing about what
  is local.
- **Recorded tree.** A tree hash in the record of wholly local trees
  (column D). Recorded implies wholly local only through the writers'
  rules (H1); being wholly local does not imply recorded.
- **Base tree.** The tree a commit applies its instructions to. Not the
  same as the parent revision's tree in a pull, where the tree a merge
  rewrites can be the upstream's.
