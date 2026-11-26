# One stored format for rules, concepts and attributes: glossary

- **Stored format.** What a replica holds that describes a rule, a
  concept or an attribute: the bytes of a rule body, the facts that index
  it, the facts that name a concept, and the identities those facts are
  keyed by. Not the tree's layout, which is storage.
- **Format** (of a rule body). The number on a body's envelope,
  `{ "format": N, "rule": .. }` (`rule/body.rs`). It says how to read
  the bytes. It is not in any identity.
- **Identity.** The merkle reference a rule, a concept or an attribute is
  named by, as an entity: `rule:..`, `concept:..`, `the:..`. A function
  of the definition's parts, not of the format it is stored in.
- **Legacy spelling.** A spelling some release wrote that the current
  one reads and does not write. Listed in the [inventory](inventory.md).
- **Refused by name.** A legacy spelling the current release recognizes
  in order to refuse it with a message naming it, rather than read it
  with another meaning or ignore it.
- **O, N.** A replica on 0.5.0, the latest tag, and one on the next
  release, syncing one space.
- **Keep, prune.** An upgrade keeps when it asserts what it rewrote
  beside the old facts and retracts nothing; a prune is the later,
  separate retraction of the old facts, run when the embedder decides.
- **Vocabulary.** The attribute names rule facts and markers are stored
  under: today `dialog.rule/source`, `conclusion`, `reads`, `derives`,
  `induces`, `on` and `dialog.concept/transient`.
- **Stranded marker.** A `dialog.concept/transient` fact under an
  identity no release of the space reads any more.
