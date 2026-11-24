# One stored format for rules, concepts and attributes: plan

The order the recommended approach (F, [direction](direction.md)) would
land in, each step a pull request that leaves main releasable. Not
decided: the maintainer chooses the approach first. Steps 1 to 6 have
to be on main before the next tag, or a live space moves twice.

1. **Export what an embedder's migration needs.** In
   `dialog_query::migration`: the identity of an attribute and a concept
   as each generation spelled it (v0 and v1 exist; tonk uses v0 only),
   and a constant naming the current identity generation. Nothing stored
   changes. Releasable, and useful to tonk for the 0.5.0 move whatever
   else is decided: with it tonk's `rekey` can move what it keyed by a
   0.5.0 identity, the 88 stranded markers included if `rekey` covers `dialog.concept/transient` (it rewrites claims about a moved identity, T3).

2. **Tests that pin what O and main do.** A marker on a concept no rule
   concludes, after `upgrade_rules`; a body in a later format indexed
   under a relation, read by a query on main (skips and lists). F1 is
   pinned already by a run on v0.5.0, recorded in the journal. Tests
   only.

3. **One format: 5 for every rule.** `TERMS_FORMAT` and `format_for` go.
   If the maintainer moves identities (open question 1), here, once, with
   the generation constant moved and the earlier generation's functions
   exported in the same pull request. `top` stays, deprecated, written
   and read in format 5, not in the new notation; `max` and `min` go by
   the listing (`feat/listed-type-order`). Every rule migration reverses
   listed `as` and turns `top` into `max`, as one of its steps, never on
   its own (the maintainer, 2026-11-24): `rewrite_body` does it for a
   format 3 body, and an embedder's reinstall writes its lists reversed.
   A 0.5.0 space has to be migrated (every rule it holds is inert until
   rewritten), so every 0.5.0 space is reversed in this version; a space
   the next release starts keeps its lists as written. Releasable: no
   release wrote format 4.

4. **A new vocabulary.** Rule facts and the transient marker are
   written and read under new attribute names. No read path reads the old
   names. Releasable: an O device keeps reading the old names, and meets
   nothing new.

5. **`upgrade_rules` narrows to rules no embedder installs, and keeps.**
   It rewrites a format 3 body it finds under the old names into format 5
   under the new, asserts beside and retracts nothing, and reports. An
   embedder that reinstalls its own definitions does not call it on them.
   Whether it is needed at all turns on T12. Releasable.

6. **Read paths read format 5 only.** `rule::body::decode` refuses any
   number but 5; the readers of formats 0 to 4 and older spellings sit
   behind the upgrade and the exported functions only; descriptor wire
   forms refuse a key they do not know (open question 3). Releasable.

   *The next tag goes here.* Tonk, in the release that takes it: key its
   reinstall on the generation constant as well as its library hash;
   reinstall without retracting the previous install (T13); rekey with
   v1 and later; reverse its three lists and turn their `top` into `max`
   in the same reinstall, since a 0.5.0 space has to be migrated.

7. **The release after: `top` is removed, and the old readers.** Every
   0.5.0 space that migrated has no `top` left. A space whose rules
   needed no migration, or one whose owner did not migrate, may hold
   `top` fields, which this release refuses; turning them into `max` with
   reversed lists is then a step of whatever rule migration that release
   brings, if any. Formats 0 to 4 and
   the old attribute names are read nowhere.

Each step gets a changelog entry under `Unreleased` and, from step 3, a
bullet under `Compatibility and migration`
(`.claude/rules/versioning-and-changelog.md`). Step 1 can land before
the maintainer decides between the approaches: every column needs it.
