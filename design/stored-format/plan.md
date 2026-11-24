# One stored format for rules, concepts and attributes: plan

The order the recommended approach (E, [direction](direction.md)) would
land in, each step a pull request that leaves main releasable. Not
decided: the maintainer chooses the approach first. Steps 1 to 6 have
to be on main before the next tag, or a live space moves twice.

1. **Tests that pin what O and main do today, and nothing else.** On
   main: a transient marker on a concept no rule concludes, under its
   0.5.0 identity, after `upgrade_rules` (expected to stay where it is,
   [blast radius](blast-radius.md)); a branch holding a body in format 6
   indexed under a relation, read by a query (expected to skip and
   list). No behaviour changes. Releasable: tests only.

2. **The upgrade takes the concepts it moves markers for.** An
   embedder hands `upgrade_rules` the concepts it knows; a marker under
   any earlier identity of one of them is moved, whether a rule
   concludes it or not. Fixes the stranded markers on main whatever
   approach is chosen, so it is first. Releasable: adds an argument,
   moves more markers. A breaking change to the upgrade's signature.

3. **The upgrade keeps, and a prune deletes.** The upgrade asserts the
   rewritten rule and the moved marker beside the old facts and retracts
   nothing; `prune` retracts the old ones, on the embedder's call
   (the maintainer's opinion of 2026-11-18). Releasable: an older
   replica keeps its rules where it lost them before.

4. **One format: 5 for every rule.** `TERMS_FORMAT` and `format_for`
   go; every body is written in 5. The upgrade rewrites a format 4 body
   too. If the maintainer moves identities (open question 1), it is
   here, once, with the reproduction of the format 4 identity added to
   `dialog_query::migration` for the upgrade. `top` stays: deprecated in
   words, written and read in format 5, absent from the new notation
   (the maintainer, 2026-11-24, [listed order](listed-order.md)).
   Nothing is reversed automatically. `max` and `min` over a listed type
   go by the listing, least first, as `feat/listed-type-order` makes
   them. Releasable: no release has written format 4.

   Whether the upgrade of this version also offers the reversal, as a
   step an owner opts into, or the reversal belongs to the next
   version's upgrade, is open question 5 in the top story.

5. **A new vocabulary.** Rule facts and the transient marker are
   written and read under new attribute names. Every read path reads
   the new names only. The upgrade reads the old names, writes the new.
   Releasable: an older replica keeps reading the old names.

6. **Read paths read format 5 only.** `rule::body::decode` refuses any
   number but 5; the readers of formats 0 to 4, the v0 and v1
   identities, `rewrite_body`, the picks a batch spelled and the
   attribute and field spellings refused by name move behind the
   upgrade, and the descriptor wire forms refuse a key they do not know
   (open question 3). Releasable: nothing on a read path reads an
   older spelling.

   *The next tag goes here.*

7. **The release after: `top` is removed, and the upgrade's readers.**
   By then every listed `as` a space holds has to be least first, with
   every `top` a `max`, by a migration its owner ran (the coordinator's
   reading of the maintainer's words, open question 5). A space whose
   owner did not migrate holds `top` fields this release refuses. Formats 0 to 4,
   `dialog_query::migration`, the old attribute names. What a space
   still holds under them then stays inert and is listed by the
   embedder's prune. Releasable: a replica that skipped a release
   upgrades through the release before.

Each step gets a changelog entry under `Unreleased` and, from step 2,
a bullet under `Compatibility and migration`
(`.claude/rules/versioning-and-changelog.md`).
