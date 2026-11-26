# One stored format for rules, concepts and attributes: plan

The order the recommended approach (D, the fence, with the rules
migrated in place; [direction](direction.md)) would land in. Revised
2026-11-24 after tonk's answers to T12 to T15; the earlier orders, for E
and then F, are in the journal. Not decided: the maintainer chooses.
Every dialog pull request leaves main releasable. Steps 1 to 5 have to
be on main before the next tag, or a live space moves twice.

## Dialog

Checked against tonk's last migration of every rule on 2026-11-24
([last migration](last-migration.md)); the order puts the fixture and
the gate before any format change, which #1058 lacked.

1. **Export what an embedder's migration needs.** Approved 2026-11-24,
   in flight on `feat/identity-generations`. Each generation's identity
   of an attribute and a concept, including the 0.5.0 identity of an
   attribute read under a ranking pick (`aggregated_reference_v1`,
   private today, the gap tonk #1329 names), and the identity generation
   constant. Nothing stored changes.

2. **A fixture the v0.5.0 binary wrote, and tests that pin today.** The
   space directory a capture program writes from a `v0.5.0` checkout,
   committed with the tests (where and how: [last migration](last-migration.md),
   "A fixture the real v0.5.0 binary wrote"). Tests: every rule listed
   inert on main, with the count of what each relation derives; v0.5.0
   failing a query over a newer body (F1). Tests only.

3. **`Branch::migrate` in one publish, and the fence as the gate.** It
   stages the embedder's changes (reinstall, rekey), then
   `upgrade_rules`, then the fence (a critical manifest field holding the
   identity generation), and publishes once. A commit on a tree whose
   fence is below the current generation fails with `NeedsMigration` if
   the tree holds rule facts or markers, and fences the tree itself if it
   holds none, so a branch with no rules never fails (#1113, #1304). If
   tonk's `rekey` does not move transient markers (U2), the upgrade takes
   the embedder's concepts and moves them. Tested on the fixture: counts
   before and after, a second run that does nothing, an interrupted run
   that leaves the head unfenced, v0.5.0 refusing the fenced tree.

4. **A pull rewrites the rules that arrive after the fence.** An incoming
   rule body below the current format is rewritten in the merge, an
   incoming marker moved where a rule names its concept, the rest listed
   ([last migration](last-migration.md), "Rules that arrive by sync").
   Closes U3 and #1058's "inert until the next process start". Tested on
   the fixture's diverged head.

5. **One format, 5, for every rule, with the reversal.** `TERMS_FORMAT`
   and `format_for` go; `rewrite_body` writes format 5, reversing every
   listed `as` and turning every `top` into `max` (a step of this rule
   migration, as decided); `top` stays readable and deprecated, not in
   the notation. The generation constant moves, so every fence below it
   asks for the migration again; nothing is tagged in between. If the
   maintainer moves identities (open question 1), here.

6. **Read paths read format 5 only.** Older formats and spellings are
   read only by `rewrite_body` and the exported functions; descriptor
   wire forms refuse a key they do not know (open question 3).

   *The next tag goes here.*

7. **The release after.** `top` is removed; the readers of formats 0 to
   4 and the v0 and v1 functions go. A tree fenced below the generation
   is refused, with the release before named as the way through.

## Tonk, in the release that takes the tag

- Tonk #1329 is this half. Stage the library reinstall and the rekey into
  `Branch::migrate`, which runs `upgrade_rules` after them and sets the
  fence, in one publish; do not commit them separately. The reinstall
  comes before `upgrade_rules`, so dialog rewrites no rule a library
  install commit holds and tonk's replay stays exact.
- Gate `migrate_once` on the fence, not on the storage encoding: a
  0.5.0 space needs no storage rewrite.
- Key the seed upgrade on dialog's identity generation as well as the
  library's hash, so a dialog upgrade reinstalls.
- Use v1 (and any later generation) in `rekey`, not only v0, for
  routes, sites, names, `db.concept.with/*`, views, and the transient
  markers if step 3 is not taken.
- Reverse the three listed attributes, write `pick: "max"` on the fields
  that ranked them, and rewrite `db.attribute/among` reversed and
  `db.attribute/pick` to match (T15). Update the tests and constants
  that pin the order (T11).
- Ship every client (web, worker, CLI, host worker, view peer) on the
  release before migrating a space, and say what an old client shows
  when it meets a fenced space.
