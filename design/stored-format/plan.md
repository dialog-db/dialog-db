# One stored format for rules, concepts and attributes: plan

The order the recommended approach (D, the fence, with the rules
migrated in place; [direction](direction.md)) would land in. Revised
2026-11-24 after tonk's answers to T12 to T15; the earlier orders, for E
and then F, are in the journal. Not decided: the maintainer chooses.
Every dialog pull request leaves main releasable. Steps 1 to 5 have to
be on main before the next tag, or a live space moves twice.

## Dialog

1. **Export what an embedder's migration needs.** In
   `dialog_query::migration`: the identity of an attribute and a concept
   as each generation spelled it (v0, v1 exist; tonk uses v0 only), and a
   constant naming the current identity generation. Nothing stored
   changes. Useful to tonk now.

2. **One format, 5, for every rule, with the migration's rewrite.**
   `TERMS_FORMAT` and `format_for` go. `rewrite_body` turns a format 3
   body into format 5: picks into cardinalities and fields, every listed
   `as` reversed and every `top` a `max` (a step of this rule migration,
   as the maintainer decided). `top` stays readable, deprecated, for a
   format 5 body that spells it; it is not in the new notation. If the
   maintainer moves identities (open question 1), here, with the
   generation constant moved and the earlier generation exported.

3. **The upgrade moves every marker it is given concepts for.**
   `upgrade_rules` takes the embedder's concepts and moves a marker under
   any earlier identity of one of them, whether a rule concludes it or
   not. Not needed if tonk's `rekey`, given v1, moves the markers itself
   (U2).

4. **The fence.** A critical manifest field naming the rule format a tree
   is migrated to. `Branch::migrate` sets it after the rules are
   rewritten; a commit on a tree without it fails with `NeedsMigration`;
   a pull that meets an unfenced tree aligns it (its rules rewritten) as
   it aligns a tree from before the number band. v0.5.0 refuses the
   fenced tree by name. The one step that adds machinery; it copies the
   number band's.

5. **Read paths read format 5 only.** `rule::body::decode` refuses every
   other number; formats 0 to 4, picks and older spellings are read only
   by `rewrite_body` and the exported identity functions. Descriptor wire
   forms refuse a key they do not know (open question 3).

   *The next tag goes here.*

6. **The release after.** `top` is removed; the readers of formats 0 to
   4 and the v0 and v1 functions go. A space still unfenced is refused,
   with the release before named as the way through.

## Tonk, in the release that takes the tag

- Run, in this order: `Branch::migrate` (storage, then the fence), the
  library reinstall, `upgrade_rules` for whatever else is left, then
  `rekey` with the exported functions. The reinstall comes before
  `upgrade_rules`, so dialog rewrites no rule a library install commit
  holds and tonk's replay stays exact.
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
