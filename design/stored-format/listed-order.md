# The order of a listed `as`, from format 4 to 5

## Decided

Irakli, 2026-11-24, relayed by the coordinator:

- "when migrating from format 4 → 5 we'll reorder all the variants to
  match left to right order". A listed `as` was written best first and
  read by `top` (the first listed value present). Format 5 writes it
  least first and reads the winning claim with `max` (the last listed
  value present).
- Of the two rewrites that keep a ranked read's answer (keep the list and
  make `top` `min`, or reverse the list and make `top` `max`), the
  second.
- Asked whether to reverse every list or only a list some `top` field
  reads: "No lets reverse all please". Every listed `as` is reversed by
  the migration, whether a ranked field reads it or not, and every `top`
  becomes `max`.

- And later the same day: "We can keep top just deprecate it and not
  have it in notation, we can remove top in the next version allowing
  anyone wishing to migrate to do so".

So, this version: `top` stays in the engine and in what is stored,
deprecated in words; `max` and `min` over a listed type go by the
listing, least first (`feat/listed-type-order`); the new notation has no
`top`; nothing is reversed automatically. Next version: `top` is
removed. The coordinator reads "allowing anyone wishing to migrate to do
so" as: the reversal and `top` becoming `max` have to have happened for
a space by then, by a migration its owner runs. That reading was the
coordinator's, and the maintainer replaced it the same day:

- "If we have to migrate rules we can just as well reverse, if we don't
  then we keep them as is". Decided: the reversal, and `top` becoming
  `max`, is a step of a rule migration when one has to run anyway, and
  never a migration of its own. Where no rule migration is needed, lists
  stay as written and `top` stays, deprecated.

A space 0.5.0 wrote does need one: every rule it holds is inert on the
next release until rewritten ([claims](claims.md), the row "a space
0.5.0 wrote has to have its rules migrated"). So for every 0.5.0 space,
tonk's included, the reversal is part of this version's migration. Under
approach F that migration is tonk's reinstall of its library, which
writes its three lists reversed with `max` (T11), and dialog's
`rewrite_body` for rules no library installs. A space first written by
the next release has no rule to migrate; its lists stay as written.

Overruled, the coordinator's opinion of 2026-11-24: a list no ranked
field reads may already have been written least first, by an author who
found that order natural (`[todo, doing, done]`), and reversing it
changes no answer and makes it read worse.

`top` itself is being removed, and `max` and `min` made to go by the
listing, on `feat/listed-type-order`. That work is not redone here; this
note is about what the migration does to what is stored.

## A list read by a deprecated `top` and by `max`, meanwhile

Until a space migrates, its lists stay as written, best first. `top`
picks the first listed value present; `max` by the listing picks the
last listed present, `min` the first. So `top` over a list is `min` over
the same list, whichever order the list is in, unlisted values
included: an unlisted value loses to every listed one under `top`
(`selective.rs:46` to `:48`) and, as the engine worker reports
`feat/listed-type-order` (47a6a3c, to re-check when pushed), under `min`
and `max` alike. A concept with one
field of each is well defined: the two read opposite ends. What can
mislead is the author's intent: over a list written best first, `max`
picks the worst value. A field picking `max` over a list that some
`top` field also reads is a sign the list is still best first.

## What the decision has going for it

Under "reverse only where `top` reads it", one relation could end up
with two orders. An attribute descriptor is embedded whole in every
concept that reads it (`concept/descriptor.rs:280` to `:305` hashes each
field's attribute; the attribute's identity hashes `among` as an ordered
list, `attribute/descriptor.rs:815` to `:819`). A concept with a `top`
field over `job/state` would be rewritten to `[done, todo]` and one that
only constrains `job/state` would keep `[todo, done]`: two attributes,
two identities, one relation, and `max` meaning opposite values in the
two. Nothing in dialog would break (facts and indexes are keyed by the
relation, not the attribute's identity), and a reader would be misled.
Reversing every list leaves one relation one order.

## How many listed attributes are read by `top`, in this repository

Every listed `as` in `rust/`, `notes/` and `design/` (before this topic),
by whether a `top` field reads it. A list of concepts under `as` (a
concept-typed field, `type_system/wire.rs:611`, `notes/notation.md:630`)
is not an enumeration and is left out.

| where | list | read by `top`? | order as written |
|---|---|---|---|
| `dialog-query/src/concept/query/invariants.rs:1212` | `["suspended", "active"]` | yes | best first |
| `dialog-query/src/concept/query/fixpoint.rs:2219` | four generated values | yes | best first |
| `notes/aggregation.md:138` | `["suspended", "active", "registered"]` | yes | best first |
| `dialog-repository/src/repository/branch/transaction/fixtures/README.md:205` | `["todo", "done"]`, no pick, which 0.4 read as `top` | yes | as written by a test: `done` ranks below `todo` |
| `fixtures/README.md:204` | `["todo", "done"]` under `pick: last:` | no | least first, the order the overruled opinion describes |
| `dialog-query/src/attribute/descriptor.rs:921` | `["https://example.com", "home"]` | no | no order meant: a test of text written bare |
| `attribute/descriptor.rs:956` to `:972` | `["case:suspended", "case:active"]` | no | best first, by the test's own words |
| `notes/glossary.md:66` | `["case:suspended", "case:active"]` | no | best first, as the glossary defines a listing |
| `notes/notation.md:229` | `["diy.cook/tsp", "diy.cook/mls"]` | no | no order meant: units |

4 read by `top`, 5 not. Of the 5, one is written least first, two carry
no order, two best first. No Rust code builds a listing outside these
(no `among(` or `listing(` call but the descriptor's own accessors).

The tonk-team fixture holds no listed `as`: none in its 135 rule bodies
(searched for a dag-cbor `as` key followed by an array) and none among
the 841 fields of its 284 named concepts. The concept export keeps one
`db.attribute/as` value per attribute (`tonk_dataset.rs`, `one`), so a
listing tonk stores as several values would not show there; T10.

## What the migration needs to reverse safely

1. **It must know which order a list is in, and the list does not say.**
   Reversing is not idempotent: a second run would undo the first. Inside
   a rule body the envelope's format number says it: reverse when reading
   a body below 5, never at 5. Outside a body there is no number. A
   concept or attribute an embedder ships in its source, or keeps in its
   own records, reads the same in either order, and nothing can refuse
   the old order by name. So the embedder reverses its own definitions in
   the same release that runs the migration (T10, T11), and the format 5
   reader takes any listing it is given as least first.
2. **Every identity that embeds the attribute moves.** The attribute's
   (it hashes `among` in order), every concept's with a field over it,
   every rule's that concludes or reads such a concept, and every
   transient marker keyed by such a concept. The identities move before
   the next tag in any case ([claims](claims.md)), so this adds no second
   move if it lands in the same release, which the maintainer's answer
   of 2026-11-24 makes it for every 0.5.0 space. The upgrade's reproduction of
   the earlier identity has to reverse too, or it will not find a marker.
3. **`top` becomes `max` in the same rewrite, and a `{ "top:": [..] }`
   ranking is reversed before it is written under `as`.** Main's
   `rewrite_body` keeps a `top` and its list best first (`migration.rs:
   396` to `:400`), while its own doc says the listing is "reversed to
   rank least first" (`:267`). The code and the doc disagree on main
   today.
4. **Stored values do not move.** A claim's value is the same text or
   entity in either order, facts are keyed by relation, and a
   cardinality-one write succeeds the newest claim, not a ranked one
   (Unreleased changelog, "What the tree stores is unchanged"). No user
   fact is rewritten by the reversal.
5. **Comparisons in rule bodies.** On main `max` and `min` compare values
   by their own order, and only `top` reads the listing
   (`selective.rs:87` to `:94`); no constraint or formula reads `among`.
   If `feat/listed-type-order` makes a comparison in a rule body go by
   the listing too, a format 3 or 4 rule comparing listed values compared
   them by value order and would compare them by listing after the
   upgrade. The upgrade then has to report such a rule rather than
   rewrite it silently. To check when that branch lands.

## What tonk holds

Reported by tonk @ b125f0d.

- **T10.** Five declarations of three listed attributes, all in tonk's
  `profile.yaml`, all written best first, none with a pick: tonk's
  analyzer makes a list imply `top`
  (`tonk-analyzer/src/analyzer/declaration.rs:118` to `:121`).
  `xyz.tonk.space/presence` `[seeding, replicating, replicated, remote]`,
  `xyz.tonk.space/sync` `[paused, synced, local]`,
  `xyz.tonk.account/status` `[suspended, active, registered, retired]`.
  No `max` or `min` anywhere in the library. A list is stored in tonk's
  records as a JSON string under `db.attribute/among`, best first.
  So every listed attribute tonk has is ranked and best first: the
  overruled worry (a list already least first) does not arise in tonk.
- **T11.** Reversing is more than the YAML: tests pin the order
  (`tonk-worker/tests/standard_library.rs:2234` to `:2246`,
  `analyzer.rs:1194`, `:1635`, `:1670`,
  `tonk-schema/src/concept.rs:2166` to `:2230`,
  `tonk-template/src/resolve.rs:798`), Rust constants pin it
  (`tonk-schema/src/account.rs:35` to `:47`), and docs describe it. The
  `db.attribute/among` records are tonk facts and move with tonk's
  `rekey`, since the attribute's identity hashes the list in order.
- `xyz.tonk.account/status` has two descriptors already: a Rust
  `#[pick(last)]` attribute typed entity, and the ranked list in YAML,
  which leaves out `case:onboarding` that the Rust constants include.
  One relation, two attributes, two identities, before any reversal.
  That is tonk's to reconcile; it is the case "one relation, two orders"
  warned about, in another form.
- On main a listed `as` without a pick is an enumerated type and ranks
  nothing; 0.5.0 read it as `top`. Whether tonk's lowering writes the
  pick or relies on the implied one is T15. If it relies on it, tonk's
  three ranked reads change meaning on the dialog upgrade whatever is
  decided here about the reversal.
