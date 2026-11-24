# Who names a rule or a concept by its entity: the blast radius of an identity change

The question that decides the cost: when a rule's or a concept's entity
moves, which stored facts name the old one? If only dialog's index facts
do, a migration rewrites index facts. If user data does, a migration
rewrites user data, which dialog does not own and cannot find by itself.

## A rule's entity

Named only as the `of` of the `dialog.rule/*` facts its install writes:
`source`, `conclusion`, `reads`, `derives` for a deductive rule,
`source`, `induces`, `on` for an inductive one
(`dialog-query/src/rule/statement.rs:280` to `:422`).
`dialog.rule/invalid` and `dialog.rule/quarantined` name it too, and are
never stored: a branch answers them from the bodies it holds
(`statement.rs:78` to `:95`).

Nothing dialog writes holds a rule entity as a value: no `Value::Entity`
of a rule's `this()` is written anywhere outside the index facts
(searched across `dialog-query/src`, `dialog-repository/src`). An
induced fact carries no cause naming its rule (`induce.rs`, every emitted
claim has `cause: None`).

So for a rule, the blast radius inside dialog is its own index facts.
User data could name one only by an embedder writing a `rule:` entity
into a fact of its own. Not determinable here; question T3 below.

## A concept's entity

Named by:

| fact | where it lives | moved by `upgrade_rules` today? |
|---|---|---|
| `dialog.rule/conclusion`, `dialog.rule/induces` (as the value) | wherever the rule is | yes: the rule is asserted again, its new index facts name the new entity (`repository/branch/upgrade.rs:313` to `:316`) |
| `dialog.concept/transient` (as `of`) | the branch's tree, or wherever an embedder asserted it | only for a concept some rule on the branch concludes (`upgrade.rs:263` to `:267`, `:321` to `:342`) |
| `dialog.provider/command` (as the value) | `session:`, the stack's own layer | no; `Stack::provide` writes the current entity at registration, the earlier row stays (`stack/effect.rs:459` to `:496`) |
| `dialog.command/answered` (as the value) | `control:`, one tick, unless a rule copies it into a layer that keeps it | no |
| tonk's `db.name/referent`, `db.concept.with/*` (if their entities are dialog identities) | user data | no; dialog cannot find them |

Inside rule bodies a concept is embedded whole, never by entity
(`concept/descriptor.rs:2156`), so a body names no concept identity.

### The transient markers are the case that bites, and it bites today

In the tonk-team fixture, 89 concepts carry `dialog.concept/transient`.
One is concluded by a rule; 76 are concepts tonk names and no rule
concludes; 12 match no concept the fixture names. All 89 are keyed by
the 0.5.0 identity (inventory, "What tonk-team holds", run 2026-11-24).

`upgrade_rules` moves a marker only for a concept it found as a rule's
conclusion. On main, then, upgrading tonk-team from 0.5.0 moves 1 marker
of 89. The other 88 stay on identities nothing reads: a fact of those
concepts is no longer a command, and is committed as durable data
(`rules.rs:100` to `:105`: the marker is what makes a concept's facts
live for one round and never committed). The one test of the move,
`a_concept_the_pick_release_marked_transient_stays_transient_once_upgraded`
(`repository/branch/transaction/pick_migration.rs:387`), marks a concept
a rule concludes, so it does not see this.

Unless tonk marks its command concepts again each time it starts (T1),
this is a defect in the move from 0.5.0 that main already makes, before
any format 5 work. An upgrade cannot fix it from rules alone: given only
an entity it cannot tell which release named it, nor which concept it
was. It needs the concepts, either from the embedder or from a record
of them in the space.

## What decides the cost

| if | a migration rewrites | dialog can do it alone? |
|---|---|---|
| only dialog's own facts name rule and concept entities | the `dialog.rule/*` facts and the transient markers | yes, given the concepts to recompute identities for |
| tonk's records name concept entities by dialog identity | also tonk's records | no: tonk runs a migration of its own, with `dialog_query::migration` to map old to new |
| tonk's records use entities of its own (`id:..`, a minted entity) | nothing of tonk's | yes |

The fixture says tonk marks concepts transient by dialog's identity (76
markers match the 0.5.0 identity of a named concept). It does not say
what `db.name/referent` points at.

## What this repository cannot answer: questions for tonk

- **T1.** Does tonk assert `Transient(concept)` for its command concepts
  each time it opens a space, or once when the concept is first
  created? If each time, the 88 stale markers above are harmless after
  an upgrade; if once, they are commands turned into durable facts.
- **T2.** In `db.name/referent`, `db.concept.with/<field>` and the
  attribute records (`db.attribute/*`): is the entity a concept or
  attribute record lives at dialog's `concept:..` / `the:..` identity of
  the descriptor, or an entity tonk mints?
- **T3.** Does any tonk fact hold a `rule:..` or `concept:..` entity as a
  value: a view, a saved query, a permission, a link from a document to
  the rule or concept that made it?
- **T4.** Does tonk install its rules each time it opens a space (assert
  every rule it ships), or only once? And does it ever retract one?
- **T5.** Does tonk install rules or transient markers anywhere but a
  branch's primary tree: a durable or memory layer of a stack, the
  session overlay? `upgrade_rules` reads only the branch's claims
  (`upgrade.rs:231` to `:237`).
- **T6.** Which dialog revision does each tonk client in use run: only
  tagged releases (0.5.0 being the latest), or any untagged revision of
  main? A client on an untagged revision after 181b8bd may have written
  format 4 bodies.
- **T7.** Does any live tonk space hold a rule body in format 0, 1 or 2
  (written before 0.3)? The fixture holds format 3 only.
- **T8.** Does tonk serialize a `Changes` batch anywhere that outlives a
  process (an outbox, a queue in storage)? A batch spells cardinalities,
  and reads the picks 0.3 to 0.5 wrote (`model/update.rs:58` to `:80`).
- **T10.** How many listed attributes do tonk's spaces and tonk's own
  source define, how many does a ranked field (`top`, `max`, `min`)
  read, and in what order were the unranked ones written? Does tonk
  store a listing in its records (`db.attribute/as`) as one value or
  several?
- **T11.** Will tonk reverse the listed `as` in the concepts it ships in
  the same release that runs the migration? A definition outside a rule
  body carries no format number, so nothing tells a reader which order
  it is in.
- **T9.** How many devices of one space can be on different releases at
  once, and for how long: is there a way for tonk to know every device
  of a space has upgraded?

## What tonk answered

From a read-only session on tonk-labs/tonk, default branch `main` at
b125f0d, relayed by the coordinator on 2026-11-24. Each is a fact as
reported, backed "tonk @ b125f0d" with the file and line it gave; none
was read in this session.

- **Version.** Every tonk workspace pins dialog at revision 875cc7f1,
  in all four lockfiles. That revision is the commit the `v0.5.0` tag
  points at (`chore(release): 0.5.0 (#845)`, checked here), so O is
  exactly 0.5.0. T6 is answered: no tonk client has written format 4.
- **T1.** Transient markers are durable facts written when a library is
  installed, not marked again at each open. The web worker reinstalls on
  mount only if the library's hash differs from the recorded
  `xyz.tonk.seed/version`; the CLI installs once at space creation. A
  dialog upgrade does not change the library's hash, so tonk would not
  reinstall, and the 88 stranded markers stay stranded.
- **T2.** `db.name/referent` sits on a tonk entity `id:<name>`; its value
  is dialog's `concept:..` identity, or an attribute identity.
  `db.concept.with/*` values are attribute identities.
- **T3.** Tonk facts hold dialog identities as values:
  `xyz.tonk.route/concept`, `xyz.tonk.site/concept` (each open tab, in
  the session overlay), `db.name/referent`, and the views, descriptions
  and template concepts a space wrote about library concepts and
  attributes. Tonk has its own `rekey`
  (`tonk-worker/src/router/repository/identity.rs:159` to `:213`), which
  rewrites claims whose value is a moved identity and claims about one,
  and `rekey_sites` for overlay tabs. Providers are matched by a concept
  identity the Rust derive computes, which must equal the YAML's.
- **T4.** A reinstall is three staged commits behind one publish: retract
  everything the previous install commit asserted (read back from branch
  history, rules included), install the new library, write the record;
  moved identities are then rekeyed. `Branch::upgrade_rules` is called in
  `migrate_branch` after `migrate_storage` and tonk's
  `upgrade_definitions` (`dialog-reactor/src/branch/reference.rs:89` to
  `:105`), only when `needs_migration` (the storage layout) says so, once
  per process per branch. Tonk's upgrade knows an earlier identity only
  through `attribute_uri_v0` and `concept_identity_v0`.
- **T5.** Definitions are committed to the branch, none to an overlay.
  Overlays and `session:` hold rows naming concepts: tab sites, intent
  answers, attributes declared `in: "session:"`.
- **T7.** A site from before branches is refused with an export and
  re-import remedy; the import writes a current-layout tree, which
  `needs_migration` would not send through `upgrade_rules`.
- **T8.** No batch is kept. Uninstall replays an earlier install commit's
  assertions from dialog's own history.
- **T9.** No version handshake, capability flag or minimum-version check
  between replicas, or between a client and the worker. Versions can mix
  in one space (the CLI updates itself, the web bundle deploys
  separately), and tonk cannot tell when every device has upgraded.
- **T10, T11.** In [listed order](listed-order.md).

So the claim "no user fact moves with an identity" holds of dialog and
not of tonk. Dialog keys no fact by an identity; tonk writes identities
as values in its own facts, and moves them itself with `rekey`.

### Who rewrites tonk's facts, and what dialog must export for it

Tonk does, as it already does when a library changes: `rekey` and
`rekey_sites`, given a map from each earlier identity to the current
one. Dialog cannot: the facts are tonk's, under tonk's attributes, some
in overlays only a running tonk holds.

What dialog has to give it:

1. **The identity of a definition as each generation spelled it.** Main
   already exports the 0.3 to 0.5.0 generation (`attribute_uri_v1`,
   `concept_identity_v1`, `migration.rs:152`, `:213`), which tonk at
   0.5.0 never had; tonk uses v0 only. Any further move (open question
   1, the reversal of listed `as`) needs one more, exported in the same
   release as the move.
2. **A generation number for identities**, a constant tonk can record
   beside its library's hash, so `upgrade_seed` reinstalls and rekeys
   when dialog's identities move, not only when the library changes.
   Without it, tonk's reinstall does not run on a dialog upgrade (T1).
3. **The rewrite of a stored body, as a pure function** (`rewrite_body`,
   with the reversal), for rules no library installs.

### Additional questions this raises

- **T12.** Does a tonk space hold rules no library installs (written by a
  user, an agent, a view)? Those are the only rules a dialog-side
  rewrite would still be needed for.
- **T13.** When a reinstall under new identities retracts the previous
  install, an O device loses the library's rules and markers. Can tonk
  reinstall without retracting the previous install, and retract it
  later (a prune it schedules)?
- **T14.** When N rekeys a tonk fact (a route, a tab) to a new identity,
  an O device reading it names a concept it does not know. Can `rekey`
  assert the new value beside the old for a while, as the rule facts
  would be kept?
- **T15.** tonk's analyzer makes a listed `as` imply `top`. On main a
  listed `as` without a pick is an enumerated type and ranks nothing
  (the Unreleased changelog). Does the lowering write `pick: "top"` on
  the field, or does tonk rely on the implied `top` that main no longer
  has?
