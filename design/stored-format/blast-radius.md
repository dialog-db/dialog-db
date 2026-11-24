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
