# A release reads every spelling of a rule, a concept or an attribute that any earlier release stored

**Phase: Directing.** Approaches and a [matrix](stored-format/direction.md)
are written, with the agent's recommendation; the maintainer decides.
The [journal](stored-format/journal.md) has where the topic stands, the
[ledger](stored-format/claims.md) what it rests on. Working notes: the
[inventory](stored-format/inventory.md) of what is stored and read, the
[blast radius](stored-format/blast-radius.md) of an identity change with
the questions for tonk, [two releases in one space](stored-format/mixed-fleets.md),
[the order of a listed `as`](stored-format/listed-order.md),
[the last migration of every rule](stored-format/last-migration.md), and
the [plan](stored-format/plan.md).

## Description

The maintainer, Irakli, 2026-11-24:

- "we should not keep format 5 concerned with how format 4 worked, this
  is our opportunity to clean things up not carry forward mistakes"
- "All rules and concepts would be rewritten to format 5 and their
  identities will change ... again we should not try to carry format 4
  legacy"

- "when migrating from format 4 → 5 we'll reorder all the variants to
  match left to right order", and, asked whether only lists a `top`
  reads: "No lets reverse all please". Decided: every listed `as` is
  reversed to least first and every `top` becomes `max`
  ([listed order](stored-format/listed-order.md)).
- "We can keep top just deprecate it and not have it in notation, we can
  remove top in the next version allowing anyone wishing to migrate to
  do so", and "If we have to migrate rules we can just as well reverse,
  if we don't then we keep them as is".

And 2026-11-18, in the rule-format ledger: a release should read the
format it writes and no other, with a migration that keeps the earlier
body and deletes it on a flag.

What is on main (2b12204), from the [inventory](stored-format/inventory.md):

- A rule body is written in format 4 when the rule holds no pattern and
  5 when it holds one (#853), and read in every format from 0 to 5
  (`rust/dialog-query/src/rule/body.rs`). No tag has written format 4 or
  5: every release up to 0.5.0 writes 3.
- An attribute or a concept is never stored as a fact of its own. Both
  are embedded in rule bodies and named by entity in index facts and the
  transient marker. Their wire forms read spellings no release writes
  any more (`pick: "last"`, `"all"` on an attribute, legacy type names)
  and refuse others by name (`select`, a chain, `top:`), and ignore a key
  they do not know.
- Every attribute, concept and rule identity moved between 0.5.0 and
  main. `Branch::upgrade_rules` rewrites a 0.5.0 space's rules under the
  new identities, retracts the old facts, and moves the transient
  marker of each concept a rule concludes.
- Tonk (reported from tonk @ b125f0d) pins dialog at the v0.5.0 commit,
  writes dialog identities into its own facts and moves them with a
  `rekey` of its own, reinstalls its library by replaying the previous
  install's retractions, and cannot tell which release each device of a
  space runs ([blast radius](stored-format/blast-radius.md)).
- Run against the tonk-team fixture in this repository: its 135 rule
  bodies are in format 3, all inert on main until upgraded; of its 89
  transient markers, the upgrade would move 1.
- On main every load path skips a body it cannot read and lists it. On
  0.5.0, which live spaces run, two of five paths fail the query instead.

## Problem

Each release reads what every earlier release stored, so code that a
body from any peer can reach grows with every format, and no format is
ever retired. Retiring one means rewriting what live spaces hold: rule
bodies, the indexes that name rules, and the markers keyed by a
concept's identity. That rewrite cannot be done by dialog alone, because
a concept's entity does not say which release named it, and the concepts
an embedder marks are not all ones a stored rule names. And it happens
while replicas of one space still run the older release, which read the
same index keys and, on 0.5.0, fail a query over a body they cannot read.

## Approach

Not decided. The agent recommends approach D of the
[matrix](stored-format/direction.md), revised twice on 2026-11-24 as
tonk's answers came in (E, then F, then D): every rule body migrated in
place to format 5, the library first by the embedder's reinstall and
every other rule by `upgrade_rules`, and the migrated tree fenced with a
critical manifest field that 0.5.0 refuses by name, so a device left on
0.5.0 cannot sync with the space rather than fail or misbehave silently.
No second vocabulary, nothing kept, nothing pruned. All before the next
tag, so a live space moves once.

Open questions for the maintainer:

1. **What is an identity a hash of, once there is one format?** Today a
   rule is a hash of its head concept's reference, its premises and its
   reduce; a concept of its fields' attribute references and options; an
   attribute of its relation, cardinality, type and listed values
   ([inventory](stored-format/inventory.md)). No identity holds the
   format number, so writing format 5 moves no identity by itself. One
   moves only if this function changes. Which part should change, and
   why? If none changes in meaning, a rule keeps its entity and only its
   body is rewritten, which E's new names keep apart from the old one
   anyway.
2. **Keep and prune, or retract?** Main's upgrade retracts; the decision
   of 2026-11-18 says keep and delete on a flag. The matrix assumes keep.
3. **Refuse a key a descriptor does not know?** Without that, removing a
   refusal by name makes the old spelling read as something else
   (`pick` on an attribute would be ignored and read as cardinality one).
4. **Formats 0 to 2.** Delete now, or with 3? It turns on whether any
   live space holds one (T7).
5. **Answered 2026-11-24:** "If we have to migrate rules we can just as
   well reverse, if we don't then we keep them as is". A 0.5.0 space has
   to have its rules migrated, so for it the reversal is a step of this
   version's migration ([listed order](stored-format/listed-order.md)).
