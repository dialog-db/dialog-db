# A release reads every spelling of a rule, a concept or an attribute that any earlier release stored

**Phase: Directing.** Approaches and a [matrix](stored-format/direction.md)
are written, with the agent's recommendation; the maintainer decides.
The [journal](stored-format/journal.md) has where the topic stands, the
[ledger](stored-format/claims.md) what it rests on. Working notes: the
[inventory](stored-format/inventory.md) of what is stored and read, the
[blast radius](stored-format/blast-radius.md) of an identity change with
the questions for tonk, [two releases in one space](stored-format/mixed-fleets.md),
and the [plan](stored-format/plan.md).

## Description

The maintainer, Irakli, 2026-11-24:

- "we should not keep format 5 concerned with how format 4 worked, this
  is our opportunity to clean things up not carry forward mistakes"
- "All rules and concepts would be rewritten to format 5 and their
  identities will change ... again we should not try to carry format 4
  legacy"

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

Not decided. The agent recommends approach E of the
[matrix](stored-format/direction.md): one format, 5, for every rule,
read on every read path and nothing else; rule facts and the transient
marker written under new attribute names an older release does not
read; an upgrade, given the embedder's concepts, that writes the new
beside the old and a prune that deletes the old when the embedder says;
and all of it before the next tag, so a live space moves once.

Open questions for the maintainer:

1. **What moves in an identity, and why?** No identity holds the format
   number, so writing format 5 moves no identity by itself. One moves
   only if the identity function changes. Which part should change? If
   none changes in meaning, a rule keeps its entity and only its body is
   rewritten, which E's new names keep apart from the old one anyway.
2. **Keep and prune, or retract?** Main's upgrade retracts; the decision
   of 2026-11-18 says keep and delete on a flag. The matrix assumes keep.
3. **Refuse a key a descriptor does not know?** Without that, removing a
   refusal by name makes the old spelling read as something else
   (`pick` on an attribute would be ignored and read as cardinality one).
4. **Formats 0 to 2.** Delete now, or with 3? It turns on whether any
   live space holds one (T7).
