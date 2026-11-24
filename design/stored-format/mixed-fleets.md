# Two replicas of one space on two releases

O is a replica on 0.5.0, the latest tag. N is one on the next release.
Both sync one space. What each reads, fires and writes, and the three
hazards the brief named: a rule firing twice, a rule retracted under a
replica that still needs it, a rule coming back after a retraction.

## What a release does with a body it cannot read

On main, every load path skips such a body and none fails:

| path | on main | at v0.5.0 |
|---|---|---|
| a concept's rules from a line's tree (resolution) | `pending_body` returns `Ok(None)` on a decode error (`repository/branch/session.rs:1942` to `:1947`) | `pending_body(..)?` propagates the error (`session.rs:1962` at v0.5.0), and `pending_body` returns it from `hydrate(bytes)?` (`:1868`) |
| a concept's rules from a session overlay | skips | `pending_body(..)?` propagates (`session.rs:1815` at v0.5.0) |
| a concept's rules from a staged layer | skips (`let Ok(Some(rule))`) | skips (`session.rs:1840` at v0.5.0) |
| every rule, for the analysis | skips (`session.rs:2412` to `:2417`) | skips (`session.rs:2288` at v0.5.0) |
| an inductive rule at commit | skips (`transaction/induce.rs:832` to `:838`) | skips (`induce.rs:784` at v0.5.0) |

Main also answers `dialog.rule/invalid` with the reason, `older:`,
`newer:`, `undecodable:` or `forged:` (`session.rs:2195` to `:2225`).
The Unreleased changelog records the v0.5.0 side: "a query that met an
undecodable body used to fail with `rule hydrate`, so one bad rule
arriving by merge stopped every read of its relation".

So the rule-format ledger's "three of four load paths skip silently"
no longer holds on main: all of them skip, and a branch lists what it
skipped. It does not describe v0.5.0 either, which is what O runs: there
two of five paths fail.

**F1.** On O, a query over a relation that a format 4 or 5 body is
indexed under fails with `rule hydrate`. Run on a v0.5.0 worktree,
2026-11-24: a read of `org/salary` returned `[700]`, and after a format 4
body was committed with `derives on:org/salary`, the same read returned
`Err("Fact store: rule hydrate: the rule body is in format 4, and this
release reads up to 3")`.

A rule's head and trigger indexes are keyed by relation
(`on:<domain>/<name>`, `rule/statement.rs:100`), so a format 5 rule N
installs is found by O under the same keys as its own. Nothing about a
new body tells O to look away.

## Before a migration

N runs, the space holds what O wrote: format 3 bodies under 0.5.0
identities, markers under 0.5.0 identities.

- N reads none of O's rules. A body spelling no pick decodes and does
  not hash to the entity it is under (`forged:`); one spelling a pick is
  refused (`older:`). Inert, listed. Run on main against tonk-team's
  bodies: 126 of 135 decode, 9 are refused, and every concluded concept's
  identity moved, so all 135 are inert ([inventory](inventory.md)).
- N reads none of O's markers: they are keyed by the 0.5.0 identity of
  each concept. A fact of a command concept that N writes is committed as
  durable data, unless tonk marks its concepts again under the identity
  N gives them (T1).
- O reads everything it wrote, as before.

## During and after a migration on main as it stands (approach A)

`upgrade_rules` on N asserts each rule under its identity in format 4 or
5 and retracts every `dialog.rule/*` fact the earlier release wrote under
the old entity (`repository/branch/upgrade.rs:297` to `:316`), and moves
the marker of each concept a rule concludes (`:321` to `:342`).

- **Retracted under a replica that needs it.** Yes. After sync O holds
  none of its rules: the facts under the old entities are retracted, and
  the new bodies are in a format it does not read. O derives nothing,
  and fires nothing at its own commits.
- **What O meets.** N's rules, indexed under the relations O queries. By
  F1, O's queries over every relation a migrated rule derives fail.
- **A rule comes back.** If O asserts its rules again (T4), the facts
  return: a merge is observed-remove, and an assertion made after the
  retraction is a new claim, not a resurrection of the old one
  (`dialog-corpus/src/merge.rs:1` to `:25`). On N they are inert and
  listed `older:` or `forged:`; the next `upgrade_rules` retracts them
  again.
- **A rule fires twice.** Each replica induces the facts a pull brought
  at its next commit (`induce.rs:929` to `:1030`, the watermark lag
  includes arrivals). A stimulus O wrote fires O's rule at O, and the
  equivalent rule at N when N next commits. A head that writes what the
  view already holds contributes nothing (`induce.rs:18` to `:26`), so a
  durable conclusion is written once. This is how one rule held by two
  replicas behaves today, format change or not. A head that dispatches a
  command does not pass a novelty check; whether its provider runs at
  both replicas is a hypothesis this topic does not take on (F4 in the
  ledger).

## With the old facts kept (approaches B and E)

The upgrade asserts beside and retracts nothing until a prune.

- O keeps its rules and markers and runs as before.
- Under B, O still meets N's format 5 bodies under the relations it
  queries, and F1 still holds.
- Under E, N's rule facts sit under attribute names O never reads
  (`dialog.rule/source`, `conclusion`, `derives`, `reads`, `induces`,
  `on` are the names O reads, `rules.rs:85` to `:88` at both). O meets
  nothing it cannot read, and N meets nothing of O's but in the upgrade.
- An O user authoring a rule after the migration writes it under the
  old names in format 3. N does not see it until the upgrade runs again.
  When to run it, once or on every pull, stays the embedder's choice, as
  `upgrade.rs:53` to `:56` already says.
- After a prune, a rule O asserts again comes back under the old names,
  where only the upgrade looks.

## Two bodies under one entity

If a format change rewrote a body without moving its entity (format 4
to 5 for a rule of terms, identities unchanged), one entity would hold
two bodies until the old one is retracted. Every reader takes the first
body it meets: `source_bytes` takes the first bytes value
(`induce.rs:915` to `:922`), and `every_rule` marks the entity seen
before it decodes (`session.rs:2406` to `:2410`). Which body that is
depends on the order the scan yields, so a reader can take the body it
cannot read and skip a rule it could have read. The upgrade avoids this
by retracting and asserting in one commit (`upgrade.rs:276` to `:284`);
a kept old body under the same entity would not. An identity move, or
E's new names, keeps old and new under different keys.
