# Rules that state one body each evaluate it

**Phase: Designing.** Seen while measuring what rules cost at commit (the research topic `rule-heads`, #788). An approach is built beside this file, in the same pull request, for the maintainer to judge. Nothing here is decided by him yet.

## Description

At commit an inductive rule is fired for the rows the commit writes: each row is bound into the premise that reads it and the body is evaluated with that binding fixed (`fire_seeded` in `rust/dialog-repository/src/repository/branch/transaction/induce.rs`). Every rule a row reaches is evaluated on its own.

The notation being designed (#773) has one head a rule. To write two facts on one condition an author writes two rules and the condition twice. The research topic measured that against one rule with a wide head, which evaluates the condition once ([results](https://github.com/dialog-db/dialog-db/blob/research/rule-heads/research/rule-heads/results/2026-11-12-induce-cost.md)): the body written again costs 27% to 87% more instructions at one command a commit and 61% to 148% more at 500.

```
 one command, a condition of three premises, two facts written

 one rule, a wide head          move ∧ list ∧ owner ─▶ { to, by }      the body once
 a rule a head                  move ∧ list ∧ owner ─▶ { to }          the body
                                move ∧ list ∧ owner ─▶ { by }          and the body again
```

## Problem

A commit pays for a condition once for every rule that states it. An author who writes one condition and several conclusions, as one rule or as several, means one condition, and what it costs should not depend on which way it was written.

## Approach

The seeded rules of a round that state one body have it evaluated once. The first of them is evaluated as it would be alone. Every other one reads the first's matches through a renaming of variables and writes its own head.

```
 candidates of a round, in firing order      parts          evaluations

 rule A   move ∧ list ∧ owner  ─▶ a ┐
 rule B   move ∧ list ∧ owner  ─▶ b ├─────── body 1 ─────── once, under A
 rule C   move ∧ list ∧ owner  ─▶ c ┘                       B and C read A's matches
 rule D   move ∧ team          ─▶ d ──────── alone ──────── once, as before
 rule E   (fired in full)      ─▶ e ──────── alone ──────── once, as before
```

Three decisions make it up, each with its own matrix:

- [What makes two bodies one](share-body/design-same.md): the premises as the evaluation holds them and the type of every variable, equal under a renaming of variables that keeps `this`.
- [What the order of matches may decide](share-body/design-order.md): nothing. A rule whose writes depend on the order its matches come in is evaluated on its own.
- What is left out: a rule fired in full, and a rule that reduces. Each is evaluated alone, as before.

What holds it to what the engine did before is a differential check. In `dialog-repository`'s tests every rule fired from another rule's matches is also evaluated on its own, and the two must write the same novelty and the same transients (`oracle::shares`). The tests of sharing run their commits a second time with sharing off and compare what was committed.

The [claims](share-body/claims.md) say which of this is read from the code, which is measured and which is still a hypothesis. The [journal](share-body/journal.md) says where it stands.
