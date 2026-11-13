# A rule that leaves a premise's subject unnamed is seeded by value alone

**Phase: Designing.** Seen while measuring what rules cost at commit (the research topic `rule-heads`, #788). Reproduced on main, diagnosed, and an approach built on #791 for the maintainer to decide. It changes what a commit writes, so the decision is his.

## Description

At commit an inductive rule is fired for the rows the commit writes. Each row is bound into the premise that reads its attribute, a seed, and the body is evaluated with that binding fixed (`fire_seeded`, `seed` and `bind_seed` in `rust/dialog-repository/src/repository/branch/transaction/induce.rs`). A seed binds the row's value to the variable of the premise's field, and the row's entity to the variable the premise gives `this`. A premise that gives `this` no variable has nothing for the entity to be bound to, and the entity is left free.

```
 the commit writes    evt/kind of evt-2 is "click"

 { user: evt/user, kind: evt/kind }  where  this: ?event, user: ?this, kind: ?seen
     the seed binds   ?seen = "click", ?event = evt-2      the premise is read at evt-2

 { user: evt/user, kind: evt/kind }  where  user: ?this, kind: ?seen
     the seed binds   ?seen = "click"                      the premise is read at every
                                                           event whose kind is "click"
```

Two things were observed, on main at dd19d6c.

**The rule fires for an entity the commit did not touch.** `alice` has an event of kind `click`. `bob`'s event changes its kind to `click`, in a commit that writes nothing of `alice`'s. The rule above, its head transient, is fired for `alice` and for `bob`. The same rule with the event named, in a variable nothing else reads, is fired for `bob` alone. So is the unnamed one when the seed is made to bind the entity. Pinned by `it_fires_for_an_entity_the_commit_did_not_touch` on the branch `spike/induce-cost` (`rust/dialog-perf/src/scenario/heads.rs`), which reads who was fired for from the transients the commit reports.

The module's own account of seeding says "every new match this round must bind at least one new row into at least one positive premise". The match for `alice` binds no row the commit wrote.

Where the seed binds every field of the premise, as when a commit writes a whole command, the entities found are those that hold all of the same values, which is as a rule the one written. The harness of the research topic is that case, and its three shapes write the same facts either way.

**The read by value is estimated before it is made.** With the entity free, each field of the premise is a read by value over every entity. The conjunction of the fields asks the tree how wide each read is before choosing to merge or to fold them (`Conjunction::scans_balanced_for`, `rust/dialog-query/src/planner/conjunction.rs`), once an evaluation, so once a seed. With the entity bound the same conjunction folds and asks nothing.

Measured under callgrind, the median of three runs, for a rule that reads a command of four fields and one more attribute and writes two facts (`research/rule-heads/results/2026-11-12-induce-cost.md` on #788's branch):

| | as it is | the seed binds the entity |
|---|---|---|
| one commit of 500 commands, millions of instructions | 901.7 | 563.7 |
| one commit of one command | 4.51 | 3.80 |

An estimate costs 153 to 160 thousand instructions, and there are four an evaluation: 309 of the 901 million of the commit of 500, and 0.66 of the 4.5 million of the commit of one.

The second column is a prototype: every positive concept premise that leaves `this` unnamed is given a variable for it before the rule is seeded (`spike::Body` on the same branch, behind `DIALOG_SPIKE=subject`). With it on, the three ways of writing the research topic measures write the same facts as before, and `dialog-repository`'s 706 library tests pass as they do with it off. No test was written for what it changes.

Not looked at: a premise under `unless`, which is never seeded; a rule fired in full, which is not seeded either; and whether a deductive rule's premise read from a bound row meets the same estimate.

## Reproduced on main

Run on main at dd19d6c with one test file added and nothing else changed (the scenarios of `rust/dialog-repository/src/repository/branch/transaction/induce/subject_tests.rs` on #791). `alice` has an event of kind `click`, `bob` one of kind `view`.

```
 a commit writes      evt/kind of bob's event is "click"

 rule                                       fires for, on main        with the seed bound
 event{user: ?this, kind: ?k}               alice, bob                bob
 event{this: ?e, user: ?this, kind: ?k}     bob                       bob
```

The firing for `alice` is not only work done twice. It writes:

| the rule's head | on main | with the seed bound |
|---|---|---|
| a command, `seen` for the user | issued for `alice` and for `bob` | issued for `bob` |
| the same command, and a second rule that counts the commands a user was sent (both at 1 after the rules are installed) | `alice` 2, `bob` 2 | `alice` 1, `bob` 2 |
| a durable fact under `last`, the user's last kind, after `alice`'s was written over by hand | `alice`'s is put back to `click` | `alice`'s stays as written by hand |

**Which entities main fires for depends on how many fields a commit writes.** A commit that writes both fields of a new event seeds them together (#763), and together they find that event alone. Seeded one by one, the kind alone finds `alice`'s event too. On main `dialog-repository`'s own check of one against the other fails on such a commit: "grouped seeding must write the transients seeding row by row writes". Outside tests the check does not run, and the commit fires for the new event alone. So one field written fires for every holder of its value and two fields written do not.

## Problem

What a rule is fired for depends on whether its author named a variable nothing reads, and on how many fields of an entity a commit happens to write. A firing for an entity the commit did not touch repeats what was concluded before, over whatever was written since.

## Approach

Built on #791, not decided. A premise that leaves its subject unnamed is seeded with the subject named, in a variable nothing else in the rule reads (`subject_named`). The seed then binds the entity the row is about. The rule is not rewritten and nothing stored changes: the name exists for the seeded evaluation only.

A second change comes with it, because the first would otherwise lose something main finds by accident.

```
 evt/kind of carol's event is derived: it mirrors alice's event
 a commit writes      evt/kind of alice's event is "scroll"

 rule                                       on main            seed bound, nothing else    #791
 event{user: ?this, kind: ?k}               alice, carol       alice                       alice, bob, carol
 event{this: ?e, user: ?this, kind: ?k}     alice              alice                       alice, bob, carol
```

`carol`'s event changed its kind too, derivedly. A premise that changed derivedly is what induction evaluates in full for, but main counts an attribute as derived only when no row of the commit writes it, so an attribute derived from itself never counted. The rule was seeded. With its subject unnamed the value alone was bound and happened on `carol`'s derived event, which holds the same value. With its subject named, on main, `carol` is missed. #791 counts a written attribute as derived too when a deductive rule concludes it from something touched, and fires in full: for `carol`, and for `bob`, whose event matches as it did.

### Where results differ, all of them

1. A rule with an unnamed subject no longer fires for untouched entities that hold a value written. Their heads are not written again: no second command, no durable `last` value put back.
2. One field written and two fields written now fire for the same entities.
3. A rule premised on an attribute derived from itself, when that attribute is written, fires in full: more entities than before (every match), and the derived ones main missed.

### What is not changed

- A rule fired in full: installed over existing state, a premise changed derivedly, or enabled by a retraction under `unless`. It fires for every match, as before (`it_still_fires_in_full_for_every_match`).
- A premise under `unless`, which is never seeded.
- A premise whose subject is a variable or a constant already.
- Only a concept premise is seeded, so only a concept premise is named. A stored rule has no other kind of premise that reads facts.

### What it costs and saves

Measured on #788's harness, where the command premise names no subject (`research/rule-heads/results/2026-11-13-sound.md` on that branch). Instructions under callgrind, millions, the median of three runs interleaved with main:

| | body, heads | main | #791 | |
|---|---|---|---|---|
| one commit of 500 commands, one rule of a wide head | 1 read, 2 heads | 902.3 | 563.9 | -37.5% |
| one commit of 500 commands, one rule of a wide head | 4 reads, 4 heads | 1563.8 | 1221.2 | -21.9% |
| one commit of one command, one rule of a wide head | 1 read, 2 heads | 4.51 | 3.80 | -15.7% |
| one commit of one command, one rule of a wide head | 4 reads, 4 heads | 6.25 | 5.46 | -12.7% |

A rule a head saves that for each rule: 25% to 47%. What it costs: a rule over an attribute derived from itself is evaluated in full where it was seeded, each time that attribute is written. Not measured: no scenario of the harness has one.

## For the maintainer to decide

Whether a rule that leaves a subject unnamed should fire only for the entity written. The agent's opinion is yes: it is what the module says seeding finds, what the same rule with a named subject already does, and what a commit of several fields already does. It is a change in what commits write, so #791 is marked breaking.

Whether case 3 belongs with it or apart. It is a fix of its own (the named subject misses `carol` on main), and it is in #791 because without it the narrower seed loses her for the unnamed subject too.
