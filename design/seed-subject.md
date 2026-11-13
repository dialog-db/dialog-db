# A rule that leaves a premise's subject unnamed is seeded by value alone

**Phase: Describing.** Seen while measuring what rules cost at commit (the research topic `rule-heads`, #788). Not picked up.

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
