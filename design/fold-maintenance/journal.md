# A folded value is recomputed for its subscriber: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* How a subscription is kept current today and where a fold makes it ask again. How tonk keeps a view current and what it works around. A problem statement is proposed. A hypothesis: fold what the inner query's subscription reports. What the literature asks of each kind of fold ([literature.md](literature.md)). | *Where are we at?* Delimiting. The statement waits on the maintainer; the cost of a fold under a subscription waits on #773. |
| **Agenda** | *What do we need to know?* Whether the statement is the problem. Whether a float sum may be folded from changes at all. What the inner query's own maintenance costs for the bodies views have. A number for "lightning fast". | *Where are we going?* The measurement; the statement confirmed; the hypothesis tested on the folding query. |

## 2026-11-18, Describing: asked for efficiency, with prior art

**Asked.** The maintainer, after a view on #773 measured slow for reasons
of its own: the engine's aggregation should apply, incremental maintenance
included; grouping rows after the query, as the JavaScript precursor did,
is the last resort; and the literature should show how to do it very
efficiently.

**Learned.** The request arrived as a remedy. Under it: what a subscriber
of a folded value pays for one change, which nobody has measured. The
aggregation note names the gap itself, as A5. Four ways are in the
literature, and they differ in where the state lives: nowhere, in the
subscription, or in the tree.

**Decided.** Not decided. The agent's hypothesis is in the ledger: fold the
changes, state in the subscription.

## 2026-11-19, Diagnosing to Delimiting: what a subscription does, and what tonk does

**Asked.** What a subscription does today when a fact changes, and how tonk keeps a view current, each read from the code by a thread of its own. The maintainer asked that most of the design effort go here.

**Learned.** A subscription does not ask its query again: it derives the touched entity again and reports what started and what stopped holding. A fold breaks that, since it is evaluated over all of its body at once; so a fold in reach means asking again. tonk, for its part, folds on the page: every change costs every row there, and counts are an element's script.

The agent had feared that folding the changes needed something the engine does not make, signed rows of a rule's body. It does not make them inside. It reports them at a subscription's edge, which is enough: the fold can stand on a subscription of its inner query.

**Decided.** Not decided. A problem statement and the hypothesis are written, for the maintainer.

## 2026-11-19, Delimiting: what the literature asks of each fold

**Asked.** How systems keep a fold current when facts are retracted, read from papers and docs by a thread of its own ([literature.md](literature.md)).

**Learned.** Folding the changes is three costs and not one: a number for `count`, `sum` and `avg`; a count for each value for a set; the group's values for `min` and `max`. The hypothesis was wrong on `min` and `max`: the kept answer is not indexed by group. A float sum folded from changes need not equal one folded from the rows, which matters where two replicas must agree. Recursion through a fold is what the heavy machinery in the literature is for, and a query no rule reads does not have it.

**Decided.** Not decided. The hypothesis stands for `count`, integer `sum` and a set of values, and falls back to folding the group again for the rest. Rests on the open hypothesis about float sums.


## 2026-11-19, Delimiting: the fold measured, and delta queries read

**Asked.** The maintainer: he had meant to build DBSP whole and left it because it assumes a full replica and because dialog's machinery looked like enough. He wants an engine that keeps a view current at the cost of the change and works on a partial replica, and asked that delta queries and 3DF be read again and the code be read for what it can do.

**Learned.** The fold costs every group: 138 ms for one tag among 1000 to-dos, where a view's rows cost 4 to 5 ms and a plain subscription 0.24. Dialog already has the hard parts of a delta query: a signed diff scoped to what the query read, which works on a partial replica, and the rest of a body asked with a changed fact's bindings. It throws the rows away and keeps the entities ([delta-queries.md](delta-queries.md)). The papers' rule counts derivations and dialog's rows are a set; the backward/forward check answers that without counts. No source read handles negation or, Noria apart, partial input.

The agent proposed a wider problem, "a subscriber pays for the unit derived again, not for the change", and withdraws it on this evidence: for a join the unit is within a factor of the change. The order is lost in the fold, as the topic says, and in the scan of the kept answer.

**Decided.** Not decided. The statement still waits on the maintainer.

## 2026-11-19, Building: direction by the maintainer, and what the state is

**Asked.** Whether the statement is the problem, twice, and then what views as queries are for. The maintainer: he is not doing design here. The notation and its views were arrived at with evals and refinements already written down, the prototype's scaling was a prototype's, and he will not go through arriving at views again. What is needed is an efficient implementation of views and the gaps closed so it can land.

**Learned.** The agent kept asking for a problem statement for a decision already made. The statement in the topic stands as proposed and is not put to him again.

**Decided.** By the maintainer: build. The agent's plan, which he liked: index the kept answer by entity after a profile; the fold kept from signed rows; the rows of a seeded join kept, later and only if measured. What a fold's subscription holds is in [design-fold-state.md](design-fold-state.md), the agent's decision: each group's rows folded again by the engine's own fold, so it equals asking again by construction, with a value changed by a step as a later refinement. Rests on the open hypothesis that the inner subscriptions cost the change.

## 2026-11-19, Measuring: ten thousand rows, release, and where one poll goes

**Question.** Three hypotheses of the agent's: the scan of the kept answer is what grows; a view's change is within 2 times a plain subscription on a release build; a change through another entity costs the rows it changes.

**Method.** `cargo run --release -p dialog-notation --example cost -- 5` at 10, 100, 1000 and 10000 on `wip/engine-fold` at `56b5a13`, with probes inside one poll, since removed. [Output](cost-release-2026-11-19.txt).

**Results.** All three refuted.

| one change, ms | 100 | 1000 | 10000 |
|---|---|---|---|
| plain subscription, rename | 0.36 | 0.58 | 1.66 |
| a view, rename | 0.81 | 3.66 | 296 |
| one row reached through another entity | 1.32 | 2.88 | 115 |
| a list renamed, N rows change | 10.9 | 111 | 5639 |
| the fold asked again | 9.1 | 105 | 3059 |

**Learned.** What grows for one changed row is the diff. A subscription whose rule is keyed by an entity records a range for each entity it read, and the diff scoped to ten thousand ranges costs 80 to 200 ms where the rest of the poll is under 4. Nobody had guessed it. The scan of the kept answer is real and second: it is most of a plain subscription's growth, and it is 4 s of the 5.6 when N rows change, entities times rows.

So there are three things to fix in the engine, and the fold is the third: the diff over many ranges, the answer compared by a scan, the fold asked again.

**Decided.** The agent's, in the maintainer's absence: fix them in that order of what they cost, the first two on main as a defect of the subscription, the fold on its own branch.

## 2026-11-20, Measuring: the whole with the subscription's fix

**Question.** With #838's two fixes applied, does one change to a view cost the same at 1000 and 10000 rows, within 5 times a plain subscription?

**Method.** `cost.rs`, release, 3 runs, on `wip/engine-fold` and on an unpushed branch with #838's two commits picked onto it. The machine was shared, load 3 to 4 on 4 cores.

| one change, ms, without the fix and with | 1000 | 10000 | with the fix: growth, and times plain |
|---|---|---|---|
| a view, rename | 4.2, 2.9 | 301, 46 | 16, 38 |
| a view, tag added | 3.8, 2.4 | 312, 26 | 10.6, 29 |
| a view, to-do added | 4.7, 2.8 | 346, 55 | 19, 36 |
| select one card | 2.9, 3.0 | 4.2, 4.1 | 1.4 |
| plain subscription, rename | 0.58, 0.59 | 1.9, 1.2 | 2.1 |
| tags folded by the engine | 0.80, 0.81 | 3.0, 1.3 | 1.7, 1.5 |
| one row through another entity | 3.0, 1.9 | 78, 6.9 | 3.6, 5.7 |
| a list renamed, N rows | 126, 128 | 4639, 1351 | 68 us a row, flat |

**Learned.** The fix is worth 6 to 12 times at ten thousand rows and the bars are still missed by every view measurement. What is left, by probes since removed: a subscription whose rule joins for each entity has a cover of 10006 to 20012 ranges, and each poll builds the scope from it again (2.6 to 5.7 ms), walks the difference (2 ms) and drops the scope (1.5 to 3.5 ms): 6 to 12 ms a poll for each such layer, where a cover of 6 to 9 ranges costs 0.7. Comparing kept rows is 20 us now. The notation's maintainer took 15 to 30 ms counting ten thousand instances again, which a view's count on the fold removes for this card. The fold and `select` of one card meet their bars.

So the cover itself is the next thing: a range for each entity is built, merged and dropped on every poll.

**Decided.** The agent's: keep the cover from poll to poll and make it coarser where it holds a range for each entity of one attribute, both as a change of their own after #838.

## 2026-11-20, Measuring asked for: what a person waits for is round trips

**Asked.** dialog-db/dialog-db#844 made a poll's cover smaller and failed the Replication gate on one row: the same 82 requests and the same bytes, in 11 round trips where main takes 8. With the points left out of the cover the scoped diff no longer fetches the leaves of the entities about to be derived again, and they are fetched by re-derivation, 16 entities at a time. That half was reverted and #844 is the other half, a poll that borrows its cover. In passing: with the smaller cover and a window of 64 the two phases take 5 and 6 round trips, fewer than main's 11 and 8. Whether to measure the window was put to the maintainer.

**The maintainer:** yes. "Many waves means many network roundtrips which means longer waits for ppl. So things may be improving perf locally but degrading actual experience."

**Learned.** Every number this topic has chased is of a replica that holds its data: instructions, allocations, milliseconds in memory. For a replica that does not, the cost a person feels is the number of waves, and a change can lower the first while raising the second. The bar for fast has two parts and only one was written.

**Decided.** By the maintainer: measure. A research topic on its own branch, `research/rederive-window`: what decides the round trips of one poll on a replica that does not hold the data, across every replication report and network shape, for windows from 16 to unbounded with the cover as on main and smaller; what a wider window costs where nothing is fetched; and whether a poll that fetches in one wave what its re-derivations will read is better than tuning a number. Hypotheses and bars are written there before any run.

## 2026-11-23, A rule fired in full fires for every match, and that is to be fixed by maintenance

**Asked.** dialog-db/dialog-db#791 fixes what 0.5.0 misses: a rule over an attribute a deductive rule derives from what a commit writes was seeded with the row written, and a seed finds the row's value at the row's entity. Three shapes are missed by any seed: the attribute derived for another entity (an event mirroring another's kind), derived at the entity written but at another value (an alias beside a kind), and derived from another attribute written in the same commit (a kind from a tag). A narrower test, the agent's, was tried and is unsound: the second and third derive at the entity read. So such a rule is evaluated in full.

**What that costs.** A full firing fires for every match, the old ones too. In this case a command is issued again for entities the commit did not touch, and a `last` head is written over a later write by hand. Main does the same already for an attribute that changed only derivedly. Measured, one row a commit, a rule over events and a deductive rule restating an event's kind: 283M instructions on main and 379M with the fix at 1,000 events, 2.79G and 3.76G at 10,000, against 4.3M and 5.6M with no deductive rule. So main was already proportional to the relation here, and the fix adds a third.

**The maintainer:** "Things should be sound", and of the firing for every match: "we should followup with proper incremental maintenance fix".

**Learned.** Induction has the problem subscriptions had. A subscription finds the affected entities by joining the change sideways at the old state and at the new, derives those again, and reports what was asserted and what was retracted. Induction fires on matches, has no old state to compare with, and so chooses between a seed that can miss and a scan that repeats.

**Decided.** By the maintainer: follow #791 with the proper fix. What it has to hold, to be made a problem statement before any approach:

- a rule fires for the matches a commit makes, and for no match that stood before it, whether the change reached the rule through a row or through a deductive rule;
- the work is proportional to the change, not to the relation, also where a premise reads a derived attribute;
- the tests of #791 that assert a firing for an untouched entity (`fired`, beside `made`) are the ones that change: `fired` becomes `made`.

Not decided: whether induction keeps what it matched before, as a subscription keeps its answer, or derives the old matches again at the commit's base; and whether one mechanism can serve both a subscription's poll and a commit's induction.
