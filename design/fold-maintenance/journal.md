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
