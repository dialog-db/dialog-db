# A folded value is recomputed for its subscriber: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* How a subscription is kept current today and where a fold makes it ask again. How tonk keeps a view current and what it works around. A problem statement is proposed. A hypothesis: fold what the inner query's subscription reports. | *Where are we at?* Delimiting. The statement waits on the maintainer; the cost of a fold under a subscription waits on #773. |
| **Agenda** | *What do we need to know?* Whether the statement is the problem. What the literature asks of a substrate for each kind of fold (a thread is reading it). What the inner query's own maintenance costs for the bodies views have. A number for "lightning fast". | *Where are we going?* The measurement; the statement confirmed; the hypothesis tested on the folding query. |

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

