# A folded value is recomputed for its subscriber: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* The engine folds behind a gate, and leaves keeping a fold current for later. A plain subscription does not grow with the page. Prior art for four ways of keeping a fold current is laid out. | *Where are we at?* Diagnosing. No problem is stated: nothing about a fold under a subscription is measured. |
| **Agenda** | *What do we need to know?* What a reducing rule under a subscription costs as its groups grow. Whether a subscription reports what stopped holding. What "lightning fast" is, as a number. | *Where are we going?* The measurement on #773; then a problem statement; then the matrix with its `?` filled. |

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
