# A value computed over many facts cannot be given a name: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | The notation's derived attributes and views are deductive rules that fold. dialog-query refuses a fold in a deductive rule so that a merge of rule sets never fails a query. The machinery for a fold in a rule is there, behind the refusal. | **Directing.** Five approaches are in [direction.md](direction.md). None is chosen. |
| **Agenda** | Whether a cycle through a fold can be a quarantine and not a failure, and chosen alike on every replica. What a subscription over a folded concept does when its rows change. Whether a query can state a fold today, and whether an inductive rule can. What #827 does with a folded value beside a stored one. | A spike on dialog-query for the first hypothesis in the [ledger](claims.md). Then the maintainer chooses. |

## 2026-11-18, Directing: from a refused getter to a problem

**Asked.** Building the notation (#773), a derived attribute that folds was refused by dialog-query. The maintainer was asked whether a view should compile to what tonk renders today or to the notation's design, which is a rule that folds. He answered: "Not sure what engine, but seems like design would be a right approach."

**Learned.** "The engine" was the wrong word for it: the refusal is one check in dialog-query, `open_rule_error`, in front of machinery that is there. The analysis already marks an edge from a reducing rule as aggregating and already checks stratification; `reduce_rows` already folds. What differs from `unless` is what a cycle does: a cycle through an absence test quarantines a rule, a cycle through a fold fails the evaluation. The refusal when a rule is compiled exists to keep a merge from reaching that failure.

The notation's rule that a fold concludes only into a concept's own domain answers a different question: it keeps a folded value from standing beside a stored one. It does not stop a cycle.

**Decided.** Not decided. The direction rests on one open hypothesis, the agent's: that a cycle through a fold can be quarantined as a cycle through `unless` is. A spike will say.
