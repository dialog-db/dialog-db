# A folded value is recomputed for its subscriber, not maintained

Phase: **Building.** Directed by the maintainer on 2026-11-19; the problem statement stays as proposed. What is built first is in [design-fold-state.md](fold-maintenance/design-fold-state.md).
phases mean, and the [journal](fold-maintenance/journal.md) for where the
topic stands.

## Description

Dialog can fold: a deductive rule may carry a `reduce` clause that groups
its rows by the head's other fields and folds each group
([`notes/aggregation.md`](../notes/aggregation.md),
`rust/dialog-query/src/reduce.rs`). An authored rule with one is refused
today by `open_rule_error` in `rust/dialog-query/src/rule.rs`; the
maintainer has said the engine may allow it, with the notation the only
place a fold is written and only in a query, which no rule reads
(dialog-db/dialog-db#773, `design/macros/design-queries.md`).

The aggregation note leaves keeping a fold current to its last milestone:
"A5 (later): incremental maintenance under subscriptions
(recompute-per-poll is correct first)".

A view built on the notation was measured on #773 (2026-11-18,
`rust/dialog-notation/examples/cost.rs`). A plain subscription of a concept
answers one changed fact in under a millisecond among 10 to-dos and among
1000. The notation's view, which put rows together in memory, took 110 to
145 ms among 1000. That cost was the notation's own doing and is being
removed there; what a reducing rule under a subscription costs has not been
measured.

The maintainer: if a view cannot be lightning fast it will not fly. In
dialog's JavaScript precursor he grouped rows after the query, and would
keep that as the last resort.

## Problem

**Proposed, not confirmed.** Whoever subscribes to a value folded from many facts pays, for one changed fact, for every row of every group and not for the change. A subscription keeps rows current by deriving again the entity a change touched; a fold is evaluated over the whole of its body at once and has no entity to derive again, so with a fold in reach the subscription asks its whole query again
([as built](fold-maintenance/subscriptions-as-built.md), sections 5 and 6). A view that counts or collects therefore costs what its page holds, on every change.

Read from the code and pinned by a spike's test (5 recomputes, 0 maintenances); what it costs in time as groups grow is being measured on #773.

What a user could do were it solved is in [use cases](fold-maintenance/use-cases.md), taken from what tonk works around today ([as built](fold-maintenance/tonk-as-built.md)).

## Approach

Not decided. [direction.md](fold-maintenance/direction.md) has the prior art and a hypothesis that fits what dialog already does: a folding query's subscription is a subscription of its inner query, whose polls already say what started and what stopped holding, with a fold of those changes held beside it.
