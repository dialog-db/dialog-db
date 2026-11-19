# A folded value is recomputed for its subscriber, not maintained

Phase: **Diagnosing.** See the [design index](README.md) for what the
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

Not stated yet. A candidate, to be confirmed by measuring: what one changed
fact costs the subscriber of a folded value grows with the group it belongs
to, or with every group, where a subscriber of the rows pays for the change
alone.

## Approach

Not decided. [Prior art](fold-maintenance/direction.md) is gathered, with
what each would ask of dialog.
