# A value computed from many writers' claims either loses writes or keeps every one

Phase: **Directing.** See the [design index](README.md) for what the
phases mean, and the [journal](computed-values/journal.md) for where the
topic stands.

## Description

Tonk's guide teaches a counter as one stored `count` and a rule that, on
each `increment` command, reads the count and asserts it plus one. Tonk
measured N concurrent increments, each writer on a branch of its own,
merged into main: 4 ended at a count of 1, and 8 at 1 or 2, with or
without rules run after each pull (tonk `research/concurrent-cli-writes`,
2026-11-25). Irakli says the counter is modeled wrong, not merged wrong: a
counter should keep every increment, like a set, and count them.

Kept that way, it works. With each increment a stored claim and the count
derived on read, 18 runs of the same harness ended at N every time
([evidence](computed-values/tonk-counter.md)). But tonk's notation has no
fold, so the count was derived by ranking the increments through
recursion and negation: 0.7 s to read 16 of them, 10 s for 32. Dialog's
query layer has `count` and `sum` (`Fold`, the `reduce` clause), and a
deductive rule an author writes is refused one (`ReduceInOpenRule`)
because "where a computed value belongs is not settled"
([`notes/aggregation.md`](../notes/aggregation.md)).

Irakli does not want to keep every increment just to say how many there
were, and asks whether CRDTs do better. They do: a state-based G-Counter
keeps one entry per writer, each writer changing only its own, and sums
the entries on read; Yjs builds its counter that way. He sees this as
what fits dialog, an index that grows with the writers and not with the
writes, and asks whether it is more than counters: a merge policy that
writes to a cell the writer owns and answers a read with the aggregate
across them. He keeps values per replica already, mostly to avoid shared
state.

## Problem

An application wants a value that many writers change at once, offline or
on separate branches, and that reads as one value: a count, a total, an
average, a set of members, the latest of each device's readings combined.
Concurrent writes to one claim keep one of them, so a value written by
reading it and writing a successor loses every concurrent write. Keeping
each write as a claim and computing the value on read loses nothing, but
the claims grow with the writes, and an authored rule cannot compute the
value (no computing aggregate is admitted, for want of a place for its
result to stand without competing with stored claims).

So an application can have a value many writers change, or a value whose
storage does not grow with its history, or a value it can read through a
rule, and not all three.

## Approach

Not decided. The [direction matrix](computed-values/direction.md) weighs
five approaches. The one the maintainer leans toward, and the one the
matrix was built to examine, is the third column: **owned cells and a fold
on read**. A writer writes only a cell keyed by it as the owner, under
cardinality one, so its own writes succeed one another and nothing
concurrent ever meets in that cell; a read folds the cells of an entity
with a computing aggregator (`sum`, `count`, `avg`, `union`) or reads its
own cell alone. A G-Counter is that with `sum`; replica-local state is
that with "mine".

Open before it can be chosen: what the owner is (a replica, a branch, an
agent, a member) and how a writer knows it; whether the history index
keeps every succeeded claim, which would make storage grow with the
writes anyway; where the folded value stands, the question that keeps
`reduce` out of authored rules.
