# Journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | A value written by reading it and committing its successor loses concurrent writes (tonk, 4 to 1). Keeping each write as a claim and computing on read keeps all of them (18 runs), but stores every write, and tonk can only compute it by recursion, at more than cubic cost. Dialog folds in queries and refuses a fold in authored rules, for want of a place for a computed value. A G-Counter, one cell per owner and a sum on read, is the general shape the maintainer leans toward; dialog already elects within a cell by standing, which an owned cell relies on. | Directing: problem stated, five approaches in the matrix, nothing decided. |
| **Agenda** | Whether history keeps every succeeded claim, which decides whether owned cells bound storage at all. Whether an owner's newest claim is always elected after a merge. What the owner is, and where a writer learns it. Where a fold over owned cells stands: a computing pick on a field, or something else, and whether that answers `ReduceInOpenRule`. Which values each column cannot hold, written as use cases. | Next sitting: the history count (a research topic), then the owner. |

## 2026-11-25, Describing to Directing: from tonk's lost increments to owned cells

**Asked.** Can tonk's notation derive a count from stored facts, and if so
does a counter kept as its increments survive writer branches? Answer:
the notation has no fold (`picks.rs`: `last`, `all`, `top`, `max`, `min`);
a count can be had by ranking increments with `<`, `unless:` and
recursion, and so modeled it ends at N in every run where the guide's
model ends at 1 ([evidence](tonk-counter.md)).

Irakli: how do Automerge, Yjs and other CRDTs do counters, and can dialog
do better than holding every increment just to count them? Answer:
Automerge's `Counter` adds concurrent increments up and, as far as is
known, keeps them in history; Yjs has no counter and teaches one as a map
keyed by client ID, each client writing its own key, summed on read; that
is a state-based G-Counter, and a PN-Counter is two.

Irakli: the G-Counter fits dialog, an index that grows with writers and
not writes. Is there a generalization, a merge policy that writes to a
cell the writer owns and reads the aggregate across them? He keeps values
per replica, mostly to avoid shared state.

**Learned.** The generalization is about which aggregates may be merged
by repetition: a selective one (`last`, `max`, `min`) is idempotent and
dialog already uses it as a pick; a computing one (`sum`, `count`) is not,
and becomes exact when each contribution has one owner and the fold runs
on read over disjoint owners. Per-replica state is the same storage read
as "mine". The shape covers sums, counts, averages (as a sum and a
count), distributions, unions and each owner's latest; it does not cover
an invariant across writers, which needs coordination (CALM).

Two things found in dialog change what the question is. A deductive rule
an author writes is refused a `reduce` because a computed value would
compete with the stored claims of its attribute; a fold on read over
owned cells is a candidate answer to where it belongs. And the history
index records each claim each revision writes, with nothing found that
prunes it, so an owned cell may bound the value indexes and not the
store: Irakli's objective is met only if that is false or history can be
pruned.

Contradiction found: Irakli's earlier opinion in tonk, "a counter should
keep every increment, like a set", and his opinion here, that the store
should not grow with the increments. Both are written in the ledger; the
second is what this topic takes as the objective, since he gave it last.

The guide's failure reads more precisely in CALM's terms than as a
modeling error: it commits (`assert!`) a value it read non-monotonically.
Deriving on read is safe whatever the value; committing is safe only when
one writer owns what was read.

**Decided.** Not decided. The maintainer leans to column C; the history
claim and the owner are what would decide it.
