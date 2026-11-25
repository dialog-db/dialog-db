# Direction

**Decision:** how dialog lets many writers change a value that reads as
one (a count, a total, an average, a combination of each device's own
value), concurrently and across replicas.

**For:** concurrent writes to one claim keep one of them, keeping each
write grows the store with the writes, and an authored rule cannot
compute the value ([the problem](../computed-values.md#problem)).

| criterion \ approach | A. Status quo: a rule reads the value and commits its successor into one cell | B. Every contribution a claim, folded on read | C. Owned cells, folded on read: each writer writes only the cell keyed by its owner; a read folds the cells | D. Every contribution a claim, compacted once stable: B, with contributions every replica has seen folded into one base claim | E. One writer at a time: writes to the value take turns, in a queue, an owner process or a lease |
|---|---|---|---|---|---|
| concurrent writes, on branches or replicas, after a merge | 🔴 one cell, one claim elected by standing: all but one write lost ([evidence](tonk-counter.md)) | 🟢 every contribution is a claim of its own and the merge is a union; N of N kept ([evidence](tonk-counter.md)) | 🟢 no two writers share a cell, so nothing concurrent meets; ? that each owner's newest claim is the one elected after a merge (claims ledger) | 🟢 as B until compaction; compaction itself is a write only one replica may make, ? which | 🟢 within one replica, where tonk's queue counted N of N; 🔴 across replicas offline, where no one grants the turn |
| what the value indexes grow with | one claim per value | 🔴 the contributions, for ever | 🟢 the owners | the contributions not yet stable | one claim per value |
| what the history index grows with | the writes, ? (claims ledger) | the writes | 🟡 the writes, ? if nothing prunes history (claims ledger) | the writes, ? unless compaction also drops their history | the writes, ? |
| what a writer has to know | nothing | nothing, but a body no other contribution has: the content-addressed entity of two equal bodies is one, and tonk's `.timeStamp` repeats across replicas | 🟡 its owner, stable and its own; ? where it comes from (a replica's identity, a branch, an agent's key, a member) | as B | where the turn is taken |
| where the computed value stands | as a stored claim, competing with the next write, which is what loses writes | only on read; no claim holds it, so nothing competes with it | only on read, as a field's fold over the owned cells; nothing written holds it, so ? it answers `ReduceInOpenRule`'s question of where a computed value belongs | on read, over the base claim and what followed it | as a stored claim, written by the one writer |
| cost of a read | one claim | 🟡 a fold over every contribution; a standing fold steps a group's value per change (`Reduce::runs`) | a fold over the owners | a fold over the base and what is not yet stable | one claim |
| values beyond a count: sum, average, distribution, set, each device's latest, "mine" | 🔴 only what one writer at a time may set | any computing aggregator over the contributions; "mine" is a filter by who wrote | 🟢 any aggregator that combines per-owner partials (sum, count, a sum and a count for an average, union, max of each); "mine" is one cell; 🟡 an average or distinct count needs partials, not the values | as B, for aggregates whose partials compact (not a distinct count without keeping the values) | any |
| read in a rule, and decided on | yes, as any claim | 🔴 only by recursion and negation today (more than cubic, [evidence](tonk-counter.md)); `reduce` is refused in authored rules | ? a field's fold is a computing pick, which no pick is today; an aggregating edge like any fold, so not inside its own cycle | as B | yes |
| an invariant across writers: a unique next ident, one claimant, a positive balance | 🔴 lost | 🔴 not kept | 🔴 not kept; a per-owner budget (escrow) keeps some, by splitting the bound among owners | 🔴 not kept | 🟢 kept, by the turn |
| a writer that stops writing for ever | nothing to do | its contributions stay, as they should | 🟡 its cell stays; dropping it loses its contribution, and folding it into a retired cell is a write only one replica may make | its contributions compact into the base | nothing to do |
| what exists to build on | built | 🟡 `Fold` and standing folds in queries; not in rules, not in tonk's notation | 🟡 cardinality one, picks, `Fold`; no owner key, no computing pick | 🔴 nothing knows which contributions every replica has seen | 🟡 tonk's eval queue on one replica; nothing across replicas |

Signs read off the matrix:

- No row separates B from D but storage and what exists; D is B with a
  coordination problem added, so it is kept only to show that compaction
  is not free.
- C is the only column whose storage grows with something other than the
  writes in the value indexes, and the history row may take that away:
  the claim in the ledger decides whether C meets Irakli's objective or
  only moves the growth.
- E is the only column that keeps invariants. It is not a rival of C but
  the answer for the values C cannot hold; the matrix says which values
  need which.
- No column is green on "read in a rule": every approach but A and E
  needs a computed value somewhere a rule can read, which is the
  question `ReduceInOpenRule` leaves open.
