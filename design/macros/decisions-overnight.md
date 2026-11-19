# Decisions made while the maintainer was away, 2026-11-19 to 20

He asked that the work go as far as it can overnight on the agent's judgment, each decision noted so it can be revisited. Each row: what was chosen, why, what it rests on, how to undo it. From him before he left: an optional field is an annotation on the concept's attribute; when in doubt, do as Datomic does.

| # | decision | why | rests on | to undo |
|---|---|---|---|---|
| 1 | Commits are authored and signed by the environment's identity, R. Daneel Olivaw | the maintainer, 2026-11-19: "r daneel is what I expect". Two commits made before that carry his name unsigned (`67b6094`, `c1acb67`) | his word | nothing to undo; they squash on merge |
| 2 | A fold's subscription keeps each group's input rows and folds a touched group again with the engine's `Reduce::fold`; a running value for a group is a later step | it equals asking again by construction for every fold, a float sum included | `design/fold-maintenance/design-fold-state.md` on #837 | replace the state behind the same property test |
| 3 | The engine is fixed in the order of what one poll costs at ten thousand rows: the diff over many ranges, the kept answer compared by a scan, the fold asked again. The first two go to main as a defect of the subscription, with a reproducing test first | measured: 80 to 200 ms, up to 4 s, and 105 ms to 3 s | `design/fold-maintenance/journal.md`, the measuring entry of 2026-11-19 | each is a pull request of its own |
