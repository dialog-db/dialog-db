# Where things stand, 2026-11-24

A handoff note, written so the work can be picked up from the files alone. It is a snapshot; the [journal](journal.md) and the [ledger](claims.md) are the record, and [gaps.md](gaps.md) the inventory. The note of 2026-11-19 it replaces is in this file's history.

## Decided, by the maintainer

- Two kinds. Rules and concepts give rows and never fold. A query nests and folds, and no rule reads one. ([design-queries.md](design-queries.md), and `research/notation/design.md` on `docs/notation`, #728.)
- A query is `(query (name ?entity … is: ?answer) condition… (is ?answer shape))`; a view says `(view ?answer template)` there. Asking is `select`.
- The whole of a field is written `distinct`, a set always, and a map is made where the answer is built, `{... ?entries}`. Not built: the build still says `collect`.
- The engine carries the fold, and an authored rule still cannot fold. The notation is an interface to the engine and evaluates nothing itself where the engine can state it.
- Cardinality is the attribute's and an elector is the concept field's `pick` ([slotting-cardinality.md](slotting-cardinality.md)). A listed `as` implies no pick. Rule format 4 is #827's; a stored rule that holds a pattern is format 5.
- Rows are reported under the entity they are about, a row of several columns as one record (decision 15 of [decisions-overnight.md](decisions-overnight.md)), and that stands.

## Built on `feat/notation-poc` (#773), 2026-11-24

The branch was moved onto the engine as it will land: `feat/patterns` (#778) merged in with its engine files taken whole, which brings main through #784 (#827, #838, #839, #844). The older history of #778 and #799 it held is dropped. `wip/engine-fold` was merged at `698d828`, its notation only; its engine commits are #839's.

- A syntax tree is kept as the engine's composite value. Every tree's name moved once more.
- Cardinality: a line's `cardinality:` reaches the attribute as written, a line with none reads every claim, an elector in a head, `status: (top ?s)`, or on a line, `is: (top ?s)`, is the field's pick, a variant's cases are listed. No `Pick` is left.
- The driver, `dialog_notation::live`: `Live::select` and `Live::render` ask once through a query layer; `Live::standing` and `Live::watch` subscribe, with each read the engine folds on `Branch::subscribe_fold`.
- A collecting read of one variable or one entry is lowered to the engine's `Fold` where its shape allows, and a view's count of what its own markup collects is kept on it. `Aggregator::Distinct` is added to `dialog-query` for it, in two commits that touch nothing else, so it can go to main on its own.
- Tests: lib 123, `notation.rs` 44, `query.rs` 23, `view.rs` 14, all passing since format 5 (#853) is on main and merged in.

## In the way

1. Format 5 is on main (#853). A query whose row has more than one column beside its entity concludes it by a map pattern, so it is kept current only with format 5: 14 of 31 layers of the views in the sketches and tests, and every layer nested two levels deep.
2. The cost ([journal](journal.md), 2026-11-24, two entries): `Maintainer::apply` is flat since it keeps each parent's instances in order, and a view's count is folded alone. On the branch with format 5 in ([output](rounds/cost-2026-11-24-branch.txt)):

| one change, `Live::watch` | N=1000 | N=10000 | times plain at 1000 | at 10000 | before the fixes, 1000 / 10000 |
|---|---|---|---|---|---|
| rename | 2.4 ms | 35 ms | 7.4 | 45 | 3.3 / 56 |
| tag added | 1.2 | 4.5 | 9.4 | 6.6 | 1.3 / 5.1 |
| to-do added | 2.4 | 25 | 6.8 | 32 | 3.7 / 62 |
| `select` one card | 2.1 | 2.5 | | | 2.1 / 2.8 |

   At ten thousand most of a rename or a to-do added is the fold's subscription folding the list's one group again over its ten thousand rows, 28 to 34 ms: the engine's, and a running value per group is being built for it (`perf/fold-running-value`). Views keep counting on the fold.
3. Still evaluated in memory: a collected call to another query, a call every answer needs, a read whose conditions or rows are more than one query, a read grouped by more than one field beside a recorded row, a shape `(is ?x …)`, the joins between queries; and a view's count of a collected call, of a count in a collected query's template, of entries, or grouped by other than the root.
4. Not built: `distinct` and the spread, definitions on the branch, names and namespaces, composites for syntax trees beyond what is kept now, variant cases, layers but `:control`, a limit, a query that calls itself.
5. Not decided: `cardinality: :one` beside `top` (refused, as #827 does); rows of one head; what two entries of one key leave in a map.

## What `Unsupported` refuses, 32 sites

| refuses | sites |
|---|---|
| negation of more than one proposition: `!=`, a formula, `(:id …)` or a variable twice inside `unless` | 4, `compile.rs` |
| commands and writes not decided: a command with a `when`, a retract of a command, a retract of a collection without its key or value, a concept that names its entity and holds control lines, one that names none and holds stored lines or both, lines of several entities or domains | 9, `compile.rs`, `kernel.rs`, `expand.rs` |
| an entity a rule makes: `fresh`, and whether `id` names a domain or a concept | 3, `expand.rs` |
| what the engine refuses to derive: from no premise, a fold in a rule that derives, a report of a computed value from one read | 3, `compile.rs` |
| a layer other than `:control` | 1, `expand.rs` |
| a formula the build does not know | 1, `compile.rs` |
| values: `... x` where a value stands, a constant that is bytes, a record or a composite, a boolean in a template | 3, `compile.rs`, `syntax.rs`, `view.rs` |
| queries and views not built: conditions that bind nothing, markup not collected, a function in a shape other than `count`, a view through several definitions, a required call in markup, a shape, a count of what is not collected, or the whole of a read in markup | 8, `expand/query.rs`, `select.rs`, `view/plan.rs` |

## Other pull requests

- #778 (`feat/patterns`) is landing on main; once it has, main is merged here and not `feat/patterns`.
- #853, format 5, is on main; main is merged in at `2b12204`.
- tonk-labs/tonk#1338: the notation in tonk, pinned to a revision older than all of the above.

## To build here

One crate at a time, `CARGO_BUILD_JOBS=2` for release builds, and the browser daemon stopped before a large native build. The clippy the repository pins is 1.99 (`flake.lock`'s rust-overlay); 1.100 refuses `dialog-credentials` on main.
