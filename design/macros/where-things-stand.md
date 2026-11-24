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
- Tests: lib 123, `notation.rs` 43 of 44, `query.rs` 22 of 23, `view.rs` 11 of 14, the same in a browser. The five that fail fail on one thing, the next section.

## In the way

1. 🔴 A rule that holds a pattern cannot be stored (format 5, #799), and the session overlay refuses one. A query whose row has more than one column beside its entity concludes it as one record by a map pattern, so it runs asked once and cannot be kept current. 14 of 31 layers of the views in the sketches and tests hold one, and every layer nested two levels deep does. The coordinating session has #799 ported onto #778 as `feat/rule-format-5`; it is merged here when ready.
2. The cost on the new engine is not measured: `examples/cost.rs` stops at the card's rules (1). The bars are in the ledger and in [gaps.md](gaps.md).
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
- #799 is being ported onto #778 as format 5, `feat/rule-format-5`.
- tonk-labs/tonk#1338: the notation in tonk, pinned to a revision older than all of the above.

## To build here

One crate at a time, `CARGO_BUILD_JOBS=2` for release builds, and the browser daemon stopped before a large native build. The clippy the repository pins is 1.99 (`flake.lock`'s rust-overlay); 1.100 refuses `dialog-credentials` on main.
