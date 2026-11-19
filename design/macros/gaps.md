# What stands between the build and landing, 2026-11-19

An inventory read from the files by a thread of its own, corrected by the managing agent where it was stale. T blocks tonk's migration, L blocks landing #773, N neither. Evidence is cited so each row can be checked; nothing here was run.

The notebook bar no longer finds failures: 58 forms read, 55 expand and compile without core, 62 of 62 with it (`tests/notation.rs` 2326-2379). What it does not count are the `; GAP:` lines in the sketches: 25 in `notebook.clj`, 22 in `table.clj`, 6 in `core.clj`.

| # | gap | evidence | blocks | size | needs first |
|---|---|---|---|---|---|
| 1 | tonk#1338's back end is stale: pinned at `c938ec9`, before queries; its views came from `View::describe`, which stage 2 removed | `tonk-notebook-runs.md` 3; `tonk-notebook-bar.md` 104-108; `src/lib.rs` 31-35 | T | days | 2, and what a view lowers to for tonk (`tonk.md` 118) |
| 2 | the crate has no subscription driver: each layer is one `Branch::subscribe` put together by the caller, rules asserted into the overlay by hand; the crate does not depend on dialog-repository | `tests/view.rs` 6-12, 175-177; `examples/cost.rs` 156-158; `examples/notebook/session.rs` 41-211 | T, L | days | 3 |
| 3 | the notation still evaluates in memory: calls, shapes and grouping in `select.rs`; the fold lowering covers only a read that is one query collecting one variable, and falls back otherwise; views use no `Fold` | `select.rs` on `wip/engine-fold` 297-341, 538, 634-683, 732-800 | L, T | days | 4 |
| 4 | the engine's folding query needs a pull request of its own below #773 | checks green at `67b6094` | L | hours | none |
| 5 | a fold is asked again on every poll: 138 ms among 1000 | #837, `design-fold-state.md` | L, T | days | 4 |
| 6 | concepts pinned to an entity: the notebook's routes are not found | `tonk-notebook-runs.md` 56-58; GAP `core.clj` 13, 44 | T | a decision and a day | the maintainer |
| 7 | a command's lines default to pick `all`, which moves block entities and position keys | `tonk-notebook-runs.md` 43, 77 | T | hours | the maintainer |
| 8 | optional fields, `maybe:`, 25 uses in tonk's library, have no spelling in a rule or a template | GAP `core.clj` 15, 52; `table.clj` 104, 357 | T | days | a decision |
| 9 | templates: `with="{branch}@{repo}"`, `{dom.host/model}`, `prevent-default` (9 of 38 events), `stop-propagation`, 5 event names off the convention, `false` as an attribute | `tonk.md` 54-55; `tonk-notebook-bar.md` 119-124 | T | days | spellings |
| 10 | markup queries narrower than the design: a required call in markup, a collected value in markup, and `count` the only fold | `src/view/plan.rs` 479; `design-queries.md` 325-327 | T | a day | 3 |
| 11 | tonk's library is not ported: 74 views, 140 concepts, 63 commands, 38 events; only the notebook and the table are sketched | `tonk.md` 11-32 | T | weeks | 1, 6 to 10 |
| 12 | tonk's install and runtime: a notation form of `check` for `prepare_seed`, the library's `build.rs` bundles YAML only, the service worker and `.clj`, the inspector's generators, the wasm build | `tonk-notebook-bar.md` 141-148; `tonk-integration.md` 147 | T | days | 1 |
| 13 | definitions on the branch, decided and not built | `definitions-on-the-branch.md`; `src/analyze.rs` 9-11 | L | days | the count of definitions that refer to each other |
| 14 | `distinct` for `collect` and `{... ?entries}`, decided and not built; `design.md` on #728 still says `collect` | `claims.md`; `where-things-stand.md` 10 | L | a day | 4 |
| 15 | writes not supported: retracting a command, a partial retract, a command with a `when`, a `when` that sees the document's own writes, `fresh`; `update` parked. Retract by the entity alone is built, and the ledger's row saying otherwise is stale | `tests/notation.rs` 820, 2300; `expand.rs` 1537 | N for the notebook | a day each | decisions |
| 16 | forms of the design not built: the `top` elector, variant cases, layers but `:control`, a limit, a query that calls itself | `kernel.rs` 22; `design.md` | N | days | none |
| 17 | hygiene: no CHANGELOG entry introduces the crate; #773 is a draft on `design/macros` (#772) and not on main; no `test:web` run recorded; 47 `Unsupported(` sites; the example's runner is logic the crate does not offer | | L | a day | 2, 3 |

```
 engine:    4 ──▶ 5 ──▶ 3 ──▶ 2 ──▶ 17
                  └──▶ 14        13 beside, after 4
 tonk:      6, 7, 8 decided ──▶ 1 (on 2) ──▶ 9, 10 ──▶ 12 ──▶ 11
```

Not known: whether nesting of two levels (`list/card`, `todo/row`, `todo/chip`) has a patch asserted, since `tests/view.rs` nests one; whether `test:web` passes for the crate; what tonk#1338's head holds now.
