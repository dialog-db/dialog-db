# What stands between the build and landing, 2026-11-24

An inventory read from the files by a thread of its own on 2026-11-19, corrected by the managing agent where it was stale, and brought up to the branch on 2026-11-24 after it was moved onto the engine of #778 and #839. T blocks tonk's migration, L blocks landing #773, N neither. Evidence is cited so each row can be checked; rows marked 2026-11-24 were run.

The notebook bar no longer finds failures: 58 forms read, 55 expand and compile without core, 62 of 62 with it, and the table 54 of 54 (`it_measures_how_far_the_notebook_gets`, `it_measures_how_far_the_table_gets`; unchanged on 2026-11-24). What it does not count are the `; GAP:` lines in the sketches: 25 in `notebook.clj`, 22 in `table.clj`, 6 in `core.clj`.

| # | gap | evidence | blocks | size | needs first |
|---|---|---|---|---|---|
| 1 | tonk#1338's back end is stale: pinned at `c938ec9`, before queries; its views came from `View::describe`, which stage 2 removed | `tonk-notebook-runs.md` 3; `tonk-notebook-bar.md` 104-108; `src/lib.rs` 31-35 | T | days | 2, and what a view lowers to for tonk (`tonk.md` 118) |
| 2 | ~~the crate has no subscription driver~~ Done 2026-11-24: `dialog_notation::live` (`Live::select`, `render`, `standing`, `watch`), merged from `wip/engine-fold`. 🔴 `standing` and `watch` lay each query's rule on the session overlay, which refuses a rule that holds a pattern until format 5 (row 18) | `src/live.rs`; `it_drives_a_view_with_a_collected_part_as_the_layers_do`, `it_keeps_a_selection_standing_through_the_driver` | T, L | done; row 18 | 18 |
| 3 | the notation still evaluates in memory, where the engine cannot state it. Lowered to `Fold` 2026-11-24: a read that collects one variable or one entry `{?k ?v}`, whose terms shared with the rest are bound by conditions that are one query; and a view's count of what its own markup collects. In memory: a collected call to another query, a call every answer needs, a read whose conditions or rows are more than one query, a read grouped by more than one field beside a recorded row, a shape `(is ?x …)`, and the joins between queries | `select.rs` `fold` 342-433 and `Evaluation` 584-850; `it_puts_a_collected_call_together_in_memory`, `it_puts_a_read_grouped_beside_its_entity_together_in_memory` | L, T | days | a fold over a query's answer |
| 4 | ~~the engine's folding query needs a pull request of its own~~ Landed as #839, without `distinct`; `Aggregator::Distinct` is on this branch as two commits touching only `dialog-query` (`5a79ab2`, `bcf4c50`) | | | done | |
| 5 | ~~a fold is asked again on every poll~~ `Branch::subscribe_fold` (#839) folds again only the groups a change moved, and #838/#844 scope a poll to what it touched | #839, #838, #844 | | done; measured in row 19 | |
| 6 | concepts pinned to an entity: the notebook's routes are not found | `tonk-notebook-runs.md` 56-58; GAP `core.clj` 13, 44 | T | a decision and a day | the maintainer |
| 7 | a command's lines default to pick `all`, which moves block entities and position keys | `tonk-notebook-runs.md` 43, 77 | T | hours | the maintainer |
| 8 | optional fields, `maybe:`, 25 uses in tonk's library, have no spelling in a rule or a template | GAP `core.clj` 15, 52; `table.clj` 104, 357 | T | days | a decision |
| 9 | templates: `with="{branch}@{repo}"`, `{dom.host/model}`, `prevent-default` (9 of 38 events), `stop-propagation`, 5 event names off the convention, `false` as an attribute | `tonk.md` 54-55; `tonk-notebook-bar.md` 119-124 | T | days | spellings |
| 10 | markup queries narrower than the design: a required call in markup, a collected value in markup, and `count` the only fold | `src/view/plan.rs` 479; `design-queries.md` 325-327 | T | a day | 3 |
| 11 | tonk's library is not ported: 74 views, 140 concepts, 63 commands, 38 events; only the notebook and the table are sketched | `tonk.md` 11-32 | T | weeks | 1, 6 to 10 |
| 12 | tonk's install and runtime: a notation form of `check` for `prepare_seed`, the library's `build.rs` bundles YAML only, the service worker and `.clj`, the inspector's generators, the wasm build | `tonk-notebook-bar.md` 141-148; `tonk-integration.md` 147 | T | days | 1 |
| 13 | definitions on the branch, decided and not built | `definitions-on-the-branch.md`; `src/analyze.rs` 9-11 | L | days | the count of definitions that refer to each other |
| 14 | `distinct` for `collect` and `{... ?entries}`, decided and not built. Not taken from `wip/engine-fold` (`7b7484a`) on 2026-11-24, at the coordinating session's word | `claims.md`; `where-things-stand.md` | L | a day | 18 |
| 15 | writes not supported: retracting a command, a partial retract, a command with a `when`, a `when` that sees the document's own writes, `fresh`; `update` parked. Retract by the entity alone is built, and the ledger's row saying otherwise is stale | `tests/notation.rs` 820, 2300; `expand.rs` 1537 | N for the notebook | a day each | decisions |
| 16 | forms of the design not built: variant cases, layers but `:control`, a limit, a query that calls itself. The `top` elector is built (2026-11-24): an elector in a concept's head or on a line's value is the field's `pick`, and a variant's cases are the attribute's listed values | `kernel.rs` `PICKS`, `ELECTORS`; `it_elects_a_claim_by_the_elector_in_a_concepts_head` | N | days | none |
| 17 | hygiene. Done 2026-11-24: a CHANGELOG entry introduces the crate; `test:web` run for the crate (the browser fails the five tests row 18 fails); the example's runner is `live`. Open: #773 is a draft on `design/macros` and not on main; 47 `Unsupported(` sites, grouped in [where-things-stand.md](where-things-stand.md) | | L | hours | 18 |

| 18 | 🔴 a rule that holds a pattern cannot be stored, so a query whose row has more than one column beside its entity cannot be kept current: decision 15 concludes the row as one record by a map pattern, and the session overlay refuses the rule. Asked once it runs (a query layer holds it). 14 of 31 layers of the sketches' and tests' views hold one; every layer nested two levels deep does; no `rule` form of the sketches does (0 of 33). Waits on format 5, ported onto #778 as `feat/rule-format-5` | `it_keeps_a_struct_and_takes_it_apart_by_a_pattern`, `it_folds_a_collected_entry_keeping_its_key_beside_its_value`, `it_maintains_a_row_whose_chips_are_collected`, `it_renders_the_tables_page_and_directory`, `it_maintains_a_view_nested_two_levels_deep` fail | L, T | the port | `feat/rule-format-5` |
| 19 | the cost on the new engine is not measured: `examples/cost.rs` lays the card's rules on the overlay and stops at row 18 | `cargo run --release -p dialog-notation --example cost` | L | an hour | 18 |
| 20 | `cardinality: :one` beside `top`: the maintainer has not decided whether they may stand together; the build refuses it, as #827 does | `attribute` in `compile.rs`; `slotting-cardinality.md` | N | a decision | the maintainer |
| 21 | a view's count kept on the fold reuses the selection's fold, whose reduce holds `distinct` beside `count-distinct`: without `Aggregator::Distinct` on main the count needs a reduce of its own | `select.rs` 400-414; `Plan::folds` | L | hours | whether `distinct` lands with #773 |

```
 engine:    18 ──▶ 19 ──▶ 14 ──▶ 17
            3 beside, a fold over a query's answer first;  13 after 18
 tonk:      6, 7, 8 decided ──▶ 1 (on 2) ──▶ 9, 10 ──▶ 12 ──▶ 11
```

Not known: what tonk#1338's head holds now. Nesting of two levels has a test since 2026-11-24 (`it_maintains_a_view_nested_two_levels_deep`), red until row 18.
