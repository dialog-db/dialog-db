# What tonk has to be rewritten from

The maintainer, 2026-11-18: "I want to get to a state where we can have POC pr of tonk that uses this new notation along with compiler so we could retire old yaml based notation."

So the order gaps are closed in is the order tonk's documents need them, not the order the design moved in.

## Measured

tonk at `c006b50` (2026-11-17), `rust/tonk-core/assets/library/*.yaml`, top-level forms. The 282 files under `language/` are generated word lists (two concepts and data each) and are left out, as are the 148 `library/component!` entries, which are data.

| form | count | built in `dialog-notation` |
|---|---|---|
| `concept!` | 140 | yes, as attribute lines |
| `view!` | 74 | no. Proposed in the design; as designed it is a fold in a rule that derives, which main refuses |
| `command!` | 63 | being built: a declared concept on a control layer |
| `rule!` | 55 | yes |
| `attribute!` | 51 | an attribute needs no declaration; its type and cardinality are on the line |
| `event!` | 38 | no. In the design an event is inline in a view |
| `element!` | 24 | no, and the design does not say |
| `route!` | 17 | no, and the design lists it open |
| a concept asserted as data (`notebook!`, `table/cell!`, …) | 27 | yes, `assert` |

462 forms that are not data. 153 of them, a third, are `view!`, `event!`, `element!` and `route!`: the part of the design that is least settled is the part a tonk document cannot do without.

| document | lines | forms |
|---|---|---|
| `issue.yaml` | 100 | 6 attributes, 1 concept |
| `meta.yaml` | 129 | 4 concepts, 1 attribute |
| `notebook.yaml` | 1009 | 16 rules, 9 concepts, 6 commands, 5 views, 4 events, 2 routes, 1 element |
| `table.yaml` | 650 | 13 attributes, 10 concepts, 9 rules, 9 commands, 9 events, 6 views |
| `core.yaml` | 2996 | 48 concepts, 29 views, 12 commands, 9 rules, 8 events, 4 elements |
| `profile.yaml` | 7750 | 59 concepts, 33 commands, 25 views, 18 rules, 16 elements, 14 events, 13 routes |

13 crates in tonk depend on `tonk-notation`, the analyzer, the evaluator, the language server, the display and the template among them. tonk pins dialog at `v0.5.0-rc.6`, which this stack has merged.

A view in tonk today is an entry in a concept's dictionary: a name and a template of HTML text with `{field}` slots, rendered by tonk's own renderer. An event is declared apart, a type and paths into it.

## Not decided

- What the POC has to show. The agent's hypothesis: one document that holds every kind of form, written in the notation, compiled by `dialog-notation` and running in tonk, with its YAML deleted. `notebook.yaml` is the smallest that holds every kind. Not looked at: how tonk's analyzer and evaluator take a document, so what "running in tonk" costs.
- What a `view` compiles to for the POC: what tonk renders today (a dictionary entry and a template, no fold, no new renderer), or what the design says (a rule that folds into a markup value, and a renderer for it).
