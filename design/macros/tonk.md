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
| `event!` | 38 | no. In the design an event is inline in a view; 10 of the 38 need a flag the design has no spelling for |
| `element!` | 24 | data: an assert of a concept `core.yaml` declares. Needs a collection `by: :name` and a text that spans lines |
| `route!` | 17 | data: an assert of a concept `core.yaml` declares |
| a concept asserted as data (`notebook!`, `table/cell!`, …) | 27 | yes, `assert` |

462 forms. 112 of them are `view!` and `event!`, a quarter, and no tonk document with a page can do without them: the part of the design that is least settled.

| document | lines | forms |
|---|---|---|
| `issue.yaml` | 100 | 6 attributes, 1 concept |
| `meta.yaml` | 129 | 4 concepts, 1 attribute |
| `notebook.yaml` | 1009 | 16 rules, 9 concepts, 6 commands, 5 views, 4 events, 2 routes, 1 element |
| `table.yaml` | 650 | 13 attributes, 10 concepts, 9 rules, 9 commands, 9 events, 6 views |
| `core.yaml` | 2996 | 48 concepts, 29 views, 12 commands, 9 rules, 8 events, 4 elements |
| `profile.yaml` | 7750 | 59 concepts, 33 commands, 25 views, 18 rules, 16 elements, 14 events, 13 routes |

12 crates in tonk depend on `tonk-notation`, the analyzer, the evaluator, the language server, the display and the template among them. tonk pins dialog at `v0.5.0-rc.6`, which this stack has merged. Pointed at this stack, every tonk workspace crate out to `tonk-worker` checks with no source change; the one port is a registry entry for `entity/derive` ([tonk-dialog-move.md](tonk-dialog-move.md), measured 2026-11-18). How a document becomes running state there, and the four places a second front end could hand over, are in [tonk-integration.md](tonk-integration.md). `notebook.yaml` is written in the notation in [sketches/notebook.clj](sketches/notebook.clj), and what that found is counted in [tonk-port.md](tonk-port.md).

## The four forms the build does not have

Read in tonk's library on 2026-11-18, after the maintainer's answers: an event should become unnecessary, an element is a dictionary concept, a route is a concept an external router routes to.

**`route!` and `element!` are not forms.** `core.yaml` declares both as concepts (`tonk:route` over `xyz.tonk.route/path` and `xyz.tonk.route/concept`; the element over `xyz.tonk.element/description` and four dictionaries). `route!:` and `element!:` assert them, as `notebook!:` asserts a notebook. In the notation they are `assert`.

**A route** is a path and a concept, and 15 of the 21 name an entity of their own. None holds a condition. Three are catch-alls that overlap the rest (`/{*model}`, `/{*rest}`, `/{*entity}@{*model}!{*view}`), and which wins is the router's to say. Where a page depends on more than the path, the concept routed to decides: `tonk:hub` serves five paths. So nothing in the library asks for a route to be a rule. A rule would have to say which of two matching patterns wins, which the router says today.

**An element** is a description and up to four dictionaries: `method` (all 24), `attribute` (5), `getter` (4), `setter` (4). 9 of the 24 hold more than one. A method's value is a text of JavaScript, 39 of them included from a file (`!include/text`), the rest written in place over many lines. It needs two things the build does not have: a collection keyed `by: :name` (the design's own example is `xyz.tonk.element.method/?n`), and a text that spans lines or comes from a file, which the design lists open. The design also says a concept holds one collection, so an element is four concepts over one entity, or is asserted attribute by attribute.

**An event** is a DOM event type and paths into the event, declared apart and bound by name in a template: 38 declared, 41 places bound, so nearly every one is bound once and writing it inline repeats little. What the 38 hold:

| | of 38 | inline, as the design has it |
|---|---|---|
| a type and paths (`.detail`, `.currentTarget`, `.timeStamp`, `.submitter`) | 38 | `:on/<type> (command field: .path)` |
| the view's own entity, `"{this}"` | 6 | a variable of the conditions |
| a constant | 1 | a constant in the command |
| `prevent-default` on a `submit` | 5 | implied |
| `prevent-default` on a `click` or a `change` | 9 | no spelling: the design lists event flags open |
| `stop-propagation` | 1 | no spelling |

So an event can go, once a binding can say it prevents the default: 9 of 38 need it and are not a `submit`.

**A view** is an entry in a concept's dictionary: a name and a template of HTML text with `{field}` slots, rendered by tonk's own renderer. It is the one form of the four that is a form.

## What the port changed

Read on 2026-11-18, from the two notes above.

- The notebook folds nothing. So a fold in a rule that derives (#830) is not between the notebook and a proof of concept. It is between the design's `view` and dialog.
- tonk's display reads a view from the branch as HTML text under `xyz.tonk.view/<facet>` with a compiled `bindings` record, finds a concept by name through `db.name/referent`, and makes its descriptor up from the declaration's claims when it reads (`db.meta/source` is not stored; corrected 2026-11-18). `dialog-notation` writes none of these. So a proof of concept in tonk needs a back end that does, whatever a `view` compiles to for dialog alone.
- That separates two things the question "what does a view compile to" had as one: the form and its meaning, which are the design's, and what a back end lowers it to. For tonk as it renders today the lowering is HTML text and bindings; for dialog alone it is a rule that folds.
- The order the notebook asks the build for: collections (15 of its 53 forms), `id` as a premise (7), views with inline events (5), a parameter left out of a read (4), then `dialog/position`, `!=` and a write with `when` (2 each).
- 24 of the 53 forms hit something the design does not spell, in 15 kinds, 10 of them not in the design's Open list. They are the maintainer's, one at a time.

## Where the notation hands over to tonk, measured

Measured on 2026-11-18 on four definitions of `notebook.yaml` (a concept, a command, a rule that writes, a route), comparing the claims tonk's YAML path stores with what `dialog-notation`'s output installs ([tonk-claims-diff.md](tonk-claims-diff.md)). The hypothesis was that the cheapest hand-over is a library's claims.

| of the 39 claims the YAML path writes | the same | different | missing |
|---|---|---|---|
| the notation as it is | 0 | 7 | 32 |
| with tonk writing its declaration and name claims around it, 80 lines | 13 | 26 | 0 |
| and the command's lines saying `cardinality: :one` | 31 | 8 | 0 |

- A concept or a command declared in the notation writes nothing to a branch, and tonk's display finds a concept by what its declaration wrote. The 80 lines read the descriptor back through a compiled query, as the crate gives no declaration as data.
- A line with no `cardinality:` reads every claim here and the newest in tonk. That moves every attribute's entity and the command's. The design writes a command's lines without one and says no default.
- The 8 left are a rule's identity (5), a route's entity (2) and one description. The rule differs only in a variable used once here where tonk writes `_`: the canonical spelling renames locals and does not treat one used once as a blank.
- A concept's identity agrees on both sides, so `in: :control` is no part of it.
- Views, events and elements were not measured: the sketch's views do not compile yet.

The hypothesis holds for what was measured. What is left is three choices of default or identity, not machinery.

## Not decided

- What a line with no `cardinality:` holds. tonk's lines hold one.
- Whether a variable used once and `_` are one rule. Today they are two.
- Whether an assert that makes its entity keeps the `id` entity, or tonk's back end derives it as `Entity::of` does. A route written both ways is two rows.
- Where a declaration goes: whether declaring a concept writes it to the branch, and whether the crate gives a declaration as data.

- What the POC has to show. The agent's hypothesis: one document that holds every kind of form, written in the notation, compiled by `dialog-notation` and running in tonk, with its YAML deleted. `notebook.yaml` is the smallest that holds every kind. How tonk takes a document is in [tonk-integration.md](tonk-integration.md), and what the hand-over costs is measured above.
- What a `view` compiles to for the POC: what tonk renders today (a dictionary entry and a template, no fold, no new renderer), or what the design says (a rule that folds into a markup value, and a renderer for it).
