# A query: rows in, a tree out

**Decision:** what reads a concept and shapes the answer: for a person asking, an agent, and a renderer. One thing, or several.

**For:** a notebook with no block is no `notebook` with a block (no entry, no row, decided 2026-11-18), and still whoever asks for a notebook expects its blocks, none included. A to-do with no tag keeps its `li`. A count of nothing is 0.

**Decided** by the maintainer, by voice, on 2026-11-18, and built in two stages: rows, `select` and `collect` (stage 1), and views as queries (stage 2). How each part got here is in the [journal](journal.md).

## Two layers

```mermaid
flowchart LR
  A["attributes<br/>stored claims"] --> R["rules and concepts<br/>rows · may recurse · never collect"]
  R --> Q["queries<br/>trees · nest · collect"]
  Q --> Q
  Q --> C["whoever asks:<br/>a person, an agent, a renderer"]
  Q -. never .-> R
```

A rule's conclusion is read by other rules, so a fold in a head can feed itself. A query's answer is read by whoever asked and by a query above it, never by a rule, so it collects freely. Datomic draws the same line: an aggregate stands in `:find` only, and a tree comes from `pull`.

Folds in rules, stratified, as Datalogs with aggregates have them, would let a rule read a count. Stratifying is a check over a whole program, and dialog has none: rules arrive from peers, and one installed later can close a cycle through a fold that was sound when it was checked. So not until a rule needs one that `unless` cannot say.

## The form

```clj
(query (todo/tagged ?t is: ?pair)       ; the head: a name, an entity, fields, and what it answers
  (todo ?t title: ?title)               ; the body: conditions
  (todo/tag ?t tag: ?g)
  (is ?pair {:title ?title :tag ?g}))   ; and what the answer is
```

- **The head** is a concept's head. `is:` is the field that holds the answer. A field passed bound is a parameter, as with any relation.
- **The body** is conditions, and `(is ?x shape)`, which says what a variable is: a record, a function of other variables, or a term. `(view ?x template)` says it is markup.
- **A call** is the head applied, a condition like any other: `(todo/tagged ?t is: ?pair)`.
- **Asking** is `select`: `(select (todo/tagged ?t is: ?pair))`, or of a concept, `(select (todo ?t title: ?title))`. `query` names what is defined, beside `concept` and `rule`.
- Every condition is required: one row for each way all of them hold.

It is named under its concept, as the design's derived attributes are (`list/count-open`), and has their shape. So what was a getter is a query.

## One at a time, or the whole

A field that holds many says so on its line. In a head it is one of them, and for a keyed collection one is an entry, a key and its value:

```clj
(concept (todo/tag ?t tag: ?g)
  (xyz.todo/tag of: ?t is: ?g as: :text cardinality: :many))

(concept (notebook/blocks ?n block: {?k ?b})
  (xyz.tonk.notebook/?k of: ?n is: ?b as: :entity cardinality: :many by: :position))
```

| | one at a time: a row for each, and no row for none | the whole: one row, and empty for none |
|---|---|---|
| many values | `(todo/tag ?t tag: ?g)` | `(todo/tag ?t tag: (collect ?tags))`, a set: a map with no names |
| a keyed collection | `(notebook/blocks ?n block: {?k ?b})` | `(notebook/blocks ?n block: (collect ?blocks))`, a map |

- `collect` stands where an elector does. `(min ?s)` chooses one of a field's claims; `(collect ?s)` takes every one.
- Key and value are paired because they were never apart: an entry is one term.
- What comes out is a map either way. A keyed collection's has its keys. Many plain values have none, and a map with no names is a set: `{: "errand" : "home"}`, stored in a record as a list. No list as such: many values have no order, and a sequence's order is in its keys.
- A pattern takes one value apart; `collect` puts many rows together. `{…}` and `[…]` in a condition are always the first.
- **A read of the whole never gates**, as `unless` never binds. So every other term in it is bound by a required condition, a constant, or blank. A query of an entity that only collects is refused: nothing would say which entities. A query of no entity that only collects answers once, and that is an index.
- It is a fold, so it is refused in a `rule`.
- A write goes through one entry: `(assert (notebook/blocks ?n block: {"N3" block:x}))`. The whole is not written.

## To-dos and their tags

"Buy milk" is tagged `errand` and `home`. "Call mum" has no tag.

**All required**, `todo/tagged` above:

| `?t` | `?pair` |
|---|---|
| Buy milk | `{:title "Buy milk" :tag "errand"}` |
| Buy milk | `{:title "Buy milk" :tag "home"}` |

**The whole:**

```clj
(query (todo/card ?t is: ?card)
  (todo ?t title: ?title)
  (todo/tag ?t tag: (collect ?tags))
  (is ?card {:title ?title :tags ?tags}))
```

| `?t` | `?card` |
|---|---|
| Buy milk | `{:title "Buy milk" :tags {: "errand" : "home"}}` |
| Call mum | `{:title "Call mum" :tags {}}` |

**Folded:** `tags: (count ?tags)` is 2 and 0. A fold is a function of what was collected.

**Rendered.** A chip is built from a tag, so it is a query of its own, passed the to-do, and the row takes all its answers:

```clj
(query (todo/chip ?t tag: ?g is: ?chip)
  (todo/tag ?t tag: ?g)
  (view ?chip [:span {:class "tag"} ?g]))

(query (todo/row ?t list: ?l is: ?row)
  (todo ?t list: ?l title: ?title)
  (todo/chip ?t is: (collect ?chips))
  (view ?row [:li ?title ?chips]))
```
```html
<li>Buy milk<span class="tag">errand</span><span class="tag">home</span></li>
<li>Call mum</li>
```

Written `(todo/chip ?t is: ?chip)`, one at a time, "Call mum" has no `li`.

## A notebook and its blocks

```clj
(query (notebook/outline ?n is: ?outline)
  (notebook/named ?n title: ?title)
  (notebook/blocks ?n block: (collect ?blocks))
  (is ?outline {:title ?title :blocks ?blocks}))
```

| `?n` | `?outline` |
|---|---|
| Field notes | `{:title "Field notes" :blocks {"N1" block:a "N2" block:b}}` |
| Untitled | `{:title "Untitled" :blocks {}}` |

With each block's source, which is a join for each entry. The item is a query whose answer is an entry, and its whole is a map again:

```clj
(query (notebook/source ?n is: {?k ?s})
  (notebook/blocks ?n block: {?k ?b})
  (notebook/block ?b source: ?s))

(query (notebook/outline ?n is: ?outline)
  (notebook/named ?n title: ?title)
  (notebook/source ?n is: (collect ?sources))
  (is ?outline {:title ?title :blocks ?sources}))
```

| `?n` | `?outline` |
|---|---|
| Field notes | `{:title "Field notes" :blocks {"N1" "# Monday" "N2" "Rained."}}` |
| Untitled | `{:title "Untitled" :blocks {}}` |

**The page.** Markup under keys stands in the order of the keys:

```clj
(query (notebook/order-item ?n is: {?k ?item})
  (notebook/blocks ?n block: {?k ?b})
  (view ?item [:span {:class "notebook-order__item" :data-block ?b :data-key ?k}]))

(query (notebook/block-row ?b notebook: ?n is: ?row)
  (notebook/block ?b notebook: ?n source: ?s)
  (view ?row [:div {:class "notebook-block-row"
                    :subject ?b :data-block ?b :data-notebook ?n :data-source ?s}]))

(query (notebook/ui ?n is: ?ui)
  (notebook/named ?n title: ?title)
  (notebook/order-item ?n is: (collect ?order))
  (notebook/block-row _ notebook: ?n is: (collect ?rows))
  (view ?ui
    [:notebook-save {:style "display: block;"}
      [:tonk-notebook {:data-subject ?n :data-notebook ?n :title ?title
                       ; the four :on/ bindings, as before
                       :style "display: block;"}
        [:div {:class "notebook-data" :hidden true}
          [:div {:class "notebook-order"} ?order]
          [:div {:class "notebook-block-rows"} ?rows]]]]))
```

Two views this morning, three queries now, and a notebook with no block shows its whole page, where the marker form showed its outer element only. The order's items stand by position; the rows are a set, which is all tonk's hidden rows need.

## The table's views, and an index

This morning's six views are six queries. Four are a row each, like this one, and the page collects them:

```clj
(query (table/sheet-row ?s is: ?row)
  (table/sheet ?s table: ?table name: ?name order: ?order)
  (view ?row [:div {:class "table-sheet-row"
                    :subject ?s :data-table ?table :data-name ?name :data-order ?order}]))

(query (table/ui ?t is: ?ui)
  (table ?t name: ?name)
  (table/sheet-row _ is: (collect ?sheets))
  (table/cell-row _ is: (collect ?cells))
  (table/column-row _ is: (collect ?columns))
  (table/rowsize-row _ is: (collect ?sizes))
  (view ?ui
    [:tonk-table {:subject ?t
                  ; the nine :on/ bindings, as before
                  }
      [:div {:hidden true}
        [:div {:class "table-sheet-rows"} ?sheets]
        [:div {:class "table-cell-rows"} ?cells]
        [:div {:class "table-column-rows"} ?columns]
        [:div {:class "table-rowsize-rows"} ?sizes]]]))
```

The directory has no entity in its head, as a command's concept has none. It answers once, and stands empty when there is no table:

```clj
(query (table/directory is: ?directory)
  (table/ui _ is: (collect ?tables))
  (view ?directory [:div {:class "table-directory"} ?tables]))
```

This was the one view of eleven that repeated the wrong element under the marker rule: tonk's YAML keeps the outer `div` and repeats the display inside it, which is what this says. It is also the morning's question of what renders when there is no notebook.

## A word, not the spread

**The maintainer's: not `{... ?x}`.** It does not say whether it takes a value apart or puts rows together, and those are different things.

| | a word: `(collect ?blocks)` | the spread: `{... ?blocks}` |
|---|---|---|
| says which it is | 🟢 only ever puts together | 🔴 a pattern's rest, which takes apart; here it would put together |
| says it does not gate | 🟢 by a word an error can name | by what a rest is |
| says what comes out | a map: with its keys or with none, by the field | a map, by the braces |
| already in the notation | no; it sits where an elector does | yes, as destructuring |

`collect` was set aside an hour before on two counts, and both are since answered. It seemed to promise a list: it makes a map, always. A dictionary had no way to say how key and value pair: an entry is one term.

**Reopened by the maintainer the same evening:** a spelling of the variable in place of the word.

| | reads today as | a neighbour one space away |
|---|---|---|
| `?x...` | a variable named `x...`, with no error | `?x ...`, the open application, three times in `todo.clj` |
| `...?x` | an error: "`...` stands apart from what follows it" | `... ?x`, the rest |
| `?..x` | a variable named `..x`, with no error | none |
| `{?..x}` | a struct that ends on a key, which gets a blank value | none; and under `{?k ?b}` it is an entry with any value |

Of the four, `?..x`. Against `(collect ?x)` it is shorter and adds no form; the word explains itself, sits with the electors, and is what an error would say anyway.

**Decided 2026-11-18, by the maintainer: `(collect ?x)`.**

## What it replaces

| was | becomes |
|---|---|
| `(view concept :name template condition…)` and its call | a query named under the concept, `todo/row`, whose answer is markup, and its head applied |
| a collection as two head fields, `at: ?k block: ?b` | one field holding an entry, `block: {?k ?b}`. The line is as it was; R46 tested the line |
| the design's fold over an entity, and tonk's marker, taken this morning | a read that collects, and nothing else repeats |
| a fold in a rule's head (`list/count-open`), and the zero written by a second rule | `(count ?open)` of a set, where counting nothing gives 0. Nothing in the engine quarantines a fold (#830) |
| a getter | a query of an entity and a value |
| #712: a collection that is empty | one at a time, the row is rightly missing; the whole is `{}`, which is what #712 expected of it |
| `maybe:`, which the notation has no spelling for (it occurs 25 times in tonk's library) | the whole of a field, none or one |

## What would refute it

- **A rule that must read a fold.** tonk's library has no fold in any rule; where it counts, an element's script does.
- **A real view that reads worse.** Every repeated part built from more than one field is a query with a name. The notebook's page is three queries for two views; the table's are six for six. Neither is run.
- **Readers.** None of it has met one: not the head with `is:`, not `{?k ?b}`, not `collect` on a field.

## Open

- **A set merges equal answers.** Two to-dos with one title are one title in a set. Count entities, not what is shown of them.
- **A fold with nothing to give.** `count` and `sum` of nothing are 0; `max` has none. `pull` leaves the key out. Proposed: the same.
- **Limit.** `pull` stops at 1000 unless told. #77.
- **A query that calls itself**: a thread, an outline. `pull` allows it, with a depth, and stops at an entity it has met.
- **How a renderer finds the view** for an entity. Proposed: the name under the concept, `todo/row` for `:row` of a `todo`.
- **Whether the engine carries a map, a set or markup as a binding** through a body. Records are values since #778; not checked for this.
- **What tells one row from another** when the answer is maintained, so a to-do renamed is one text set.
- **Whether a concept is needed at all**, the maintainer's question. A condition may be a line.

## Prior art

| | rows | trees | folds |
|---|---|---|---|
| Datomic | `:where` clauses and rules | `pull`: attribute paths and nesting, no filter; a missing attribute's key is left out | in `:find` only; over nothing, no row |
| GraphQL | none; resolvers | selection sets, arguments on any field, fragments for reuse | none built in |
| EdgeQL | sets and paths | shapes, with `filter`, `order by`, `limit` at each level; nothing is the empty set | `count(.link)` in a computed field |
| here | concepts and rules | a record or markup built in a query's body | the whole of a field, and functions of it |

## As built, stage 1

Built on #773, 2026-11-18, in `rust/dialog-notation`: `src/select.rs`, `src/expand/query.rs`, and the tests in `tests/query.rs`, each a claim of this note with the answers its tables give.

- **`query` defines, `select` asks.** `(query HEAD condition… (is ?x shape))` is kept by the analyzer and compiles to nothing. `(select (todo/card ?t is: ?card))` of a query compiles to a `Selection`. `(select condition…)` of a concept is what `(query condition…)` was. `(query …)` with one part, with a head that has no `is:`, or whose head names a domain or a stored attribute, is refused: "asking is written `(select …)`".
- **How it answers.** Dialog-DB cannot fold, so the notation does. The conditions every answer needs are one Dialog-DB query; each read that collects is one more, run once and grouped in memory by the terms it shares; a call takes the called query's rows where its fields agree. A query is answered in full and then filtered by what the call passes, so a field passed bound narrows the answers and is not pushed into the read.
- **A row is kept whole.** A query of several reads read back through its head's entity crosses the values of one entity (the view plan's workaround picks the entity that fixes most). Here each row's entity is `(id ?query-row c0: ?a c1: ?b …)` of all its values, so a row is its values and nothing else.
- **The entry.** `block: {?k ?b}` in a concept's head; a head with the key as a field of its own is refused. A pattern takes `{?k ?b}` apart against one entry, the one place a key is a variable. Assert and retract go through one entry.
- **Sets.** `{: 1 : 2}` reads and prints as written. A map that names some keys and not others is refused. Written into a record it is a list, its members in the order of their values, each once; it reads back as that list.
- **Refused:** a query of an entity that only collects; a term of a collecting read bound by nothing that does not collect; `collect` in a rule, in a concept, under `unless`, in what is asked, and in a write's head ("writes the whole of a field"); a rule or a concept that applies a query; a query that calls itself; a function of a collection other than `count`.

**Where it differs from the sketch, and why:**

- `(view ?x template)` in a query's body is refused as not built: views are stage 2.
- Every variable of a query's head is bound by its body: a field that only a caller could give is refused, since a query is answered whole before a call narrows it.
- The views of `notebook-views.clj` still write a collection as `key: ?k block: ?b`. A read written so, of a concept whose field is an entry, is read as the entry, until stage 2 restates the views. A concept head written so is refused.
- A line's `Key::label` is `None` for a key that stands in an entry: the entry's label is the line's.

The pinned measures of the notebook did not move: 56 forms, 56 read, 53 expanded, 53 compiled, 5 described; with core, 60 of each and 5.

## As built, stage 2

Built on #773, 2026-11-18, in `rust/dialog-notation`: `src/view/plan.rs` and `src/view/tree.rs` reworked, the tests in `tests/view.rs`.

```mermaid
flowchart TD
  S["(select (notebook/ui ?n is: ?ui))"] --> P["Plan: one template"]
  P --> L0["layer 0: notebook/named, by ?n"]
  P --> L1["layer 1: order-item's read, by its key"]
  P --> L2["layer 2: block-row's read, by its block"]
  L0 & L1 & L2 --> M["Maintainer: rows in, patches out"]
```

- **A view is a query whose answer is markup.** `(view ?x template)` in the body. `(select …)` of one compiles to a `Plan`; asked by the session, it is rendered once and printed as Hiccup.
- **What is collected is put in place.** A query collected into markup has its template put where its variable stands, its variables named apart (`[q1]`, `[q2]` …, names no document can spell), and its required conditions a layer under the element it stands in. Calls are not mounted; the whole page is one plan, one subscription a layer.
- **What tells instances apart.** The root by the query's entity, or once for a query of no entity. A query collected by the terms of its head the call leaves free, an entry's key first, so the order stands by position and a set by its values.
- **A count** is a layer whose instances are counted and never shown, under the element that shows it: `0 open`, and `1 open` when a to-do arrives, one text set.
- **A layer that does not bind a key above it takes that layer's conditions too**, so `(table/sheet-row _ is: (collect ?sheets))` gives every sheet under each table. Its rows are re-read when the table's conditions change.
- **A query not found** under a declared concept is shown as its source, now from the collecting read, `(todo/missing _ list: ?l is: (collect ?items))`.
- **Removed:** the view form, its call, `{:view …}`, `Plan::repeating` and the marker rule, `Plan::called`, the read of `key: ?k block: ?b` as an entry. Each is refused with a message that points to a query.
- **The sketches.** `notebook-views.clj` and `table-views.clj` are queries, `:on/` bindings in full. The view forms in `notebook.clj` and `table.clj` are taken out, with a line saying where they went. The measures read the views' file after the sketch.

| measure | before | after |
|---|---|---|
| notebook | 56 forms, 56 read, 53 expanded, 53 compiled, 5 described (`notebook.clj`) | 58, 58, 55, 55, 7 (`notebook.clj` + `notebook-views.clj`) |
| notebook with core | 60, 60, 60, 60, 5 | 62, 62, 62, 62, 7 |
| table with core | 54, 54, 54, 54, 6 | 54, 54, 54, 54, 6 (`+ table-views.clj`) |

"Described" now counts the queries whose answer is markup that, asked, are laid out in a plan.

**Where it differs from the sketch, and why:**

- **Identity is a hypothesis.** An answer is told apart by its head, not by every value it binds: a to-do renamed is the same `li`, its text set. So two answers of one head show as one instance, the first row's values: `task/flat` with the tag's read required shows one `li` for a to-do with two tags. Refuted by a view where two answers of one head must both show; the test that would break is `it_maintains_a_row_whose_chips_are_collected`.
- **A required call in a markup query** (`(todo/chip ?t is: ?chip)`, one at a time) is not built; collect it, or read what it reads.
- **A value collected and put in markup** (a set of tags) is refused: markup holds markup collected, or its count.
- **Stage 1's whole-then-narrow** is not what a view does: each layer is its own subscription, so one entity changed re-reads the layers whose reads it touches, and, through a joined condition, a layer under it. `select` of a markup query in the session asks every layer whole, once.
- **Declarations** give a query's name and whether its answer is markup, where they gave a view's description.
