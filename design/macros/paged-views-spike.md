# Paged views with the notation's own views: a spike, 2026-11-26

Asked by the maintainer: "backport that virtual window thing we discussed, basically doing what sapling does for paged rendering but without custom web component and instead with our views", written once and reused, not defined per use case. Branch `spike/paged-views`, from `feat/notation-poc`.

Two versions were tried. The first is a macro (`tests/paged.rs`, `tests/fixtures/paged.clj`). The maintainer could not read its use site, so the second, a shared relation, followed (`tests/paged_shared.rs`, `tests/fixtures/paged-shared.clj`, `tests/fixtures/paged-shared-lists.clj`).

## Verdicts

1. **Doable: yes, in both versions.** A window is a range of the order key, held as facts and moved by a command. Only rows whose key is in the range are in the view. It stays live: a fact inside the window is one insert, a fact outside it is no patch, a fact of another list is no patch, and a move removes the rows it leaves and inserts the ones it reaches. No custom web component, and no Rust per list.
2. **Reusable: yes, through a shared relation, on `feat/notation-poc` as it stands.** Each list derives its rows into one relation, `xyz.list`, with one rule. One rule, written once, says which rows a window shows. One page query, written once, pages any list a window names, text keys and integer keys alike (`it_pages_two_lists_by_one_query_over_a_shared_relation`). The limit: that shared page draws every row alike, from a label the list derives. A list that wants rows of its own look writes two queries of its own, a row and a page, over the shared rows (`it_draws_a_lists_own_rows_over_the_shared_relation`). The range, the window and the scroll stay written once.
3. **The macro also works, but only with three changes to the notation's expander** (commit `23b90f4`): a macro may expand to a query, to several definitions, and may hold a query that collects. Its use site hides the names it defines.

Correction to the first version of this note: a concept's line with no `as:` takes a value of any type. So one window concept holds text bounds for one list and integer bounds for another, and the "one window per key type" gap was wrong.

## The two versions, explained

### Words used

- A **fact** is a claim stored on the branch: an entity, an attribute, a value. `todo:a` is an entity; `xyz.todo/title` an attribute; `"a"` a value.
- A **concept** names a group of attributes of one entity, so `(todo ?t title: ?title)` reads or writes them together. `?t` is a variable.
- `(assert …)` writes facts.
- A **rule** derives facts that are never stored: `(rule head premise…)`. The head holds wherever every premise holds.
- A **command** is a concept whose head names no entity. Asserted, it holds for one step, and a rule reacts to it.
- A **query** is a named question, `(query (name …) condition…)`. When its body says `(view ?x [markup])`, its answer is markup, written as Hiccup: `[:li {:attr value} child…]`.
- A query may collect another query's markup, `(other … is: (distinct ?items))`. The items stand in the order of their keys when the other query answers an entry, `is: {?key ?item}`.
- `(select (name …))` asks a query once. A front end keeps one live instead, and gets patches as facts change.
- `:on/scroll (command …)` on an element is the command the front end posts on a scroll event. `.detail.lower` is a value taken from the event.
- A **window** is an entity with a `lower:` bound, included, and an `upper:` bound, excluded, of a list's order key. Moving it is asserting new bounds.

### Shared relation

Written once (`paged-shared.clj`):

```clj
(concept (window ?w list: ?l lower: ?lo upper: ?hi)
  (xyz.window/list of: ?w is: ?l as: :entity cardinality: :one)
  (xyz.window/lower of: ?w is: ?lo cardinality: :one)        ; no as: so any type
  (xyz.window/upper of: ?w is: ?hi cardinality: :one))

(concept (window/scroll window: ?w lower: ?lo upper: ?hi)    ; a command
  (xyz.window.scroll/window of: ?c is: ?w as: :entity in: :control cardinality: :one)
  (xyz.window.scroll/lower of: ?c is: ?lo in: :control cardinality: :one)
  (xyz.window.scroll/upper of: ?c is: ?hi in: :control cardinality: :one))

(rule (assert (window ?w list: ?l lower: ?lo upper: ?hi))    ; scrolling moves the window
  (window/scroll window: ?w lower: ?lo upper: ?hi)
  (window ?w list: ?l ...))

(rule (xyz.window.row ?r window: ?w key: ?k)                 ; the rows a window shows
  (window ?w list: ?l lower: ?lo upper: ?hi)
  (xyz.list ?r in: ?l key: ?k)
  (<= ?lo : ?k)
  (< ?k : ?hi))

(query (window/item ?r window: ?w is: {?k ?item})            ; one row's markup
  (xyz.window.row ?r window: ?w key: ?k)
  (xyz.list ?r label: ?label)
  (view ?item [:li {:data-key ?k} ?label]))

(query (window/page ?w is: ?page)                            ; the page
  (window ?w ...)
  (window/item _ window: ?w is: (distinct ?items))
  (view ?page
    [:ol {:on/scroll (window/scroll window: ?w lower: .detail.lower upper: .detail.upper)}
      ?items]))
```

What a list author writes: their concept, and one rule that puts each row into `xyz.list`, naming the list, the key and the label.

```clj
(concept (todo ?t title: ?title)
  (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))

(rule (xyz.list ?t in: list:todos key: ?title label: ?title)
  (todo ?t title: ?title))
```

Nothing else is defined, and no name is made for them. The page is `window/page` for every list. A person makes a window on the list, asks for its page, and scrolls it:

```clj
(assert (todo todo:a title: "a"))  (assert (todo todo:b title: "b"))
(assert (todo todo:c title: "c"))  (assert (todo todo:d title: "d"))
(assert (window window:1 list: list:todos lower: "b" upper: "d"))
(select (window/page window:1 is: ?p))
;; [:ol [:li {:data-key "b"} "b"] [:li {:data-key "c"} "c"]]

(assert (window/scroll window: window:1 lower: "c" upper: "e"))
(select (window/page window:1 is: ?p))
;; [:ol [:li {:data-key "c"} "c"] [:li {:data-key "d"} "d"]]
```

Messages by time are the same rule with integer keys: `(rule (xyz.list ?m in: list:messages key: ?at label: ?body) (message ?m at: ?at body: ?body))`. With `(window window:2 list: list:messages lower: 15 upper: 100)`, `window/page` answers `[:ol [:li {:data-key "20"} "two"] [:li {:data-key "30"} "three"]]`.

For rows of its own look, the list adds two queries. They read the shared `xyz.window.row` and do not repeat the range:

```clj
(query (message/item ?m window: ?w is: {?at ?item})
  (xyz.window.row ?m window: ?w key: ?at)
  (message ?m at: ?at body: ?body)
  (view ?item [:p {:data-key ?at} [:time ?at] " " ?body]))

(query (message/page ?w is: ?page)
  (window ?w ...)
  (message/item _ window: ?w is: (distinct ?items))
  (view ?page [:div {:on/scroll (window/scroll window: ?w lower: .detail.lower upper: .detail.upper)} ?items]))
;; (select (message/page window:2 is: ?p))
;; [:div [:p {:data-key "20"} [:time "20"] " " "two"] [:p {:data-key "30"} [:time "30"] " " "three"]]
```

### Macro

A **macro** is a rule whose premise `(expand pattern)` matches a form, and whose head `(syntax template)` says what the form is replaced by. Written once (`paged.clj`): a window concept, a scroll command and a rule, as above but without `list:`, and the macro `paged`:

```clj
(rule (syntax (all
    (query (?rows ?e window: ?w is: {?key ?item})
      (?window ?w lower: ?lo upper: ?hi)
      ?read
      (<= ?lo : ?key)
      (< ?key : ?hi)
      (view ?item ?template))
    (query (?page ?w is: ?list)
      (?window ?w lower: ?lo upper: ?hi)
      (?rows _ window: ?w is: (distinct ?items))
      (view ?list
        [:ol {:on/scroll (?scroll window: ?w lower: .detail.lower upper: .detail.upper)}
          ?items]))))
  (expand (paged ?page rows: ?rows read: ?read entity: ?e key: ?key
                 window: ?window scroll: ?scroll row: ?template)))
```

What a list author writes: their concept and one `paged` form.

```clj
(paged todo/page rows: todo/paged read: (todo ?t title: ?title) entity: ?t key: ?title
       window: window/text scroll: window/scroll-text
       row: [:li {:data-key ?title} ?title])
```

It defines two queries, under the names the form gives: `todo/paged`, the rows in the window, and `todo/page`, the page. `window/text` and `window/scroll-text` name the window concept and command. (The fixture has one pair per key type; with untyped lines one pair would do.) A person scrolls first, which makes the window, then asks for the page:

```clj
(assert (window/scroll-text window: window:1 lower: "b" upper: "d"))
(select (todo/page window:1 is: ?p))
;; [:ol [:li {:data-key "b"} "b"] [:li {:data-key "c"} "c"]]
```

### Side by side

```
                        shared relation                         macro
 author writes          1 rule (+2 queries for an own look)     1 paged form
 names it defines       none; the page is window/page           the two it is given, todo/paged and todo/page
 own row markup         only with the 2 extra queries           yes, row: template
 window says its list   yes, list:                               no, the page query does
 needs expander change  no                                      yes, 23b90f4
```

### What breaks, or could, in the shared relation

- **The key's type**: nothing breaks. The bounds have no type, and a window only compares keys of the list it names. A window whose bounds are of another type than its list's keys was not tried.
- **Markup through the relation**: it does not flow. A rule cannot conclude markup, since `view` stands only in a query. So the shared page draws from values the list derives (`label:`), and markup of a list's own needs its own queries beside the relation.
- **One window query for both lists**: yes, as long as both lists' rows look alike.
- **A row in two lists**: `xyz.list` is three attributes of the row's entity, not one tuple. An entity in two lists with two keys would pair each list with each key. Rows of distinct entities per list, as here, are not affected. Untested.
- **The relation needs a domain name**, `xyz.list`, because a derived `list/item` needs a concept `list` declared first.

## Sapling's mechanism

I could not read `dialog-db/sapling`: attaching it was refused in this session. What follows is from `paginated-views.md`, which summarises its PDD018. Check it against the source before relying on it.

- State in the front end: which segments are open (at most 16), measured row heights, spacers, scroll position.
- A segment is a range of the order key, `lower <= key < upper`, either bound open. Never an offset: a rank needs a count in every subtree.
- Each open segment is one live query with constant bounds. The engine scans the key's range.
- On scroll the front end opens and closes segments. It picks the next bound from a one-off read of the N greatest keys below a key, or guesses from the density of the rows it already holds.
- It stays live because each segment is a subscription. A change outside every open range reaches none of them.

## Gaps, and the smallest addition for each

| # | gap | smallest addition | state |
|---|---|---|---|
| 1 | A macro cannot expand to a `(query …)`. Only a query written at the top level is defined. | `top` hands a macro's `(query …)` back to the top level to be defined. | closed here, macro only |
| 2 | A macro cannot expand to several top-level forms. `(all …)` is refused as a call. | Same change. | closed here, macro only |
| 3 | `distinct` is refused anywhere in a rule, including a query inside a macro's template. | A `(query …)` inside a `(syntax …)` template is exempt. A macro whose template is a rule that collects is still refused (`it_refuses_a_macro_whose_rule_collects`). | closed here, macro only |
| 4 | A query cannot take a query or a view as a parameter, and one query name cannot be defined twice in a document. | None: the shared relation does without it. | worked around |
| 5 | A rule cannot conclude markup. | None for the shared page, which draws from values. An own look takes two queries per list. | open |
| 6 | No limit. The window is a key range, so how many rows it holds depends on density. The front end cannot ask "the N keys after k" (gaps row 16, `paginated-views.md` row 4). | An engine read with a direction and a limit over an ordered scan, and a one-off `select` form for it. | open, engine |
| 7 | Cost: the bounds come from a join, so the range does not narrow the scan. | Investigated separately, in sapling. | open, engine |
| 8 | The window is branch facts: every scroll is a commit. | Investigated separately, in sapling. | open |
| 9 | A native `scroll` event has no `detail.lower` or `detail.upper`. Something has to turn pixels and the rows' `data-key` into bounds. | One generic front-end handler for every list. No browser was run: the tests post the command themselves. | open, untested |
| 10 | Scrollbar size needs a total or an estimate. | A count of the list as a fold subscription (`Plan::folds` exists). Not tried. | untested |

## Cost of one window move (macro version)

Measured by `it_measures_a_window_move_over_many_rows`: a window of 50 integer keys, 5 moves, with the time of `Watched::poll` after the scroll command is committed. The commit itself is not counted. Test profile (optimised), one run each:

```
 rows     first poll   one move (5 runs)   a fact outside the window
 1000     12 ms        12-13 ms            1.0 ms
 10000    78 ms        78-87 ms            1.9 ms
 30000    268 ms       239-254 ms          4.8 ms
```

A move costs about 8 µs per row in the list, not per row in the window: gap 7. The shared relation was not measured.
