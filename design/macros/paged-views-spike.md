# Paged views with the notation's own views: a spike, 2026-11-26

Asked by the maintainer: "backport that virtual window thing we discussed, basically doing what sapling does for paged rendering but without custom web component and instead with our views", written once and reused, not defined per use case. Branch `spike/paged-views`, from `feat/notation-poc`.

Three shapes were tried. A macro (`tests/paged.rs`) works, but its use site could not be read. A shared relation with the window in rules came next; the maintainer set it aside, and it is removed. This note leads with the third, which he proposed: **the window in the query** (`tests/paged_query.rs`, `tests/fixtures/paged-query.clj`, `tests/fixtures/paged-query-lists.clj`). It runs on `feat/notation-poc` without the macro's expander change.

## The use site

What a list writes to join, glossed line by line. Words first:

- A **concept** names attributes of one entity, read and written together: `(todo ?t title: ?title)` is the to-do `?t` with its title.
- A **rule** says its head holds wherever its body holds. Nothing it derives is stored.
- A **query** is a named question. When its body says `(view ?x [markup])`, its answer is markup, written as Hiccup, `[:tag {attributes} children]`.
- `list/item`, `window`, `list/row` and `list/page` are defined once, in `paged-query.clj`.

```clj
(rule (list/item ?t list: list:todos key: ?title view: "todo/row")   ; 1
  (todo ?t title: ?title))                                           ; 2
(query (todo/row ?t is: ?row)                                        ; 3
  (todo ?t title: ?title)                                            ; 4
  (view ?row [:li ?title]))                                          ; 5

(assert (window window:1 list: list:todos from: "b" to: "d"))        ; 6
(select (list/page window:1 is: ?page))                              ; 7
;; [:ol {:data-list "list:todos"}
;;   [:list-row {:subject "todo:b" :data-key "b" :data-view "todo/row"}]
;;   [:list-row {:subject "todo:c" :data-key "c" :data-view "todo/row"}]]
(select (todo/row todo:b is: ?row))                                  ; 8
;; [:li "b"]
```

1. Every to-do is a member of the list `list:todos`, ordered by its title, and drawn by the query `todo/row`.
2. Which to-dos: every one that has a title.
3. `todo/row` answers one to-do's markup.
4. It reads the to-do's title.
5. Its markup is a list item holding the title.
6. The renderer opens a window on `list:todos`, showing keys from "b" (included) to "d" (excluded). The window is the renderer's state, and no rule reads it. It belongs in the stack's `session:` layer: see "How the window moves on scroll".
7. The renderer asks for the window's page. It gets one place per member in range, in key order, each naming the member and the view to draw it with.
8. It draws each place by the view the place names. To scroll, it writes new bounds for `window:1`, and the live page patches.

Messages by time join the same way, with an integer key and their own view: `(rule (list/item ?m list: list:messages key: ?at view: "message/row") (message ?m at: ?at body: ?body))`. The same `list/page` pages both.

## Findings

1. **The hypothesis holds.** A monotone rule per list puts members into `list/item`. One query, written once, pages whichever list a window names. The window lives in no rule. Live: a member added inside the window is one row inserted, one outside it or in another list is no patch, and moving the window removes and inserts rows (`it_moves_the_window_the_renderer_keeps_and_stays_live`). Text and integer keys work in the same query, because `list/item`'s key and the window's bounds have no type.
2. **The bounds cannot be the query's own parameters.** `(query (list/span ?l from: ?lo to: ?hi is: ?s) … (<= ?lo : ?k) (< ?k : ?hi) …)` is refused: a layer compiles to a rule whose head concludes `?lo`, and a comparison binds nothing (`it_refuses_bounds_that_are_only_compared`). So the window is a fact the query reads. **Smallest addition:** where the ask gives a parameter as a constant, put the constant into the layer's query in place of concluding it, so a parameter needs no premise to bind it. That is a change to the notation's view plan. A standing view of other bounds is then a new ask, sapling's "a segment is a query with constant bounds".
3. **The window state belongs in the stack's `session:` layer, and the engine supports it; the notation's driver does not reach it yet.** See "How the window moves on scroll" below, and `it_moves_a_window_kept_in_the_session_layer`.
4. **A query's head needs an entity.** (The form the maintainer dropped is a bare dotted domain as the operator, `(xyz.list ?t in: …)`, which none of these documents writes.) I tried a head of labels only, `(list/page window: ?w is: ?p)`, asked as `(select (list/page window: window:2 is: ?p))`. Its answer merged both windows' rows into one tree under window:1, and both asks gave the same answer: an instance is keyed by the head's entity, and the ask's constant on a label was not applied. So the query forms here are `(name entity … is: ?x)`, the only form the notation has for a query that answers markup. No premise, head or write in these documents is an attribute read or written directly.
5. **Custom markup per list works as an interface, through the renderer.** A rule cannot conclude markup, so markup does not flow through `list/item`. The member carries its entity and the name of its view (`view: "todo/row"`). The list's markup is one place per member, `[:list-row {:subject … :data-view …}]`. The renderer asks that view for that entity. The test plays the renderer and draws to-dos as `[:li …]` and messages as `[:p [:time …] …]` (`it_draws_each_row_by_the_view_its_list_names`). Not built: the renderer doing this itself, as a nested view call that names its query from a value.
6. **An entity that fits several concepts.** The view is not looked up from the entity: the rule that joins the member to the list names it, so a list that holds people as authors draws them by the author view, whatever else they are. tonk's `xyz.tonk.view/model` names a concept per view and finds the view by concept, which is ambiguous for such an entity; naming the view on the member is not. An entity in two lists is not handled. `list/item` is three attributes of the member, not one record. A second list's rule would conclude a second list and key for the same entity, and its cardinality-one fields would conflict. A record keyed by member and list (an entity derived from both) would fix it. Not tried.

## How the window moves on scroll

A **stack** is the primary branch read together with the layers beside it. Its `session:` layer is in memory, belongs to that stack alone, and is never committed or synced. A read with no layer named reads every layer, so `(window ?w list: ?l from: ?lo to: ?hi)` finds a window whichever layer holds it.

1. The front end measures. On a scroll event it reads the `data-key` of the first and last rows it wants on screen, and so has the new `from:` and `to:`. Pixels never leave the front end.
2. Its handler writes the new bounds of `window:1` into `session:`. It retracts the old `from:` and `to:` and asserts the new ones, each written `.at(session_layer())`, through the stack (`Stack::assert`, `Invocation::retract`). Nothing reaches the primary, so nothing syncs.
3. The write schedules a poll of the stack's subscriptions. `Stack::settle` runs it. The subscription of the page's row layer re-reads the window and answers the rows in the new range.
4. The renderer applies what moved as patches: the rows that left are removed, the rows that came are inserted.

What is pinned (`it_moves_a_window_kept_in_the_session_layer`, on a stack):

- The window's facts written `.at(session_layer())` are read by the page's row layer, subscribed with `Stack::subscribe`. It answers todo:b and todo:c. After the handler's move, a delivery comes and it answers todo:c and todo:d.
- The primary holds no window: the notation's `(select (window …))` answers nothing.
- The notation's driver, `Live::watch`, subscribes through the branch (`Branch::subscribe`). The branch reads the primary and the branch's own overlay, not a stack's `session:`. So it shows nothing for that window.

Whether the test's window can move to `session:` as it is:

- **The concept, yes.** `window`'s lines need no change to be read from `session:`. The test writes its attributes there and the same concept reads them.
- **Writing it from the notation, no.** A line may say `in: :control` and no other layer (`primitive::LAYERS`). So a notation document cannot write the window into `session:`, and a rule or command cannot either. The handler writes it through the stack's Rust API, as the test does.
- **Keeping the page live from it, not through the crate's driver.** The test subscribes one layer of the plan over the stack itself, laying that layer's rule on the branch overlay as `Live` does. It does not maintain the markup tree.

The smallest additions:

- `in: :session` on a line, so a concept can name the layer. `Statement::at` already does it for a write.
- `Live` over a stack: the same laying and the same tree, with each layer subscribed by `Stack::subscribe` and polled after `Stack::settle`, in place of `Branch::subscribe`.

## What the engine needs for a limit, and what cannot work

A query is terminal: nothing derives from it, so an operation that is not monotone, such as a limit or "the N after key k", is sound there.

```
 the window as                 needs of the engine                                    on a change, the subscription re-runs
 bounds [from, to)             a range scan of list/item's key, narrowed to one       when a row's key is in [from, to)
                               list; today the scan is not narrowed (8 µs a row a      and its list matches; any other
                               move, tests/paged.rs)                                    change costs nothing but the check
 a cursor and a limit,         a scan in key order from k, forward or backward,       when a row's key is between k and the
 N after key k                 that stops after N rows of the list: an index whose    last key it holds; when it holds fewer
                               order is (list, key), and a reverse walk (on sapling,  than N (the end of the list), when any
                               not main)                                              row's key is after k
```

The first scan needs the members of one list to be adjacent in the order. Over the value index of `xyz.list.item/key`, every list's keys are interleaved, and a scan passes the other lists' rows. Ordering by `(list, key)` needs a composite key, or an index of that attribute pair.

What cannot work:

- An offset by count ("rows 1000 to 1050"). It needs each row's rank, so an exact count in every subtree. The search tree keeps an estimate of each subtree's size, which could size a scrollbar but cannot give a rank.
- An order no index has: by a value computed in the query, by two fields, or by a field of another entity joined in. A limit over such an order reads every row and sorts them. The query is still sound, but the cost is that of the whole list.

## The macro, for comparison

The use site of the macro (`tests/fixtures/paged.clj`):

```clj
(paged todo/page rows: todo/paged read: (todo ?t title: ?title) entity: ?t key: ?title
       window: window/text scroll: window/scroll-text
       row: [:li {:data-key ?title} ?title])
```

It defines two queries, `todo/paged` and `todo/page`, under names the form gives, and nothing on the page says so. It needs three expander changes (commit `23b90f4`): a macro may expand to a query, to several definitions, and may hold a query that collects. It keeps a window concept, scroll command and rule per key type. Since a line with no `as:` takes any type, one pair would do.

```
                         window in the query                       macro
 a list writes           1 rule, and the view of its rows          1 paged form
 names made              none: the page is list/page for all       two, hidden in the form
 rows of its own look    yes, by the view the member names         yes, row: template
 window state in rules   none                                      a scroll rule
 expander change         no                                        yes, 23b90f4
```

## Sapling's mechanism

I could not read `dialog-db/sapling`: attaching it was refused in this session. What follows is from `paginated-views.md`, which summarises its PDD018.

- The front end keeps the open segments (at most 16), measured row heights, spacers and scroll position.
- A segment is a range of the order key, `lower <= key < upper`, never an offset.
- Each open segment is one live query with constant bounds, and the engine scans the key's range.
- On scroll the front end opens and closes segments. It picks the next bound from a one-off read of the N greatest keys below a key, or from the density of the rows it holds.

## Cost of one window move (macro version)

From `it_measures_a_window_move_over_many_rows`, a window of 50 integer keys: 12 ms at 1000 rows, 80 ms at 10000, 245 ms at 30000. That is about 8 µs per row of the whole list, because the bounds come from a join and do not narrow the scan. The window-in-the-query shape reads its bounds the same way and was not measured. Scan cost is being looked at separately, in sapling.
