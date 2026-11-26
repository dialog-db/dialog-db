# Paged views with the notation's own views: a spike, 2026-11-26

Asked by the maintainer: "backport that virtual window thing we discussed, basically doing what sapling does for paged rendering but without custom web component and instead with our views", written once and reused, not defined per use case. Branch `spike/paged-views`, from `feat/notation-poc`.

## Verdicts

1. **Doable: yes.** A window is a range of the order key held as facts, moved by a command. Only rows whose key is in the range are in the view. It stays live: a fact inside the window is one insert, a fact outside it is no patch, and a move removes the rows it leaves and inserts the ones it reaches. No custom web component, no Rust per list. `tests/paged.rs`, `it_moves_the_window_by_the_scroll_command_and_stays_live`.
2. **Reusable: not on `feat/notation-poc` as it stands. With three small changes to the notation's expander, yes.** On the base branch a list needs its own copy of two queries, because a query cannot take a query or a view as a parameter and a macro cannot expand to a query. With the changes on this branch (`src/expand.rs`, `src/expand/query.rs`, about 40 lines, no engine crate), `paged` is written once as a macro in the notation (`tests/fixtures/paged.clj`), and to-dos by title and messages by time each use it with one form (`it_pages_two_lists_by_one_definition`). Still repeated: the window concept, its command and its rule, once per key type (text, integer), not once per list.

## Sapling's mechanism

I could not read `dialog-db/sapling`: attaching it was refused in this session. What follows is from `paginated-views.md`, which summarises its PDD018. Check it against the source before relying on it.

- State in the front end: which segments are open (at most 16), measured row heights, spacers, scroll position.
- A segment is a range of the order key, `lower <= key < upper`, either bound open. Never an offset: a rank needs a count in every subtree.
- Each open segment is one live query with constant bounds. The engine scans the key's range.
- On scroll the front end opens and closes segments. It picks the next bound from a one-off read of the N greatest keys below a key, or guesses from the density of the rows it already holds.
- It stays live because each segment is a subscription. A change outside every open range reaches none of them.

## The definition

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

(paged message/page rows: message/paged read: (message ?m at: ?at body: ?body) entity: ?m key: ?at
       window: window/integer scroll: window/scroll-integer
       row: [:p {:data-key ?at} ?body])
```

`window/integer` is a concept (`lower:`, `upper:`). `window/scroll-integer` is a command. One rule asserts the window from the command. A row answers an entry under its key, so the rows stand in key order. The row view does not name the key in a head of its own, because the macro writes that head.

## Gaps, and the smallest addition for each

| # | gap | smallest addition | state |
|---|---|---|---|
| 1 | A macro cannot expand to a `(query …)`. Only a query written at the top level is defined ("nothing says what (query …) is"). | `top` hands a macro's `(query …)` back to the top level to be defined. | closed here |
| 2 | A macro cannot expand to several top-level forms. `(all …)` is refused as a call. | Same change: each member of `(all …)` is expanded as if written at the top level. | closed here |
| 3 | `distinct` is refused anywhere in a rule, including a query inside a macro's template. | A `(query …)` inside a `(syntax …)` template is exempt, and is checked when expanded. A macro whose template is a rule that collects is still refused (`it_refuses_a_macro_whose_rule_collects`). | closed here |
| 4 | A query cannot take a query or a view as a parameter. | None needed once 1 to 3 land: the macro is the parameterisation. Each use compiles to its own two queries. | worked around |
| 5 | No line is of any type, and a template cannot make a name from parts. So a window concept, command and rule are written once per key type. | A template form that joins a name and a suffix (`xyz.window.<name>/lower`), so `paged` can declare its own window. Or an `:any` line in the engine. | open |
| 6 | No limit. The window is a key range, so how many rows it holds depends on density. The front end cannot ask "the N keys after k" through the notation (gaps row 16, `paginated-views.md` row 4). | An engine read with a direction and a limit over an ordered scan, and a one-off `select` form for it. Not a subscription. | open, engine |
| 7 | Cost: the bounds come from the window's facts, a join, so the range does not narrow the scan. Each move rescans the whole list. | The engine carries a comparison whose bound an earlier premise binds into the scan as a range (`paginated-views.md` row 2). | open, engine |
| 8 | The window is branch facts. Every scroll is a commit, synced and kept in history, and two devices sharing a window entity overwrite each other. | A window entity per session, or a command that writes the session overlay. The notation has no way to write the overlay today. | open |
| 9 | A native `scroll` event has no `detail.lower` or `detail.upper`. Something has to turn pixels and the rows' `data-key` into bounds. | One generic front-end handler for every list, which posts the command. That is measurement, which belongs to the front end. It is not a component per list. This spike does not run a browser: the test posts the command itself. | open, untested |
| 10 | Scrollbar size needs a total or an estimate. | A count of the list as a fold subscription (`Plan::folds` exists). Not tried. | untested |

## Cost of one window move

Measured by `it_measures_a_window_move_over_many_rows`: a window of 50 integer keys, 5 moves, with the time of `Watched::poll` after the scroll command is committed. The commit itself is not counted. Test profile (optimised), one run each:

```
 rows     first poll   one move (5 runs)   a fact outside the window
 1000     12 ms        12-13 ms            1.0 ms
 10000    78 ms        78-87 ms            1.9 ms
 30000    268 ms       239-254 ms          4.8 ms
```

A move costs about 8 µs per row in the list, not per row in the window: gap 7. A fact outside the window is cheap, but it still grows with the list.
