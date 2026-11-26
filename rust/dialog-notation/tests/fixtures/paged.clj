; Paged rendering, a spike: a list shown as a window of its order, only
; the rows whose order key falls in the window's bounds. The window is
; facts, moved by a command a front end posts on scroll. Written once,
; here; a list uses it with one `(paged …)` form.
;
; A window is a range of the order key, lower bound included, upper
; excluded, never a row number: a rank needs a count. The front end
; picks the next bounds from the keys of the rows it holds, which every
; row carries as `data-key`, and posts them.

; A window over a text order. A type takes one window concept of its
; own: a line has one type, and no type is "any".
(concept (window/text ?w lower: ?lo upper: ?hi)
  (xyz.window.text/lower of: ?w is: ?lo as: :text cardinality: :one)
  (xyz.window.text/upper of: ?w is: ?hi as: :text cardinality: :one))

(concept (window/scroll-text window: ?w lower: ?lo upper: ?hi)
  (xyz.window.scroll-text/window of: ?c is: ?w as: :entity in: :control cardinality: :one)
  (xyz.window.scroll-text/lower of: ?c is: ?lo as: :text in: :control cardinality: :one)
  (xyz.window.scroll-text/upper of: ?c is: ?hi as: :text in: :control cardinality: :one))

(rule (assert (window/text ?w lower: ?lo upper: ?hi))
  (window/scroll-text window: ?w lower: ?lo upper: ?hi))

; The same over an integer order.
(concept (window/integer ?w lower: ?lo upper: ?hi)
  (xyz.window.integer/lower of: ?w is: ?lo as: :integer cardinality: :one)
  (xyz.window.integer/upper of: ?w is: ?hi as: :integer cardinality: :one))

(concept (window/scroll-integer window: ?w lower: ?lo upper: ?hi)
  (xyz.window.scroll-integer/window of: ?c is: ?w as: :entity in: :control cardinality: :one)
  (xyz.window.scroll-integer/lower of: ?c is: ?lo as: :integer in: :control cardinality: :one)
  (xyz.window.scroll-integer/upper of: ?c is: ?hi as: :integer in: :control cardinality: :one))

(rule (assert (window/integer ?w lower: ?lo upper: ?hi))
  (window/scroll-integer window: ?w lower: ?lo upper: ?hi))

; A paged list: `(paged name rows: name read: form entity: ?e key: ?k
; window: concept scroll: command row: template)`. It makes two
; queries. `rows` answers each answer of `read` whose key `?k` is in the
; window, as an entry under its key, so the rows stand in key order.
; `name` is the window's markup: the rows, and the scroll event.
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
