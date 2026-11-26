; Paged rendering through one shared relation, a spike: no macro, and
; no query that takes a query. Read with paged-shared-lists.clj.
;
; A window shows one list, and the range of that list's order key it
; shows: lower bound included, upper excluded. Its bounds have no type,
; so one window holds text bounds for one list and integer bounds for
; another.
(concept (window ?w list: ?l lower: ?lo upper: ?hi)
  (xyz.window/list of: ?w is: ?l as: :entity cardinality: :one)
  (xyz.window/lower of: ?w is: ?lo cardinality: :one)
  (xyz.window/upper of: ?w is: ?hi cardinality: :one))

; Moving a window: the front end posts the new bounds.
(concept (window/scroll window: ?w lower: ?lo upper: ?hi)
  (xyz.window.scroll/window of: ?c is: ?w as: :entity in: :control cardinality: :one)
  (xyz.window.scroll/lower of: ?c is: ?lo in: :control cardinality: :one)
  (xyz.window.scroll/upper of: ?c is: ?hi in: :control cardinality: :one))

(rule (assert (window ?w list: ?l lower: ?lo upper: ?hi))
  (window/scroll window: ?w lower: ?lo upper: ?hi)
  (window ?w list: ?l ...))

; The shared relation every list derives its rows into: the row's
; entity, the list it is in, its order key, and a text to show.
;   (xyz.list ?row in: ?list key: ?key label: ?text)

; Which rows a window shows, for whichever list it names.
(rule (xyz.window.row ?r window: ?w key: ?k)
  (window ?w list: ?l lower: ?lo upper: ?hi)
  (xyz.list ?r in: ?l key: ?k)
  (<= ?lo : ?k)
  (< ?k : ?hi))

; A whole page, one query for every list: each row shows its label, in
; key order. Every list's rows look alike here.
(query (window/item ?r window: ?w is: {?k ?item})
  (xyz.window.row ?r window: ?w key: ?k)
  (xyz.list ?r label: ?label)
  (view ?item [:li {:data-key ?k} ?label]))

(query (window/page ?w is: ?page)
  (window ?w ...)
  (window/item _ window: ?w is: (distinct ?items))
  (view ?page
    [:ol {:on/scroll (window/scroll window: ?w lower: .detail.lower upper: .detail.upper)}
      ?items]))
