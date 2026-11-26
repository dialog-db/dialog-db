; Paged rendering with the window in the query, a spike. Read with
; paged-query-lists.clj.
;
; What a list joins: one row a member, naming the list, the member's
; order key, and the query that draws it. A rule of the list's own
; concludes it. The key has no type, so lists order by text, numbers
; or anything else that compares.
(concept (list/item ?r list: ?l key: ?k view: ?v)
  (xyz.list.item/list of: ?r is: ?l as: :entity cardinality: :one)
  (xyz.list.item/key of: ?r is: ?k cardinality: :one)
  (xyz.list.item/view of: ?r is: ?v as: :text cardinality: :one))

; A window: which list, and the range of its keys shown, `from:`
; included, `to:` excluded. The renderer keeps it and moves it: it is
; never concluded by a rule. Here it is a fact of the branch, standing
; in for one of the session layer.
(concept (window ?w list: ?l from: ?lo to: ?hi)
  (xyz.window/list of: ?w is: ?l as: :entity cardinality: :one)
  (xyz.window/from of: ?w is: ?lo cardinality: :one)
  (xyz.window/to of: ?w is: ?hi cardinality: :one))

; A row in the window: a place for the member, which says the member
; and the query that draws it. The renderer fills it.
(query (list/row ?r window: ?w is: {?k ?row})
  (window ?w list: ?l from: ?lo to: ?hi)
  (list/item ?r list: ?l key: ?k view: ?v)
  (<= ?lo : ?k)
  (< ?k : ?hi)
  (view ?row [:list-row {:subject ?r :data-key ?k :data-view ?v}]))

; The window's page: its rows, in key order.
(query (list/page ?w is: ?page)
  (window ?w list: ?l ...)
  (list/row _ window: ?w is: (distinct ?rows))
  (view ?page [:ol {:data-list ?l} ?rows]))
