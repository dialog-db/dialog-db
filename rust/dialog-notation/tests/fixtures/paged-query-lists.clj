; Two lists that join paged-query.clj: each brings one rule and the
; view of its rows.

(concept (todo ?t title: ?title)
  (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))

; A to-do joins the list list:todos, ordered by title, drawn by todo/row.
(rule (list/item ?t list: list:todos key: ?title view: "todo/row")
  (todo ?t title: ?title))

(query (todo/row ?t is: ?row)
  (todo ?t title: ?title)
  (view ?row [:li ?title]))

(concept (message ?m at: ?at body: ?body)
  (xyz.message/at of: ?m is: ?at as: :integer cardinality: :one)
  (xyz.message/body of: ?m is: ?body as: :text cardinality: :one))

; A message joins list:messages, ordered by time, drawn by message/row.
(rule (list/item ?m list: list:messages key: ?at view: "message/row")
  (message ?m at: ?at body: ?body))

(query (message/row ?m is: ?row)
  (message ?m at: ?at body: ?body)
  (view ?row [:p [:time ?at] " " ?body]))
