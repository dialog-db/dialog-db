; Two lists paged through the shared relation of paged-shared.clj.

; To-dos, ordered by title.
(concept (todo ?t title: ?title)
  (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one))

; A to-do is in the list list:todos, under its title.
(rule (xyz.list ?t in: list:todos key: ?title label: ?title)
  (todo ?t title: ?title))

; Messages, ordered by time.
(concept (message ?m at: ?at body: ?body)
  (xyz.message/at of: ?m is: ?at as: :integer cardinality: :one)
  (xyz.message/body of: ?m is: ?body as: :text cardinality: :one))

; A message is in the list list:messages, under its time.
(rule (xyz.list ?m in: list:messages key: ?at label: ?body)
  (message ?m at: ?at body: ?body))

; A list with markup of its own: the rows the window shows, each as the
; list draws it, and the page that collects them.
(query (message/item ?m window: ?w is: {?at ?item})
  (xyz.window.row ?m window: ?w key: ?at)
  (message ?m at: ?at body: ?body)
  (view ?item [:p {:data-key ?at} [:time ?at] " " ?body]))

(query (message/page ?w is: ?page)
  (window ?w ...)
  (message/item _ window: ?w is: (distinct ?items))
  (view ?page
    [:div {:on/scroll (window/scroll window: ?w lower: .detail.lower upper: .detail.upper)}
      ?items]))
