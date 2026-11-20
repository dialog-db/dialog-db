; The to-do document, written in the core forms.

(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: (:last (:entity ?l)) text: (:last (:text ?x)) state: (:top ?s)))

(rule (assert! (todo ?t list: ?l text: ?x state: :open))
  (when (add (todo list: ?l text: ?x))
        (:id xyz.todo list: ?l text: ?x is: ?t)))

(rule (assert! (xyz.todo ?t state: (:top :done)))
  (when (finish (todo ?t))
        (todo ?t)))

(add (todo list: list:home text: "Buy milk"))
(add (todo list: list:home text: "Call mom"))

(query (todo ?t text: ?x state: ?s))
