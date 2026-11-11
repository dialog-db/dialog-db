; A to-do list: a concept, the commands that change it, and a question.

(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: (:last (:entity ?l))
               text: (:last (:text ?x))
               state: (:last (:entity ?s)))
  description: "Something to do, on a list")

; `add` and `complete` are defined here and nowhere else: a command is
; whatever some rule reads.
(rule (assert! (todo ?t list: ?l text: ?x state: :open))
  (when (add (todo ?t list: ?l text: ?x)))
  description: "Adding a to-do opens it")

(rule (assert! (todo ?t state: :done))
  (when (complete (todo ?t))))

; What is left to do, derived and never stored.
(rule (xyz.pending ?t text: ?x)
  (when (todo ?t text: ?x state: :open)))

(add (todo todo:1 list: list:home text: "Buy milk"))
(add (todo todo:2 list: list:home text: "Call mum"))
(add (todo todo:3 list: list:work text: "Ship it"))
