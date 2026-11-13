; A to-do list: a concept, the commands that change it, and a question.

(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: (:last (:entity ?l))
               text: (:last (:text ?x))
               state: (:last (:entity ?s)))
  description: "Something to do, on a list")

; `add` and `complete` are defined here and nowhere else: a command is
; whatever some rule reads.
(rule (assert! (todo ?t list: ?l text: ?x state: :open))
  (add (todo ?t list: ?l text: ?x))
  description: "Adding a to-do opens it")

; A write says every field, so one that changes the state alone writes
; the relation, not the concept.
(rule (assert! (xyz.todo ?t state: (:last (:entity :done))))
  (complete (todo ?t)))

; A rule may issue a command, which is read in the round after.
(rule (complete (todo ?t))
  (archive (xyz.list ?l))
  (todo ?t list: ?l)
  description: "Archiving a list completes what is on it")

; What is left to do, derived and never stored.
(rule (xyz.pending ?t text: ?x)
  (todo ?t text: ?x state: :open))

(add (todo todo:1 list: list:home text: "Buy milk"))
(add (todo todo:2 list: list:home text: "Call mum"))
(add (todo todo:3 list: list:work text: "Ship it"))
