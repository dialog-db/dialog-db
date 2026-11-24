; A to-do list: a concept, the commands that change it, and a question.

; A comment right above a form says what the form is for. One with a
; blank line under it, like this one, is about what follows.

; Something to do, on a list. A line is one attribute: `of:` its entity,
; `is:` its value, `as:` its type, and how many claims it holds for one
; entity.
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
  (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
  (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one))

; A command is a concept whose head names no entity, its lines on the
; control layer. Asserted, it holds for the next tick and that tick
; only; its entity is derived from what it says.

; Adding a to-do, to a list.
(concept (todo/add list: ?l text: ?x)
  (xyz.todo.add/list of: ?c is: ?l as: :entity in: :control)
  (xyz.todo.add/text of: ?c is: ?x as: :text in: :control))

; Completing one, found by its list and its text.
(concept (todo/complete list: ?l text: ?x)
  (xyz.todo.complete/list of: ?c is: ?l as: :entity in: :control)
  (xyz.todo.complete/text of: ?c is: ?x as: :text in: :control))

; Archiving a list.
(concept (list/archive list: ?l)
  (xyz.list.archive/list of: ?c is: ?l as: :entity in: :control))

; Adding a to-do opens it. Its entity is the one its list and its text
; identify, so the same list and text make one to-do on every device.
(rule (assert (todo ?t list: ?l text: ?x state: :open))
  (todo/add list: ?l text: ?x)
  (id ?t list: ?l text: ?x))

; A write says every field, so one that changes the state alone writes
; the attribute, not the concept.
(rule (assert (xyz.todo/state of: ?t is: :done as: [:open :done] cardinality: :one))
  (todo/complete list: ?l text: ?x)
  (todo ?t list: ?l text: ?x ...))

; A rule may issue a command, which is read in the tick after.

; Archiving a list completes what is on it.
(rule (assert (todo/complete list: ?l text: ?x))
  (list/archive list: ?l)
  (todo ?t list: ?l text: ?x ...))

; What is left to do, derived and never stored.
(rule (xyz.pending ?t text: ?x)
  (todo ?t text: ?x state: :open ...))

(assert (todo/add list: list:home text: "Buy milk"))
(assert (todo/add list: list:home text: "Call mum"))
(assert (todo/add list: list:work text: "Ship it"))
