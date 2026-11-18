; A second document, so a second transaction: the to-dos it completes
; were added in the one before. Had `todo/add` and `todo/complete`
; shared a document they would share a tick, and the state would hold
; both.

(assert (todo/complete todo: todo:2))
(assert (list/archive list: list:work))

; A command is declared, so one misspelled, `(todo/compleet …)`, is
; refused as unknown and nothing of its document is written.

(query (xyz.pending ?t text: ?x))
