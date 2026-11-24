; A second document, so a second transaction: the to-dos it completes
; were added in the one before. Had `todo/add` and `todo/complete`
; shared a document they would share a tick, and the state would hold
; both.

(assert (todo/complete list: list:home text: "Call mum"))
(assert (list/archive list: list:work))

; A command is declared, so one misspelled, `(todo/compleet …)`, is
; refused as unknown and nothing of its document is written.

(select (xyz.pending ?t text: ?x))
