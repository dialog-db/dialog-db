; A second document, so a second transaction: the to-dos it completes
; were added in the one before. Had `add` and `complete` shared a
; document they would share a round, and the state would hold both.

(complete (todo todo:2))

; Nothing reads this one, which is said and not refused.
(compleet (todo todo:3))

(query (xyz.pending ?t text: ?x))
