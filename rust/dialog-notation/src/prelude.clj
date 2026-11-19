; The prelude: the core forms, written in the kernel.
;
; A rule's first part is its head, and every part after it is a premise.
; Two directives carry syntax:
;   (expand pattern)   among the premises, holds for the form being expanded
;   (syntax template)  as the head, says what it expands to
; A variable the pattern binds is replaced in the template. Any other
; variable in a template is the template's own, fresh at each expansion.

; Asking is a rule whose head lands in the report. With no premise it
; reads what it reports. A query, which takes the whole of a field with
; `(distinct ?x)`, is defined with `(query …)` and asked the same way;
; the walker reads both.
(rule (syntax (rule (report ?head) ?head))
  (expand (select ?head)))
(rule (syntax (rule (report ?head) ?premise ... ?more))
  (expand (select ?head ?premise ... ?more)))

; A concept is a rule about syntax: where its name is applied, the reads
; its body holds stand. Its variables tie its parameters to the fields;
; the names need not agree. What it is for is said in a comment above
; it, and not in the concept.
(rule (syntax (rule (syntax ?body)
                (expand (?name ?this ... ?fields))))
  (expand (concept (?name ?this ... ?fields) ?body)))
(rule (syntax (rule (syntax (all ?first ?second ... ?more))
                (expand (?name ?this ... ?fields))))
  (expand (concept (?name ?this ... ?fields) ?first ?second ... ?more)))

; A concept whose head names no entity is a command: its lines are on
; the control layer, and the entity they are of is derived from what is
; asserted. Asserted, it holds for the next tick and that tick only.
; Read, it is read as any concept is.
(rule (syntax (rule (syntax ?body)
                (expand (?name ... ?fields))))
  (expand (concept (?name ... ?fields) ?body)))
(rule (syntax (rule (syntax (all ?first ?second ... ?more))
                (expand (?name ... ?fields))))
  (expand (concept (?name ... ?fields) ?first ?second ... ?more)))

; A rule may say something about a form without refusing it: a
; command issued or deduced that no rule reads.
(rule (warning "no rule reads this command")
  (expand (?name ... _) in: :head)
  (:command ?name is: ?domain)
  (unless (dialog.rule _ on: ?domain)))
