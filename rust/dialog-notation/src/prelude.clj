; The prelude: the core forms, written in the kernel.
;
; Two directives carry syntax:
;   (expand pattern)   in a premise, holds for the form being expanded
;   (syntax template)  in a head, says what it expands to
; A variable the pattern binds is replaced in the template. Any other
; variable in a template is the template's own, fresh at each expansion.

; A query is a rule whose heads land in the report.
(rule (syntax (rule (report ?head) (when ?head)))
  (when (expand (query ?head))))
(rule (syntax (rule (report ?head) ... (when ?premise ...)))
  (when (expand (query ?head ... (when ?premise ...)))))
(rule (syntax (rule (report ?head) ... (when ?premise ...) (unless ?absent ...)))
  (when (expand (query ?head ... (when ?premise ...) (unless ?absent ...)))))

; A concept is a rule about syntax: where its name is applied, the reads
; its body holds stand. Its variables tie its parameters to the fields;
; the names need not agree. What it says about itself, such as
; description:, goes to the rule.
(rule (syntax (rule ...?about
                (syntax ?body)
                (when (expand (?name ?this ...?fields)))))
  (when (expand (concept (?name ?this ...?fields) ?body ...?about))))
(rule (syntax (rule ...?about
                (syntax (all ?first ?second ?more ...))
                (when (expand (?name ?this ...?fields)))))
  (when (expand (concept (?name ?this ...?fields) ?first ?second ?more ... ...?about))))

; A command is an application around an application, whose head names
; nothing. Its rows live in a domain of its own, and say what the
; command is about under `about:`. The part with no label is `of:`,
; the row itself, so that name is taken.

; In a premise it reads the command's rows.
(rule (syntax (?domain _ ...?fields))
  (when (expand (?command (?concept ...?fields)) in: :premise)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)))

(rule (syntax (?domain _ about: ?subject ...?fields))
  (when (expand (?command (?concept ?subject ...?fields)) in: :premise)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)))

; In a head, or at the top level, it issues one, which holds one round.
(rule (syntax (assert (?domain (:id ?domain ...?fields) ...?fields)))
  (when (expand (?command (?concept ...?fields)) in: :head)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)))

(rule (syntax (assert (?domain (:id ?domain about: ?subject ...?fields) about: ?subject ...?fields)))
  (when (expand (?command (?concept ?subject ...?fields)) in: :head)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)))

; A rule may say something about a form without refusing it.
(rule (warning "no rule reads this command")
  (when (expand (?command (?concept ...?fields)) in: :head)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)
          (dialog.rule _ on: ?domain)))

(rule (warning "no rule reads this command")
  (when (expand (?command (?concept ?subject ...?fields)) in: :head)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)
          (dialog.rule _ on: ?domain)))
