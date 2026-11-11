; The prelude: the core forms, written in the kernel.
; Kernel: rule (heads, when, unless), application of a relation,
; application of a formula, assert! and retract! for what stays, assert
; for one round, and report. Two directives carry syntax:
;   (expand pattern)   in a premise, holds for the form being expanded
;   (syntax template)  in a head, says what it expands to
; A variable the pattern binds is replaced in the template. Any other
; variable in a template is the template's own, fresh at each expansion.

; 1. Syntax, the short way to write such a rule.
(rule (syntax (rule (syntax ?template) (when (expand ?pattern))))
  (when (expand (syntax ?pattern ?template))))

; 2. The core forms.

; A query is a rule whose heads land in the report.
(syntax (query ?head)
  (rule (report ?head) (when ?head)))
(syntax (query ?head ... (when ?premise ...))
  (rule (report ?head) ... (when ?premise ...)))
(syntax (query ?head ... (when ?premise ...) (unless ?absent ...))
  (rule (report ?head) ... (when ?premise ...) (unless ?absent ...)))

; A concept is syntax whose template is one read.
(syntax (concept (?name ?this ...?fields) ?body)
  (syntax (?name ?this ...?fields) ?body))

; 3. Commands. An application around an application, whose head names
;    nothing, is a command. Its rows live in a domain of its own.

; In a premise it reads the command's rows.
(rule (syntax (?domain _ ...?fields))
  (when (expand (?command (?concept ...?fields)) in: :premise)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)))

(rule (syntax (?domain _ of: ?subject ...?fields))
  (when (expand (?command (?concept ?subject ...?fields)) in: :premise)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)))

; In a head, or at the top level, it issues one.
(rule (syntax (assert (?domain (:id ?domain ...?fields) ...?fields)))
  (when (expand (?command (?concept ...?fields)) in: :head)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)))

(rule (syntax (assert (?domain (:id ?domain of: ?subject ...?fields) of: ?subject ...?fields)))
  (when (expand (?command (?concept ?subject ...?fields)) in: :head)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)))

; 4. Diagnostics. A rule may say something about a form without refusing it.
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
