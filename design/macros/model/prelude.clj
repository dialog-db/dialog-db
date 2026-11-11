; The prelude: the core forms, written in the kernel.
; Kernel: rule (heads, when, unless), application of a relation,
; application of a formula, the write relations assert! and retract!, assert for one round,
; the report relation, and data with quoted forms.

; 1. The one rule that applies macros.
;    A form whose head names a macro expands by that macro's template.
(rule (dialog.syntax ?site expansion: ?out)
  (when (dialog.syntax ?site form: ?form)
        (:match ?form : '(?name _ ... ...?rest) is: ?shape)
        (:get ?shape : 'name is: ?name)
        (dialog.name ?macro is: ?name)
        (dialog.macro ?macro pattern: ?pattern template: ?template)
        (:match ?form : ?pattern is: ?holes)
        (:fill ?template : ?holes is: ?out)))

; 2. `define-syntax`, stated once in the kernel so it can define the rest.
;    It takes a name and clauses, each a pattern and a template, as
;    Racket's syntax-rules does. `_` stands where the name would.
(rule (assert! (dialog.macro macro:define-syntax
                 pattern:  '(define-syntax ?name [?pattern ?template] ...)
                 template: '(rule (assert! (dialog.macro (:id :macro name: '?name pattern: '?pattern template: '?template)
                                             pattern:  '?pattern
                                             template: '?template)) ...
                                  (assert! (dialog.name (:id :macro name: '?name pattern: '?pattern template: '?template)
                                             is: '?name)) ...)))
      (assert! (dialog.name macro:define-syntax is: 'define-syntax)))

; 3. The core forms.

; A query is a rule whose heads land in the report.
(define-syntax query
  [(_ ?head)
   (rule (report ?head) (when ?head))]
  [(_ ?head ... (when ?premise ...))
   (rule (report ?head) ... (when ?premise ...))]
  [(_ ?head ... (when ?premise ...) (unless ?absent ...))
   (rule (report ?head) ... (when ?premise ...) (unless ?absent ...))])

; A concept is syntax whose template is one read.
(define-syntax concept
  [(_ (?name ?this ...?fields) ?body)
   (define-syntax ?name
     [(_ ?this ...?fields) ?body])])

; 4. Commands. An application around an application, whose head names
;    nothing, is a command. Its rows live in a domain of its own.

; In a premise it reads the command's rows.
(rule (dialog.syntax ?site expansion: ?out)
  (when (dialog.syntax ?site form: ?form position: :premise)
        (:match ?form : '(?command (?concept ...?fields)) is: ?holes)
        (:get ?holes : 'command is: ?name)
        (:command ?form is: ?domain)
        (:bind ?holes : 'domain to: ?domain is: ?filled)
        (:fill '(?domain _ ...?fields) : ?filled is: ?out))
  (unless (dialog.name _ is: ?name)))

(rule (dialog.syntax ?site expansion: ?out)
  (when (dialog.syntax ?site form: ?form position: :premise)
        (:match ?form : '(?command (?concept ?subject ...?fields)) is: ?holes)
        (:get ?holes : 'command is: ?name)
        (:command ?form is: ?domain)
        (:bind ?holes : 'domain to: ?domain is: ?filled)
        (:fill '(?domain _ of: ?subject ...?fields) : ?filled is: ?out))
  (unless (dialog.name _ is: ?name)))

; In a head, or at the top level, it issues one.
(rule (dialog.syntax ?site expansion: ?out)
  (when (dialog.syntax ?site form: ?form position: :head)
        (:match ?form : '(?command (?concept ...?fields)) is: ?holes)
        (:get ?holes : 'command is: ?name)
        (:command ?form is: ?domain)
        (:bind ?holes : 'domain to: ?domain is: ?filled)
        (:fill '(assert (?domain (:id ?domain ...?fields) ...?fields)) : ?filled is: ?out))
  (unless (dialog.name _ is: ?name)))

(rule (dialog.syntax ?site expansion: ?out)
  (when (dialog.syntax ?site form: ?form position: :head)
        (:match ?form : '(?command (?concept ?subject ...?fields)) is: ?holes)
        (:get ?holes : 'command is: ?name)
        (:command ?form is: ?domain)
        (:bind ?holes : 'domain to: ?domain is: ?filled)
        (:fill '(assert (?domain (:id ?domain of: ?subject ...?fields) of: ?subject ...?fields)) : ?filled is: ?out))
  (unless (dialog.name _ is: ?name)))

; 5. Diagnostics. A rule may say something about a form without refusing it.

; Issuing a command no rule reads is worth a warning.
(rule (dialog.syntax ?site warning: "no rule reads this command")
  (when (dialog.syntax ?site form: ?form position: :head)
        (:match ?form : '(?command (?concept ...?fields)) is: ?holes)
        (:get ?holes : 'command is: ?name)
        (:command ?form is: ?domain))
  (unless (dialog.name _ is: ?name)
          (dialog.rule _ on: ?domain)))

(rule (dialog.syntax ?site warning: "no rule reads this command")
  (when (dialog.syntax ?site form: ?form position: :head)
        (:match ?form : '(?command (?concept ?subject ...?fields)) is: ?holes)
        (:get ?holes : 'command is: ?name)
        (:command ?form is: ?domain))
  (unless (dialog.name _ is: ?name)
          (dialog.rule _ on: ?domain)))
