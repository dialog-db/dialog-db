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

; 2. `macro`, stated once in the kernel so it can define the rest.
(rule (assert! (dialog.macro macro:macro
                pattern:  '(macro (?name ?part ... ...?fields) ?template)
                template: '(rule (assert! (dialog.macro (:id :macro pattern: '(?name ?part ... ...?fields) template: '?template)
                                           pattern:  '(?name ?part ... ...?fields)
                                           template: '?template))
                                 (assert! (dialog.name (:id :macro pattern: '(?name ?part ... ...?fields) template: '?template)
                                           is: '?name)))))
      (assert! (dialog.name macro:macro is: 'macro)))

; 3. The core forms.

; A query is a rule whose heads land in the report.
(macro (query ?head)
  (rule (report ?head) (when ?head)))
(macro (query ?head ... (when ?premise ...))
  (rule (report ?head) ... (when ?premise ...)))
(macro (query ?head ... (when ?premise ...) (unless ?absent ...))
  (rule (report ?head) ... (when ?premise ...) (unless ?absent ...)))

; A write at the top level is a rule with that head. With `!` it stays;
; without, it holds for this round, which is how a command is issued.
(macro (assert! ?fact)
  (rule (assert! ?fact)))
(macro (assert! ?fact (when ?premise ...))
  (rule (assert! ?fact) (when ?premise ...)))
(macro (retract! ?fact)
  (rule (retract! ?fact)))
(macro (retract! ?fact (when ?premise ...))
  (rule (retract! ?fact) (when ?premise ...)))
(macro (assert ?fact)
  (rule (assert ?fact)))

; A concept is a macro whose template is one read.
(macro (concept (?name ?this ...?fields) ?body)
  (macro (?name ?this ...?fields) ?body))

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
