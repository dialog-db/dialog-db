# The kernel and the core forms

The slim kernel as agreed on 2026-11-11, the core forms defined in it, and what running them through a model showed. Nothing here is implemented in dialog. Spellings are sketches until a reader round and a parser exist.

```mermaid
flowchart LR
  text["text"] -- read --> form["form"]
  form -- "walk: is the head kernel?" --> ask{{"(expand form): ask the rules"}}
  ask -- "(syntax form'): one answer" --> form
  form -- "all kernel" --> rule["kernel rule"]
  rule -- compile --> descriptor["descriptor, as dialog stores rules today"]
```

## The kernel

| | Form | In dialog today |
|---|---|---|
| Abstraction | `(rule head… (when premise…) (unless premise…))` | a rule descriptor; `when` and `unless` are its two lists |
| Read | `(xyz.todo ?t text: ?x state: ?s)`: application of a relation, one entity and the fields named | a concept query |
| Compute | `(:text ?x)`, `(:value ?a is: ?b)`, `(+ ?a : ?b is: ?c)`: application of a formula | a formula query or a constraint |
| Write | `(assert! A)`, `(retract! A)` as a rule's head: what stays | the `assert!` and `retract!` heads of an inductive rule; `Instruction::Assert(fact, pick)`, `Instruction::Retract(fact)` |
| Hold | `(assert A)` as a rule's head: holds this round and is not kept | a dispatched transient |
| Answer | `(report A)` as a rule's head | what a query returns |
| Syntax | `(expand pattern)` as a premise, `(syntax template)` as a head | new |

- `!` marks what stays. `(assert! A)` lands and is kept until retracted. `(assert A)` holds for the round, which is how a command is issued. A head with no word derives: the relation holds while the premises do.
- This departs from #728's design as of R25, where a plain `assert` head lands and `!` only re-issues a command. Irakli, 2026-11-11: without `!` the new count does not persist.
- An application has one part with no label. Every other part has its label; `:` alone is the label of a formula's second operand.
- A formula's result is `is:`. An application of a formula in a term's place stands for that value: `count: (+ ?n : 1)`.
- A type is a formula that holds for values of the type, so `(:text ?x)` can stand where `?x` would. `:value` is the type of every value, so `(:value ?a is: ?b)` is equality.
- **Proposed:** a pick wraps a term the same way, `(:last (:text ?x))`, so a read says everything about the attributes it reads. The alternative is #728's `(pick :last from: ?x)` beside the read.
- A head made only of operator characters is a formula. Names start with a letter, so it cannot be a concept or a domain.
- A write at the top level, `(assert! A)`, `(retract! A)` or `(assert A)` with a `when` and an `unless` if it has them, is a rule with that head.

## Syntax is a rule

A macro is a rule like any other (Irakli, 2026-11-11). Its premise says which form it is about, and its head says what that form becomes:

```clj
(rule (syntax (rule (report ?head) (when ?head)))
  (when (expand (query ?head))))
```

- `(expand pattern)` holds for the form being expanded when it has the pattern's shape. It binds the pattern's variables to the form's parts, as rule variables. `(expand pattern in: :head)` holds in that position only; the other position is `:premise`.
- `(syntax template)` is the form it expands to. A variable the pattern bound is replaced by what it took. Any other variable in a template is the template's own: it is renamed at each expansion, so it cannot capture a variable of the form being expanded.
- Neither is walked into. What is inside them is syntax, not premises and heads. They are the quote and the quasi-quote, and a bound variable is the unquote, as in Racket's `syntax-case` and `#'`.
- The rule's other premises are ordinary: they read facts and apply formulas over the same variables.

`expand` works as a command the walker issues on each form, and `syntax` as what the rules answer. A pattern takes bare parts in order and in number, with `p ...` for any number of them. A labelled part the form leaves out leaves its variable unbound, and `...?rest` takes the labelled parts the pattern does not name.

The walker rewrites a top-level form until it is a `rule` or a write, then every head and premise until each is a read or a formula. No answer and two different answers are both refused.

## The core forms

All of [model/prelude.clj](model/prelude.clj), 65 lines. `syntax` with two parts is the short way to write such a rule, and is itself one:

```clj
(rule (syntax (rule (syntax ?template) (when (expand ?pattern))))
  (when (expand (syntax ?pattern ?template))))

(syntax (query ?head)
  (rule (report ?head) (when ?head)))
(syntax (query ?head ... (when ?premise ...))
  (rule (report ?head) ... (when ?premise ...)))
```

A concept is syntax whose template is one read. Writing

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: (:last (:entity ?l)) text: (:last (:text ?x)) state: (:top ?s)))
```

is writing this rule, which says that `(todo …)` stands for that read of `xyz.todo`:

```clj
(rule (syntax (xyz.todo ?t list: (:last (:entity ?l)) text: (:last (:text ?x)) state: (:top ?s)))
  (when (expand (todo ?t list: ?l text: ?x state: ?s))))
```

`concept` only saves writing it out:

```clj
(syntax (concept (?name ?this ...?fields) ?body)
  (syntax (?name ?this ...?fields) ?body))
```

A command is an application around an application whose head names nothing. In a premise it reads the command's rows:

```clj
(rule (syntax (?domain _ of: ?subject ...?fields))
  (when (expand (?command (?concept ?subject ...?fields)) in: :premise)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)))
```

A rule may also say something about a form without refusing it:

```clj
(rule (warning "no rule reads this command")
  (when (expand (?command (?concept ?subject ...?fields)) in: :head)
        (:command ?command : ?concept is: ?domain))
  (unless (dialog.name _ is: ?command)
          (dialog.rule _ on: ?domain)))
```

## Describing things

A description is a fact about an entity, `dialog.meta/description`, never part of what the entity is. Dialog's descriptors already carry a description and leave it out of identity (`without_description`), and the stored body of a rule drops it; tonk keeps it as `dialog.meta/description` today.

- A `rule` takes `description:` as a labelled part. It lands as that fact on the rule's entity.
- `syntax` and `concept` pass what they are told about themselves to the rule they write:

  ```clj
  (concept (todo ?t list: ?l text: ?x state: ?s)
    description: "Something to do, on a list."
    (xyz.todo ?t list: (:last (:entity ?l)) text: (:last (:text ?x)) state: (:top ?s)))
  ```

- What has no form of its own, such as an attribute, is described with a plain write:

  ```clj
  (assert! (dialog.meta xyz.todo/text description: "What is to be done."))
  ```

- Finding is a query, since the descriptions are facts:

  ```clj
  (query (dialog.name ?thing is: ?name)
    (when (dialog.name ?thing is: ?name)
          (dialog.meta ?thing description: ?text)
          (:like ?text : "%list%")))
  ```

In the model that answers `?name = todo`. Any other labelled part of a rule is kept the same way, under `dialog.meta`.

## The to-do, expanded

[model/todo.clj](model/todo.clj) through the prelude, as the model printed it:

```clj
; (rule (assert! (todo ?t list: ?l text: ?x state: :open))
;   (when (add (todo list: ?l text: ?x))
;         (:id xyz.todo list: ?l text: ?x is: ?t)))
(rule (assert! (xyz.todo ?t list: (:last (:entity ?l)) text: (:last (:text ?x)) state: (:top :open)))
  (when (dialog.command.add.todo _ list: ?l text: ?x)
        (:id xyz.todo list: ?l text: ?x is: ?t)))

; (add (todo list: list:home text: "Buy milk"))
(rule (assert (dialog.command.add.todo (:id dialog.command.add.todo list: list:home text: "Buy milk")
                list: list:home text: "Buy milk")))

; (query (todo ?t text: ?x state: ?s))
(rule (report (xyz.todo ?t list: (:last (:entity ?l.2af3)) text: (:last (:text ?x)) state: (:top ?s)))
  (when (xyz.todo ?t list: (:last (:entity ?l.4dd8)) text: (:last (:text ?x)) state: (:top ?s))))

; report
; ?s = :open ?t = id:7a6a542c ?x = "Buy milk"
; ?s = :open ?t = id:e6156d91 ?x = "Call mom"
```

`?l.2af3` is the concept's own `?l`, which the query did not pass: renamed where it was expanded.

## What the model is

[model/model.py](model/model.py) is a throwaway in Python: a reader, pattern matching and template filling, an evaluator for kernel rules over a dictionary of facts, the walker, and rounds in which what `assert` says is seen once. It reads the prelude as text and runs it, so the definitions above are checked as written. It shares no code with dialog.

It does not model the planner, picks (the wrapper is carried and ignored, and a write to a to-do's field replaces what was there), layers, canonical identities, names at a revision, source maps, or a document whose forms are in any order: it takes them top to bottom.

```sh
cd design/macros/model
python3 -I model.py . todo.clj
python3 -I checks.py
```

## What it showed

Held, in [model/checks.py](model/checks.py):

- A later document's `(finish (todo …))` moves one to-do to `:done`.
- `(increment (xyz.counter ?c))`, declared nowhere and read by one rule, moves a counter from 0 to 1. The command is whatever that rule reads: its name, what it is around, and the fields it passes.
- `(unless (todo ?t state: :done))` excludes only the finished to-do, because the concept expands to one read.
- An unknown form is refused. Two concepts under one name are refused, and the error lists both expansions.
- A write through a concept that leaves a field out is refused, and so is one whose value is the wrong type.
- A misspelled command is issued and comes back with a warning; the right spelling comes back with none.
- A description on a concept, on a rule and on an attribute is stored as a fact, and a search for a word in them finds the concept by name.

Found:

1. **A read is one entity and several fields, never one attribute.** `unless` negates one proposition (`negation.rs`), so a concept that expanded to several reads could not be negated as a whole. An application that leaves a field out still requires it (`concept/query.rs`), which is why the concept's own variable stays in the read.
2. **No macro needed rows assembled into a form.** A pattern and a template carry the per-field information.
3. **`expand`, `syntax` and `:id` take whatever variables or labels they are given.** A formula in dialog has fixed cells, so these are premises of a new kind, with parameters per application as a concept query has.
4. **A command about something another command in the same document creates is lost.** `add` and `finish` together leave the to-do open: the to-do lands after the round, and the `finish` is gone by the next. `induce.rs` describes the same rounds.
5. **A write with a variable nothing binds has no meaning yet.** `(retract! (todo todo:1))` expands to a retract of three such variables and is refused. It has to mean "read, then retract what was read".
6. **Refusals give the wrong reason.** A parameter the concept does not have is reported as an unknown form. With diagnostics as something a rule derives, a rule that knows why can say so.
7. **A report's rows are named by the fields**, since the head expands too. Showing them as written needs the call site kept, as the source map would.
8. **A macro is a rule, so its identity is the rule's.** Whether dialog's canonical spelling of a rule extends to the syntax inside `expand` and `syntax` is not looked at.
9. **A command is defined by the rules that read it, so a misspelled one is issued and nothing happens.** That is worth a warning, never a refusal (Irakli, 2026-11-11). Dialog keeps the index the warning rule reads, as `dialog.rule/on` for inductive rules and `dialog.rule/reads` for deductive ones. It already reports without refusing in two places: a rule that closes a cycle through an absence test is quarantined and listed, and a fact outside its placement is a divergence. Diagnostics would be the one relation all three are read from, and what a language server asks for.
10. **A template that writes syntax with a variable of its own needs an escape**, as Racket's `(... ...)` is for a literal ellipsis: an unbound variable in the outer template is renamed, so it cannot be meant for the inner one. No core form needs it.
11. **A description follows the entity, so a changed definition starts without one.** A new body is a new rule and a new entity. Whether the description is carried over, or belongs to the name, is not decided.
