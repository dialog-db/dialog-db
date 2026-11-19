# R38 plan: a concept rule is one rule per attribute

Written and committed before any reader ran. The tally will be in `r38.md` beside it. Clean runner, single turn.

## Why

[R37](r37.md) found that, asked directly, readers commit to a nominal and goal-directed model. A rule that concludes `memo` makes a memo, not a note, and runs only when `memo` is asked for. The maintainer's proposal is to spell out plainly what a concept rule is:

- a concept is a two-way mapping, one attribute per field;
- a rule whose conclusion is a concept is shorthand for one rule per attribute, each concluding a single attribute.

Once that is said, nothing concludes "a memo" as such. Only attributes are concluded, and a `note` over the same attribute reads them.

## Variants

The documentation, `spec`, is the maintainer's explanation with his example renamed to `task`, `ticket` and `xyz.acme.task`. The renaming keeps the example's `memo` and `xyz.tonk.post` from colliding with the names in the code under test. It sits after the code, before the questions.

| variant | readers | document |
|---|---|---|
| `bare-note` | 10 | R32's derive case (`note` and `memo` over `xyz.tonk.post` directly, a rule concludes `memo` from `email`), with R37's direct question 2 |
| `spec-note` | 20 | the same, with `spec` |
| `spec-todo` | 20 | R30's `baseline` (the renamed derived to-do, rule B on `xyz.todo`), with `spec` |

## Scoring and bars

- `bare-note` and `spec-note`: question 2 scores on "yes", the note is found.
- `spec-todo`: as R30, (a) the rename lands, that is, rule B reads the derived title and stores "Fix login bug"; (b) the to-do shows it; question 9 reported.
- A `spec` variant works if at least 14 of 20 score. It is no help if 4 or fewer do.
- `bare-note` is the control and is expected at 0 to 2 of 10.

Prediction: both `spec` variants work. The expansion shows a rule's conclusion as attribute claims in the domain form the readers would otherwise take for storage, so B's `(xyz.todo ...)` and `note`'s `xyz.tonk.post` read them. `spec-todo` question 9 stays split, since nothing says how `last` elects.

## What the readers read

<details><summary><code>bare-note</code> (sha256 <code>ef34f89458b9c066</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (note ?note message: ?message)
  (xyz.tonk.post ?note
    content: (:last (:text ?message))))

(concept (memo ?memo message: ?message to: ?to)
  (xyz.tonk.post ?memo
    content: (:last (:text ?message))
    audience: (:last (:entity ?to))))

(concept (email ?mail body: ?body recipient: ?to)
  (xyz.mail ?mail
    body: (:last (:text ?body))
    recipient: (:last (:entity ?to))))

(rule (memo ?mail message: ?body to: ?to)
  (email ?mail body: ?body recipient: ?to))

(assert (email mail:one body: "hello world" recipient: mailto:alice@web.mail))
(query (note ?note message: ?msg))
```

Nothing is stored before the `assert`.

## Questions

1. What does the `assert` form store? List each stored fact as entity, attribute, value.
2. Will the `query` find a note with the message "hello world"? Answer yes or no, then explain why.
3. In your own words, how does a concept such as `memo` relate to the `xyz.tonk.post` form inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>spec-note</code> (sha256 <code>63b5c8e9ad25b6e3</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (note ?note message: ?message)
  (xyz.tonk.post ?note
    content: (:last (:text ?message))))

(concept (memo ?memo message: ?message to: ?to)
  (xyz.tonk.post ?memo
    content: (:last (:text ?message))
    audience: (:last (:entity ?to))))

(concept (email ?mail body: ?body recipient: ?to)
  (xyz.mail ?mail
    body: (:last (:text ?body))
    recipient: (:last (:entity ?to))))

(rule (memo ?mail message: ?body to: ?to)
  (email ?mail body: ?body recipient: ?to))

(assert (email mail:one body: "hello world" recipient: mailto:alice@web.mail))
(query (note ?note message: ?msg))
```

Nothing is stored before the `assert`.

## Documentation

A concept is a two-way mapping between its fields and attributes, one attribute per field. Asserting a concept stores one claim per attribute, and querying a concept reads them back. A rule whose conclusion is a concept is shorthand for a set of rules, one per attribute. Given

```clj
(concept (task ?task title: ?title owner: ?owner)
  (xyz.acme.task ?task
    title: ?title
    owner: ?owner))
```

the rule

```clj
(rule (task ?t title: ?title owner: ?owner)
  (ticket ?t title: ?title owner: ?owner))
```

expands to one rule per attribute:

```clj
(rule (xyz.acme.task ?t title: ?title)
  (ticket ?t title: ?title owner: _))

(rule (xyz.acme.task ?t owner: ?owner)
  (ticket ?t title: _ owner: ?owner))
```

## Questions

1. What does the `assert` form store? List each stored fact as entity, attribute, value.
2. Will the `query` find a note with the message "hello world"? Answer yes or no, then explain why.
3. In your own words, how does a concept such as `memo` relate to the `xyz.tonk.post` form inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>spec-todo</code> (sha256 <code>103d6a17110b9b81</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (todo ?t title: ?title status: ?status)
  (xyz.todo ?t title: (last ?title as: :text)
               status: (last ?status as: [:open :done])))

(concept (issue ?i title: ?title state: ?state)
  (xyz.tracker ?i title: (last ?title as: :text)
                  state: (last ?state as: :text)))

; A
(rule (todo ?i title: ?x status: :open)
  (issue ?i title: ?x state: "open"))

; B
(rule (update-title (todo ?t) old: ?old new: ?new)
  (rename (todo ?t) title: ?new)
  (xyz.todo ?t title: ?old))

; C
(rule (retract (xyz.todo ?t title: ?old))
  (update-title (todo ?t) old: ?old))

; D
(rule (assert (xyz.todo ?t title: ?new))
  (update-title (todo ?t) new: ?new))
```

These facts are stored, and nothing else:

```text
todo:1   xyz.todo/title     "Buy milk"
todo:1   xyz.todo/status    :open
issue:7  xyz.tracker/title  "Fix login"
issue:7  xyz.tracker/state  "open"
```

## Documentation

A concept is a two-way mapping between its fields and attributes, one attribute per field. Asserting a concept stores one claim per attribute, and querying a concept reads them back. A rule whose conclusion is a concept is shorthand for a set of rules, one per attribute. Given

```clj
(concept (task ?task title: ?title owner: ?owner)
  (xyz.acme.task ?task
    title: ?title
    owner: ?owner))
```

the rule

```clj
(rule (task ?t title: ?title owner: ?owner)
  (ticket ?t title: ?title owner: ?owner))
```

expands to one rule per attribute:

```clj
(rule (xyz.acme.task ?t title: ?title)
  (ticket ?t title: ?title owner: _))

(rule (xyz.acme.task ?t owner: ?owner)
  (ticket ?t title: _ owner: ?owner))
```

## Questions

1. For each of the rules A, B, C and D, which part is its conclusion and which parts are its conditions?
2. What does rule A do? Is the to-do it gives stored?
3. A document holds only `(query (todo ?t title: ?x))`. What does it give back?
4. The issue's state is changed to "closed". What does the same query give back now?
5. With the issue open again, a document holds `(rename (todo todo:1) title: "Buy oat milk")`. Walk through what each rule does. Is `update-title` stored?
6. Do the retract and the assert from question 5 take effect at once, in the same step as the rename, or later? Why?
7. Once question 5's document has taken full effect, what does `(query (todo todo:1 title: ?x))` give back? Is "Buy milk" still stored?
8. Instead, a document holds `(rename (todo issue:7) title: "Fix login bug")`. Afterwards, what title does the to-do `issue:7` show, and what is the issue's title?
9. After question 8, the issue's title is changed to "Login fails". What title does the to-do `issue:7` show now?
10. Write rules in this notation so that `(archive (note ?n))` removes the note's text, stored as `xyz.note/text`, and stores the same text as `xyz.archive/text` on the same entity.
11. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>
