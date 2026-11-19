# R39 plan: a short framing, a one-field example, and how `last` elects

Written and committed before any reader ran. The tally will be in `r39.md` beside it. Clean runner, single turn, 20 readers per variant.

## Why

In [R38](r38.md), spelling out that a concept rule expands to one rule per attribute, with a two-field example, made readers expect what dialog does:

- the note found: 19 of 20;
- the renamed derived to-do's rename landing: 16 of 20.

Two things are left to settle:

- **Election.** 9 of the 16 then said the to-do has two titles and nothing says which wins. So every variant now carries the sentence on `last` from R29, which got question 9 right whenever readers reached it.
- **How short the spec can be.** The maintainer proposed the framing "a concept is just a set of attributes that share an entity, a convenient way to express derivations over those attributes more concisely", and a simpler example.

## Variants

Each documentation sits after the code, before the questions, on two documents:

- `note`: R38's `bare-note`, the derive case with the direct yes or no question;
- `todo`: R30's `baseline`, the renamed derived to-do.

| documentation | `note` readers | `todo` readers |
|---|---|---|
| `framing`: the maintainer's framing sentence, then the `last` sentence | 20 | 20 |
| `framing-example`: the framing, a one-field example (`titled` over `xyz.acme.task` title; its rule "is the same as" the rule concluding `(xyz.acme.task ?t title: ?title)`), then the `last` sentence | 20 | 20 |
| `expansion`: R38's documentation unchanged, then the `last` sentence | 20 | 20 |

The `last` sentence: "When an attribute has several values, `last` picks the one caused most recently, whether it was stored or concluded by a rule. A concluded value is as recent as the newest fact it was concluded from."

## Scoring and bars

- `note`: question 2 scores on "yes".
- `todo`: (a) the rename lands; (b) the to-do shows "Fix login bug" and the issue keeps "Fix login"; question 9 scores on "Login fails". (b) and question 9 now count, since every variant states the election.
- A variant works if at least 14 of 20 score. It is no help if 4 or fewer do.

Predictions:

- `expansion` repeats R38 on `note` and on (a), and now also works on (b) and question 9.
- `framing-example` works as well as `expansion`.
- `framing` alone lands between the bars on `note` and (a): every prose framing before R38 failed, and this one shows nothing.

## What the readers read

<details><summary><code>framing-note</code> (sha256 <code>ba2b633568d213fa</code>)</summary>

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

A concept is just a set of attributes that share an entity: a convenient way to express derivations over those attributes more concisely.

When an attribute has several values, `last` picks the one caused most recently, whether it was stored or concluded by a rule. A concluded value is as recent as the newest fact it was concluded from.

## Questions

1. What does the `assert` form store? List each stored fact as entity, attribute, value.
2. Will the `query` find a note with the message "hello world"? Answer yes or no, then explain why.
3. In your own words, how does a concept such as `memo` relate to the `xyz.tonk.post` form inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>framing-example-note</code> (sha256 <code>cc5ff931e14b60df</code>)</summary>

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

A concept is just a set of attributes that share an entity: a convenient way to express derivations over those attributes more concisely. For example, given

```clj
(concept (titled ?t title: ?title)
  (xyz.acme.task ?t title: ?title))
```

the rule

```clj
(rule (titled ?t title: ?title)
  (ticket ?t title: ?title))
```

is the same as

```clj
(rule (xyz.acme.task ?t title: ?title)
  (ticket ?t title: ?title))
```

When an attribute has several values, `last` picks the one caused most recently, whether it was stored or concluded by a rule. A concluded value is as recent as the newest fact it was concluded from.

## Questions

1. What does the `assert` form store? List each stored fact as entity, attribute, value.
2. Will the `query` find a note with the message "hello world"? Answer yes or no, then explain why.
3. In your own words, how does a concept such as `memo` relate to the `xyz.tonk.post` form inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>expansion-note</code> (sha256 <code>bce60785ca2a0a8a</code>)</summary>

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

When an attribute has several values, `last` picks the one caused most recently, whether it was stored or concluded by a rule. A concluded value is as recent as the newest fact it was concluded from.

## Questions

1. What does the `assert` form store? List each stored fact as entity, attribute, value.
2. Will the `query` find a note with the message "hello world"? Answer yes or no, then explain why.
3. In your own words, how does a concept such as `memo` relate to the `xyz.tonk.post` form inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>framing-todo</code> (sha256 <code>c092e07ea8005de8</code>)</summary>

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

A concept is just a set of attributes that share an entity: a convenient way to express derivations over those attributes more concisely.

When an attribute has several values, `last` picks the one caused most recently, whether it was stored or concluded by a rule. A concluded value is as recent as the newest fact it was concluded from.

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

<details><summary><code>framing-example-todo</code> (sha256 <code>204f8eda70f9b18a</code>)</summary>

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

A concept is just a set of attributes that share an entity: a convenient way to express derivations over those attributes more concisely. For example, given

```clj
(concept (titled ?t title: ?title)
  (xyz.acme.task ?t title: ?title))
```

the rule

```clj
(rule (titled ?t title: ?title)
  (ticket ?t title: ?title))
```

is the same as

```clj
(rule (xyz.acme.task ?t title: ?title)
  (ticket ?t title: ?title))
```

When an attribute has several values, `last` picks the one caused most recently, whether it was stored or concluded by a rule. A concluded value is as recent as the newest fact it was concluded from.

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

<details><summary><code>expansion-todo</code> (sha256 <code>ccc24064dff4fc1b</code>)</summary>

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

When an attribute has several values, `last` picks the one caused most recently, whether it was stored or concluded by a rule. A concluded value is as recent as the newest fact it was concluded from.

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
