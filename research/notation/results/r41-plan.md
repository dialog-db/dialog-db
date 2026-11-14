# R41 plan: does the attribute-line spelling keep what R38 to R40 established?

Written and committed before any reader ran. The tally will be in `r41.md` beside it. Clean runner, single turn, 20 readers per document.

## Why

After R40 the maintainer settled on writing each attribute on its own line, by its full name, as a claim:

```clj
(concept (todo ?i list: ?l title: ?t state: ?s)
  (xyz.todo/title of: ?i is: ?t as: :text pick: :last)
  (xyz.todo/state of: ?i is: ?s as: [:open :done] pick: :last)
  (xyz.list/task of: ?l is: ?i as: :entity pick: :all))
```

with `in:` naming where a line's claims are kept. Under the design, a location is a set of facts and rules run over it: a read `in: :replica` sees what the replica stores and what rules conclude from it, and a condition that names its own `in:` reads that place whatever the read names.

The spec opening that made readers expect dialog's behavior (R39) and the misreading R40 found were both measured on the old spelling. This round reruns them on the new one, and adds the first test of `in:`.

## Documents

Every document carries the new spec: a paragraph on attribute lines and `pick:` (with R39's sentence on `last`), then the framing and the one-field example, translated. Two changes from R39's wording: the framing drops "that share an entity", since a line may read another entity (`xyz.list/task of: ?l`), and the election sentence now speaks of claims and `pick: :last`.

| document | from | readers |
|---|---|---|
| `note` | R39 `framing-example-note`, translated | 20 |
| `todo` | R39 `framing-example-todo`, translated | 20 |
| `todo-bare` | `todo` without the one-field example | 20 |
| `list` | R40's list case with `pick: :last` on the `xyz.list/task` line; facts ordered so that "each list keeps its newest task" and "each to-do keeps its newest list" give different answers | 20 |
| `place-param` | a `draft` concept taking `at:`, drafts asserted in memory, a rule concluding a `note` from each draft, a note asserted in the replica | 20 |
| `place-fixed` | the same, but `draft` fixes `in: :memory` | 20 |

## Scoring and bars

- `note`: question 2 scores on "yes".
- `todo` and `todo-bare`: as R39. (a) the rename lands on question 8; (b) the to-do shows "Fix login bug" and the issue keeps "Fix login"; question 9 scores on "Login fails".
- `list`: question 1 scores on `todo:b` alone; question 2 on `list:work` alone; question 3 when the pick elects among each list's tasks.
- `place-param`: question 1 scores on `note:1` alone; question 2 on `draft:1` alone.
- `place-fixed`: the design's answer to question 1 is `note:1` and `draft:1`, since the rule's condition reads memory whatever the query names; question 2 on `draft:1` alone.

A document reads right if at least 14 of 20 score on its main question (`note` 2, `todo` (a), `list` 1, `place` 1), and misleads if 4 or fewer do.

Predictions:

- `note` and `todo` read right, as in R39 (20 and 18).
- `todo-bare` reads right, against R39's 10 for the framing alone: a conclusion written `(xyz.todo/title of: ?t is: ?new)` already has the shape of a condition, which is what the example was there to show.
- `list` reads right: the pick sits on a line naming the list's claims.
- `place-param` reads right.
- `place-fixed` lands between the bars. Readers expect a read `at: :replica` to show only replica notes, and the design shows the draft's note there too.

## What the readers read

<details><summary><code>note</code> (sha256 <code>80891702a86fbaa6</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (note ?note message: ?message)
  (xyz.tonk.post/content of: ?note is: ?message as: :text pick: :last))

(concept (memo ?memo message: ?message to: ?to)
  (xyz.tonk.post/content of: ?memo is: ?message as: :text pick: :last)
  (xyz.tonk.post/audience of: ?memo is: ?to as: :entity pick: :last))

(concept (email ?mail body: ?body recipient: ?to)
  (xyz.mail/body of: ?mail is: ?body as: :text pick: :last)
  (xyz.mail/recipient of: ?mail is: ?to as: :entity pick: :last))

(rule (memo ?mail message: ?body to: ?to)
  (email ?mail body: ?body recipient: ?to))

(assert (email mail:one body: "hello world" recipient: mailto:alice@web.mail))
(query (note ?note message: ?msg))
```

Nothing is stored before the `assert`.

## Documentation

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text pick: :last)` reads the claims of one attribute, `xyz.todo/title`, of one entity, `?t`, and each claim's value binds `?title`. `as:` is the type a value must have. One attribute of one entity can hold several claims, and each `assert` adds one. `pick:` elects among them: `:last` reads the one caused most recently, whether it was stored or concluded by a rule, and `:all` reads every claim. Without a pick, every claim is read. A concluded value is as recent as the newest fact it was concluded from.

A concept is just a set of attributes: a convenient way to express derivations over those attributes more concisely. For example, given

```clj
(concept (titled ?t title: ?title)
  (xyz.acme.task/title of: ?t is: ?title))
```

the rule

```clj
(rule (titled ?t title: ?title)
  (ticket ?t title: ?title))
```

is the same as

```clj
(rule (xyz.acme.task/title of: ?t is: ?title)
  (ticket ?t title: ?title))
```

## Questions

1. What does the `assert` form store? List each stored fact as entity, attribute, value.
2. Will the `query` find a note with the message "hello world"? Answer yes or no, then explain why.
3. In your own words, how does a concept such as `memo` relate to the lines inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>todo</code> (sha256 <code>68a1d18f1d137cfa</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (todo ?t title: ?title status: ?status)
  (xyz.todo/title of: ?t is: ?title as: :text pick: :last)
  (xyz.todo/status of: ?t is: ?status as: [:open :done] pick: :last))

(concept (issue ?i title: ?title state: ?state)
  (xyz.tracker/title of: ?i is: ?title as: :text pick: :last)
  (xyz.tracker/state of: ?i is: ?state as: :text pick: :last))

; A
(rule (todo ?i title: ?x status: :open)
  (issue ?i title: ?x state: "open"))

; B
(rule (update-title (todo ?t) old: ?old new: ?new)
  (rename (todo ?t) title: ?new)
  (xyz.todo/title of: ?t is: ?old))

; C
(rule (retract (xyz.todo/title of: ?t is: ?old))
  (update-title (todo ?t) old: ?old))

; D
(rule (assert (xyz.todo/title of: ?t is: ?new))
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

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text pick: :last)` reads the claims of one attribute, `xyz.todo/title`, of one entity, `?t`, and each claim's value binds `?title`. `as:` is the type a value must have. One attribute of one entity can hold several claims, and each `assert` adds one. `pick:` elects among them: `:last` reads the one caused most recently, whether it was stored or concluded by a rule, and `:all` reads every claim. Without a pick, every claim is read. A concluded value is as recent as the newest fact it was concluded from.

A concept is just a set of attributes: a convenient way to express derivations over those attributes more concisely. For example, given

```clj
(concept (titled ?t title: ?title)
  (xyz.acme.task/title of: ?t is: ?title))
```

the rule

```clj
(rule (titled ?t title: ?title)
  (ticket ?t title: ?title))
```

is the same as

```clj
(rule (xyz.acme.task/title of: ?t is: ?title)
  (ticket ?t title: ?title))
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

<details><summary><code>todo-bare</code> (sha256 <code>c6cb9207319f6641</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (todo ?t title: ?title status: ?status)
  (xyz.todo/title of: ?t is: ?title as: :text pick: :last)
  (xyz.todo/status of: ?t is: ?status as: [:open :done] pick: :last))

(concept (issue ?i title: ?title state: ?state)
  (xyz.tracker/title of: ?i is: ?title as: :text pick: :last)
  (xyz.tracker/state of: ?i is: ?state as: :text pick: :last))

; A
(rule (todo ?i title: ?x status: :open)
  (issue ?i title: ?x state: "open"))

; B
(rule (update-title (todo ?t) old: ?old new: ?new)
  (rename (todo ?t) title: ?new)
  (xyz.todo/title of: ?t is: ?old))

; C
(rule (retract (xyz.todo/title of: ?t is: ?old))
  (update-title (todo ?t) old: ?old))

; D
(rule (assert (xyz.todo/title of: ?t is: ?new))
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

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text pick: :last)` reads the claims of one attribute, `xyz.todo/title`, of one entity, `?t`, and each claim's value binds `?title`. `as:` is the type a value must have. One attribute of one entity can hold several claims, and each `assert` adds one. `pick:` elects among them: `:last` reads the one caused most recently, whether it was stored or concluded by a rule, and `:all` reads every claim. Without a pick, every claim is read. A concluded value is as recent as the newest fact it was concluded from.

A concept is just a set of attributes: a convenient way to express derivations over those attributes more concisely.

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

<details><summary><code>list</code> (sha256 <code>13398b785f67bc46</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (todo ?i list: ?l title: ?t)
  (xyz.todo/title of: ?i is: ?t as: :text pick: :last)
  (xyz.list/task of: ?l is: ?i as: :entity pick: :last))

(assert (xyz.todo/title of: todo:a is: "Buy milk"))
(assert (xyz.todo/title of: todo:b is: "Call mom"))
(assert (xyz.list/task of: list:work is: todo:a))
(assert (xyz.list/task of: list:home is: todo:a))
(assert (xyz.list/task of: list:home is: todo:b))
```

Nothing is stored before the first `assert`, and the asserts run in the order written.

## Documentation

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text pick: :last)` reads the claims of one attribute, `xyz.todo/title`, of one entity, `?t`, and each claim's value binds `?title`. `as:` is the type a value must have. One attribute of one entity can hold several claims, and each `assert` adds one. `pick:` elects among them: `:last` reads the one caused most recently, whether it was stored or concluded by a rule, and `:all` reads every claim. Without a pick, every claim is read. A concluded value is as recent as the newest fact it was concluded from.

A concept is just a set of attributes: a convenient way to express derivations over those attributes more concisely. For example, given

```clj
(concept (titled ?t title: ?title)
  (xyz.acme.task/title of: ?t is: ?title))
```

the rule

```clj
(rule (titled ?t title: ?title)
  (ticket ?t title: ?title))
```

is the same as

```clj
(rule (xyz.acme.task/title of: ?t is: ?title)
  (ticket ?t title: ?title))
```

## Questions

1. What does `(query (todo ?i list: list:home))` return? List every row.
2. What does `(query (todo todo:a list: ?l))` return? List every row.
3. What does `pick: :last` on the `xyz.list/task` line do in this concept?
````

</details>

<details><summary><code>place-param</code> (sha256 <code>1d9e73df94791f5e</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (draft ?d text: ?x at: ?where)
  (xyz.draft/text of: ?d is: ?x as: :text pick: :last in: ?where))

(concept (note ?n text: ?x at: ?where)
  (xyz.note/text of: ?n is: ?x as: :text pick: :last in: ?where))

(rule (note ?d text: ?x)
  (draft ?d text: ?x))

(assert (draft draft:1 text: "half-written idea" at: :memory))
(assert (note note:1 text: "groceries" at: :replica))
```

Nothing is stored before the first `assert`.

## Documentation

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text pick: :last)` reads the claims of one attribute, `xyz.todo/title`, of one entity, `?t`, and each claim's value binds `?title`. `as:` is the type a value must have. One attribute of one entity can hold several claims, and each `assert` adds one. `pick:` elects among them: `:last` reads the one caused most recently, whether it was stored or concluded by a rule, and `:all` reads every claim. Without a pick, every claim is read. A concluded value is as recent as the newest fact it was concluded from.

A concept is just a set of attributes: a convenient way to express derivations over those attributes more concisely. For example, given

```clj
(concept (titled ?t title: ?title)
  (xyz.acme.task/title of: ?t is: ?title))
```

the rule

```clj
(rule (titled ?t title: ?title)
  (ticket ?t title: ?title))
```

is the same as

```clj
(rule (xyz.acme.task/title of: ?t is: ?title)
  (ticket ?t title: ?title))
```

`in:` names where a line's claims are kept: a read sees the claims kept there, and a write puts its claim there. A concept can take the place as a parameter, as `note` does with `at:`.

## Questions

1. What does `(query (note ?n text: ?x at: :replica))` give back? List every row.
2. What does `(query (note ?n text: ?x at: :memory))` give back? List every row.
3. Is the note text concluded for `draft:1` stored anywhere? If so, where?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>place-fixed</code> (sha256 <code>295f03eec291ac69</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (draft ?d text: ?x)
  (xyz.draft/text of: ?d is: ?x as: :text pick: :last in: :memory))

(concept (note ?n text: ?x at: ?where)
  (xyz.note/text of: ?n is: ?x as: :text pick: :last in: ?where))

(rule (note ?d text: ?x)
  (draft ?d text: ?x))

(assert (draft draft:1 text: "half-written idea"))
(assert (note note:1 text: "groceries" at: :replica))
```

Nothing is stored before the first `assert`.

## Documentation

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text pick: :last)` reads the claims of one attribute, `xyz.todo/title`, of one entity, `?t`, and each claim's value binds `?title`. `as:` is the type a value must have. One attribute of one entity can hold several claims, and each `assert` adds one. `pick:` elects among them: `:last` reads the one caused most recently, whether it was stored or concluded by a rule, and `:all` reads every claim. Without a pick, every claim is read. A concluded value is as recent as the newest fact it was concluded from.

A concept is just a set of attributes: a convenient way to express derivations over those attributes more concisely. For example, given

```clj
(concept (titled ?t title: ?title)
  (xyz.acme.task/title of: ?t is: ?title))
```

the rule

```clj
(rule (titled ?t title: ?title)
  (ticket ?t title: ?title))
```

is the same as

```clj
(rule (xyz.acme.task/title of: ?t is: ?title)
  (ticket ?t title: ?title))
```

`in:` names where a line's claims are kept: a read sees the claims kept there, and a write puts its claim there. A concept can take the place as a parameter, as `note` does with `at:`.

## Questions

1. What does `(query (note ?n text: ?x at: :replica))` give back? List every row.
2. What does `(query (note ?n text: ?x at: :memory))` give back? List every row.
3. Is the note text concluded for `draft:1` stored anywhere? If so, where?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

