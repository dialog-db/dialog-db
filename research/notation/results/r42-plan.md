# R42 plan: a line as a claim, both ways

Written and committed before any reader ran. The tally will be in `r42.md` beside it. Clean runner, single turn, 20 readers per document.

## Why

In [R41](r41.md) the note slipped from 20 of 20 (R39) to 13 of 20. The 7 misses matched by name ("concludes `memo`, not `note`") and several called a concept "a view" whose lines "describe what it reads". R41's spec introduced a line as something that "reads the claims of one attribute".

## Documents

R41's `note` and `todo`, with the first sentences of the line paragraph replaced:

> Each line such as `(xyz.todo/title of: ?t is: ?title as: :text pick: :last)` is a claim: the attribute `xyz.todo/title` of the entity `?t` is `?title`. When a concept is asked for, its lines are read. When a concept is concluded by a rule or asserted, its lines are what is concluded or asserted.

The rest of the documentation, the code and the questions are unchanged.

| document | readers |
|---|---|
| `note-claim` | 20 |
| `todo-claim` | 20 |

## Scoring and bars

As R41: `note` question 2 on "yes"; `todo` (a) the rename lands. At least 14 of 20 reads right, 4 or fewer misleads.

Predictions:

- `note-claim` reads right, at 18 or more.
- `todo-claim` stays at 18 or more.

## What the readers read

<details><summary><code>note-claim</code> (sha256 <code>66cad51198fe4950</code>)</summary>

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

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text pick: :last)` is a claim: the attribute `xyz.todo/title` of the entity `?t` is `?title`. When a concept is asked for, its lines are read. When a concept is concluded by a rule or asserted, its lines are what is concluded or asserted. `as:` is the type a value must have. One attribute of one entity can hold several claims, and each `assert` adds one. `pick:` elects among them: `:last` reads the one caused most recently, whether it was stored or concluded by a rule, and `:all` reads every claim. Without a pick, every claim is read. A concluded value is as recent as the newest fact it was concluded from.

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

<details><summary><code>todo-claim</code> (sha256 <code>0d09d251228d275a</code>)</summary>

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

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text pick: :last)` is a claim: the attribute `xyz.todo/title` of the entity `?t` is `?title`. When a concept is asked for, its lines are read. When a concept is concluded by a rule or asserted, its lines are what is concluded or asserted. `as:` is the type a value must have. One attribute of one entity can hold several claims, and each `assert` adds one. `pick:` elects among them: `:last` reads the one caused most recently, whether it was stored or concluded by a rule, and `:all` reads every claim. Without a pick, every claim is read. A concluded value is as recent as the newest fact it was concluded from.

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

