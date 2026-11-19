# R29 plan: state the skipped step, or show it

Written and committed before any reader ran. The tally will be in `r29.md` beside it.

## Why

In [R28](r28.md) one sentence saying that a domain call reads stored and derived values did not help: 2 of 10 readers saw that renaming a derived to-do lands, against 0 of 5 without it. Readers skipped a different step. Rule A concludes a `todo`, and the `todo` concept maps `title` to `xyz.todo/title`, so rule A's title is a value of `xyz.todo/title` that rule B reads. The two who scored took that step themselves, then asked how `last` elects between a stored and a derived value. This round tests two ways of giving readers that step: saying it, and showing it.

## Variants

| variant | readers | documentation, otherwise R28's document |
|---|---|---|
| `steps` | 10 | two sentences: a rule that concludes a concept gives values to that concept's domain fields, just as storing them would; `last` picks the value caused most recently, stored or derived, and a derived value is as recent as the newest fact it was derived from |
| `example` | 10 | a worked example on other concepts (a rule deriving a `book` from a `paper`), where a domain call `(xyz.book ...)` returns the derived title. It shows nothing about a rename or about `last` |
| `baseline` | 10 | R28's `stated` document, unchanged (sha256 `8b65e1339e9ad87d`) |

`steps` gives both the step and the election rule; `example` gives only the step. That difference is deliberate: an example that showed a stored value beating a derived one would answer question 8 by analogy. It means `example` can be judged on part (a) below and not fairly on the whole of question 8.

## Scoring

Question 8 is scored in two parts.

- **(a) The rename lands:** the reader says rule B reads the derived title "Fix login", fires, and "Fix login bug" is stored as `xyz.todo/title` for `issue:7`.
- **(b) The whole of question 8,** as in R28: the to-do `issue:7` shows "Fix login bug", and the issue keeps "Fix login".

A reader who scores (b) without (a) is reported, not counted. Question 9's expected answer is "Login fails", now for dialog's reason: the issue's change is newer than the stored "Fix login bug". Items 1 to 5, 7 and 10 are scored as in R27.

## Bars

`haiku`, read-only, all 30 started at once, variants interleaved.

- `steps` works if at least 7 of 10 score (b). It is no help if 2 or fewer do.
- `example` works if at least 7 of 10 score (a). It is no help if 2 or fewer do.
- `baseline` is expected to score (b) at most 3 times in 10, as R28's `stated` scored it 2. If it scores more, R28's result does not hold on the day, and that is reported before anything else.

Prediction: `steps` passes on (b); `example` passes on (a) and scores (b) less often.

## What the readers read

<details><summary><code>steps</code> (sha256 <code>087f852bcf45f5a7</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is two sentences. A rule that concludes a concept gives values to that concept's domain fields, just as storing them would. When a field has several values, `last` picks the one caused most recently, whether it was stored or derived, and a derived value is as recent as the newest fact it was derived from. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

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

<details><summary><code>example</code> (sha256 <code>cbbd12f026181eee</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is this example, which is not part of the code below:

```clj
(concept (book ?b title: ?title)
  (xyz.book ?b title: (last ?title as: :text)))

(concept (paper ?p title: ?title)
  (xyz.paper ?p title: (last ?title as: :text)))

(rule (book ?p title: ?t)
  (paper ?p title: ?t))
```

With only `paper:3  xyz.paper/title  "On Datalog"` stored, `(query (xyz.book ?b title: ?t))` gives back `?b = paper:3, ?t = "On Datalog"`: the book the rule concludes is read through `xyz.book` like a stored one.

Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

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

<details><summary><code>baseline</code> (sha256 <code>8b65e1339e9ad87d</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is one sentence: a domain call such as `(xyz.todo ?t title: ?x)` reads every value of that field, whether it was stored or derived by a rule. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

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
