# R30 plan: is it the domain spelling, or the facts table?

Written and committed before any reader ran. The tally will be in `r30.md` beside it.

## Why

In R27 to R29, every reader who said renaming a derived to-do does nothing followed one script: look up the prompt's table of stored facts, find no `xyz.todo/title` line for `issue:7`, conclude that rule B's `(xyz.todo ?t title: ?old)` cannot match. No documentation tried displaced it reliably ([r29](r29.md)). Yet no reader has ever missed that a concept call, `(todo ...)`, returns derived values. Two explanations fit:

- **The spelling.** The `xyz.` prefix reads as the stored layer, so a domain call reads as stored only, whatever the documentation says.
- **The table.** The prompt lists stored facts under the very attribute names rule B uses, and readers check B against it.

This round separates them. It adds no documentation: every variant is R27's `bare` document, which readers get wrong.

## Variants

| variant | readers | change to R27's `bare` document |
|---|---|---|
| `concept` | 10 | rule B's second condition reads through the concept: `(todo ?t title: ?old)` instead of `(xyz.todo ?t title: ?old)` |
| `prose` | 10 | the stored facts are one sentence of prose, naming no attribute, instead of the table |
| `baseline` | 10 | unchanged (sha256 `b682e1b73bb7777e`) |

Changing rule B does not change what dialog does: the `todo` concept reads `xyz.todo/title` with `last`, so B binds the derived "Fix login" either way.

## Scoring

As in R29, question 8 is scored in two parts. **(a)** The rename lands: rule B reads the derived "Fix login", fires, and "Fix login bug" is stored as the to-do's title. **(b)** The to-do shows "Fix login bug" and the issue keeps "Fix login". No variant says how `last` elects between a stored and a derived value, so (a) is the main item and (b) is reported. Question 9 ("Login fails"), items 1 to 5, 7 and 10 are scored as before.

## Bars

`haiku`, read-only, variants interleaved; at most 20 run at once, so the last 10 start as the first finish.

- A variant works if at least 7 of 10 score (a). It is no help if 2 or fewer do.
- `baseline` is expected to score (a) at most once in 10: R27 scored it 0 in 20 and R28's control 0 in 5. If it scores more, that is reported first.

Prediction: `concept` works and `prose` is no help. If both work, the table was the anchor; if neither does, readers doubt that a rule's conclusion is ever read by another rule's condition.

## What the readers read

<details><summary><code>concept</code> (sha256 <code>04f991ff52825319</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

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
  (todo ?t title: ?old))

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

<details><summary><code>prose</code> (sha256 <code>dba1a4db5557644d</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

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

What is stored: the to-do `todo:1` has the title "Buy milk" and the status `:open`, and the issue `issue:7` has the title "Fix login" and the state "open". Nothing else is stored.

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

<details><summary><code>baseline</code> (sha256 <code>b682e1b73bb7777e</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

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
