# R27 plan: rules whose first form is the conclusion, read cold

Written and committed before any reader ran. The tally will be in `r27.md` beside it.

## Why

The maintainer proposes rules without `when`: `(rule conclusion condition…)`, the first form the conclusion and the rest its conditions. A rule with several effects derives one directive and lets one rule per effect react to it, so no rule needs two conclusions. `assert` and `retract` are the conclusions that change state; a bare concept conclusion, such as a to-do derived from an issue, is a deduction that stores nothing. In another agent's trial, readers scored the same with and without `when`. This round checks that with the rules as they now look, and adds what the maintainer wants to see understood: a derived to-do, what renaming it does, and a rule written from a description.

## Variants

| variant | rules |
|---|---|
| `bare` | `(rule conclusion condition…)` |
| `when` | `(rule conclusion (when condition…))` |

Everything else is the same: two concepts, `todo` and `issue`; four rules (A derives a to-do from every open issue; B turns a `rename` into an `update-title` that captures the old and new title; C retracts the old title; D asserts the new one); four stored facts. No documentation is given.

## Scoring

Seven points, from the first answer only:

| item | point when the reader says |
|---|---|
| 1 | in all four rules, the first form is the conclusion and the rest are its conditions |
| 2 | A makes every open issue a to-do with the issue's title, and that to-do is not stored |
| 3 | `todo:1` "Buy milk" and `issue:7` "Fix login" |
| 4 | only `todo:1` |
| 5 | B derives `update-title` with old "Buy milk" and new "Buy oat milk", C retracts the old title, D asserts the new one, and `update-title` is not stored |
| 7 | "Buy oat milk" only; "Buy milk" is no longer stored |
| 10 | rules in the form shown (in `bare`, no `when`) that retract the note's text and assert it as `xyz.archive/text`, the text bound from the note |

Reported, not scored, because the code alone cannot settle them:

- 6: whether the retract and assert take effect in the same step or later. The design says the next tick.
- 8: what renaming the derived to-do does. The maintainer expects the to-do to show "Fix login bug", since the stored title is newer than the issue fact the to-do was derived from, and the issue to keep "Fix login".
- 9: what a later change to the issue's title does. By dialog's code the derived title is then the newest and wins: "Login fails".
- 10, in `bare`: how many readers put two conclusions in one rule, which the `bare` form cannot tell from a conclusion and a condition.
- 11: the rating and the hardest thing to guess.

## Bars

Ten readers per variant, `haiku`, read-only, started at once, interleaved.

- Items 1 and 10 are the ones the form can change, 20 points per variant. `bare` reads as well as `when` if its readers score no more than 2 fewer of those points, and no more than 4 fewer of all 70.
- Otherwise `when` is doing work that `bare` loses, and the result says which item.

Predictions, reported against what happens:

- Item 1 is the same in both variants.
- Some `bare` readers write the archive as one rule with two conclusions.

## What the readers read

<details><summary><code>bare</code> (sha256 <code>b682e1b73bb7777e</code>)</summary>

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

<details><summary><code>when</code> (sha256 <code>69a359b7600155be</code>)</summary>

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
  (when (issue ?i title: ?x state: "open")))

; B
(rule (update-title (todo ?t) old: ?old new: ?new)
  (when (rename (todo ?t) title: ?new)
        (xyz.todo ?t title: ?old)))

; C
(rule (retract (xyz.todo ?t title: ?old))
  (when (update-title (todo ?t) old: ?old)))

; D
(rule (assert (xyz.todo ?t title: ?new))
  (when (update-title (todo ?t) new: ?new)))
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
