# R31 plan: a domain call as an open-ended concept

Written and committed before any reader ran. The tally will be in `r31.md` beside it.

## Why

[R30](r30.md) showed readers take a domain call, `(xyz.todo ?t title: ?old)`, for the stored layer from its spelling, and a concept call for stored and derived values together. Rule B reading `(todo ?t title: ?old)` took the rename from 0 to 10 of 10. The engine stays as it is. It reads derived values through a domain call because a concept over a subset of another concept's fields has to see what a rule concludes for the larger one, and when it did not, everyone was surprised, the maintainer included.

So the model to give readers is that a domain call is a concept too: an open-ended one, duck typed, with no name and just the fields it lists, beside the well-typed concepts people define. R28's sentence said a domain call reads "stored or derived" values, which may have kept the two layers in place. This round says it is a concept, once in a sentence and once in an example of the subset case.

## Variants

All three are R27's `bare` document with rule B reading `xyz.todo`, which no reader has scored on without help.

| variant | readers | documentation |
|---|---|---|
| `open` | 10 | one sentence: a domain call such as `(xyz.todo ?t title: ?x)` is an open-ended concept, a concept with no name made of just the fields it lists, and it reads exactly what any concept over those fields reads |
| `subset` | 10 | a worked example: a rule derives a `book` from a `paper`, and a second concept, `titled`, over only the `xyz.book` title, returns the derived book. It shows nothing about a domain call, a rename or `last` |
| `baseline` | 10 | none (sha256 `b682e1b73bb7777e`) |

## Scoring

As in R30. **(a)** The rename lands: rule B reads the derived "Fix login", fires, and "Fix login bug" is stored as the to-do's title. **(b)** The to-do shows "Fix login bug" and the issue keeps "Fix login". (a) is the main item, since no variant says how `last` elects. Question 9 ("Login fails"), items 1 to 5, 7 and 10 are scored as before.

## Bars

`haiku`, read-only, variants interleaved; at most 20 run at once, so the last 10 start as the first finish.

- A variant works if at least 7 of 10 score (a). It is no help if 2 or fewer do.
- `baseline` is expected to score (a) at most once in 10, as R27, R28 and R30 scored it 0 in 35.

Prediction: `open` works. `subset` scores lower, since R29's example on a domain call did not transfer to rule B, and this one has to transfer one step further, from a named concept to a domain call.

## What the readers read

<details><summary><code>open</code> (sha256 <code>d31d6ef03e929cb5</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is one sentence: a domain call such as `(xyz.todo ?t title: ?x)` is an open-ended concept, a concept with no name made of just the fields it lists, and it reads exactly what any concept over those fields reads. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

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

<details><summary><code>subset</code> (sha256 <code>faf4f2eb24b1117c</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is this example, which is not part of the code below:

```clj
(concept (book ?b title: ?title author: ?author)
  (xyz.book ?b title: (last ?title as: :text)
               author: (last ?author as: :text)))

(concept (titled ?b title: ?title)
  (xyz.book ?b title: (last ?title as: :text)))

(concept (paper ?p title: ?title author: ?author)
  (xyz.paper ?p title: (last ?title as: :text)
                author: (last ?author as: :text)))

(rule (book ?p title: ?t author: ?a)
  (paper ?p title: ?t author: ?a))
```

With only `paper:3`'s title "On Datalog" and author "Ada" in the space, `(query (titled ?b title: ?t))` gives back `?b = paper:3, ?t = "On Datalog"`: `titled` reads only the `xyz.book` title, and it sees the book the rule concludes.

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

## Addendum, after the first 20 readers

Written after 20 of the 30 readers had answered and before anything was scored, because the maintainer asked whether readers were biased by memory or other data. They were exposed to it. Every reader agent in R17 to R31 started with this session's context:

- the maintainer's memory snapshot, which names Dialog DB and a project of "Dialog rules";
- this repository's `.claude/rules`;
- the instructions of the session's MCP servers, among them the Tonk connector's, which describe concept heads and domain heads.

No reader read a memory file or called anything but Read, and every variant in a round saw the same context. So comparisons within a round stand. The levels may not: the Tonk text in particular may push readers to treat a domain call as a lower layer.

So the round changes runner. The 20 answers already in are kept and reported as the `agent` runner. Then all three variants run 10 readers each again, as the `clean` runner:

- headless `claude -p --model haiku`;
- the system prompt replaced by "You are a careful reader.";
- no MCP servers, no settings sources, no tools;
- the document given as the prompt itself.

A probe of this runner reported no memory, no Dialog, Tonk or maintainer context. It still carries the harness's user-email line and one browser connector's instructions. The bars apply to the `clean` runner. R30's `concept` and `baseline` variants are also rerun on the `clean` runner, 10 each, with R30's bars, to check whether R30's result came from the context.
