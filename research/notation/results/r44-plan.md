# R44 plan: a write with no `in:` goes to the primary

Written and committed before any reader ran. The tally will be in `r44.md` beside it. Clean runner, single turn, 20 readers per document.

## Why

The layers work (`notes/layers.md` on `claude/layers-model`, after `ec9c86fb`) and the maintainer settled what an omitted `in:` means:

- a read with no `in:` reads every layer at once, the composite;
- a write with no `in:` goes to the control layer if it is a command, and to the primary otherwise. A retraction too: like erasing on one layer of an image, a claim on another layer stays visible.

So a reader who retracts the title they see, when it sits on `:session`, removes nothing they see. That is the expectation to check.

## Documents

A `todo` concept with `at: ?where` as its line's `in:`. `todo:1` has a title on the primary; `todo:2` has "Call mom" on the primary and a newer "Call mom tonight" on `:session`. Documentation: R42's line paragraph, framing and example, then a paragraph on `in:` (constant or field; a read with `at:` bound reads that layer, left out reads every layer and the pick elects over all; the primary is stored and synced, `:session` is memory until the session ends).

| document | adds to the `in:` paragraph | readers |
|---|---|---|
| `bare` | nothing about writes | 20 |
| `rule` | "A write with `at:` left out goes to the primary, an assert and a retraction alike." | 20 |
| `image` | `rule`, plus "As in an image editor, erasing on one layer leaves whatever another layer shows." | 20 |

## Scoring and bars

1. Question 1 scores on `todo:1` "Buy milk" and `todo:2` "Call mom tonight".
2. Question 2 scores on `todo:2` "Call mom tonight" alone.
3. Question 3 scores on `todo:2` still showing "Call mom tonight": the retraction went to the primary, which holds no such claim.
4. Question 4 scores on a retraction with `at: :session`.

A document reads right if at least 14 of 20 score on question 3, and misleads if 4 or fewer do.

Predictions:

- Question 1 at 16 or more on every document.
- `bare` misleads on question 3: readers retract what they see.
- `rule` reads right on question 3.
- `image` scores at least as well as `rule`.

## What the readers read

<details><summary><code>bare</code> (sha256 <code>dc7d077b23f5300a</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (todo ?i title: ?t at: ?where)
  (xyz.todo/title of: ?i is: ?t as: :text pick: :last in: ?where))

(assert (todo todo:1 title: "Buy milk"))
(assert (todo todo:2 title: "Call mom"))
(assert (todo todo:2 title: "Call mom tonight" at: :session))
```

Nothing is stored before the first `assert`, and the asserts run in the order written, in one session.

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

`in:` names the layer a line's claims are on. It may be a constant, or a field of the concept, as `at:` is here. A read with `at:` bound reads that layer alone. A read with `at:` left out reads every layer at once, and `pick:` elects among the claims of all of them. The primary layer is stored and synced. The `:session` layer is kept in memory until the session ends.

## Questions

1. What does `(query (todo ?i title: ?t))` give back? List every row.
2. What does `(query (todo ?i title: ?t at: :session))` give back? List every row.
3. A document now holds `(retract (todo todo:2 title: "Call mom tonight"))`. Afterwards, what does the query from question 1 give back for `todo:2`?
4. Write a document that makes `todo:2` show no title "Call mom tonight" in the query from question 1.
5. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>rule</code> (sha256 <code>bcf9269ddbb238d0</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (todo ?i title: ?t at: ?where)
  (xyz.todo/title of: ?i is: ?t as: :text pick: :last in: ?where))

(assert (todo todo:1 title: "Buy milk"))
(assert (todo todo:2 title: "Call mom"))
(assert (todo todo:2 title: "Call mom tonight" at: :session))
```

Nothing is stored before the first `assert`, and the asserts run in the order written, in one session.

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

`in:` names the layer a line's claims are on. It may be a constant, or a field of the concept, as `at:` is here. A read with `at:` bound reads that layer alone. A read with `at:` left out reads every layer at once, and `pick:` elects among the claims of all of them. The primary layer is stored and synced. The `:session` layer is kept in memory until the session ends. A write with `at:` left out goes to the primary, an assert and a retraction alike.

## Questions

1. What does `(query (todo ?i title: ?t))` give back? List every row.
2. What does `(query (todo ?i title: ?t at: :session))` give back? List every row.
3. A document now holds `(retract (todo todo:2 title: "Call mom tonight"))`. Afterwards, what does the query from question 1 give back for `todo:2`?
4. Write a document that makes `todo:2` show no title "Call mom tonight" in the query from question 1.
5. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>image</code> (sha256 <code>9b19debe9f5b89ce</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (todo ?i title: ?t at: ?where)
  (xyz.todo/title of: ?i is: ?t as: :text pick: :last in: ?where))

(assert (todo todo:1 title: "Buy milk"))
(assert (todo todo:2 title: "Call mom"))
(assert (todo todo:2 title: "Call mom tonight" at: :session))
```

Nothing is stored before the first `assert`, and the asserts run in the order written, in one session.

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

`in:` names the layer a line's claims are on. It may be a constant, or a field of the concept, as `at:` is here. A read with `at:` bound reads that layer alone. A read with `at:` left out reads every layer at once, and `pick:` elects among the claims of all of them. The primary layer is stored and synced. The `:session` layer is kept in memory until the session ends. A write with `at:` left out goes to the primary, an assert and a retraction alike. As in an image editor, erasing on one layer leaves whatever another layer shows.

## Questions

1. What does `(query (todo ?i title: ?t))` give back? List every row.
2. What does `(query (todo ?i title: ?t at: :session))` give back? List every row.
3. A document now holds `(retract (todo todo:2 title: "Call mom tonight"))`. Afterwards, what does the query from question 1 give back for `todo:2`?
4. Write a document that makes `todo:2` show no title "Call mom tonight" in the query from question 1.
5. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

