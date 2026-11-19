# R35 plan: `message` and `audience` left undefined

Written and committed before any reader ran. The tally will be in `r35.md` beside it. Clean runner, single turn, R32's questions, 20 readers per variant as [r34](r34.md) concluded.

## Why

In R33 and R34, `note` and `memo` were built from `message` and `audience`, defined either as one-attribute concepts over `xyz.tonk.post` or as attribute declarations. A rule's memo was found as a note 14 times in 30, against none in 20 when `note` and `memo` read `xyz.tonk.post` directly (R32). The maintainer asks whether defining `message` and `audience` helps at all, or makes it worse: this round removes their definitions and keeps everything else.

## Variants

| variant | readers | code |
|---|---|---|
| `undef-assert` | 20 | the maintainer's: `note` and `memo` over `message` and `audience`, nothing defining those; assert a memo, query notes |
| `undef-derive` | 20 | R33's `derive` without the `message` and `audience` definitions: a rule concludes `memo` from an `email` over `xyz.mail`; query notes |

## Scoring and bars

Question 2 scores if the query gives back the memo's entity with "hello world". A variant works if at least 14 of 20 score; it is no help if 4 or fewer do.

Predictions:

- `undef-assert` near the ceiling, as every assert variant has been.
- `undef-derive` at least as high as the defined versions' half: with nothing below `message`, there is no domain form to read as storage, so readers chain by name. I expect 10 to 16 of 20.

If `undef-derive` scores higher than the defined versions, the definitions are what pulls readers back to a stored layer.

## What the readers read

<details><summary><code>undef-assert</code> (sha256 <code>ce099a847c62f557</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (note ?note message: ?message)
  (message ?note is: ?message))

(concept (memo ?memo message: ?message to: ?to)
  (message ?memo is: ?message)
  (audience ?memo is: ?to))

(assert (memo test:one message: "hello world" to: mailto:alice@web.mail))
(query (note ?note message: ?msg))
```

Nothing is stored before the `assert`.

## Questions

1. What does the `assert` form store? List each stored fact as entity, attribute, value.
2. What does the `query` give back? Why?
3. In your own words, how does a concept such as `memo` relate to the forms inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>undef-derive</code> (sha256 <code>ccc5237cb539d384</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (note ?note message: ?message)
  (message ?note is: ?message))

(concept (memo ?memo message: ?message to: ?to)
  (message ?memo is: ?message)
  (audience ?memo is: ?to))

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
2. What does the `query` give back? Why?
3. In your own words, how does a concept such as `memo` relate to the forms inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>
