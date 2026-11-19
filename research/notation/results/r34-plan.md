# R34 plan: one-attribute concepts against attribute declarations

Written and committed before any reader ran. The tally will be in `r34.md` beside it. Clean runner, single turn, R32's questions.

## Why

In [R33](r33.md), with `message` and `audience` as one-attribute concepts, 7 of 10 readers found a rule-concluded memo as a note, against 0 of 10 in R32. Readers chained through named concepts. The maintainer proposes spelling `message` and `audience` as attribute declarations instead, keeping `note` and `memo` as they are:

```clj
(attribute message
  the: xyz.tonk.post/content
  as: :text
  pick: :last)
```

This round asks whether the declaration keeps the chain, or whether "attribute" reads as storage and breaks it. It also replicates R33's `derive`, which sat exactly on the bar.

## Variants

| variant | readers | code |
|---|---|---|
| `concept-derive` | 10 | R33's `derive`, unchanged (sha256 `8caa5d1f539effcf`) |
| `attr-derive` | 10 | the same, with `message` and `audience` as `attribute` declarations |
| `attr-assert` | 10 | R33's `assert`, with the same declarations |

## Scoring and bars

Question 2 scores if the query gives back the memo's entity with "hello world". A variant works if at least 7 of 10 score; it is no help if 2 or fewer do.

Predictions:

- `concept-derive` repeats R33, 6 to 8 of 10.
- `attr-derive` falls: 2 or fewer. "attribute" reads as a stored field, as the domain form did, so a rule's memo stays in its own layer.
- `attr-assert` stays at the ceiling, as every assert variant has.

If `attr-derive` matches `concept-derive`, what carries the conclusion is the shared name, not the word `concept`.

## What the readers read

<details><summary><code>concept-derive</code> (sha256 <code>8caa5d1f539effcf</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (message ?of is: ?is)
  (xyz.tonk.post ?of content: (:last (:text ?is))))

(concept (audience ?of is: ?is)
  (xyz.tonk.post ?of audience: (:last (:entity ?is))))

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

<details><summary><code>attr-derive</code> (sha256 <code>bbb41381db09c9d3</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(attribute message
  the: xyz.tonk.post/content
  as: :text
  pick: :last)

(attribute audience
  the: xyz.tonk.post/audience
  as: :entity
  pick: :last)

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

<details><summary><code>attr-assert</code> (sha256 <code>3e90e49a3c71b636</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(attribute message
  the: xyz.tonk.post/content
  as: :text
  pick: :last)

(attribute audience
  the: xyz.tonk.post/audience
  as: :entity
  pick: :last)

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
