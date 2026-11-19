# R33 plan: concepts built from one-attribute concepts

Written and committed before any reader ran. The tally will be in `r33.md` beside it. Clean runner, single turn.

## Why

In [R32](r32.md) readers found an asserted memo as a note every time (20 of 20) and a rule-concluded memo never (0 of 20). They also read `(xyz.tonk.post ?m content: ...)` as a record with a local field. The maintainer's idea: give every attribute its own concept, `message` and `audience`, and build `note` and `memo` from those. The sharing then shows in the code: `note` is `message`, and `memo` is `message` plus `audience`.

## Variants

Both use the maintainer's code: `message` and `audience` over `xyz.tonk.post`, then `note` and `memo` built from them.

| variant | readers | then |
|---|---|---|
| `assert` | 10 | the maintainer's: assert a `memo`, query `note` |
| `derive` | 10 | as R32's `derive`: a rule concludes `memo` from an `email` over `xyz.mail`; assert an email, query `note` |

The questions are R32's turn 1.

## Scoring and bars

Question 2 scores if the query gives back the memo's entity with "hello world". A variant works if at least 7 of 10 score; it is no help if 2 or fewer do. The comparison is R32's `assert-bare` (10 of 10) and `derive-bare` (0 of 10), whose code differs only in `note` and `memo` reading `xyz.tonk.post` directly.

Prediction:

- `assert` stays at the ceiling.
- `derive` lands between the bars. Readers chain predicates freely, and 39 of 40 in R32 said a concept is two-way. So with `memo` defined as `message` and `audience`, some will read a concluded memo as implying `message`, which `note` reads. Others will keep the rule's memo in its own derived layer.

## What the readers read

<details><summary><code>assert</code> (sha256 <code>08330e016623d664</code>)</summary>

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

<details><summary><code>derive</code> (sha256 <code>8caa5d1f539effcf</code>)</summary>

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
