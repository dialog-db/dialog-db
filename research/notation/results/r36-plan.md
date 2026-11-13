# R36 plan: "a rule that concludes a concept concludes each attribute it is made of"

Written and committed before any reader ran. The tally will be in `r36.md` beside it. Clean runner, single turn, R32's questions, 20 readers per variant.

## Why

On the derive case, a rule concludes `memo` and the query asks for `note`. Readers found the memo as a note:

- none of 20 times when `note` and `memo` read `xyz.tonk.post` directly (R32);
- 25 of 50 times when they share a name below them, `message` (R33 to R35).

The misses in the shared-name form take a rule as reached only through its head's name. Every round's misses take a rule's conclusion as never reaching the attributes. The maintainer's sentence for it: a rule that concludes a concept concludes each individual attribute that concept is made of.

## Variants

Both are the derive case: an `email` over `xyz.mail` is asserted, a rule concludes `memo` from it, and `note` is queried.

| variant | readers | base | without the sentence |
|---|---|---|---|
| `domain-sentence` | 20 | R32's `derive-bare`: `note` and `memo` read `xyz.tonk.post` directly | 0 of 20 (R32) |
| `shared-sentence` | 20 | R35's `undef-derive`: `note` and `memo` built from `message` and `audience`, which are not defined | 11 of 20 (R35) |

The only change to each base is the sentence, as the documentation.

## Scoring and bars

Question 2 scores if the query gives back `mail:one` with "hello world". A variant works if at least 14 of 20 score; it is no help if 4 or fewer do.

Prediction:

- `shared-sentence` works: the sentence answers what the shared-name misses said.
- `domain-sentence` lands between the bars, since readers there also take `xyz.tonk.post` for storage that a rule does not write.

If both work, the sentence is the fix and the shared names are optional.

## What the readers read

<details><summary><code>domain-sentence</code> (sha256 <code>836e6096181fb03a</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is one sentence: a rule that concludes a concept concludes each individual attribute that concept is made of. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (note ?note message: ?message)
  (xyz.tonk.post ?note
    content: (:last (:text ?message))))

(concept (memo ?memo message: ?message to: ?to)
  (xyz.tonk.post ?memo
    content: (:last (:text ?message))
    audience: (:last (:entity ?to))))

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
3. In your own words, how does a concept such as `memo` relate to the `xyz.tonk.post` form inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>shared-sentence</code> (sha256 <code>cc1e003a53261dfe</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is one sentence: a rule that concludes a concept concludes each individual attribute that concept is made of. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

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
