# R37 plan: ask directly whether the note is found

Written and committed before any reader ran. The tally will be in `r37.md` beside it. Clean runner, single turn, 20 readers.

## Why

On the rule case of [R35](r35.md), 11 of 20 readers found the rule's memo as a note when asked "what does the `query` give back?". Many of the misses hedged, giving the empty answer as the plain reading and the memo as the alternative. The maintainer asks what readers say when asked directly: will a note with the message "hello world" be found, yes or no, and why.

## Variant

| variant | readers | document |
|---|---|---|
| `direct` | 20 | R35's `undef-derive` (sha256 `ccc5237cb539d384`), with question 2 replaced by: Will the `query` find a note with the message "hello world"? Answer yes or no, then explain why. |

## Scoring and bars

Question 2 scores on "yes". The comparison is R35's `undef-derive`, 11 of 20 on the open question. The reasons are grouped:

- through the rule's conclusion and the shared `message`;
- no, because the rule concludes `memo` and not `note`;
- no, because rules run only when their head is asked for;
- other.

Prediction: about the same as R35, 9 to 13 of 20. A forced yes or no turns the hedges into whichever reading the reader leans to, and in R35 the hedges leaned both ways.

## What the readers read

<details><summary><code>direct</code> (sha256 <code>9b7711835da9e2dd</code>)</summary>

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
2. Will the `query` find a note with the message "hello world"? Answer yes or no, then explain why.
3. In your own words, how does a concept such as `memo` relate to the forms inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>
