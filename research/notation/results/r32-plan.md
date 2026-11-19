# R32 plan: do readers take a concept for a one-way view or a two-way lens?

Written and committed before any reader ran. The tally will be in `r32.md` beside it. Clean runner, as set in [r31](r31.md).

## Why

In R27 to R31, readers said a rule's to-do never reaches `xyz.todo`. In R31, 18 of the 50 clean readers called a concept a "view", and 16 said outright that nothing is written to `xyz.todo`. The maintainer's hypothesis: models read a concept as a Datalog-style rule, a one-way view computed from its domain, rather than as a bidirectional lens. In a one-way view, a rule's conclusion lands in the view and no other reader of the domain sees it. In a lens, concluding a concept gives values to its domain's attributes. The maintainer's [anatomy of Dialog](https://gozala.io/dialog/anatomy), which describes a concept as a bidirectional lens onto an associative layer of claims, made the model click for many people.

The maintainer's test: two concepts, `note` and `memo`, over the same `xyz.tonk.post/content`. Assert a memo and query notes. Is the memo found as a note?

A caveat, from the earlier rounds: readers already grant that asserting through a concept writes to its domain, since rules C and D did. So asserting a memo may not separate the two models: a one-way reader can still answer that the memo's content is stored and the note query finds it. What separates them is a memo that a rule concludes. This round runs both.

## Variants

| variant | readers | code | documentation |
|---|---|---|---|
| `assert-bare` | 10 | the maintainer's: assert a `memo`, query `note` | none |
| `assert-spec` | 10 | same | a paraphrase of the anatomy's account of claims, layers and concepts |
| `derive-bare` | 10 | a rule concludes `memo` from an `email` over `xyz.mail`; assert an email, query `note` | none |
| `derive-spec` | 10 | same | the same paraphrase |

The paraphrase is in my words, since the essay itself could not be fetched verbatim. It keeps only what the essay says of claims, layers and concepts: a concept is a bidirectional lens onto the associative layer, querying it composes claims and asserting it decomposes into claims. It says nothing about rules or about two concepts sharing an attribute, which the essay does not say either.

The maintainer's code wrote `(xyz.tonk.post ?note ...)` inside `memo`, whose head binds `?memo`. Readers get `?memo`, so that the question is the mapping and not an unbound variable.

## Procedure

Each reader is a clean `claude -p` session, as in R31. Turn 1 is the document. Turn 2, in the same session, reveals dialog's answer: the query gives back the memo, because a concept is a two-way mapping onto its domain's attributes, so asserting a memo, or a rule concluding one, gives values to `xyz.tonk.post/content` that `note` reads. It then asks whether the reader expected that, and what in the notation or its documentation would have made it obvious, with any notation change shown as code.

## Scoring

- **Main item, question 2:** the query gives back the memo's entity (`test:one`, or `mail:one`) with "hello world".
- **Question 3:** classified as two-way, one-way, or other.
- **Turn 2:** the suggestions are grouped by kind and counted; no score.

## Bars

- A variant works if at least 7 of 10 score question 2. It is no help if 2 or fewer do.

Predictions:

- `assert-bare` works, since readers grant that an assert writes through.
- `derive-bare` is no help, as in R27 to R31.
- `derive-spec` works only if "bidirectional lens" carries over from an assert to a rule's conclusion. I expect it lands between the bars.
- On question 3, most `bare` readers say one-way and most `spec` readers say two-way.

If `derive-spec` scores no better than `derive-bare`, the lens framing teaches the assert direction and not the rule direction. The documentation would then have to say what a rule's conclusion is.

## What the readers read

<details><summary><code>assert-bare</code> (sha256 <code>508f32ee6740e382</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (note ?note message: ?message)
  (xyz.tonk.post ?note
    content: (:last (:text ?message))))

(concept (memo ?memo message: ?message to: ?to)
  (xyz.tonk.post ?memo
    content: (:last (:text ?message))
    audience: (:last (:entity ?to))))

(assert (memo test:one message: "hello world" to: mailto:alice@web.mail))
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

<details><summary><code>assert-spec</code> (sha256 <code>f7f6030411f017ed</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (note ?note message: ?message)
  (xyz.tonk.post ?note
    content: (:last (:text ?message))))

(concept (memo ?memo message: ?message to: ?to)
  (xyz.tonk.post ?memo
    content: (:last (:text ?message))
    audience: (:last (:entity ?to))))

(assert (memo test:one message: "hello world" to: mailto:alice@web.mail))
(query (note ?note message: ?msg))
```

Nothing is stored before the `assert`.

## Documentation

Every fact is kept as a claim: an entity, an attribute such as `xyz.tonk.post/content`, and a value. All claims together are the associative layer, the memory. Above it sits the semantic layer, where a concept names a set of attributes that share an entity, much like a type that is applied when data is read. A concept is a bidirectional lens onto the associative layer: querying a concept composes the claims that match into a result, and asserting a concept decomposes it into claims.

## Questions

1. What does the `assert` form store? List each stored fact as entity, attribute, value.
2. What does the `query` give back? Why?
3. In your own words, how does a concept such as `memo` relate to the `xyz.tonk.post` form inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>derive-bare</code> (sha256 <code>dd61107cf3ac62ab</code>)</summary>

````text
Below is code in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

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

<details><summary><code>derive-spec</code> (sha256 <code>4036952edd592c47</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

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

## Documentation

Every fact is kept as a claim: an entity, an attribute such as `xyz.tonk.post/content`, and a value. All claims together are the associative layer, the memory. Above it sits the semantic layer, where a concept names a set of attributes that share an entity, much like a type that is applied when data is read. A concept is a bidirectional lens onto the associative layer: querying a concept composes the claims that match into a result, and asserting a concept decomposes it into claims.

## Questions

1. What does the `assert` form store? List each stored fact as entity, attribute, value.
2. What does the `query` give back? Why?
3. In your own words, how does a concept such as `memo` relate to the `xyz.tonk.post` form inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary>turn 2 for <code>assert-*</code> (sha256 <code>3b595160559ec23c</code>)</summary>

````text
In dialog, the query gives back `?note = test:one, ?msg = "hello world"`. A concept is a two-way mapping onto the attributes of its domain: asserting a memo gives values to `xyz.tonk.post/content` and `xyz.tonk.post/audience` on `test:one`, and any concept over `xyz.tonk.post/content`, `note` included, reads them.

1. Did you expect this? Say which of your answers it changes.
2. What in the notation or its documentation would have made this obvious to you? Name concrete changes, most important first, and show any notation change as code.
````

</details>

<details><summary>turn 2 for <code>derive-*</code> (sha256 <code>8eb909dbd9d172a4</code>)</summary>

````text
In dialog, the query gives back `?note = mail:one, ?msg = "hello world"`. A concept is a two-way mapping onto the attributes of its domain: a rule that concludes a memo gives values to `xyz.tonk.post/content` and `xyz.tonk.post/audience` on `mail:one`, and any concept over `xyz.tonk.post/content`, `note` included, reads them.

1. Did you expect this? Say which of your answers it changes.
2. What in the notation or its documentation would have made this obvious to you? Name concrete changes, most important first, and show any notation change as code.
````

</details>
