# R43 plan: `in:` as #795 built it

Written and committed before any reader ran. The tally will be in `r43.md` beside it. Clean runner, single turn, 20 readers per document.

## Why

R41's `in:` documents tested a model of places I proposed, not the one [#795](https://github.com/dialog-db/dialog-db/pull/795) built ([notes/layers.md](../../../notes/layers.md) on its branch). As built:

- `in:` places an attribute on one layer, and the layer is part of its identity;
- an attribute's claims are read from and written to its layer, and a session reads every layer at once, each attribute from its own;
- a rule's conclusion lands on the layer of the attribute it concludes, whatever layers its facts come from;
- the session layer lives in memory for the session.

R41 suggested readers place a conclusion with its facts instead (14 of 20 kept a draft's note out of a replica read).

## Documents

A `draft` concept placed `in: :session`, a `note` concept on the primary, a rule concluding a note from each draft, a draft and a note asserted. The documentation is R42's (line paragraph, framing, one-field example) and a paragraph on `in:`.

| document | the `in:` paragraph | readers |
|---|---|---|
| `base` | placement: read from and written to its layer, the same name on another layer is another attribute, no `in:` is the primary (stored and synced), `:session` is kept in memory until the session ends, a read sees every layer | 20 |
| `lands` | the same, plus: "A rule's conclusion lands on the layer of the attribute it concludes, whichever layers the facts it came from are on." | 20 |

## Scoring and bars

1. Question 1 scores on `note:1` and `draft:1`.
2. Question 2 scores on `note:1` alone: the draft was in memory, so nothing concludes the second note.
3. Question 3 scores on the primary.

A document reads right if at least 14 of 20 score on question 1, and misleads if 4 or fewer do.

Predictions:

- `base` lands between the bars on question 1, at 10 or fewer.
- `lands` reads right on question 1.
- On question 2, `lands` scores no higher than `base`: "lands" may read as stored, so some readers keep the draft's note after the session ends.
- Question 3 scores 16 or more on both.

## What the readers read

<details><summary><code>base</code> (sha256 <code>f4b9cd786e592aa5</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (draft ?d text: ?x)
  (xyz.draft/text of: ?d is: ?x as: :text pick: :last in: :session))

(concept (note ?n text: ?x)
  (xyz.note/text of: ?n is: ?x as: :text pick: :last))

(rule (note ?d text: ?x)
  (draft ?d text: ?x))

(assert (draft draft:1 text: "half-written idea"))
(assert (note note:1 text: "groceries"))
```

Nothing is stored before the first `assert`. Both asserts run in one session.

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

`in:` places an attribute on a layer: its claims are read from and written to that layer, and the same name on another layer is another attribute. A line without `in:` is on the primary layer, which is stored and synced. The `:session` layer is kept in memory until the session ends. A read sees every layer at once, each attribute from its own layer.

## Questions

1. What does `(query (note ?n text: ?x))` give back? List every row.
2. The session ends, and a new session opens on the same primary. What does the same query give back now? List every row.
3. In the first session, a document holds `(assert (note draft:1 text: "kept"))`. On which layer is that claim kept?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>lands</code> (sha256 <code>f5b600aabbe7148e</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (draft ?d text: ?x)
  (xyz.draft/text of: ?d is: ?x as: :text pick: :last in: :session))

(concept (note ?n text: ?x)
  (xyz.note/text of: ?n is: ?x as: :text pick: :last))

(rule (note ?d text: ?x)
  (draft ?d text: ?x))

(assert (draft draft:1 text: "half-written idea"))
(assert (note note:1 text: "groceries"))
```

Nothing is stored before the first `assert`. Both asserts run in one session.

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

`in:` places an attribute on a layer: its claims are read from and written to that layer, and the same name on another layer is another attribute. A line without `in:` is on the primary layer, which is stored and synced. The `:session` layer is kept in memory until the session ends. A read sees every layer at once, each attribute from its own layer. A rule's conclusion lands on the layer of the attribute it concludes, whichever layers the facts it came from are on.

## Questions

1. What does `(query (note ?n text: ?x))` give back? List every row.
2. The session ends, and a new session opens on the same primary. What does the same query give back now? List every row.
3. In the first session, a document holds `(assert (note draft:1 text: "kept"))`. On which layer is that claim kept?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

