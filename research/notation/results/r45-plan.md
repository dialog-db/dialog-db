# R45 plan: `cardinality:` in place of `pick:`, and a ranking in a rule's conclusion

Written and committed before any reader ran. The tally will be in `r45.md` beside it. Clean runner, single turn, 20 readers per document.

## Why

The maintainer is retiring `pick:` for `cardinality: :one | :many`: a write under `:one` replaces the claim it read and a read gives the newest, while under `:many` a write adds and a read gives every claim. `min`, `max` and `top` become folds in a rule's conclusion:

```clj
(rule (account ?a status: ([:suspended :active :registered] ?state))
  (account/state ?a is: ?state))
```

The cardinality work (`claude/pick-strategy-rules-crdt-tvdpm7`, Refs #793) lets a deductive rule fold. `top` ranks by its list, best first, with an unlisted value last. A value a choosing fold returns stands as the fact it was chosen from, so it competes with stored claims by recency.

Two expectations to check:

- "Cardinality one" may make a stored and a concluded value on one attribute read as a conflict, which R38 found before R39's election sentence (9 of 16 said the to-do had two titles).
- The ranking's order, and `:many` writes adding, may not be what readers guess.

## Documents

R42's documentation with `pick:` replaced: "`cardinality:` says how a line treats them. Under `:one`, a write replaces the claim it read, and a read gives the one caused most recently, whether it was stored or concluded by a rule. Under `:many`, a write adds a claim and a read gives every claim." The line before it now reads "One attribute of one entity can hold several claims, stored or concluded by rules." Every `pick: :last` in the code is `cardinality: :one`.

| document | content | readers |
|---|---|---|
| `note-card` | R42 `note-claim`, translated | 20 |
| `todo-card` | R42 `todo-claim`, translated | 20 |
| `account-bare` | `account/status` (`:one`) concluded by ranking `account/state` (`:many`); `:registered`, `:active`, `:suspended` asserted in order | 20 |
| `account-rank` | the same, plus: "In a rule's conclusion, `([:a :b :c] ?x)` ranks: of every value `?x` takes for the rest of the conclusion, it keeps the one listed first. A value not listed ranks last." | 20 |

## Scoring and bars

- `note-card`: question 2 on "yes".
- `todo-card`: (a) the rename lands; (b) the to-do shows "Fix login bug" and the issue keeps "Fix login"; question 9 on "Login fails".
- `account`: question 1 on `:suspended`; question 2 on `:suspended` (the write adds, `:suspended` still ranks first); question 3 on `:active`; question 4, the branch's answer, `:active`: a stored `account/status` is newer than the `:suspended` fact the fold chose. Question 4 is scored but has no bar.

A document reads right if at least 14 of 20 score on its main question (`note` 2, `todo` (b), `account` 1), and misleads if 4 or fewer do.

Predictions:

- `note-card` reads right.
- `todo-card` (b) scores below R42's 20, at 16 or fewer: readers call a stored and a concluded title on a `:one` attribute a conflict.
- `account-bare` question 1 between the bars: nothing says which end of the list wins.
- `account-rank` question 1 reads right.
- Question 2 at 14 or more on both `account` documents.
- Question 4 at 8 or fewer on both: readers take the concluded field as read-only or as overridden by the fold.

## What the readers read

<details><summary><code>note-card</code> (sha256 <code>cf156b34659dc966</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (note ?note message: ?message)
  (xyz.tonk.post/content of: ?note is: ?message as: :text cardinality: :one))

(concept (memo ?memo message: ?message to: ?to)
  (xyz.tonk.post/content of: ?memo is: ?message as: :text cardinality: :one)
  (xyz.tonk.post/audience of: ?memo is: ?to as: :entity cardinality: :one))

(concept (email ?mail body: ?body recipient: ?to)
  (xyz.mail/body of: ?mail is: ?body as: :text cardinality: :one)
  (xyz.mail/recipient of: ?mail is: ?to as: :entity cardinality: :one))

(rule (memo ?mail message: ?body to: ?to)
  (email ?mail body: ?body recipient: ?to))

(assert (email mail:one body: "hello world" recipient: mailto:alice@web.mail))
(query (note ?note message: ?msg))
```

Nothing is stored before the `assert`.

## Documentation

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text cardinality: :one)` is a claim: the attribute `xyz.todo/title` of the entity `?t` is `?title`. When a concept is asked for, its lines are read. When a concept is concluded by a rule or asserted, its lines are what is concluded or asserted. `as:` is the type a value must have. One attribute of one entity can hold several claims, stored or concluded by rules. `cardinality:` says how a line treats them. Under `:one`, a write replaces the claim it read, and a read gives the one caused most recently, whether it was stored or concluded by a rule. Under `:many`, a write adds a claim and a read gives every claim. A concluded value is as recent as the newest fact it was concluded from.

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

## Questions

1. What does the `assert` form store? List each stored fact as entity, attribute, value.
2. Will the `query` find a note with the message "hello world"? Answer yes or no, then explain why.
3. In your own words, how does a concept such as `memo` relate to the lines inside it? Does information flow one way between them, or both ways?
4. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>todo-card</code> (sha256 <code>ef1b9f9a7b16e2a0</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (todo ?t title: ?title status: ?status)
  (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one)
  (xyz.todo/status of: ?t is: ?status as: [:open :done] cardinality: :one))

(concept (issue ?i title: ?title state: ?state)
  (xyz.tracker/title of: ?i is: ?title as: :text cardinality: :one)
  (xyz.tracker/state of: ?i is: ?state as: :text cardinality: :one))

; A
(rule (todo ?i title: ?x status: :open)
  (issue ?i title: ?x state: "open"))

; B
(rule (update-title (todo ?t) old: ?old new: ?new)
  (rename (todo ?t) title: ?new)
  (xyz.todo/title of: ?t is: ?old))

; C
(rule (retract (xyz.todo/title of: ?t is: ?old))
  (update-title (todo ?t) old: ?old))

; D
(rule (assert (xyz.todo/title of: ?t is: ?new))
  (update-title (todo ?t) new: ?new))
```

These facts are stored, and nothing else:

```text
todo:1   xyz.todo/title     "Buy milk"
todo:1   xyz.todo/status    :open
issue:7  xyz.tracker/title  "Fix login"
issue:7  xyz.tracker/state  "open"
```

## Documentation

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text cardinality: :one)` is a claim: the attribute `xyz.todo/title` of the entity `?t` is `?title`. When a concept is asked for, its lines are read. When a concept is concluded by a rule or asserted, its lines are what is concluded or asserted. `as:` is the type a value must have. One attribute of one entity can hold several claims, stored or concluded by rules. `cardinality:` says how a line treats them. Under `:one`, a write replaces the claim it read, and a read gives the one caused most recently, whether it was stored or concluded by a rule. Under `:many`, a write adds a claim and a read gives every claim. A concluded value is as recent as the newest fact it was concluded from.

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

<details><summary><code>account-bare</code> (sha256 <code>63338f61785b4eb5</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (account ?a status: ?status)
  (account/status of: ?a is: ?status as: [:suspended :active :registered] cardinality: :one))

(rule (account ?a status: ([:suspended :active :registered] ?state))
  (account/state of: ?a is: ?state as: [:suspended :active :registered] cardinality: :many))

(assert (account/state of: acct:1 is: :registered))
(assert (account/state of: acct:1 is: :active))
(assert (account/state of: acct:1 is: :suspended))
```

Nothing is stored before the first `assert`, and the asserts run in the order written.

## Documentation

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text cardinality: :one)` is a claim: the attribute `xyz.todo/title` of the entity `?t` is `?title`. When a concept is asked for, its lines are read. When a concept is concluded by a rule or asserted, its lines are what is concluded or asserted. `as:` is the type a value must have. One attribute of one entity can hold several claims, stored or concluded by rules. `cardinality:` says how a line treats them. Under `:one`, a write replaces the claim it read, and a read gives the one caused most recently, whether it was stored or concluded by a rule. Under `:many`, a write adds a claim and a read gives every claim. A concluded value is as recent as the newest fact it was concluded from.

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

## Questions

1. What does `(query (account acct:1 status: ?s))` give back?
2. A document then holds `(assert (account/state of: acct:1 is: :active))`. What does the query from question 1 give back now?
3. A document then holds `(retract (account/state of: acct:1 is: :suspended))`. What does the query from question 1 give back now?
4. Starting again from the three asserts in the code, a document holds `(assert (account acct:1 status: :active))`. What does the query from question 1 give back now, and what is stored?
5. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>account-rank</code> (sha256 <code>366d14cc4eeb1e08</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (account ?a status: ?status)
  (account/status of: ?a is: ?status as: [:suspended :active :registered] cardinality: :one))

(rule (account ?a status: ([:suspended :active :registered] ?state))
  (account/state of: ?a is: ?state as: [:suspended :active :registered] cardinality: :many))

(assert (account/state of: acct:1 is: :registered))
(assert (account/state of: acct:1 is: :active))
(assert (account/state of: acct:1 is: :suspended))
```

Nothing is stored before the first `assert`, and the asserts run in the order written.

## Documentation

Each line such as `(xyz.todo/title of: ?t is: ?title as: :text cardinality: :one)` is a claim: the attribute `xyz.todo/title` of the entity `?t` is `?title`. When a concept is asked for, its lines are read. When a concept is concluded by a rule or asserted, its lines are what is concluded or asserted. `as:` is the type a value must have. One attribute of one entity can hold several claims, stored or concluded by rules. `cardinality:` says how a line treats them. Under `:one`, a write replaces the claim it read, and a read gives the one caused most recently, whether it was stored or concluded by a rule. Under `:many`, a write adds a claim and a read gives every claim. A concluded value is as recent as the newest fact it was concluded from.

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

In a rule's conclusion, `([:a :b :c] ?x)` ranks: of every value `?x` takes for the rest of the conclusion, it keeps the one listed first. A value not listed ranks last.

## Questions

1. What does `(query (account acct:1 status: ?s))` give back?
2. A document then holds `(assert (account/state of: acct:1 is: :active))`. What does the query from question 1 give back now?
3. A document then holds `(retract (account/state of: acct:1 is: :suspended))`. What does the query from question 1 give back now?
4. Starting again from the three asserts in the code, a document holds `(assert (account acct:1 status: :active))`. What does the query from question 1 give back now, and what is stored?
5. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

