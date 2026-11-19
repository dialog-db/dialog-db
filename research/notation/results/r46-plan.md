# R46 plan: a variable attribute name, and a label for the kind of key

Written and committed before any reader ran. The tally will be in `r46.md` beside it. Clean runner, single turn, 20 readers per document.

## Why

With concepts written one line per attribute, a collection's line names its domain and leaves the key as a variable in the attribute's name: `(xyz.tonk.element.method/?n of: ?e is: ?s)`, one row per key. Dialog keys a collection's attributes either by name (`Keyed::Dictionary`) or by position (`Keyed::Sequence`), and that kind is part of its identity, so it goes on the line. The maintainer floated `access:`, `form:`, `format:`, `layout:` and `via:` for the label; `keyed:` is the engine's word. `access:`, `form:`, `format:` and `layout:` each already mean something in dialog or tonk, and are left out.

Two questions: do readers read `/?n` as every key without being told, and does a label tell a dictionary from a sequence?

## Documents

`element-method` reads a dictionary of methods, `notebook-entry` a sequence of blocks, each by `/?n` and `/?p`. Two methods and one entry are asserted. Documentation is R45's (the line paragraph with `cardinality:`, the framing and the one-field example); nothing explains `/?n` or the label.

| document | label on the two lines | readers |
|---|---|---|
| `none` | none | 20 |
| `keyed` | `keyed: :dictionary`, `keyed: :sequence` | 20 |
| `via` | `via: :name`, `via: :position` | 20 |
| `shape` | `shape: :dictionary`, `shape: :sequence` | 20 |

## Scoring and bars

1. Question 1 scores when `?n` is the method's name, the key, in the attribute's name.
2. Question 2 scores on two rows, `run` and `stop`.
3. Question 3 scores on all three entries after the sync: entries at different positions do not conflict.
4. Question 4 scores when one is keyed by name and the other by position or order.

A document reads right if at least 14 of 20 score on questions 2 and 4, and misleads if 4 or fewer do.

Predictions:

- Question 2 reads right on every document: `/?n` reads as every key without help.
- Question 4 is lowest on `none`, and reads right on the three labeled documents.
- `via` scores at least as well as `keyed` and `shape` on question 4: its values say what a key is.

## What the readers read

<details><summary><code>none</code> (sha256 <code>7e1b1358ef2b6a25</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (element-method ?e name: ?n source: ?s)
  (xyz.tonk.element.method/?n of: ?e is: ?s as: :text cardinality: :one))

(concept (notebook-entry ?b at: ?p block: ?x)
  (xyz.tonk.notebook/?p of: ?b is: ?x as: :entity cardinality: :one))

(assert (xyz.tonk.element.method/run of: element:1 is: "start()"))
(assert (xyz.tonk.element.method/stop of: element:1 is: "halt()"))
(assert (xyz.tonk.notebook/N1 of: notebook:1 is: block:a))
```

Nothing is stored before the first `assert`.

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

1. In `element-method`, what does `?n` stand for?
2. What does `(query (element-method element:1 name: ?n source: ?s))` give back? List every row.
3. Device A asserts `(xyz.tonk.notebook/N2 of: notebook:1 is: block:b)` while device B, offline, asserts `(xyz.tonk.notebook/N3 of: notebook:1 is: block:c)`. After they sync, what does `(query (notebook-entry notebook:1 at: ?p block: ?x))` give back? List every row.
4. How do `element-method` and `notebook-entry` differ in what their keys are? Answer in a sentence or two.
5. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>keyed</code> (sha256 <code>bedd4a067228fc6d</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (element-method ?e name: ?n source: ?s)
  (xyz.tonk.element.method/?n of: ?e is: ?s as: :text cardinality: :one keyed: :dictionary))

(concept (notebook-entry ?b at: ?p block: ?x)
  (xyz.tonk.notebook/?p of: ?b is: ?x as: :entity cardinality: :one keyed: :sequence))

(assert (xyz.tonk.element.method/run of: element:1 is: "start()"))
(assert (xyz.tonk.element.method/stop of: element:1 is: "halt()"))
(assert (xyz.tonk.notebook/N1 of: notebook:1 is: block:a))
```

Nothing is stored before the first `assert`.

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

1. In `element-method`, what does `?n` stand for?
2. What does `(query (element-method element:1 name: ?n source: ?s))` give back? List every row.
3. Device A asserts `(xyz.tonk.notebook/N2 of: notebook:1 is: block:b)` while device B, offline, asserts `(xyz.tonk.notebook/N3 of: notebook:1 is: block:c)`. After they sync, what does `(query (notebook-entry notebook:1 at: ?p block: ?x))` give back? List every row.
4. How do `element-method` and `notebook-entry` differ in what their keys are? Answer in a sentence or two.
5. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>via</code> (sha256 <code>577ab4752bfb7460</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (element-method ?e name: ?n source: ?s)
  (xyz.tonk.element.method/?n of: ?e is: ?s as: :text cardinality: :one via: :name))

(concept (notebook-entry ?b at: ?p block: ?x)
  (xyz.tonk.notebook/?p of: ?b is: ?x as: :entity cardinality: :one via: :position))

(assert (xyz.tonk.element.method/run of: element:1 is: "start()"))
(assert (xyz.tonk.element.method/stop of: element:1 is: "halt()"))
(assert (xyz.tonk.notebook/N1 of: notebook:1 is: block:a))
```

Nothing is stored before the first `assert`.

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

1. In `element-method`, what does `?n` stand for?
2. What does `(query (element-method element:1 name: ?n source: ?s))` give back? List every row.
3. Device A asserts `(xyz.tonk.notebook/N2 of: notebook:1 is: block:b)` while device B, offline, asserts `(xyz.tonk.notebook/N3 of: notebook:1 is: block:c)`. After they sync, what does `(query (notebook-entry notebook:1 at: ?p block: ?x))` give back? List every row.
4. How do `element-method` and `notebook-entry` differ in what their keys are? Answer in a sentence or two.
5. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>shape</code> (sha256 <code>5c3b91e8b0117f88</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (element-method ?e name: ?n source: ?s)
  (xyz.tonk.element.method/?n of: ?e is: ?s as: :text cardinality: :one shape: :dictionary))

(concept (notebook-entry ?b at: ?p block: ?x)
  (xyz.tonk.notebook/?p of: ?b is: ?x as: :entity cardinality: :one shape: :sequence))

(assert (xyz.tonk.element.method/run of: element:1 is: "start()"))
(assert (xyz.tonk.element.method/stop of: element:1 is: "halt()"))
(assert (xyz.tonk.notebook/N1 of: notebook:1 is: block:a))
```

Nothing is stored before the first `assert`.

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

1. In `element-method`, what does `?n` stand for?
2. What does `(query (element-method element:1 name: ?n source: ?s))` give back? List every row.
3. Device A asserts `(xyz.tonk.notebook/N2 of: notebook:1 is: block:b)` while device B, offline, asserts `(xyz.tonk.notebook/N3 of: notebook:1 is: block:c)`. After they sync, what does `(query (notebook-entry notebook:1 at: ?p block: ?x))` give back? List every row.
4. How do `element-method` and `notebook-entry` differ in what their keys are? Answer in a sentence or two.
5. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>


## Addendum: a fifth document, `by`

Added while the first four ran, before any `by` reader ran, at the maintainer's suggestion: `by: :name` and `by: :position`, the same values as `via`. 20 readers, scored as the others. Prediction: `by` scores as `via` does on question 4, within 2.

<details><summary><code>by</code> (sha256 <code>eddd77a62ac1c492</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (element-method ?e name: ?n source: ?s)
  (xyz.tonk.element.method/?n of: ?e is: ?s as: :text cardinality: :one by: :name))

(concept (notebook-entry ?b at: ?p block: ?x)
  (xyz.tonk.notebook/?p of: ?b is: ?x as: :entity cardinality: :one by: :position))

(assert (xyz.tonk.element.method/run of: element:1 is: "start()"))
(assert (xyz.tonk.element.method/stop of: element:1 is: "halt()"))
(assert (xyz.tonk.notebook/N1 of: notebook:1 is: block:a))
```

Nothing is stored before the first `assert`.

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

1. In `element-method`, what does `?n` stand for?
2. What does `(query (element-method element:1 name: ?n source: ?s))` give back? List every row.
3. Device A asserts `(xyz.tonk.notebook/N2 of: notebook:1 is: block:b)` while device B, offline, asserts `(xyz.tonk.notebook/N3 of: notebook:1 is: block:c)`. After they sync, what does `(query (notebook-entry notebook:1 at: ?p block: ?x))` give back? List every row.
4. How do `element-method` and `notebook-entry` differ in what their keys are? Answer in a sentence or two.
5. Rate how readable this notation is from 1 to 5, and name the one thing you found hardest to guess.
````

</details>
