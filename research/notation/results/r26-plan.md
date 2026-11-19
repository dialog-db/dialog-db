# R26 plan: how a type constraint is spelled, read cold

Written and committed before any reader ran. The tally will be in `r26.md` beside it.

## Why

The design spells a type constraint `(dialog.type ?x is: :text)`. The maintainer finds it reads as a domain call, which reads stored facts, while a constraint touches none; and the dictionary sketch needed a `dialog.key` that is only a constraint on a label variable. He proposed spellings that look unlike a domain call. This round asks which of them readers, given no documentation, take for a constraint and not for stored data.

## Variants

The same samples and questions, with every constraint in one spelling. No documentation is given.

| variant | `?x` is text | `?s` is a case |
|---|---|---|
| `formula` | `(dialog/type ?x is: :text)` | `(dialog/type ?s is: [:open :done])` |
| `colon` | `(: ?x as: :text)` | `(: ?s as: [:open :done])` |
| `var` | `(?x as: :text)` | `(?s as: [:open :done])` |
| `keyword` | `(:text ?x)` | `([:open :done] ?s)` |
| `name` | `(text ?x)` | `(one-of ?s in: [:open :done])` |

`formula` replaces the design's `dialog.type`, which the maintainer ruled out: the `/` puts it with formulas such as `dialog/position`. `name` treats a type as a concept with only a primary; `one-of` is the experimenter's spelling for a variant, since a variant has no name.

| sample | what it holds |
|---|---|
| A | a concept over `xyz.todo` with three constraints |
| B | stored facts, one to-do whose text is `42`, a question, and an assert with a state outside the variant |
| C | a dictionary concept, every entry of `xyz.notebook`, with its label constrained to a position |

## Scoring

Eight points, from the first answer only:

| item | point when the reader says |
|---|---|
| 1 | the `xyz.todo` line reads stored data, and every constraint line neither reads nor writes it |
| 2 | the state is `:open` or `:done` |
| 3 | only `todo:1`: `todo:2`'s text is not text |
| 4 | it is refused, an error, and nothing is stored |
| 5 | every entry of `?n` in `xyz.notebook`, the label as `?p` and the value as `?b` |
| 6 | it requires `?p`, the label, to be a position |
| 7 | no fact about types is stored |
| 8 | it says what type a value must have, a check or a constraint, not stored data |

Item 9, the rating and the hardest thing to guess, is reported.

## Bars

Five readers per variant, `haiku`, read-only, all started at once and interleaved.

- Items 1, 7 and 8 are the ones the spelling can change. A spelling reads better than another when its readers score at least 3 more of those 15 points.
- If none leads another by 3, the spellings read alike cold, and the choice is the maintainer's.

Predictions, reported against what happens:

- At least 2 of the 5 `keyword` readers read `(:text ?x)` as reading a `text` field of `?x`, as Clojure's `(:text m)` does.
- `name` readers say a type line reads stored data more often than `colon` readers, since `(text ?x)` looks like a concept call.

## What the readers read

<details><summary><code>formula</code> (sha256 <code>0f11133721b7d388</code>)</summary>

````text
Below are code samples in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think each sample does. A short reason with every answer. Answer every question, numbered.

## Sample A

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (dialog/type ?l is: :entity)
  (dialog/type ?x is: :text)
  (dialog/type ?s is: [:open :done]))
```

1. Take each line in the body of the concept, the lines after its head. For each, say whether it reads stored data, writes stored data, or neither, and what it does.
2. What does the line with `[:open :done]` in it mean?

## Sample B

The concept from Sample A is defined, and these facts are stored:

```text
todo:1  xyz.todo/list   list:home
todo:1  xyz.todo/text   "Buy milk"
todo:1  xyz.todo/state  :open
todo:2  xyz.todo/list   list:home
todo:2  xyz.todo/text   42
todo:2  xyz.todo/state  :open
```

```clj
(query (todo ?t text: ?x))
```

3. Which to-dos does this give back, and why?
4. Instead, a document holds `(assert (todo todo:3 list: list:home text: "Call mom" state: :later))`. What happens?

## Sample C

```clj
(concept (notebook-entry ?n at: ?p block: ?b)
  (xyz.notebook ?n ?p: ?b)
  (dialog/type ?p is: :position)
  (dialog/type ?b is: :entity))
```

5. What does `(xyz.notebook ?n ?p: ?b)` read?
6. What does the line about `?p` do?

## Overall

7. Does defining the concept in Sample A, or asserting a to-do through it, store any fact about types? Why or why not?
8. In a few words, what does a line like `(dialog/type ?x is: :text)` mean in this notation?
9. Rate how readable these samples are from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>colon</code> (sha256 <code>520b67c51fc47192</code>)</summary>

````text
Below are code samples in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think each sample does. A short reason with every answer. Answer every question, numbered.

## Sample A

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (: ?l as: :entity)
  (: ?x as: :text)
  (: ?s as: [:open :done]))
```

1. Take each line in the body of the concept, the lines after its head. For each, say whether it reads stored data, writes stored data, or neither, and what it does.
2. What does the line with `[:open :done]` in it mean?

## Sample B

The concept from Sample A is defined, and these facts are stored:

```text
todo:1  xyz.todo/list   list:home
todo:1  xyz.todo/text   "Buy milk"
todo:1  xyz.todo/state  :open
todo:2  xyz.todo/list   list:home
todo:2  xyz.todo/text   42
todo:2  xyz.todo/state  :open
```

```clj
(query (todo ?t text: ?x))
```

3. Which to-dos does this give back, and why?
4. Instead, a document holds `(assert (todo todo:3 list: list:home text: "Call mom" state: :later))`. What happens?

## Sample C

```clj
(concept (notebook-entry ?n at: ?p block: ?b)
  (xyz.notebook ?n ?p: ?b)
  (: ?p as: :position)
  (: ?b as: :entity))
```

5. What does `(xyz.notebook ?n ?p: ?b)` read?
6. What does the line about `?p` do?

## Overall

7. Does defining the concept in Sample A, or asserting a to-do through it, store any fact about types? Why or why not?
8. In a few words, what does a line like `(: ?x as: :text)` mean in this notation?
9. Rate how readable these samples are from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>var</code> (sha256 <code>dfd483410be5dbbc</code>)</summary>

````text
Below are code samples in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think each sample does. A short reason with every answer. Answer every question, numbered.

## Sample A

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (?l as: :entity)
  (?x as: :text)
  (?s as: [:open :done]))
```

1. Take each line in the body of the concept, the lines after its head. For each, say whether it reads stored data, writes stored data, or neither, and what it does.
2. What does the line with `[:open :done]` in it mean?

## Sample B

The concept from Sample A is defined, and these facts are stored:

```text
todo:1  xyz.todo/list   list:home
todo:1  xyz.todo/text   "Buy milk"
todo:1  xyz.todo/state  :open
todo:2  xyz.todo/list   list:home
todo:2  xyz.todo/text   42
todo:2  xyz.todo/state  :open
```

```clj
(query (todo ?t text: ?x))
```

3. Which to-dos does this give back, and why?
4. Instead, a document holds `(assert (todo todo:3 list: list:home text: "Call mom" state: :later))`. What happens?

## Sample C

```clj
(concept (notebook-entry ?n at: ?p block: ?b)
  (xyz.notebook ?n ?p: ?b)
  (?p as: :position)
  (?b as: :entity))
```

5. What does `(xyz.notebook ?n ?p: ?b)` read?
6. What does the line about `?p` do?

## Overall

7. Does defining the concept in Sample A, or asserting a to-do through it, store any fact about types? Why or why not?
8. In a few words, what does a line like `(?x as: :text)` mean in this notation?
9. Rate how readable these samples are from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>keyword</code> (sha256 <code>ffe61c230979565b</code>)</summary>

````text
Below are code samples in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think each sample does. A short reason with every answer. Answer every question, numbered.

## Sample A

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (:entity ?l)
  (:text ?x)
  ([:open :done] ?s))
```

1. Take each line in the body of the concept, the lines after its head. For each, say whether it reads stored data, writes stored data, or neither, and what it does.
2. What does the line with `[:open :done]` in it mean?

## Sample B

The concept from Sample A is defined, and these facts are stored:

```text
todo:1  xyz.todo/list   list:home
todo:1  xyz.todo/text   "Buy milk"
todo:1  xyz.todo/state  :open
todo:2  xyz.todo/list   list:home
todo:2  xyz.todo/text   42
todo:2  xyz.todo/state  :open
```

```clj
(query (todo ?t text: ?x))
```

3. Which to-dos does this give back, and why?
4. Instead, a document holds `(assert (todo todo:3 list: list:home text: "Call mom" state: :later))`. What happens?

## Sample C

```clj
(concept (notebook-entry ?n at: ?p block: ?b)
  (xyz.notebook ?n ?p: ?b)
  (:position ?p)
  (:entity ?b))
```

5. What does `(xyz.notebook ?n ?p: ?b)` read?
6. What does the line about `?p` do?

## Overall

7. Does defining the concept in Sample A, or asserting a to-do through it, store any fact about types? Why or why not?
8. In a few words, what does a line like `(:text ?x)` mean in this notation?
9. Rate how readable these samples are from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>name</code> (sha256 <code>b170eb644da30000</code>)</summary>

````text
Below are code samples in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think each sample does. A short reason with every answer. Answer every question, numbered.

## Sample A

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (entity ?l)
  (text ?x)
  (one-of ?s in: [:open :done]))
```

1. Take each line in the body of the concept, the lines after its head. For each, say whether it reads stored data, writes stored data, or neither, and what it does.
2. What does the line with `[:open :done]` in it mean?

## Sample B

The concept from Sample A is defined, and these facts are stored:

```text
todo:1  xyz.todo/list   list:home
todo:1  xyz.todo/text   "Buy milk"
todo:1  xyz.todo/state  :open
todo:2  xyz.todo/list   list:home
todo:2  xyz.todo/text   42
todo:2  xyz.todo/state  :open
```

```clj
(query (todo ?t text: ?x))
```

3. Which to-dos does this give back, and why?
4. Instead, a document holds `(assert (todo todo:3 list: list:home text: "Call mom" state: :later))`. What happens?

## Sample C

```clj
(concept (notebook-entry ?n at: ?p block: ?b)
  (xyz.notebook ?n ?p: ?b)
  (position ?p)
  (entity ?b))
```

5. What does `(xyz.notebook ?n ?p: ?b)` read?
6. What does the line about `?p` do?

## Overall

7. Does defining the concept in Sample A, or asserting a to-do through it, store any fact about types? Why or why not?
8. In a few words, what does a line like `(text ?x)` mean in this notation?
9. Rate how readable these samples are from 1 to 5, and name the one thing you found hardest to guess.
````

</details>
