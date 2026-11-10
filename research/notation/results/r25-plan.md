# R25 plan: what each word suggests, read cold

Written and committed before any reader ran. The tally will be in `r25.md` beside it.

## Why

R24 found `query`, `select`, `find` and `?` read alike once the spec says what the form does, and every reader chose the word it had read. The maintainer proposed testing the words without a spec: show code samples and ask readers what they think each does, including a concept the sample defines and one it does not.

## Variants

The same four samples and questions, with the question form spelled `query`, `select`, `find` or `?`. No documentation is given.

| sample | what it holds |
|---|---|
| A | a concept and a question over it |
| B | a concept, a rule on an `add` command, an `add`, and a question |
| C | only a question over `project`, which nothing defines |
| D | only a question over the domain `xyz.todo` |

## Scoring

Seven points, from the first answer only:

| item | point when the reader says |
|---|---|
| 1 | it returns the matching to-dos: those on `list:home` with state `:open` |
| 2 | it changes nothing stored |
| 3 | `add` sets the rule off, which creates an open to-do |
| 4 | yes, the question sees the to-do the `add` made |
| 5 | an error: `project` is not defined |
| 6 | D reads the `xyz.todo` domain directly and returns what it holds, where C has no definition |
| 7 | `W` asks a question: it reads and returns matches, changing nothing |

Item 8, the rating and the hardest thing to guess, is reported. The tally also reports any reading a word invites that the design does not mean: `select` as projecting columns, `find` as returning one match, `?` as a variable or an optional.

## Bars

Five readers per word, `haiku`, read-only, all started at once and interleaved.

- Items 2, 5 and 7 are the ones a word can change. A word reads better than another when its readers score at least 3 more of those 15 points.
- If no word leads another by 3, the words are equal cold too, and the choice is the maintainer's.

## What the readers read

<details><summary><code>query</code> (sha256 <code>67c94837c8b5f5e5</code>)</summary>

````text
Below are code samples in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think each sample does. A short reason with every answer. Answer every question, numbered.

## Sample A

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s))

(query (todo ?t list: list:home state: :open))
```

1. What does the last form, `(query …)`, do, and what does it give back?
2. Does running this sample change any stored data? Why or why not?

## Sample B

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s))

(rule (assert (todo ?t list: ?l text: ?x state: :open))
  (when (add (todo list: ?l text: ?x))
        (id ?t list: ?l text: ?x)))

(add (todo list: list:home text: "Buy milk"))

(query (todo ?t text: ?x))
```

3. What do you think `(add …)` does here?
4. Does the `(query …)` form give back a "Buy milk" to-do? Why or why not?

## Sample C

This is a whole sample. Nothing else is defined or installed.

```clj
(query (project ?p name: ?n))
```

5. What happens when this runs?

## Sample D

This is a whole sample too.

```clj
(query (xyz.todo ?t text: ?x))
```

6. What happens when this runs, and how does it differ from Sample C?

## Overall

7. In a few words, what does `query` mean in this notation?
8. Rate how readable these samples are from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>select</code> (sha256 <code>aa542c9fa2bc8b06</code>)</summary>

````text
Below are code samples in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think each sample does. A short reason with every answer. Answer every question, numbered.

## Sample A

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s))

(select (todo ?t list: list:home state: :open))
```

1. What does the last form, `(select …)`, do, and what does it give back?
2. Does running this sample change any stored data? Why or why not?

## Sample B

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s))

(rule (assert (todo ?t list: ?l text: ?x state: :open))
  (when (add (todo list: ?l text: ?x))
        (id ?t list: ?l text: ?x)))

(add (todo list: list:home text: "Buy milk"))

(select (todo ?t text: ?x))
```

3. What do you think `(add …)` does here?
4. Does the `(select …)` form give back a "Buy milk" to-do? Why or why not?

## Sample C

This is a whole sample. Nothing else is defined or installed.

```clj
(select (project ?p name: ?n))
```

5. What happens when this runs?

## Sample D

This is a whole sample too.

```clj
(select (xyz.todo ?t text: ?x))
```

6. What happens when this runs, and how does it differ from Sample C?

## Overall

7. In a few words, what does `select` mean in this notation?
8. Rate how readable these samples are from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>find</code> (sha256 <code>ef633a519f413dc9</code>)</summary>

````text
Below are code samples in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think each sample does. A short reason with every answer. Answer every question, numbered.

## Sample A

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s))

(find (todo ?t list: list:home state: :open))
```

1. What does the last form, `(find …)`, do, and what does it give back?
2. Does running this sample change any stored data? Why or why not?

## Sample B

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s))

(rule (assert (todo ?t list: ?l text: ?x state: :open))
  (when (add (todo list: ?l text: ?x))
        (id ?t list: ?l text: ?x)))

(add (todo list: list:home text: "Buy milk"))

(find (todo ?t text: ?x))
```

3. What do you think `(add …)` does here?
4. Does the `(find …)` form give back a "Buy milk" to-do? Why or why not?

## Sample C

This is a whole sample. Nothing else is defined or installed.

```clj
(find (project ?p name: ?n))
```

5. What happens when this runs?

## Sample D

This is a whole sample too.

```clj
(find (xyz.todo ?t text: ?x))
```

6. What happens when this runs, and how does it differ from Sample C?

## Overall

7. In a few words, what does `find` mean in this notation?
8. Rate how readable these samples are from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

<details><summary><code>mark</code> (sha256 <code>6c965dbc06f19c0a</code>)</summary>

````text
Below are code samples in a notation you have not seen before. There is no documentation: answer from the code alone, saying what you think each sample does. A short reason with every answer. Answer every question, numbered.

## Sample A

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s))

(? (todo ?t list: list:home state: :open))
```

1. What does the last form, `(? …)`, do, and what does it give back?
2. Does running this sample change any stored data? Why or why not?

## Sample B

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s))

(rule (assert (todo ?t list: ?l text: ?x state: :open))
  (when (add (todo list: ?l text: ?x))
        (id ?t list: ?l text: ?x)))

(add (todo list: list:home text: "Buy milk"))

(? (todo ?t text: ?x))
```

3. What do you think `(add …)` does here?
4. Does the `(? …)` form give back a "Buy milk" to-do? Why or why not?

## Sample C

This is a whole sample. Nothing else is defined or installed.

```clj
(? (project ?p name: ?n))
```

5. What happens when this runs?

## Sample D

This is a whole sample too.

```clj
(? (xyz.todo ?t text: ?x))
```

6. What happens when this runs, and how does it differ from Sample C?

## Overall

7. In a few words, what does `?` mean in this notation?
8. Rate how readable these samples are from 1 to 5, and name the one thing you found hardest to guess.
````

</details>

