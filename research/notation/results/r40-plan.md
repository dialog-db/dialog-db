# R40 plan: type and policy on a variable line, or in place of the variable

Written and committed before any reader ran. The tally will be in `r40.md` beside it. Clean runner, single turn, 20 readers per variant.

## Why

The maintainer wants a concept to give each variable's type, policy and place on one line of its own:

```clj
(concept (todo ?i list: ?l title: ?t state: ?s)
  (?t as: :text policy: :last in: :memory)
  (?l as: [:open :done] policy: :last in: :memory)
  (?s as: :entity policy: :all in: :memory)
  (xyz.todo ?i title: ?t state: ?s)
  (xzy.list ?l task: ?i))
```

The journal entry of 2026-11-08 set this spelling aside: a policy elects among the claims of an attribute, and a line about a variable does not say which attribute's claims. The maintainer's own draft shows the risk. `list:` is stored as `xyz.list/task`, whose claims belong to the list and whose value is `?i`. The `policy: :last` on `?l` reads as "a to-do has one list", but `?l` binds no value, so it elects nothing, and design.md calls a pick on an entity-only variable an error. A to-do on two lists is on both.

Changes from the draft, so the round tests where the constraint sits and nothing else:

- the types of `?l` and `?s` are swapped back (`?l` is an entity, `?s` a state);
- `?s` takes `:last`;
- `xzy` reads `xyz`;
- `in:` is left out, to be tested on its own.

## Variants

- `lines`: the draft as above, a line per variable.
- `inline`: the same constraints written in place of the variable where it appears, so `?l`'s lands in the entity position of `xyz.list`.

Both carry the same facts (to-do `a` on lists `home` and `work`, to-do `b` on `home`) and the same documentation of attributes, claims and policies, plus one sentence on their own spelling.

## Scoring and bars

1. Question 1 scores on exactly `todo:a` and `todo:b`.
2. Question 2 scores on two lists, `home` and `work`, or on the concept being an error because `?l` binds no value.
3. Question 3 scores when the policy on `?l` elects nothing or is an error (there is no claim with `?l` as its value). It does not score on "a to-do keeps its newest list".

A variant reads right if at least 14 of 20 score on question 2, and misleads if 4 or fewer do.

Predictions:

- `lines` misleads on question 2: at most 6 of 20 say two lists. Most say `list:work` only.
- `inline` lands between: 8 to 14. The policy sits on the entity position, which the documentation says holds no claims, but readers have to put that together.
- Question 1 scores 16 or more on both.

## What the readers read

<details><summary><code>lines</code> (sha256 <code>95e44d81e70855e8</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (todo ?i list: ?l title: ?t state: ?s)
  (?t as: :text policy: :last)
  (?s as: [:open :done] policy: :last)
  (?l as: :entity policy: :last)
  (xyz.todo ?i title: ?t state: ?s)
  (xyz.list ?l task: ?i))

(assert (xyz.todo todo:a title: "Buy milk" state: :open))
(assert (xyz.todo todo:b title: "Call mom" state: :open))
(assert (xyz.list list:home task: todo:a))
(assert (xyz.list list:home task: todo:b))
(assert (xyz.list list:work task: todo:a))
```

Nothing is stored before the first `assert`, and the asserts run in the order written.

## Documentation

`(xyz.todo ?i title: ?t)` reads the attribute `xyz.todo/title` of the entity `?i`, and its value binds `?t`. One attribute of one entity can hold several claims; each `assert` adds one. A policy elects among the claims of one attribute of one entity: `:last` reads the most recently asserted, `:all` reads every claim. Without a policy, every claim is read. `as:` is the type a value must have.

A line such as `(?t as: :text policy: :last)` sets the type and the policy for the variable `?t`.

## Questions

1. What does `(query (todo ?i list: list:home))` return? List every row.
2. Does `(query (todo todo:a list: ?l))` return one list or two? Name them.
3. What does the `policy: :last` given for `?l` do in this concept?
````

</details>

<details><summary><code>inline</code> (sha256 <code>ee7cce155f84a5e5</code>)</summary>

````text
Below is code in a notation you have not seen before. Its only documentation is the passage after it. Otherwise answer from the code alone, saying what you think it does. Give a short reason with every answer. Answer every question, numbered.

## The code

```clj
(concept (todo ?i list: ?l title: ?t state: ?s)
  (xyz.todo ?i
    title: (?t as: :text policy: :last)
    state: (?s as: [:open :done] policy: :last))
  (xyz.list (?l as: :entity policy: :last)
    task: ?i))

(assert (xyz.todo todo:a title: "Buy milk" state: :open))
(assert (xyz.todo todo:b title: "Call mom" state: :open))
(assert (xyz.list list:home task: todo:a))
(assert (xyz.list list:home task: todo:b))
(assert (xyz.list list:work task: todo:a))
```

Nothing is stored before the first `assert`, and the asserts run in the order written.

## Documentation

`(xyz.todo ?i title: ?t)` reads the attribute `xyz.todo/title` of the entity `?i`, and its value binds `?t`. One attribute of one entity can hold several claims; each `assert` adds one. A policy elects among the claims of one attribute of one entity: `:last` reads the most recently asserted, `:all` reads every claim. Without a policy, every claim is read. `as:` is the type a value must have.

Writing `(?t as: :text policy: :last)` in place of a variable sets the type and the policy for that variable there.

## Questions

1. What does `(query (todo ?i list: list:home))` return? List every row.
2. Does `(query (todo todo:a list: ?l))` return one list or two? Name them.
3. What does the `policy: :last` given for `?l` do in this concept?
````

</details>

