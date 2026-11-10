# R17 plan: is the modifier design read right?

Written and committed before any reader ran. The tally will be in `r17.md` beside it.

## What changed since R16

The design as agreed on 2026-11-09 (see the journal): `!` is mutation, and `(x! …)` is short for `(! (x …))`; concepts are stored and carried, and are rules that run both ways; commands are modifiers applied to a concept's shape, never carried, stored or synced; `assert` and `retract` are modifiers that come with the language, `assert` deriving the entity from content and `retract` naming only the entity; every top-level form starts with its verb; an installed rule's `!` head commits only when a `!` triggered it.

## Variants

Every task has no worked example in the documents except task 1, which follows the `open` rule on purpose: R16 showed readers copy the nearest example, and the other tasks test what no example shows.

| variant | what the reader gets | lines |
|---|---|---|
| `card` | a reference card of the design, then the tasks | 103 |
| `prelude` | the same card with the four rules that define `assert` and `retract`, then the tasks | 129 |

The question the variants answer: does seeing the language's own forms written as rules help a reader predict them, or is the prose enough?

## Scoring

Thirteen points per reader, from its first answer only:

| item | point when |
|---|---|
| 1a | the premise reads `add` on a `todo` shape with list and text, with no `!` in the premise |
| 1b | the entity is `(id ?t list: ?l text: ?x)`, over list and text only |
| 1c | the head writes the to-do with `!`, passing the entity and all of list, text and `state: :open` |
| 1d | the rule is installed: `rule!` |
| 2 | one to-do |
| 3 | (a) yes and (b) nothing of the `add` |
| 4 | (a) no and (b) no permission needed |
| 5 | no |
| 6 | a one-attribute write with `!`: `(xyz.todo/state! todo:9c1e is: :done)`, not `assert` and not a partial `todo!` |
| 7 | two to-dos |
| 8 | no: its text was retracted with the to-do |
| 9 | a compile error, because no rule reads `state` on `add` |
| 10 | no: a bare concept call at the top level is an error |

A rename that keeps the meaning (variable names, `(! (todo …))` for `todo!`) still scores. Item 11, the rating, is reported, not scored.

## Bars

Five readers per variant, `haiku`, read-only, all started at once and interleaved.

- **Per item:** an item is read right when at least 4 of 5 readers of a variant get it, and fails at 2 of 5 or fewer. A failing item changes its spelling or its wording before it goes into `design.md`.
- **Hypotheses from the design session**, each passing at 4 of 5 in either variant: lifetime (items 3 and 4), no leak into a concept query (5), convergence (2), an update names its entity and does not `assert` (6), identity from content includes every field (7), a retract reaches a concept that shares the attribute (8), the rules decide a modifier's parameters (9).
- **Variants:** the prelude helps if its median is at least 2 above `card`'s, hurts if at least 2 below, and makes no difference otherwise. If both medians are 12 or 13, the round says the design reads, and nothing about the documents.

## What the readers read

<details><summary><code>card</code> (sha256 <code>78ea91a937fdffa2</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts: an entity, an attribute and a value. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

## `!` means mutation

`(x! …)` is short for `(! (x …))`: the fact goes into the next tick. Without `!`, a fact holds in the current tick only. Only a form with `!` changes anything. A document with no `!` in it changes nothing and needs no permission to write: it can still be run, and its result lists what its rules would have done, uncommitted.

## Concepts

A concept's facts are stored, and carried from tick to tick until retracted.

```clj
(concept! (bug ?b title: ?t author: ?a status: ?s)
  (xyz.bug ?b title: ?t author: ?a status: ?s)
  (dialog.type ?t is: :text)
  (dialog.type ?a is: :text)
  (dialog.type ?s is: [:todo :done]))
```

`(xyz.bug ?b title: ?t …)` names the attributes `xyz.bug/title`, `xyz.bug/author` and `xyz.bug/status`. Each attribute keeps its latest value. A concept is a rule that runs both ways: read, the attributes make the concept; written, writing the concept writes each attribute.

Writing a concept with an entity you name passes every parameter:
`(bug! bug:7f3a title: "Crash" author: "ada" status: :todo)`. A write that leaves a parameter out is a compile error. To change one attribute, write the attribute: `(xyz.bug/title! bug:7f3a is: "Crash on save")`.

## The top level

Every top-level form starts with a word that says what it does: `query`, `rule`, `rule!`, `concept!`, or a modifier (below). A bare concept call at the top level is an error.

```clj
(query (bug ?b title: ?t status: :todo))
```

## Modifiers

A modifier is applied to a concept's shape: `(open (bug title: "Crash" author: "ada"))`.

- A modifier's facts are not the concept's facts. They are tagged with the modifier's name, so a `bug` query never sees them.
- They are never carried. Without `!` they hold in this tick; with `!`, `(open! (bug …))`, they hold in the next tick only. After that they are gone. They are never stored and never synced.
- A modifier needs no declaration. The rules that read it decide what it takes: an application that passes a parameter no rule reads is a compile error.
- Modifiers nest one deep.

`assert` and `retract` are modifiers that come with the language:

- `(assert (bug title: "Crash" author: "ada" status: :todo))`: the bug holds in this tick. `assert` takes every parameter of the concept and no entity: the bug's entity is the `id` of every attribute and value it states.
- `(assert! (bug …))`: the same, stored.
- `(retract (bug bug:7f3a))` and `(retract! (bug bug:7f3a))` name only the entity. Every attribute of the concept on that entity is retracted: in this tick, or stored.

## Rules

`(rule head… (when premise…))`. `rule!` installs a rule, which then runs every tick; a `rule` belongs to its document. A head is a call, and with `!` it is a mutation.

Premises are calls, `(unless call)`, and formulas: `(id ?b title: ?t author: ?a)` binds `?b` to the `id` of those fields, `(== ?x ?y)` compares. Every head variable is bound by a premise. In a premise, `(bug …)` matches what holds now and `(bug! …)` what is stored.

An installed rule's `!` head commits only when the facts that triggered it came from a `!`. Otherwise its effect is listed in the result and not committed.

```clj
(rule! (bug! ?b title: ?t author: ?a status: :todo)
  (when (open (bug title: ?t author: ?a))
        (id ?b title: ?t author: ?a)))
```

`(open! (bug title: "Crash" author: "ada"))` then stores a bug whose entity is the `id` of its title and author.

## Conflicts

Conflicts are never errors. Concurrent writes of one attribute are elected: the later write wins.

# Tasks

These concepts are installed:

```clj
(concept! (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (dialog.type ?l is: :entity)
  (dialog.type ?x is: :text)
  (dialog.type ?s is: [:open :done]))

(concept! (note ?n text: ?x)
  (xyz.todo ?n text: ?x)
  (dialog.type ?x is: :text))
```

Answer every task, numbered. Write code where a task asks for it, and a short reason for every answer.

1. Write an installed rule for a command `add`, so that `(add! (todo list: list:home text: "Buy milk"))` stores an open to-do whose entity is derived from its list and its text only.
2. With your rule from task 1 installed, two devices that have not synced each run `(add! (todo list: list:home text: "Buy milk"))`. After they sync, how many to-dos are stored?
3. With your rule installed, a document holds only `(add! (todo list: list:home text: "Buy milk"))`. After it and everything it triggers have run: (a) is a to-do stored? (b) is anything of the `add` left?
4. With your rule installed, a document holds only `(add (todo list: list:home text: "Buy milk"))`, with no `!`. After it runs: (a) is a to-do stored? (b) does running it need permission to write?
5. A document holds `(add (todo list: list:home text: "Buy milk"))` and `(query (todo ?t text: ?x))`. Does the query return a "Buy milk" to-do?
6. Write the form that marks the stored to-do `todo:9c1e` as done. You do not know its list or its text.
7. One document runs `(assert! (todo list: list:home text: "Buy milk" state: :open))`. A later document runs `(assert! (todo list: list:home text: "Buy milk" state: :done))`. How many to-dos are stored after both?
8. The entity `todo:9c1e` is a to-do and, because it has `xyz.todo/text`, also a note. A document runs `(retract! (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
9. With only your rule from task 1 installed, a document runs `(add! (todo list: list:home text: "Buy milk" state: :done))`. What happens?
10. Is `(todo todo:9c1e list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
11. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

<details><summary><code>prelude</code> (sha256 <code>3147d76dbc089e86</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts: an entity, an attribute and a value. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

## `!` means mutation

`(x! …)` is short for `(! (x …))`: the fact goes into the next tick. Without `!`, a fact holds in the current tick only. Only a form with `!` changes anything. A document with no `!` in it changes nothing and needs no permission to write: it can still be run, and its result lists what its rules would have done, uncommitted.

## Concepts

A concept's facts are stored, and carried from tick to tick until retracted.

```clj
(concept! (bug ?b title: ?t author: ?a status: ?s)
  (xyz.bug ?b title: ?t author: ?a status: ?s)
  (dialog.type ?t is: :text)
  (dialog.type ?a is: :text)
  (dialog.type ?s is: [:todo :done]))
```

`(xyz.bug ?b title: ?t …)` names the attributes `xyz.bug/title`, `xyz.bug/author` and `xyz.bug/status`. Each attribute keeps its latest value. A concept is a rule that runs both ways: read, the attributes make the concept; written, writing the concept writes each attribute.

Writing a concept with an entity you name passes every parameter:
`(bug! bug:7f3a title: "Crash" author: "ada" status: :todo)`. A write that leaves a parameter out is a compile error. To change one attribute, write the attribute: `(xyz.bug/title! bug:7f3a is: "Crash on save")`.

## The top level

Every top-level form starts with a word that says what it does: `query`, `rule`, `rule!`, `concept!`, or a modifier (below). A bare concept call at the top level is an error.

```clj
(query (bug ?b title: ?t status: :todo))
```

## Modifiers

A modifier is applied to a concept's shape: `(open (bug title: "Crash" author: "ada"))`.

- A modifier's facts are not the concept's facts. They are tagged with the modifier's name, so a `bug` query never sees them.
- They are never carried. Without `!` they hold in this tick; with `!`, `(open! (bug …))`, they hold in the next tick only. After that they are gone. They are never stored and never synced.
- A modifier needs no declaration. The rules that read it decide what it takes: an application that passes a parameter no rule reads is a compile error.
- Modifiers nest one deep.

`assert` and `retract` are modifiers that come with the language:

- `(assert (bug title: "Crash" author: "ada" status: :todo))`: the bug holds in this tick. `assert` takes every parameter of the concept and no entity: the bug's entity is the `id` of every attribute and value it states.
- `(assert! (bug …))`: the same, stored.
- `(retract (bug bug:7f3a))` and `(retract! (bug bug:7f3a))` name only the entity. Every attribute of the concept on that entity is retracted: in this tick, or stored.

## Rules

`(rule head… (when premise…))`. `rule!` installs a rule, which then runs every tick; a `rule` belongs to its document. A head is a call, and with `!` it is a mutation.

Premises are calls, `(unless call)`, and formulas: `(id ?b title: ?t author: ?a)` binds `?b` to the `id` of those fields, `(== ?x ?y)` compares. Every head variable is bound by a premise. In a premise, `(bug …)` matches what holds now and `(bug! …)` what is stored.

An installed rule's `!` head commits only when the facts that triggered it came from a `!`. Otherwise its effect is listed in the result and not committed.

```clj
(rule! (bug! ?b title: ?t author: ?a status: :todo)
  (when (open (bug title: ?t author: ?a))
        (id ?b title: ?t author: ?a)))
```

`(open! (bug title: "Crash" author: "ada"))` then stores a bug whose entity is the `id` of its title and author.


## How `assert` and `retract` are defined

`?concept` stands for any concept and `… ?params` for all of its parameters except the primary. These four rules come with the language:

```clj
(rule (?concept! ?entity … ?params)                   ; stored stays stored
  (when (?concept! ?entity … ?params)
        (unless (retract! (?concept ?entity)))))

(rule (?concept ?entity … ?params)                    ; holds now: what is stored
  (when (?concept! ?entity … ?params)
        (unless (retract (?concept ?entity)))))

(rule (?concept ?entity … ?params)                    ; holds now: what is asserted now
  (when (assert (?concept … ?params))
        (id ?entity … ?params)
        (unless (retract (?concept ?entity)))))

(rule (?concept! ?entity … ?params)                   ; assert! stores, by content
  (when (assert! (?concept … ?params))
        (id ?entity … ?params)))
```

A modifier on a concept applies to each of the concept's attribute lines, so `(retract! (bug bug:7f3a))` retracts `xyz.bug/title`, `xyz.bug/author` and `xyz.bug/status` on `bug:7f3a`.

## Conflicts

Conflicts are never errors. Concurrent writes of one attribute are elected: the later write wins.

# Tasks

These concepts are installed:

```clj
(concept! (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (dialog.type ?l is: :entity)
  (dialog.type ?x is: :text)
  (dialog.type ?s is: [:open :done]))

(concept! (note ?n text: ?x)
  (xyz.todo ?n text: ?x)
  (dialog.type ?x is: :text))
```

Answer every task, numbered. Write code where a task asks for it, and a short reason for every answer.

1. Write an installed rule for a command `add`, so that `(add! (todo list: list:home text: "Buy milk"))` stores an open to-do whose entity is derived from its list and its text only.
2. With your rule from task 1 installed, two devices that have not synced each run `(add! (todo list: list:home text: "Buy milk"))`. After they sync, how many to-dos are stored?
3. With your rule installed, a document holds only `(add! (todo list: list:home text: "Buy milk"))`. After it and everything it triggers have run: (a) is a to-do stored? (b) is anything of the `add` left?
4. With your rule installed, a document holds only `(add (todo list: list:home text: "Buy milk"))`, with no `!`. After it runs: (a) is a to-do stored? (b) does running it need permission to write?
5. A document holds `(add (todo list: list:home text: "Buy milk"))` and `(query (todo ?t text: ?x))`. Does the query return a "Buy milk" to-do?
6. Write the form that marks the stored to-do `todo:9c1e` as done. You do not know its list or its text.
7. One document runs `(assert! (todo list: list:home text: "Buy milk" state: :open))`. A later document runs `(assert! (todo list: list:home text: "Buy milk" state: :done))`. How many to-dos are stored after both?
8. The entity `todo:9c1e` is a to-do and, because it has `xyz.todo/text`, also a note. A document runs `(retract! (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
9. With only your rule from task 1 installed, a document runs `(add! (todo list: list:home text: "Buy milk" state: :done))`. What happens?
10. Is `(todo todo:9c1e list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
11. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

