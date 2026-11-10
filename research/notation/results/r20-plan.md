# R20 plan: a modifier named after its concept

Written and committed before any reader ran. The tally will be in `r20.md` beside it.

**Amended after a false start:** the first ten readers were stopped before any answered, and nothing from them is kept. Both documents still showed a one-field change as an attribute write, `(xyz.bug/title! bug:7f3a is: …)`, which the design replaced with domain calls; so did R17 to R19's. The documents below write it through the domain, `(xyz.bug! bug:7f3a title: …)`, and say "field" where they said "attribute".

## What changed since R19

Nothing in the design. R19 readers named one thing most confusing: `(add! (todo …))` and `(todo! …)` are spelled alike, while a modifier's facts last one tick and a concept's are stored. Eleven of fifteen proposed marking modifiers apart. The maintainer proposed naming the modifier after its concept instead, with the concept's parameters flat: `(todo/add! list: … text: …)`, as dialog's concept operations were once spelled (`bug/report`).

## Variants

| variant | how a modifier is written | lines |
|---|---|---|
| `nested` | R19's `baseline` unchanged: `(add! (todo list: … text: …))`, `(retract! (todo todo:9c1e))` | 111 |
| `slash` | `(todo/add! list: … text: …)`, `(todo/retract! todo:9c1e)`; "modifiers nest one deep" is gone | 110 |

Everything else, the tasks included, is the same; the tasks use each variant's spelling.

## Scoring

R19's fifteen points, item for item, in each variant's spelling: 1a is the premise `(add (todo list: ?l text: ?x))` or `(todo/add list: ?l text: ?x)`, with no `!`. A one-field update in item 5 is `(xyz.todo! todo:<id> state: :done)`; a full `(todo! todo:<id> …)` or a command still scores. Item 13 is reported and counted, not scored.

## Bars

Five readers per variant, `haiku`, read-only, all started at once and interleaved.

- **Reads right:** both medians at least 13 of 15.
- **`slash` lands better** if at most 1 of its 5 readers names `!` or modifiers looking like concepts as most confusing, against R19's 5 of 5 for this document, with no item below 4 of 5 and a median no more than 1 below `nested`'s. If `nested`'s own readers this round name it at most once too, the difference is not the spelling.
- **The risk:** `todo/add` and the domain `xyz.todo` are both qualified names. The tally reports every reader who takes a command for a domain write or the other way round.

## What the readers read

<details><summary><code>nested</code> (sha256 <code>d3e38d6aea5895f8</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts about entities. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

## `!` means mutation

`(x! …)` is short for `(! (x …))`: a mutation. Without `!`, a fact holds in this tick only. A mutation is not seen in the tick that makes it: rules see it once this tick is over.

What reaches storage is decided by the top-level form that set it off. A top-level form with `!` is committed, together with every mutation its rules make. A top-level form without `!` is not: its result lists the mutations its rules would have made, uncommitted. A document with no `!` changes nothing and needs no permission to write.

In a rule, a head with `!` is a mutation, and whether it is committed is decided by the top-level form that set the rule off.

What a `!` leaves behind depends on what it is: a concept's facts are carried from tick to tick until retracted; a modifier's facts are seen once by the rules they trigger, and then gone.

## Concepts

A concept's facts are stored, and carried from tick to tick until retracted.

```clj
(concept! (bug ?b title: ?t author: ?a status: ?s)
  (xyz.bug ?b title: ?t author: ?a status: ?s)
  (dialog.type ?t is: :text)
  (dialog.type ?a is: :text)
  (dialog.type ?s is: [:todo :done]))
```

`(xyz.bug ?b title: ?t …)` is a domain call: it reads the `title`, `author` and `status` of an entity in the `xyz.bug` domain. Each field keeps its latest value. A concept is a rule that runs both ways: read, its domain calls make the concept; written, writing the concept writes each field.

A fact names its entity and passes every parameter: `(bug! bug:7f3a title: "Crash" author: "ada" status: :todo)`. Leaving out the entity or any parameter is a compile error. To change some fields only, write through the domain, which takes any of its fields: `(xyz.bug! bug:7f3a title: "Crash on save")`.

## The top level

A top-level form is a fact, a modifier applied to a concept, `retract`, `query`, `rule`, `rule!` or `concept!`. A question is always a `query`:

```clj
(query (bug ?b title: ?t status: :todo))
```

A fact or a modifier at the top level must be ground: a variable in one is an error.

## Modifiers

A modifier is applied to a concept's shape: `(open (bug title: "Crash" author: "ada"))`.

- A modifier's facts are not the concept's facts. They are tagged with the modifier's name, so a `bug` query never sees them.
- They are never carried, stored or synced. Without `!` they hold in this tick; with `!` the rules they trigger see them once, after this tick.
- A modifier needs no declaration. The rules that read it decide what it takes: an application that passes a parameter no rule reads is a compile error.
- Modifiers nest one deep.

`retract` is a modifier that comes with the language. `(retract (bug bug:7f3a))` and `(retract! (bug bug:7f3a))` name only the entity. Every field of the concept on that entity is retracted.

## Creating things

Nothing gets an entity by itself. A new thing is made by a command whose rule computes its entity, with one of two formulas:

- `(id ?b title: ?t author: ?a)` binds `?b` to an entity derived from those fields. The same fields give the same entity, on every device.
- `(fresh ?b)` binds `?b` to a new entity, never equal to any other.

```clj
(rule! (bug! ?b title: ?t author: ?a status: :todo)
  (when (open (bug title: ?t author: ?a))
        (id ?b title: ?t author: ?a)))
```

`(open! (bug title: "Crash" author: "ada"))` then stores a bug whose entity is derived from its title and author.

## Rules

`(rule head… (when premise…))`. `rule!` installs a rule, which then runs every tick; a `rule` belongs to its document. A head is a call. Premises are calls, `(unless call)`, and formulas: `id`, `fresh`, and `(== ?x ?y)`. Every head variable is bound by a premise. A premise matches what holds now; `!` never appears in a premise. A head without `!` derives into this tick: a query in the same tick sees it, and it is never stored.

## Conflicts

Conflicts are never errors. Concurrent writes of one field are elected: the later write wins.

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
5. With your rule installed, someone wants to create a to-do "Call mom" on `list:home`, and in a later document mark it done. Write both documents.
6. Write an installed rule for a command `jot`, so that `(jot! (note text: "idea"))` stores a new note every time it runs, even when an earlier note has the same text.
7. A document holds `(todo! list: list:home text: "Buy milk" state: :open)`. What happens?
8. A document holds `(todo todo:9c1e list: list:home text: "Buy milk" state: :open)` and `(query (todo ?t text: ?x))`. (a) Does the query return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add! (todo list: list:home text: "Call mom"))`. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `text` in the `xyz.todo` domain, also a note. A document runs `(retract! (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a document runs `(add! (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

<details><summary><code>slash</code> (sha256 <code>a852698af2272dc5</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts about entities. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

## `!` means mutation

`(x! …)` is short for `(! (x …))`: a mutation. Without `!`, a fact holds in this tick only. A mutation is not seen in the tick that makes it: rules see it once this tick is over.

What reaches storage is decided by the top-level form that set it off. A top-level form with `!` is committed, together with every mutation its rules make. A top-level form without `!` is not: its result lists the mutations its rules would have made, uncommitted. A document with no `!` changes nothing and needs no permission to write.

In a rule, a head with `!` is a mutation, and whether it is committed is decided by the top-level form that set the rule off.

What a `!` leaves behind depends on what it is: a concept's facts are carried from tick to tick until retracted; a modifier's facts are seen once by the rules they trigger, and then gone.

## Concepts

A concept's facts are stored, and carried from tick to tick until retracted.

```clj
(concept! (bug ?b title: ?t author: ?a status: ?s)
  (xyz.bug ?b title: ?t author: ?a status: ?s)
  (dialog.type ?t is: :text)
  (dialog.type ?a is: :text)
  (dialog.type ?s is: [:todo :done]))
```

`(xyz.bug ?b title: ?t …)` is a domain call: it reads the `title`, `author` and `status` of an entity in the `xyz.bug` domain. Each field keeps its latest value. A concept is a rule that runs both ways: read, its domain calls make the concept; written, writing the concept writes each field.

A fact names its entity and passes every parameter: `(bug! bug:7f3a title: "Crash" author: "ada" status: :todo)`. Leaving out the entity or any parameter is a compile error. To change some fields only, write through the domain, which takes any of its fields: `(xyz.bug! bug:7f3a title: "Crash on save")`.

## The top level

A top-level form is a fact, a modifier applied to a concept, `retract`, `query`, `rule`, `rule!` or `concept!`. A question is always a `query`:

```clj
(query (bug ?b title: ?t status: :todo))
```

A fact or a modifier at the top level must be ground: a variable in one is an error.

## Modifiers

A modifier is named after the concept it applies to, and takes that concept's parameters: `(bug/open title: "Crash" author: "ada")` applies `open` to a bug's shape.

- A modifier's facts are not the concept's facts. They are tagged with the modifier's name, so a `bug` query never sees them.
- They are never carried, stored or synced. Without `!` they hold in this tick; with `!` the rules they trigger see them once, after this tick.
- A modifier needs no declaration. The rules that read it decide what it takes: an application that passes a parameter no rule reads is a compile error.

`retract` is a modifier that comes with every concept. `(bug/retract bug:7f3a)` and `(bug/retract! bug:7f3a)` name only the entity. Every field of the concept on that entity is retracted.

## Creating things

Nothing gets an entity by itself. A new thing is made by a command whose rule computes its entity, with one of two formulas:

- `(id ?b title: ?t author: ?a)` binds `?b` to an entity derived from those fields. The same fields give the same entity, on every device.
- `(fresh ?b)` binds `?b` to a new entity, never equal to any other.

```clj
(rule! (bug! ?b title: ?t author: ?a status: :todo)
  (when (bug/open title: ?t author: ?a)
        (id ?b title: ?t author: ?a)))
```

`(bug/open! title: "Crash" author: "ada")` then stores a bug whose entity is derived from its title and author.

## Rules

`(rule head… (when premise…))`. `rule!` installs a rule, which then runs every tick; a `rule` belongs to its document. A head is a call. Premises are calls, `(unless call)`, and formulas: `id`, `fresh`, and `(== ?x ?y)`. Every head variable is bound by a premise. A premise matches what holds now; `!` never appears in a premise. A head without `!` derives into this tick: a query in the same tick sees it, and it is never stored.

## Conflicts

Conflicts are never errors. Concurrent writes of one field are elected: the later write wins.

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

1. Write an installed rule for a command `todo/add`, so that `(todo/add! list: list:home text: "Buy milk")` stores an open to-do whose entity is derived from its list and its text only.
2. With your rule from task 1 installed, two devices that have not synced each run `(todo/add! list: list:home text: "Buy milk")`. After they sync, how many to-dos are stored?
3. With your rule installed, a document holds only `(todo/add! list: list:home text: "Buy milk")`. After it and everything it triggers have run: (a) is a to-do stored? (b) is anything of the `todo/add` left?
4. With your rule installed, a document holds only `(todo/add list: list:home text: "Buy milk")`, with no `!`. After it runs: (a) is a to-do stored? (b) does running it need permission to write?
5. With your rule installed, someone wants to create a to-do "Call mom" on `list:home`, and in a later document mark it done. Write both documents.
6. Write an installed rule for a command `note/jot`, so that `(note/jot! text: "idea")` stores a new note every time it runs, even when an earlier note has the same text.
7. A document holds `(todo! list: list:home text: "Buy milk" state: :open)`. What happens?
8. A document holds `(todo todo:9c1e list: list:home text: "Buy milk" state: :open)` and `(query (todo ?t text: ?x))`. (a) Does the query return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(todo/add list: list:home text: "Buy milk")` and `(todo/add! list: list:home text: "Call mom")`. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `text` in the `xyz.todo` domain, also a note. A document runs `(todo/retract! todo:9c1e)`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a document runs `(todo/add! list: list:home text: "Buy milk" state: :done)`. What happens?
13. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

