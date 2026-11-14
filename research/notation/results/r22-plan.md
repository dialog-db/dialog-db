# R22 plan: commands, with `assert` and `retract` the only ones that change a concept

Written and committed before any reader ran. The tally will be in `r22.md` beside it.

## What changed since R21

The maintainer's decisions after R21:

- What were modifiers are **commands**, written around their concept, `(add! (todo …))`, which the maintainer prefers to `(todo/add! …)`.
- **`assert` comes back, with an entity required**, as the command that writes a concept, beside `retract`. They are the only commands that change a concept; every other command does so through rules that issue them. There is no `(todo! …)`: every `!` is on a command. This is Bloom's model: a table changes only through `<+` and `<-`, and channels carry the messages rules react to.
- **`retract` takes fields**, so one value can be taken back: `(retract (xyz.todo todo:9c1e text: "Buy milk"))`. Without fields it retracts every field of the concept.
- R21's `scope` wording: without `!` a command is scoped to the transaction, with `!` it is global.

## Variants

| variant | what the reader gets | lines |
|---|---|---|
| `baseline` | R21's `scope` document: `(todo! …)` writes, `(todo/add! …)` commands, `(todo/retract! todo:9c1e)` by entity only | 115 |
| `command` | the design above | 114 |

The tasks are R21's in each variant's spelling, plus two: 13, take back one value; 14, a rule that archives a note by retracting it and storing an `archived`. A concept `archived` is installed in both.

## Scoring

Seventeen points per reader. Items 1 to 12 as R21's, in each spelling: in `command`, 1c is `(assert! (todo ?t …))`, 5's update `(assert! (xyz.todo todo:<id> state: :done))`, 7 a compile error for an `assert` with no entity, and 8 a scoped `(assert (todo todo:9c1e …))` seen by the same-tick query and not stored.

| item | point when |
|---|---|
| 13 | `command`: `(retract! (xyz.todo todo:9c1e text: "Buy milk"))`. `baseline`: the reader says its document has no form for it, since a retract names only the entity |
| 14 | the rule is installed, reads the archive command on the note and the note's text, and has two heads: one storing `archived` with the note's entity and text, one retracting the note |

Item 15 is reported and counted, not scored.

## Bars

Five readers per variant, `haiku`, read-only, all started at once and interleaved.

- **Reads right:** both medians at least 14 of 17.
- **`command` holds:** its median is at least `baseline`'s, items 13 and 14 reach 4 of 5, and no item is below 4 of 5.
- **`command` reads better:** additionally, at most 2 of its 5 readers name `!` or how writes and commands are spelled as most confusing, against R21's 5 of 5 for `scope`.

## What the readers read

<details><summary><code>baseline</code> (sha256 <code>4513abf3202a0fba</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts about entities. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

## `!` makes an assertion global

Every document runs as a transaction. An assertion without `!` is scoped to the transaction: it holds while the transaction runs, and is discarded with it. An assertion with `!` is global: it reaches the replica, and everyone who reads the replica. `(x! …)` is short for `(! (x …))`. Rules see a global assertion once this tick is over.

A rule's head with `!` is global when a global assertion set the rule off. When a scoped assertion set it off, the head stays scoped to the transaction: the result lists what it would have made global, and nothing changes. A document with no `!` changes nothing and needs no permission to write.

What a global assertion leaves behind depends on what it is: a concept's facts are carried from tick to tick until retracted; a modifier's facts are seen once by the rules they trigger, and then gone.

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

(concept! (archived ?a text: ?x)
  (xyz.archive ?a text: ?x)
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
13. Write the form that takes back only the text "Buy milk" of the stored to-do `todo:9c1e`, leaving its list and its state as they are.
14. Write an installed rule for a command `note/archive`, so that `(note/archive! note:3f1c)` removes the note `note:3f1c` and stores an `archived` with the same entity and the same text.
15. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

<details><summary><code>command</code> (sha256 <code>75088041e3e7120b</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts about entities, grouped into concepts. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

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

## Commands

Everything that changes anything is a command applied to a concept: `(open (bug title: "Crash" author: "ada"))` issues the command `open` with a bug's shape. A command always wraps a concept or a domain.

- A command's own facts are not the concept's facts. They are tagged with the command's name, so a `bug` query never sees them, and they are never carried, stored or synced: rules see them once, and then they are gone.
- A command needs no declaration. The rules that read it decide what it takes: issuing one with a parameter no rule reads is a compile error.

`assert` and `retract` are the two commands that come with the language, and the only ones that change a concept:

- `(assert (bug bug:7f3a title: "Crash" author: "ada" status: :todo))` names the entity and passes every parameter. Leaving out the entity or a parameter is a compile error. To change some fields only, assert through the domain, which takes any of its fields: `(assert (xyz.bug bug:7f3a title: "Crash on save"))`.
- `(retract (bug bug:7f3a))` retracts every field of the concept on that entity. `(retract (xyz.bug bug:7f3a title: "Crash"))` retracts just that value.

Every other command changes concepts only through rules that issue `assert` or `retract`.

## `!` makes a command global

Every document runs as a transaction. A command without `!` is scoped to the transaction: it holds while the transaction runs, and is discarded with it. A command with `!` is global: it reaches the replica, and everyone who reads the replica. `(x! …)` is short for `(! (x …))`. Rules see a global command once this tick is over.

A rule's head with `!` is global when a global command set the rule off. When a scoped command set it off, the head stays scoped to the transaction: the result lists what it would have made global, and nothing changes. A document with no `!` changes nothing and needs no permission to write.

## The top level

A top-level form is a command, `query`, `rule`, `rule!` or `concept!`. A question is always a `query`:

```clj
(query (bug ?b title: ?t status: :todo))
```

A command at the top level must be ground: a variable in one is an error.

## Creating things

Nothing gets an entity by itself. A new thing is made by a command whose rule computes its entity, with one of two formulas:

- `(id ?b title: ?t author: ?a)` binds `?b` to an entity derived from those fields. The same fields give the same entity, on every device.
- `(fresh ?b)` binds `?b` to a new entity, never equal to any other.

```clj
(rule! (assert! (bug ?b title: ?t author: ?a status: :todo))
  (when (open (bug title: ?t author: ?a))
        (id ?b title: ?t author: ?a)))
```

`(open! (bug title: "Crash" author: "ada"))` then stores a bug whose entity is derived from its title and author.

## Rules

`(rule head… (when premise…))`. `rule!` installs a rule, which then runs every tick; a `rule` belongs to its document. A head is a command the rule issues, or a relation the rule defines; a rule may have several heads. Premises are calls, `(unless call)`, and formulas: `id`, `fresh`, and `(== ?x ?y)`. Every head variable is bound by a premise. A premise matches what holds now; `!` never appears in a premise. A head without `!` holds in this tick only: a query in the same tick sees it, and it is never stored.

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

(concept! (archived ?a text: ?x)
  (xyz.archive ?a text: ?x)
  (dialog.type ?x is: :text))
```

Answer every task, numbered. Write code where a task asks for it, and a short reason for every answer.

1. Write an installed rule for a command `add`, so that `(add! (todo list: list:home text: "Buy milk"))` stores an open to-do whose entity is derived from its list and its text only.
2. With your rule from task 1 installed, two devices that have not synced each run `(add! (todo list: list:home text: "Buy milk"))`. After they sync, how many to-dos are stored?
3. With your rule installed, a document holds only `(add! (todo list: list:home text: "Buy milk"))`. After it and everything it triggers have run: (a) is a to-do stored? (b) is anything of the `add` left?
4. With your rule installed, a document holds only `(add (todo list: list:home text: "Buy milk"))`, with no `!`. After it runs: (a) is a to-do stored? (b) does running it need permission to write?
5. With your rule installed, someone wants to create a to-do "Call mom" on `list:home`, and in a later document mark it done. Write both documents.
6. Write an installed rule for a command `jot`, so that `(jot! (note text: "idea"))` stores a new note every time it runs, even when an earlier note has the same text.
7. A document holds `(assert! (todo list: list:home text: "Buy milk" state: :open))`. What happens?
8. A document holds `(assert (todo todo:9c1e list: list:home text: "Buy milk" state: :open))` and `(query (todo ?t text: ?x))`. (a) Does the query return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add! (todo list: list:home text: "Call mom"))`. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `text` in the `xyz.todo` domain, also a note. A document runs `(retract! (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a document runs `(add! (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Write the form that takes back only the text "Buy milk" of the stored to-do `todo:9c1e`, leaving its list and its state as they are.
14. Write an installed rule for a command `archive`, so that `(archive! (note note:3f1c))` removes the note `note:3f1c` and stores an `archived` with the same entity and the same text.
15. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

