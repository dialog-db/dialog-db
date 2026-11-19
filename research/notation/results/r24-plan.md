# R24 plan: `query`, `select` or `find` for the question form

Written and committed before any reader ran. The tally will be in `r24.md` beside it.

**Amended while the first three variants' readers ran, before any answered:** the maintainer asked to test `(? (todo ?t …))` too. The `mark` variant is added; its five readers start after the others, so they are not interleaved with them, and its task 18 offers four words, `?` among them. `?` also begins every variable, which this variant tests as well.

## What changed since R23

Nothing in the design: R23's `mode` document, where a document is submitted as a query or a transaction. That gives "query" two meanings, the question form `(query (todo ?t …))` and a way of submitting a document, and a question can sit in a document submitted as a transaction. The maintainer asked which word reads best for agents.

## Variants

| variant | the question form | lines |
|---|---|---|
| `query` | `(query (todo ?t …))`, R23's `mode` as it was | 120 |
| `select` | `(select (todo ?t …))` | 120 |
| `find` | `(find (todo ?t …))` | 120 |
| `mark` | `(? (todo ?t …))` | 120 |

Only the question form's word changes; the submission mode is called a query in all three. Task 17 is R23's replaced: a document holding an `add` and a question, submitted as a transaction. Task 18 asks which of the three words the reader would choose.

## Scoring

Nineteen points: R23's items 1a to 15 in each spelling, and:

| item | point when |
|---|---|
| 17 | (a) yes: the rule's `assert` head holds within the tick, so the question sees the to-do; and (b) yes: the transaction keeps it |

Items 16, the rating and what is most confusing, and 18, the word chosen, are reported and counted.

## Bars

Five readers per variant, `haiku`, read-only, all started at once and interleaved.

- **Reads right:** every median at least 16 of 19.
- **A word reads:** items 8, 9 and 17 at least 4 of 5 in its document, and at most 1 of its readers naming the word, or the question form against the submission mode, as most confusing.
- **The choice:** a word chosen by at least 8 of the 15 readers of `query`, `select` and `find`, or by at least 11 of all 20, is preferred. Each reader has just worked with one word, so a word chosen mostly by its own readers shows only that.

## What the readers read

<details><summary><code>query</code> (sha256 <code>587cdb181d5e14da</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts about entities, grouped into concepts. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a `query` or a premise, a parameter left out matches anything.

## Concepts

A concept's facts are stored, and carried from tick to tick until retracted.

```clj
(concept (bug ?b title: ?t author: ?a status: ?s)
  (xyz.bug ?b title: ?t author: ?a status: ?s)
  (dialog.type ?t is: :text)
  (dialog.type ?a is: :text)
  (dialog.type ?s is: [:todo :done]))
```

`(xyz.bug ?b title: ?t …)` is a domain call: it reads the `title`, `author` and `status` of an entity in the `xyz.bug` domain. Each field keeps its latest value. A concept is a rule that runs both ways: read, its domain calls make the concept; written, writing the concept writes each field.

## Commands

Everything that changes anything is a command applied to a concept: `(open (bug title: "Crash" author: "ada"))` issues the command `open` with a bug's shape. A command always wraps a concept or a domain.

- A command's own facts are not the concept's facts. They are tagged with the command's name, so a `bug` question never sees them, and they are never carried, stored or synced: rules see them once, and then they are gone.
- A command needs no declaration. The rules that read it decide what it takes: issuing one with a parameter no rule reads is a compile error.

`assert` and `retract` are the two commands that come with the language, and the only ones that change a concept:

- `(assert (bug bug:7f3a title: "Crash" author: "ada" status: :todo))` names the entity and passes every parameter. Leaving out the entity or a parameter is a compile error. To change some fields only, assert through the domain, which takes any of its fields: `(assert (xyz.bug bug:7f3a title: "Crash on save"))`.
- `(retract (bug bug:7f3a))` retracts every field of the concept on that entity. `(retract (xyz.bug bug:7f3a title: "Crash"))` retracts just that value.

Every other command changes concepts only through rules that issue `assert` or `retract`.

## Queries and transactions

A document is submitted in one of two ways. As a **query**, it runs and returns what it derived and what would have landed, and changes nothing, so it needs no permission to write. As a **transaction**, it runs the same way and then advances the replica's revision with everything that landed.

A form without `!` is deductive: it holds within this tick, and rules derive from it to a fixpoint. A form with `!` is inductive: it lands after this tick, where rules see it again. Top-level forms never carry `!`.

`assert` and `retract` are where concepts change. What is asserted holds in this tick and lands after it; what is retracted stops holding now and is gone after it. So a rule never needs `!` to keep a concept: `!` is for a rule head that issues a command again after this tick.

## The top level

A top-level form is a command, `query`, `rule` or `concept`. In a transaction, a top-level `rule` or `concept` is installed and runs from then on; in a query it lives only for that run. A question is always a `query`:

```clj
(query (bug ?b title: ?t status: :todo))
```

A command at the top level must be ground: a variable in one is an error.

## Creating things

Nothing gets an entity by itself. A new thing is made by a command whose rule computes its entity, with one of two formulas:

- `(id ?b title: ?t author: ?a)` binds `?b` to an entity derived from those fields. The same fields give the same entity, on every device.
- `(fresh ?b)` binds `?b` to a new entity, never equal to any other.

```clj
(rule (assert (bug ?b title: ?t author: ?a status: :todo))
  (when (open (bug title: ?t author: ?a))
        (id ?b title: ?t author: ?a)))
```

`(open (bug title: "Crash" author: "ada"))`, submitted as a transaction, then stores a bug whose entity is derived from its title and author.

## Rules

`(rule head… (when premise…))`. A head is a command the rule issues, or a relation the rule defines; a rule may have several heads. Premises are calls, `(unless call)`, and formulas: `id`, `fresh`, and `(== ?x ?y)`. Every head variable is bound by a premise. A premise matches what holds now; `!` never appears in a premise. A head without `!` holds in this tick; a head with `!` lands after it.

## Conflicts

Conflicts are never errors. Concurrent writes of one field are elected: the later write wins.


# Tasks

These concepts are installed:

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (dialog.type ?l is: :entity)
  (dialog.type ?x is: :text)
  (dialog.type ?s is: [:open :done]))

(concept (note ?n text: ?x)
  (xyz.todo ?n text: ?x)
  (dialog.type ?x is: :text))

(concept (archived ?a text: ?x)
  (xyz.archive ?a text: ?x)
  (dialog.type ?x is: :text))
```

Answer every task, numbered. Write code where a task asks for it, and a short reason for every answer.

1. Write the rule, as it would appear in a transaction that installs it, for a command `add`, so that `(add (todo list: list:home text: "Buy milk"))` submitted as a transaction stores an open to-do whose entity is derived from its list and its text only.
2. With your rule from task 1 installed, two devices that have not synced each submit `(add (todo list: list:home text: "Buy milk"))` as a transaction. After they sync, how many to-dos are stored?
3. With your rule installed, a transaction holds only `(add (todo list: list:home text: "Buy milk"))`. After it and everything it triggers have run: (a) is a to-do stored? (b) is anything of the `add` left?
4. With your rule installed, the same document is submitted as a query instead. After it runs: (a) is a to-do stored? (b) does running it need permission to write?
5. With your rule installed, someone wants to create a to-do "Call mom" on `list:home`, and in a later transaction mark it done. Write both documents.
6. Write the rule, as it would appear in a transaction that installs it, for a command `jot`, so that `(jot (note text: "idea"))` submitted as a transaction stores a new note every time, even when an earlier note has the same text.
7. A transaction holds `(assert (todo list: list:home text: "Buy milk" state: :open))`. What happens?
8. A document submitted as a query holds `(assert (todo todo:9c1e list: list:home text: "Buy milk" state: :open))` and `(query (todo ?t text: ?x))`. (a) Does the `query` return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add (todo list: list:home text: "Call mom"))`, and is submitted as a query. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `text` in the `xyz.todo` domain, also a note. A transaction runs `(retract (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a transaction runs `(add (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Write the form that takes back only the text "Buy milk" of the stored to-do `todo:9c1e`, leaving its list and its state as they are.
14. Write the rule, as it would appear in a transaction that installs it, for a command `archive`, so that `(archive (note note:3f1c))` submitted as a transaction removes the note `note:3f1c` and stores an `archived` with the same entity and the same text.
15. Write the rule, as it would appear in a transaction that installs it, for a command `remind`, so that once `(remind (todo todo:9c1e))` is submitted as a transaction, the `remind` is issued again every tick for as long as the to-do is open, and stops once it is done.
16. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
17. With only your rule from task 1 installed, a document holds `(add (todo list: list:home text: "Buy milk"))` and `(query (todo ?t text: ?x))`, and is submitted as a transaction. (a) Does the `query` return a "Buy milk" to-do? (b) Is a "Buy milk" to-do stored afterwards?
18. The question form could be spelled `(query …)`, `(select …)` or `(find …)`, with the same meaning. Which would you choose, and why?
````

</details>

<details><summary><code>select</code> (sha256 <code>e75eca0c7740bc23</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts about entities, grouped into concepts. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a `select` or a premise, a parameter left out matches anything.

## Concepts

A concept's facts are stored, and carried from tick to tick until retracted.

```clj
(concept (bug ?b title: ?t author: ?a status: ?s)
  (xyz.bug ?b title: ?t author: ?a status: ?s)
  (dialog.type ?t is: :text)
  (dialog.type ?a is: :text)
  (dialog.type ?s is: [:todo :done]))
```

`(xyz.bug ?b title: ?t …)` is a domain call: it reads the `title`, `author` and `status` of an entity in the `xyz.bug` domain. Each field keeps its latest value. A concept is a rule that runs both ways: read, its domain calls make the concept; written, writing the concept writes each field.

## Commands

Everything that changes anything is a command applied to a concept: `(open (bug title: "Crash" author: "ada"))` issues the command `open` with a bug's shape. A command always wraps a concept or a domain.

- A command's own facts are not the concept's facts. They are tagged with the command's name, so a `bug` question never sees them, and they are never carried, stored or synced: rules see them once, and then they are gone.
- A command needs no declaration. The rules that read it decide what it takes: issuing one with a parameter no rule reads is a compile error.

`assert` and `retract` are the two commands that come with the language, and the only ones that change a concept:

- `(assert (bug bug:7f3a title: "Crash" author: "ada" status: :todo))` names the entity and passes every parameter. Leaving out the entity or a parameter is a compile error. To change some fields only, assert through the domain, which takes any of its fields: `(assert (xyz.bug bug:7f3a title: "Crash on save"))`.
- `(retract (bug bug:7f3a))` retracts every field of the concept on that entity. `(retract (xyz.bug bug:7f3a title: "Crash"))` retracts just that value.

Every other command changes concepts only through rules that issue `assert` or `retract`.

## Queries and transactions

A document is submitted in one of two ways. As a **query**, it runs and returns what it derived and what would have landed, and changes nothing, so it needs no permission to write. As a **transaction**, it runs the same way and then advances the replica's revision with everything that landed.

A form without `!` is deductive: it holds within this tick, and rules derive from it to a fixpoint. A form with `!` is inductive: it lands after this tick, where rules see it again. Top-level forms never carry `!`.

`assert` and `retract` are where concepts change. What is asserted holds in this tick and lands after it; what is retracted stops holding now and is gone after it. So a rule never needs `!` to keep a concept: `!` is for a rule head that issues a command again after this tick.

## The top level

A top-level form is a command, `select`, `rule` or `concept`. In a transaction, a top-level `rule` or `concept` is installed and runs from then on; in a query it lives only for that run. A question is always a `select`:

```clj
(select (bug ?b title: ?t status: :todo))
```

A command at the top level must be ground: a variable in one is an error.

## Creating things

Nothing gets an entity by itself. A new thing is made by a command whose rule computes its entity, with one of two formulas:

- `(id ?b title: ?t author: ?a)` binds `?b` to an entity derived from those fields. The same fields give the same entity, on every device.
- `(fresh ?b)` binds `?b` to a new entity, never equal to any other.

```clj
(rule (assert (bug ?b title: ?t author: ?a status: :todo))
  (when (open (bug title: ?t author: ?a))
        (id ?b title: ?t author: ?a)))
```

`(open (bug title: "Crash" author: "ada"))`, submitted as a transaction, then stores a bug whose entity is derived from its title and author.

## Rules

`(rule head… (when premise…))`. A head is a command the rule issues, or a relation the rule defines; a rule may have several heads. Premises are calls, `(unless call)`, and formulas: `id`, `fresh`, and `(== ?x ?y)`. Every head variable is bound by a premise. A premise matches what holds now; `!` never appears in a premise. A head without `!` holds in this tick; a head with `!` lands after it.

## Conflicts

Conflicts are never errors. Concurrent writes of one field are elected: the later write wins.


# Tasks

These concepts are installed:

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (dialog.type ?l is: :entity)
  (dialog.type ?x is: :text)
  (dialog.type ?s is: [:open :done]))

(concept (note ?n text: ?x)
  (xyz.todo ?n text: ?x)
  (dialog.type ?x is: :text))

(concept (archived ?a text: ?x)
  (xyz.archive ?a text: ?x)
  (dialog.type ?x is: :text))
```

Answer every task, numbered. Write code where a task asks for it, and a short reason for every answer.

1. Write the rule, as it would appear in a transaction that installs it, for a command `add`, so that `(add (todo list: list:home text: "Buy milk"))` submitted as a transaction stores an open to-do whose entity is derived from its list and its text only.
2. With your rule from task 1 installed, two devices that have not synced each submit `(add (todo list: list:home text: "Buy milk"))` as a transaction. After they sync, how many to-dos are stored?
3. With your rule installed, a transaction holds only `(add (todo list: list:home text: "Buy milk"))`. After it and everything it triggers have run: (a) is a to-do stored? (b) is anything of the `add` left?
4. With your rule installed, the same document is submitted as a query instead. After it runs: (a) is a to-do stored? (b) does running it need permission to write?
5. With your rule installed, someone wants to create a to-do "Call mom" on `list:home`, and in a later transaction mark it done. Write both documents.
6. Write the rule, as it would appear in a transaction that installs it, for a command `jot`, so that `(jot (note text: "idea"))` submitted as a transaction stores a new note every time, even when an earlier note has the same text.
7. A transaction holds `(assert (todo list: list:home text: "Buy milk" state: :open))`. What happens?
8. A document submitted as a query holds `(assert (todo todo:9c1e list: list:home text: "Buy milk" state: :open))` and `(select (todo ?t text: ?x))`. (a) Does the `select` return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add (todo list: list:home text: "Call mom"))`, and is submitted as a query. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `text` in the `xyz.todo` domain, also a note. A transaction runs `(retract (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a transaction runs `(add (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Write the form that takes back only the text "Buy milk" of the stored to-do `todo:9c1e`, leaving its list and its state as they are.
14. Write the rule, as it would appear in a transaction that installs it, for a command `archive`, so that `(archive (note note:3f1c))` submitted as a transaction removes the note `note:3f1c` and stores an `archived` with the same entity and the same text.
15. Write the rule, as it would appear in a transaction that installs it, for a command `remind`, so that once `(remind (todo todo:9c1e))` is submitted as a transaction, the `remind` is issued again every tick for as long as the to-do is open, and stops once it is done.
16. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
17. With only your rule from task 1 installed, a document holds `(add (todo list: list:home text: "Buy milk"))` and `(select (todo ?t text: ?x))`, and is submitted as a transaction. (a) Does the `select` return a "Buy milk" to-do? (b) Is a "Buy milk" to-do stored afterwards?
18. The question form could be spelled `(query …)`, `(select …)` or `(find …)`, with the same meaning. Which would you choose, and why?
````

</details>

<details><summary><code>find</code> (sha256 <code>6340717639ed8fe1</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts about entities, grouped into concepts. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a `find` or a premise, a parameter left out matches anything.

## Concepts

A concept's facts are stored, and carried from tick to tick until retracted.

```clj
(concept (bug ?b title: ?t author: ?a status: ?s)
  (xyz.bug ?b title: ?t author: ?a status: ?s)
  (dialog.type ?t is: :text)
  (dialog.type ?a is: :text)
  (dialog.type ?s is: [:todo :done]))
```

`(xyz.bug ?b title: ?t …)` is a domain call: it reads the `title`, `author` and `status` of an entity in the `xyz.bug` domain. Each field keeps its latest value. A concept is a rule that runs both ways: read, its domain calls make the concept; written, writing the concept writes each field.

## Commands

Everything that changes anything is a command applied to a concept: `(open (bug title: "Crash" author: "ada"))` issues the command `open` with a bug's shape. A command always wraps a concept or a domain.

- A command's own facts are not the concept's facts. They are tagged with the command's name, so a `bug` question never sees them, and they are never carried, stored or synced: rules see them once, and then they are gone.
- A command needs no declaration. The rules that read it decide what it takes: issuing one with a parameter no rule reads is a compile error.

`assert` and `retract` are the two commands that come with the language, and the only ones that change a concept:

- `(assert (bug bug:7f3a title: "Crash" author: "ada" status: :todo))` names the entity and passes every parameter. Leaving out the entity or a parameter is a compile error. To change some fields only, assert through the domain, which takes any of its fields: `(assert (xyz.bug bug:7f3a title: "Crash on save"))`.
- `(retract (bug bug:7f3a))` retracts every field of the concept on that entity. `(retract (xyz.bug bug:7f3a title: "Crash"))` retracts just that value.

Every other command changes concepts only through rules that issue `assert` or `retract`.

## Queries and transactions

A document is submitted in one of two ways. As a **query**, it runs and returns what it derived and what would have landed, and changes nothing, so it needs no permission to write. As a **transaction**, it runs the same way and then advances the replica's revision with everything that landed.

A form without `!` is deductive: it holds within this tick, and rules derive from it to a fixpoint. A form with `!` is inductive: it lands after this tick, where rules see it again. Top-level forms never carry `!`.

`assert` and `retract` are where concepts change. What is asserted holds in this tick and lands after it; what is retracted stops holding now and is gone after it. So a rule never needs `!` to keep a concept: `!` is for a rule head that issues a command again after this tick.

## The top level

A top-level form is a command, `find`, `rule` or `concept`. In a transaction, a top-level `rule` or `concept` is installed and runs from then on; in a query it lives only for that run. A question is always a `find`:

```clj
(find (bug ?b title: ?t status: :todo))
```

A command at the top level must be ground: a variable in one is an error.

## Creating things

Nothing gets an entity by itself. A new thing is made by a command whose rule computes its entity, with one of two formulas:

- `(id ?b title: ?t author: ?a)` binds `?b` to an entity derived from those fields. The same fields give the same entity, on every device.
- `(fresh ?b)` binds `?b` to a new entity, never equal to any other.

```clj
(rule (assert (bug ?b title: ?t author: ?a status: :todo))
  (when (open (bug title: ?t author: ?a))
        (id ?b title: ?t author: ?a)))
```

`(open (bug title: "Crash" author: "ada"))`, submitted as a transaction, then stores a bug whose entity is derived from its title and author.

## Rules

`(rule head… (when premise…))`. A head is a command the rule issues, or a relation the rule defines; a rule may have several heads. Premises are calls, `(unless call)`, and formulas: `id`, `fresh`, and `(== ?x ?y)`. Every head variable is bound by a premise. A premise matches what holds now; `!` never appears in a premise. A head without `!` holds in this tick; a head with `!` lands after it.

## Conflicts

Conflicts are never errors. Concurrent writes of one field are elected: the later write wins.


# Tasks

These concepts are installed:

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (dialog.type ?l is: :entity)
  (dialog.type ?x is: :text)
  (dialog.type ?s is: [:open :done]))

(concept (note ?n text: ?x)
  (xyz.todo ?n text: ?x)
  (dialog.type ?x is: :text))

(concept (archived ?a text: ?x)
  (xyz.archive ?a text: ?x)
  (dialog.type ?x is: :text))
```

Answer every task, numbered. Write code where a task asks for it, and a short reason for every answer.

1. Write the rule, as it would appear in a transaction that installs it, for a command `add`, so that `(add (todo list: list:home text: "Buy milk"))` submitted as a transaction stores an open to-do whose entity is derived from its list and its text only.
2. With your rule from task 1 installed, two devices that have not synced each submit `(add (todo list: list:home text: "Buy milk"))` as a transaction. After they sync, how many to-dos are stored?
3. With your rule installed, a transaction holds only `(add (todo list: list:home text: "Buy milk"))`. After it and everything it triggers have run: (a) is a to-do stored? (b) is anything of the `add` left?
4. With your rule installed, the same document is submitted as a query instead. After it runs: (a) is a to-do stored? (b) does running it need permission to write?
5. With your rule installed, someone wants to create a to-do "Call mom" on `list:home`, and in a later transaction mark it done. Write both documents.
6. Write the rule, as it would appear in a transaction that installs it, for a command `jot`, so that `(jot (note text: "idea"))` submitted as a transaction stores a new note every time, even when an earlier note has the same text.
7. A transaction holds `(assert (todo list: list:home text: "Buy milk" state: :open))`. What happens?
8. A document submitted as a query holds `(assert (todo todo:9c1e list: list:home text: "Buy milk" state: :open))` and `(find (todo ?t text: ?x))`. (a) Does the `find` return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add (todo list: list:home text: "Call mom"))`, and is submitted as a query. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `text` in the `xyz.todo` domain, also a note. A transaction runs `(retract (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a transaction runs `(add (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Write the form that takes back only the text "Buy milk" of the stored to-do `todo:9c1e`, leaving its list and its state as they are.
14. Write the rule, as it would appear in a transaction that installs it, for a command `archive`, so that `(archive (note note:3f1c))` submitted as a transaction removes the note `note:3f1c` and stores an `archived` with the same entity and the same text.
15. Write the rule, as it would appear in a transaction that installs it, for a command `remind`, so that once `(remind (todo todo:9c1e))` is submitted as a transaction, the `remind` is issued again every tick for as long as the to-do is open, and stops once it is done.
16. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
17. With only your rule from task 1 installed, a document holds `(add (todo list: list:home text: "Buy milk"))` and `(find (todo ?t text: ?x))`, and is submitted as a transaction. (a) Does the `find` return a "Buy milk" to-do? (b) Is a "Buy milk" to-do stored afterwards?
18. The question form could be spelled `(query …)`, `(select …)` or `(find …)`, with the same meaning. Which would you choose, and why?
````

</details>

<details><summary><code>mark</code> (sha256 <code>e9749e1c932a6a26</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts about entities, grouped into concepts. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a `?` or a premise, a parameter left out matches anything.

## Concepts

A concept's facts are stored, and carried from tick to tick until retracted.

```clj
(concept (bug ?b title: ?t author: ?a status: ?s)
  (xyz.bug ?b title: ?t author: ?a status: ?s)
  (dialog.type ?t is: :text)
  (dialog.type ?a is: :text)
  (dialog.type ?s is: [:todo :done]))
```

`(xyz.bug ?b title: ?t …)` is a domain call: it reads the `title`, `author` and `status` of an entity in the `xyz.bug` domain. Each field keeps its latest value. A concept is a rule that runs both ways: read, its domain calls make the concept; written, writing the concept writes each field.

## Commands

Everything that changes anything is a command applied to a concept: `(open (bug title: "Crash" author: "ada"))` issues the command `open` with a bug's shape. A command always wraps a concept or a domain.

- A command's own facts are not the concept's facts. They are tagged with the command's name, so a `bug` question never sees them, and they are never carried, stored or synced: rules see them once, and then they are gone.
- A command needs no declaration. The rules that read it decide what it takes: issuing one with a parameter no rule reads is a compile error.

`assert` and `retract` are the two commands that come with the language, and the only ones that change a concept:

- `(assert (bug bug:7f3a title: "Crash" author: "ada" status: :todo))` names the entity and passes every parameter. Leaving out the entity or a parameter is a compile error. To change some fields only, assert through the domain, which takes any of its fields: `(assert (xyz.bug bug:7f3a title: "Crash on save"))`.
- `(retract (bug bug:7f3a))` retracts every field of the concept on that entity. `(retract (xyz.bug bug:7f3a title: "Crash"))` retracts just that value.

Every other command changes concepts only through rules that issue `assert` or `retract`.

## Queries and transactions

A document is submitted in one of two ways. As a **query**, it runs and returns what it derived and what would have landed, and changes nothing, so it needs no permission to write. As a **transaction**, it runs the same way and then advances the replica's revision with everything that landed.

A form without `!` is deductive: it holds within this tick, and rules derive from it to a fixpoint. A form with `!` is inductive: it lands after this tick, where rules see it again. Top-level forms never carry `!`.

`assert` and `retract` are where concepts change. What is asserted holds in this tick and lands after it; what is retracted stops holding now and is gone after it. So a rule never needs `!` to keep a concept: `!` is for a rule head that issues a command again after this tick.

## The top level

A top-level form is a command, `?`, `rule` or `concept`. In a transaction, a top-level `rule` or `concept` is installed and runs from then on; in a query it lives only for that run. A question is always a `?`:

```clj
(? (bug ?b title: ?t status: :todo))
```

A command at the top level must be ground: a variable in one is an error.

## Creating things

Nothing gets an entity by itself. A new thing is made by a command whose rule computes its entity, with one of two formulas:

- `(id ?b title: ?t author: ?a)` binds `?b` to an entity derived from those fields. The same fields give the same entity, on every device.
- `(fresh ?b)` binds `?b` to a new entity, never equal to any other.

```clj
(rule (assert (bug ?b title: ?t author: ?a status: :todo))
  (when (open (bug title: ?t author: ?a))
        (id ?b title: ?t author: ?a)))
```

`(open (bug title: "Crash" author: "ada"))`, submitted as a transaction, then stores a bug whose entity is derived from its title and author.

## Rules

`(rule head… (when premise…))`. A head is a command the rule issues, or a relation the rule defines; a rule may have several heads. Premises are calls, `(unless call)`, and formulas: `id`, `fresh`, and `(== ?x ?y)`. Every head variable is bound by a premise. A premise matches what holds now; `!` never appears in a premise. A head without `!` holds in this tick; a head with `!` lands after it.

## Conflicts

Conflicts are never errors. Concurrent writes of one field are elected: the later write wins.


# Tasks

These concepts are installed:

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (dialog.type ?l is: :entity)
  (dialog.type ?x is: :text)
  (dialog.type ?s is: [:open :done]))

(concept (note ?n text: ?x)
  (xyz.todo ?n text: ?x)
  (dialog.type ?x is: :text))

(concept (archived ?a text: ?x)
  (xyz.archive ?a text: ?x)
  (dialog.type ?x is: :text))
```

Answer every task, numbered. Write code where a task asks for it, and a short reason for every answer.

1. Write the rule, as it would appear in a transaction that installs it, for a command `add`, so that `(add (todo list: list:home text: "Buy milk"))` submitted as a transaction stores an open to-do whose entity is derived from its list and its text only.
2. With your rule from task 1 installed, two devices that have not synced each submit `(add (todo list: list:home text: "Buy milk"))` as a transaction. After they sync, how many to-dos are stored?
3. With your rule installed, a transaction holds only `(add (todo list: list:home text: "Buy milk"))`. After it and everything it triggers have run: (a) is a to-do stored? (b) is anything of the `add` left?
4. With your rule installed, the same document is submitted as a query instead. After it runs: (a) is a to-do stored? (b) does running it need permission to write?
5. With your rule installed, someone wants to create a to-do "Call mom" on `list:home`, and in a later transaction mark it done. Write both documents.
6. Write the rule, as it would appear in a transaction that installs it, for a command `jot`, so that `(jot (note text: "idea"))` submitted as a transaction stores a new note every time, even when an earlier note has the same text.
7. A transaction holds `(assert (todo list: list:home text: "Buy milk" state: :open))`. What happens?
8. A document submitted as a query holds `(assert (todo todo:9c1e list: list:home text: "Buy milk" state: :open))` and `(? (todo ?t text: ?x))`. (a) Does the `?` return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add (todo list: list:home text: "Call mom"))`, and is submitted as a query. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `text` in the `xyz.todo` domain, also a note. A transaction runs `(retract (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a transaction runs `(add (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Write the form that takes back only the text "Buy milk" of the stored to-do `todo:9c1e`, leaving its list and its state as they are.
14. Write the rule, as it would appear in a transaction that installs it, for a command `archive`, so that `(archive (note note:3f1c))` submitted as a transaction removes the note `note:3f1c` and stores an `archived` with the same entity and the same text.
15. Write the rule, as it would appear in a transaction that installs it, for a command `remind`, so that once `(remind (todo todo:9c1e))` is submitted as a transaction, the `remind` is issued again every tick for as long as the to-do is open, and stops once it is done.
16. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
17. With only your rule from task 1 installed, a document holds `(add (todo list: list:home text: "Buy milk"))` and `(? (todo ?t text: ?x))`, and is submitted as a transaction. (a) Does the `?` return a "Buy milk" to-do? (b) Is a "Buy milk" to-do stored afterwards?
18. The question form could be spelled `(? …)`, `(query …)`, `(select …)` or `(find …)`, with the same meaning. Which would you choose, and why?
````

</details>

