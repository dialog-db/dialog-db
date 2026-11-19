# R23 plan: `!` as induction, with the commit said by the document or by the mode

Written and committed before any reader ran. The tally will be in `r23.md` beside it.

## What changed since R22

The maintainer's decisions after R22: `!` marks induction, the same at the top level and in a rule head; rules run as plain Datalog, so a head without `!` is deductive (this tick, to a fixpoint) and one with `!` inductive (it lands after this tick). Nothing is gated on what set a rule off. What is open is where the commit is decided, so this round compares the two ways the maintainer proposed.

## Variants

| variant | where the commit is decided | lines |
|---|---|---|
| `syntax` | in the text: a document with a `!` at the top level is a transaction and advances the revision with everything its run lands; without one it is a query, which returns what would have landed. Rule heads write `(assert! (todo …))`; installing is `rule!`, `concept!` | 117 |
| `mode` | by how the document is submitted, as a query or a transaction. Top-level forms never carry `!`; a top-level `rule` or `concept` in a transaction is installed. `assert` holds now and lands after the tick, so heads write `(assert (todo …))`; `!` is only on a head that issues a command again after this tick | 119 |

Both documents are R22's `command` document with only the section on `!` and the forms it touches changed. Task 15 is new: a `remind` command a rule issues again every tick while the to-do is open, which needs induction. Task 17 shows each reader a summary of the other variant and asks which it would rather write in.

## Scoring

Eighteen points, items 1a to 1d and 2 to 15, in each variant's spelling. Where the variants' answers differ:

| item | `syntax` | `mode` |
|---|---|---|
| 1c | `(assert! (todo ?t …))` | `(assert (todo ?t …))` |
| 1d | `rule!` | a top-level `rule` |
| 4 | not stored; no permission | not stored; no permission (a query) |
| 10 | both to-dos: the document has a top-level `!`, and the rule's `!` head fired for both | none: a query keeps nothing |
| 13, 14 | `retract!`, `assert!` | `retract`, `assert` |
| 15 | a head `(remind! (todo ?t))` with a premise that the to-do is open | the same |

Everything else as R22. Items 16, the rating and what is most confusing, and 17, the preference, are reported and counted.

## Bars

Five readers per variant, `haiku`, read-only, all started at once and interleaved.

- **Reads right:** both medians at least 15 of 18.
- **Induction reads:** item 15 at least 4 of 5 in each.
- **The preference:** a variant is preferred when at least 7 of the 10 readers choose it, counting both groups. Each reader has just worked in its own variant, which biases it towards A; if the two groups each prefer their own, the preference shows nothing beyond that.

## What the readers read

<details><summary><code>syntax</code> (sha256 <code>3d1500837a4468af</code>)</summary>

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

## `!` is induction

A form without `!` is deductive: it holds within this tick, and rules derive from it to a fixpoint. A form with `!` is inductive: it lands after this tick, where rules see it again. `(x! …)` is short for `(! (x …))`. This is the same at the top level and in a rule head.

A document with a `!` at the top level is a transaction: it advances the replica's revision with everything its run lands, including what its rules land. A document with no `!` at the top level is a query: it returns what it derived and what would have landed, and advances nothing, so it needs no permission to write.

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

`(rule head… (when premise…))`. `rule!` installs a rule, which then runs every tick; a `rule` belongs to its document. A head is a command the rule issues, or a relation the rule defines; a rule may have several heads. Premises are calls, `(unless call)`, and formulas: `id`, `fresh`, and `(== ?x ?y)`. Every head variable is bound by a premise. A premise matches what holds now; `!` never appears in a premise. A head without `!` holds in this tick only: a query in the same tick sees it, and it never lands.

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
8. A document holds only `(assert (todo todo:9c1e list: list:home text: "Buy milk" state: :open))` and `(query (todo ?t text: ?x))`. (a) Does the query return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add! (todo list: list:home text: "Call mom"))`. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `text` in the `xyz.todo` domain, also a note. A document runs `(retract! (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a document runs `(add! (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Write the form that takes back only the text "Buy milk" of the stored to-do `todo:9c1e`, leaving its list and its state as they are.
14. Write an installed rule for a command `archive`, so that `(archive! (note note:3f1c))` removes the note `note:3f1c` and stores an `archived` with the same entity and the same text.
15. Write an installed rule for a command `remind`, so that once `(remind! (todo todo:9c1e))` is run, the `remind` is issued again every tick for as long as the to-do is open, and stops once it is done.
16. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
17. Here is another way the same notation could work. Instead of `!` at the top level, you choose how to submit a document: as a query, which keeps nothing, or as a transaction, which advances the revision. Top-level forms never carry `!`, and a `(concept …)` or `(rule …)` submitted in a transaction is installed. Rule heads write `(assert (todo …))`, and `!` appears only on a rule head that issues a command again after this tick. Would you rather write in the notation you just used (A) or this one (B)? Answer A or B, and why.
````

</details>

<details><summary><code>mode</code> (sha256 <code>4a44bb190f4bef79</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts about entities, grouped into concepts. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

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

- A command's own facts are not the concept's facts. They are tagged with the command's name, so a `bug` query never sees them, and they are never carried, stored or synced: rules see them once, and then they are gone.
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
8. A query holds `(assert (todo todo:9c1e list: list:home text: "Buy milk" state: :open))` and `(query (todo ?t text: ?x))`. (a) Does the query return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add (todo list: list:home text: "Call mom"))`, and is submitted as a query. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `text` in the `xyz.todo` domain, also a note. A transaction runs `(retract (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a transaction runs `(add (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Write the form that takes back only the text "Buy milk" of the stored to-do `todo:9c1e`, leaving its list and its state as they are.
14. Write the rule, as it would appear in a transaction that installs it, for a command `archive`, so that `(archive (note note:3f1c))` submitted as a transaction removes the note `note:3f1c` and stores an `archived` with the same entity and the same text.
15. Write the rule, as it would appear in a transaction that installs it, for a command `remind`, so that once `(remind (todo todo:9c1e))` is submitted as a transaction, the `remind` is issued again every tick for as long as the to-do is open, and stops once it is done.
16. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
17. Here is another way the same notation could work. Instead of choosing how to submit a document, a document says it with `!`: a `!` on any top-level form, such as `(add! (todo …))`, makes the whole document a transaction, and with no `!` at the top level it is a query. Rule heads write `(assert! (todo …))`, and installing is written `rule!` and `concept!`. Would you rather write in the notation you just used (A) or this one (B)? Answer A or B, and why.
````

</details>

