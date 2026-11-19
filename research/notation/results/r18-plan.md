# R18 plan: does `!` read better with one meaning?

Written and committed before any reader ran. The tally will be in `r18.md` beside it.

## What changed since R17

R17 scored 125 of 130, but every reader rated the notation 3 of 5 and 9 of 10 named `!` as the most confusing thing. The maintainer decided three of the cases they raised:

- **No `!` in a premise.** A premise matches what holds now. Two R17 readers had read `(add! …)` in a premise as "came from a `!`".
- **Committing belongs to the invocation.** A head with `!` always derives into the next tick. What is committed is decided by the top-level form that set it off: with `!` it is committed, without `!` its result lists what was derived, uncommitted.
- **A same-tick query sees what is derived into this tick,** not what is derived into the next.

The fourth case is tested here: a concept's `!` lasts until retracted and a modifier's lasts one tick. Would writing every concept through `(assert! (todo todo:9c1e …))`, so that only modifiers take `!`, read better?

## Variants

| variant | what the reader gets | lines |
|---|---|---|
| `short` | the R17 card with the three decisions above; concepts written `(todo! …)`, attributes `(xyz.todo/state! …)` | 109 |
| `assert` | the same card, but only modifiers take `!`: concepts written `(assert! (todo todo:9c1e …))` and attributes `(assert! (xyz.todo/state todo:9c1e is: :done))`; `assert`'s entity is optional, the content's `id` when left out | 109 |

The tasks are R17's with two added: 11 (a head without `!` is seen by a same-tick query and never stored) and 12 (a document mixing a `!` and a non-`!` invocation commits only the first).

## Scoring

Fifteen points per reader, from its first answer only. Items 1 to 10 are R17's, with the spelling of each variant: 1c is `(todo! ?t …)` in `short` and `(assert! (todo ?t …))` in `assert`, and 6 is `(xyz.todo/state! todo:9c1e is: :done)` in `short` and `(assert! (xyz.todo/state todo:9c1e is: :done))` in `assert`. 1a also requires no `!` in the premise.

| item | point when |
|---|---|
| 11 | (a) yes and (b) no |
| 12 | only "Call mom" |

Item 13, the rating and what is most confusing, is reported and counted, not scored.

## Bars

Five readers per variant, `haiku`, read-only, all started at once and interleaved. R17 is the baseline: clarity 3 from all ten readers, `!` named most confusing by nine.

- **Reads right:** both medians at least 13 of 15; otherwise the revision lost something, and the missed items say what.
- **Clarity improves** in a variant whose median rating is at least 4, or where at most 2 of 5 readers name `!` as most confusing.
- **`assert` beats `short`** if its median rating is at least 1 higher, or at least 2 fewer of its readers name `!`, with no median score more than 1 lower. If neither, keep `todo!`.
- **The decisions read:** no reader writes `!` in a premise (1a), and items 11 and 12 reach 4 of 5 in each variant.

A caution written before the run: every R17 reader gave 3. If every R18 reader gives 3 as well, the rating does not discriminate for this reader, and the count of readers naming `!` decides.

## What the readers read

<details><summary><code>short</code> (sha256 <code>af1b16cca071e54a</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts: an entity, an attribute and a value. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

## `!` means mutation

A fact either holds in this tick or is derived into the next one. `(x! …)` is short for `(! (x …))`: `x` goes into the next tick. Without `!`, it holds in this tick only.

What reaches storage is decided by the top-level form that set it off. A top-level form with `!` is committed, together with everything its rules derive into the next tick. A top-level form without `!` is not: its result lists what was derived into the next tick, uncommitted. A document with no `!` changes nothing and needs no permission to write.

How long a fact lasts once it arrives depends on what it is: a concept's facts are carried from tick to tick until retracted, a modifier's facts are not.

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

`(rule head… (when premise…))`. `rule!` installs a rule, which then runs every tick; a `rule` belongs to its document. A head is a call.

Premises are calls, `(unless call)`, and formulas: `(id ?b title: ?t author: ?a)` binds `?b` to the `id` of those fields, `(== ?x ?y)` compares. Every head variable is bound by a premise. A premise matches what holds now; `!` never appears in a premise.

A head with `!` derives into the next tick, and whether that is committed is decided by the top-level form that set the rule off. A head without `!` derives into this tick: a query in the same tick sees it, and it is never stored.

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
11. A document holds `(rule (assert (todo list: ?l text: ?x state: :open)) (when (add (todo list: ?l text: ?x))))`, `(add (todo list: list:home text: "Buy milk"))` and `(query (todo ?t text: ?x))`. (a) Does the query return a "Buy milk" to-do? (b) Is a to-do stored afterwards?
12. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add! (todo list: list:home text: "Call mom"))`. Which to-dos are stored afterwards?
13. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

<details><summary><code>assert</code> (sha256 <code>d38f09fb23b72554</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts: an entity, an attribute and a value. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

## `!` means mutation

A fact either holds in this tick or is derived into the next one. Only modifiers take `!`: `(m! …)` is short for `(! (m …))`, and puts `m` into the next tick. Without `!`, it holds in this tick only.

What reaches storage is decided by the top-level form that set it off. A top-level form with `!` is committed, together with everything its rules derive into the next tick. A top-level form without `!` is not: its result lists what was derived into the next tick, uncommitted. A document with no `!` changes nothing and needs no permission to write.

How long a fact lasts once it arrives depends on what it is: a concept's facts are carried from tick to tick until retracted, a modifier's facts are not.

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

A concept is written with `assert`, which passes every parameter:
`(assert! (bug bug:7f3a title: "Crash" author: "ada" status: :todo))`. A write that leaves a parameter out is a compile error. To change one attribute, assert the attribute: `(assert! (xyz.bug/title bug:7f3a is: "Crash on save"))`.

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

- `(assert (bug title: "Crash" author: "ada" status: :todo))`: the bug holds in this tick. `assert` takes every parameter of the concept. The entity is optional: when given it is used, and when left out it is the `id` of every attribute and value the form states.
- `(assert! (bug …))`: the same, stored.
- `(retract (bug bug:7f3a))` and `(retract! (bug bug:7f3a))` name only the entity. Every attribute of the concept on that entity is retracted: in this tick, or stored.

## Rules

`(rule head… (when premise…))`. `rule!` installs a rule, which then runs every tick; a `rule` belongs to its document. A head is a call: a modifier applied to a concept, or a relation the rule defines.

Premises are calls, `(unless call)`, and formulas: `(id ?b title: ?t author: ?a)` binds `?b` to the `id` of those fields, `(== ?x ?y)` compares. Every head variable is bound by a premise. A premise matches what holds now; `!` never appears in a premise.

A head with `!` derives into the next tick, and whether that is committed is decided by the top-level form that set the rule off. A head without `!` derives into this tick: a query in the same tick sees it, and it is never stored.

```clj
(rule! (assert! (bug ?b title: ?t author: ?a status: :todo))
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
11. A document holds `(rule (assert (todo list: ?l text: ?x state: :open)) (when (add (todo list: ?l text: ?x))))`, `(add (todo list: list:home text: "Buy milk"))` and `(query (todo ?t text: ?x))`. (a) Does the query return a "Buy milk" to-do? (b) Is a to-do stored afterwards?
12. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add! (todo list: list:home text: "Call mom"))`. Which to-dos are stored afterwards?
13. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

