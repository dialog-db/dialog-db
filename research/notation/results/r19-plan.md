# R19 plan: no content identity, and three ways to say what `!` does

Written and committed before any reader ran. The tally will be in `r19.md` beside it.

**Amended before any reader ran:** the maintainer asked that no document mention the next tick, which R17 and R18 readers found confusing. The `next` variant is dropped, and the other three say when rules see a `!` ("once this tick is over") without naming the next tick.

## What changed since R18

The maintainer's decisions after R18:

- **No content identity.** The built-in `assert` is gone. A fact names its entity and every parameter, `(todo! todo:9c1e …)`, and leaving the entity out is a compile error. A new thing is made by a command whose rule computes its entity: `(id ?t list: ?l text: ?x)`, the same on every device, or `(fresh ?n)`, never equal to another. `retract` stays the one built-in modifier.
- **A ground fact may stand at the top level.** `(todo todo:9c1e …)` holds this tick and is never stored; `(todo! …)` commits it. A variable in a top-level fact is an error: questions are `query`.

R17 and R18 readers named two things most confusing: content identity (1 of 10, then 3 of 10) and `!` (9 of 10, then 7 of 10). This round tests whether the first is gone, and two ways the maintainer proposed of saying what `!` does against R18's.

## Variants

The notation and the tasks are the same in every document; only the section that says what `!` does differs.

| variant | how `!` is explained | lines |
|---|---|---|
| `baseline` | R18's wording, without the next tick: `!` is mutation, rules see it once this tick is over, and the top-level form that set a rule off decides whether its effect is committed | 111 |
| `commit` | `!` is a commit that rules observe once this tick is over; only a top-level commit moves the branch, and without one the branch stays at its revision and the result lists the commits | 111 |
| `induction` | without `!` a form is deductive, with `!` inductive, carried beyond this tick; an inductive rule fires only on inductive assertions, and the evaluator reports what it would have done otherwise | 109 |

## Scoring

Fifteen points per reader, from its first answer only:

| item | point when |
|---|---|
| 1a | the premise reads `add` on a `todo` shape with list and text, with no `!` |
| 1b | the entity is `(id ?t list: ?l text: ?x)`, over list and text only |
| 1c | the head is `(todo! ?t …)` passing list, text and `state: :open` |
| 1d | `rule!` |
| 2 | one to-do |
| 3 | (a) yes and (b) nothing of the `add` |
| 4 | (a) no and (b) no permission needed |
| 5 | the second document changes the first's to-do, naming its entity (a URI, or one found by `query` or computed with `id`) with `!`; it does not create another to-do |
| 6 | the entity is `(fresh ?n)`, and the head is `(note! ?n text: ?x)` in a `rule!` |
| 7 | a compile error: the entity is missing |
| 8 | (a) yes and (b) no |
| 9 | no: a variable in a top-level fact is an error, and a question is a `query` |
| 10 | only "Call mom" |
| 11 | no: the text was retracted with the to-do |
| 12 | a compile error: no rule reads `state` on `add` |

Item 13, the rating and what is most confusing, is reported and counted, not scored. For item 5, the tally also reports whether the to-do was created through `add!` or by a direct `(todo! …)` with an entity the reader made up.

## Bars

Five readers per document, `haiku`, read-only, all started at once and interleaved.

- **Reads right:** every document's median at least 13 of 15.
- **Content identity is fixed:** item 5 at least 4 of 5 in every document, and across all fifteen readers at most 2 name identity as most confusing.
- **The new forms read:** items 6, 7, 8 and 9 at least 4 of 5 in every document.
- **A framing of `!` wins** when at most 1 of its 5 readers names `!` as most confusing, against R18's 7 of 10, with no item below 4 of 5. If several win, the one with the fewest; if none does, how `!` is explained is not what readers object to, and the explanation stays `baseline`'s.

## What the readers read

<details><summary><code>baseline</code> (sha256 <code>86c6201b61de3ed0</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts: an entity, an attribute and a value. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

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

`(xyz.bug ?b title: ?t …)` names the attributes `xyz.bug/title`, `xyz.bug/author` and `xyz.bug/status`. Each attribute keeps its latest value. A concept is a rule that runs both ways: read, the attributes make the concept; written, writing the concept writes each attribute.

A fact names its entity and passes every parameter: `(bug! bug:7f3a title: "Crash" author: "ada" status: :todo)`. Leaving out the entity or any parameter is a compile error. To change one attribute, write the attribute: `(xyz.bug/title! bug:7f3a is: "Crash on save")`.

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

`retract` is a modifier that comes with the language. `(retract (bug bug:7f3a))` and `(retract! (bug bug:7f3a))` name only the entity. Every attribute of the concept on that entity is retracted.

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
5. With your rule installed, someone wants to create a to-do "Call mom" on `list:home`, and in a later document mark it done. Write both documents.
6. Write an installed rule for a command `jot`, so that `(jot! (note text: "idea"))` stores a new note every time it runs, even when an earlier note has the same text.
7. A document holds `(todo! list: list:home text: "Buy milk" state: :open)`. What happens?
8. A document holds `(todo todo:9c1e list: list:home text: "Buy milk" state: :open)` and `(query (todo ?t text: ?x))`. (a) Does the query return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add! (todo list: list:home text: "Call mom"))`. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `xyz.todo/text`, also a note. A document runs `(retract! (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a document runs `(add! (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

<details><summary><code>commit</code> (sha256 <code>499db7949f28e5db</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts: an entity, an attribute and a value. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

## `!` is a commit

`(x! …)` is short for `(! (x …))`, and is a commit: `x` is asserted, and rules observe it once this tick is over. Without `!`, a form holds in this tick only.

A rule's head with `!` is a commit too: when the rule fires, it commits its head.

Only a top-level commit moves the branch. A document with a top-level `!` moves its branch to a new revision, with every commit its rules make. A document without one still runs its rules and computes their commits, but the branch stays at its current revision: the result lists those commits, and nothing changes. Such a document needs no permission to write.

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

`(xyz.bug ?b title: ?t …)` names the attributes `xyz.bug/title`, `xyz.bug/author` and `xyz.bug/status`. Each attribute keeps its latest value. A concept is a rule that runs both ways: read, the attributes make the concept; written, writing the concept writes each attribute.

A fact names its entity and passes every parameter: `(bug! bug:7f3a title: "Crash" author: "ada" status: :todo)`. Leaving out the entity or any parameter is a compile error. To change one attribute, write the attribute: `(xyz.bug/title! bug:7f3a is: "Crash on save")`.

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

`retract` is a modifier that comes with the language. `(retract (bug bug:7f3a))` and `(retract! (bug bug:7f3a))` name only the entity. Every attribute of the concept on that entity is retracted.

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
5. With your rule installed, someone wants to create a to-do "Call mom" on `list:home`, and in a later document mark it done. Write both documents.
6. Write an installed rule for a command `jot`, so that `(jot! (note text: "idea"))` stores a new note every time it runs, even when an earlier note has the same text.
7. A document holds `(todo! list: list:home text: "Buy milk" state: :open)`. What happens?
8. A document holds `(todo todo:9c1e list: list:home text: "Buy milk" state: :open)` and `(query (todo ?t text: ?x))`. (a) Does the query return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add! (todo list: list:home text: "Call mom"))`. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `xyz.todo/text`, also a note. A document runs `(retract! (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a document runs `(add! (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

<details><summary><code>induction</code> (sha256 <code>bc67919a87bb9a7a</code>)</summary>

````text
You are going to write in Dialog notation. Its documentation comes first, then your tasks.

# Dialog notation

A replica holds facts: an entity, an attribute and a value. Time moves in ticks. A document is a set of top-level forms evaluated as one tick; their order does not matter. Everything a document reads, it reads from the facts as they stood when the tick began, plus whatever is asserted or deduced in that tick.

**Values.** Variable `?b`. Text `"triage"`. Keyword `:done`. Entity: a URI such as `bug:7f3a`. An enumeration type `[:todo :done]`. `;` starts a comment.

**Calls.** `(name primary label: value …)`. One primary, written bare; every other argument has its label. In a query or a premise, a parameter left out matches anything.

## `!` is induction

A form without `!` is deductive: it holds in this tick. `(x! …)` is short for `(! (x …))`, and is inductive: it carries `x` beyond this tick, and is committed.

A rule's head with `!` is inductive, and an inductive rule fires only on inductive assertions. When a document makes no inductive assertion, nothing is committed, but the evaluator still reports what its inductive rules would have done had its assertions been inductive. Such a document needs no permission to write.

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

`(xyz.bug ?b title: ?t …)` names the attributes `xyz.bug/title`, `xyz.bug/author` and `xyz.bug/status`. Each attribute keeps its latest value. A concept is a rule that runs both ways: read, the attributes make the concept; written, writing the concept writes each attribute.

A fact names its entity and passes every parameter: `(bug! bug:7f3a title: "Crash" author: "ada" status: :todo)`. Leaving out the entity or any parameter is a compile error. To change one attribute, write the attribute: `(xyz.bug/title! bug:7f3a is: "Crash on save")`.

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

`retract` is a modifier that comes with the language. `(retract (bug bug:7f3a))` and `(retract! (bug bug:7f3a))` name only the entity. Every attribute of the concept on that entity is retracted.

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
5. With your rule installed, someone wants to create a to-do "Call mom" on `list:home`, and in a later document mark it done. Write both documents.
6. Write an installed rule for a command `jot`, so that `(jot! (note text: "idea"))` stores a new note every time it runs, even when an earlier note has the same text.
7. A document holds `(todo! list: list:home text: "Buy milk" state: :open)`. What happens?
8. A document holds `(todo todo:9c1e list: list:home text: "Buy milk" state: :open)` and `(query (todo ?t text: ?x))`. (a) Does the query return a "Buy milk" to-do? (b) Is it stored afterwards?
9. Is `(todo ?t list: list:home text: "Buy milk" state: :open)` valid as a top-level form? Why?
10. With only your rule from task 1 installed, one document holds both `(add (todo list: list:home text: "Buy milk"))` and `(add! (todo list: list:home text: "Call mom"))`. Which to-dos are stored afterwards?
11. The entity `todo:9c1e` is a to-do and, because it has `xyz.todo/text`, also a note. A document runs `(retract! (todo todo:9c1e))`. Is `todo:9c1e` still a note afterwards?
12. With only your rule from task 1 installed, a document runs `(add! (todo list: list:home text: "Buy milk" state: :done))`. What happens?
13. Rate how clear this notation is from 1 to 5, name the most confusing thing in it, and propose a spelling that would fix it.
````

</details>

