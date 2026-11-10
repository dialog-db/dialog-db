# Dialog notation

One notation for dialog's queries, rules and transactions. Nothing is implemented yet. Everything here is agreed unless it is marked **Proposed**, and what is still open is listed at the end. Forms are spelled as Lisp while the semantics settle; the YAML spelling comes after.

This is the current decision of the [notation research](../notation.md). The [journal](journal.md) has the reader rounds behind each choice, R1 to R25. The [sketches](sketches) redo tonk's `core.yaml` and `notebook.yaml` in the notation as it stood at R16, before commands, and have not been redone since.

## Model

A replica holds facts: an entity, an attribute and a value, with the cause and version it was written at. Facts belong to domains, such as `xyz.todo`, and concepts are views over domains.

Time moves in ticks, after [Dedalus](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2009/EECS-2009-173.html). A tick reads the facts as they stood when it began, plus whatever is asserted or deduced in it.

- **Deduction** happens within a tick. A form or a rule head without `!` holds in this tick, and rules derive from it to a fixpoint.
- **Induction** carries one tick into the next. A rule head with `!` lands after this tick, where rules see it again.

```mermaid
flowchart LR
  subgraph tick["a tick"]
    start["facts as the tick began"] --> derive["commands, asserts,<br/>deductions to a fixpoint"]
    derive --> derive
  end
  derive -- "what assert and retract land" --> after["after the tick"]
  derive -- "heads with !" --> after
  after -- "the next tick, while a ! head produced something" --> tick
```

A run goes on tick by tick while inductive heads produce something, up to a turn limit.

## Terms

| Term | Spelling |
|---|---|
| Variable | `?t`, `?state`. It belongs to the one rule or question it appears in. |
| Text | `"Buy milk"` |
| Number | `16`, `0.5` |
| Keyword | `:done`, `:text`. A keyword is an entity: `:done` is `done:`, with the colon in front so it is not read as a label. Keywords serve where other languages use them: types, picks and the cases of a variant. |
| Entity | a URI: `todo:9c1e`, `did:key:z6Mk…`, `tonk:notebook/edge` |
| Variant | `[:open :done]`, a type whose value is one of these cases |
| Record | `{"N1" id:notebook/scratch/1}`, the value of a collection |
| Domain | `xyz.todo`, where facts live |
| Name | `todo`, the name of a concept |

`;` starts a comment.

## Calls

A call names a concept, a domain, a command or a formula, and passes its parameters. Every definition has exactly one primary, written without a label. Every other parameter has its label, and arguments are never punned. The primary may also be written `of:`, in any position, as in [conduct](https://github.com/Gozala/conduct).

```clj
(todo ?t list: ?l state: :open)
(todo state: :open of: ?t)
```

In a question or a premise, a parameter left out matches anything. In an `assert` through a concept, leaving one out is a compile error (see [Commands](#commands)).

## Documents

A document is a set of top-level forms evaluated as one run. The order of its forms does not matter. A top-level form is one of:

- a question, `(query …)`;
- a command, such as `(add (todo list: list:home text: "Buy milk"))` or `(assert (xyz.todo todo:9c1e state: :done))`;
- a `rule`;
- a `concept`.

Top-level forms never carry `!`. A command at the top level is ground, or has a `when` that binds its variables (see [Writes succeed what they read](#writes-succeed-what-they-read)). A bare call, with variables or without, is not a top-level form.

A document is submitted in one of two ways:

- As a **query**, it runs and returns what it derived and what would have landed. It changes nothing, so it needs no permission to write.
- As a **transaction**, it runs the same way, then advances the replica's revision with everything that landed.

So one document can be run as a query to see what it would do, then as a transaction. In a transaction a top-level `rule` or `concept` is installed and runs from then on; in a query it lives for that run only.

In R23, 8 of 10 readers preferred this to a `!` at the top level deciding the commit: `!` keeps one meaning, and a stray `!` cannot turn a read into a write.

## Questions

```clj
(query (todo ?t list: list:home state: :open))
```

A question's answer is the rows of variable bindings that match what holds in the tick, including what the document asserts and deduces. It changes nothing, in either kind of run.

A concept a question names must be declared, installed or in the same document, or the question is a compile error. A domain call needs no declaration: `(query (xyz.todo ?t text: ?x))` reads whatever the domain holds. A reader's default is the other way: in R25, 16 of 20 expected an undeclared concept to answer nothing, as an unknown relation does in Datalog.

`query`, `select`, `find` and `(? …)` read alike, with documentation (R24) and without (R25), and no reader misread any of them. `query` is the maintainer's choice. A query form and a document run as a query mean the same thing: they read and change nothing.

## Concepts

```clj
(concept (todo ?t list: ?l text: ?x state: ?s)
  (xyz.todo ?t list: ?l text: ?x state: ?s)
  (: ?l as: :entity)
  (: ?x as: :text)
  (: ?s as: [:open :done])
  (pick :last from: ?l)
  (pick :last from: ?x)
  (pick :top from: ?s))
```

A concept is a rule that runs both ways. Read, its body makes the concept; asserted, each of its fields goes into its domain. Its facts are stored, and carried from tick to tick until retracted.

A body holds domain calls, concept calls and constraints, and every variable in it appears in the head. That is what lets every concept be read and written, as an updatable view in SQL maps each column back to one base column; anything else is a rule. A concept may not contain itself.

A concept's primary need not be the entity it reads. Read from the value's side, a field gives the reverse relation:

```clj
(concept (list-item ?l todo: ?t)
  (xyz.todo ?t list: ?l))

(query (list-item list:home todo: ?t))    ; every to-do on list:home
```

A concept says what facts make it up, not how its entity is made. Construction belongs to a command's rule (see [Creating things](#creating-things)).

### Domain calls

`(xyz.todo ?t text: ?x)` reads the `text` field of `?t` in the `xyz.todo` domain. A domain is never declared; each label names a field. Fields belong to the domain, not to a concept, so concepts can share them:

```clj
(concept (note ?n text: ?x)
  (xyz.todo ?n text: ?x)
  (: ?x as: :text)
  (pick :last from: ?x))
```

Every to-do is also a note, and `(retract (todo todo:9c1e))` takes the `text` a note reads, so it stops being a note.

A domain call names no type and no pick. It reads every live claim of its fields, whatever their values, and types and picks are constraints on the variables it binds (below). Claims are stored under their field, `xyz.todo/state`, so a concept and a bare domain call that read one field differently are two views of the same claims.

### Collections

A domain's fields whose names start lowercase are its dictionary; those whose names start uppercase are positions, its sequence, as in dialog today. A label written as a variable reads every entry, one row per entry, with the label bound to the variable:

```clj
(xyz.tonk.element.method ?e run: ?source)     ; one key
(xyz.tonk.element.method ?e ?name: ?source)   ; every key
(xyz.tonk.notebook ?n ?at: ?block)            ; every position
```

A concept over a collection gives the key a parameter of its own, and holds one collection only. Two collections in one concept would give a row for every pair of their entries.

```clj
(concept (element-method ?e name: ?n source: ?s)
  (xyz.tonk.element.method ?e ?n: ?s)
  (: ?n as: :symbol)
  (: ?s as: :text))

(concept (notebook-entry ?n at: ?p block: ?b)
  (xyz.tonk.notebook ?n ?p: ?b)
  (: ?p as: :position)
  (: ?b as: :entity))
```

An entry is a fact, `(assert (xyz.tonk.notebook notebook:scratch N5: block:2))`, so entries written on two devices merge key by key. A record value, `{"N5" block:2}`, is one value: it is opaque to queries, and two concurrent writes of it elect one whole record. Records are for data that is one value.

In R26, 22 of 25 readers named the variable label as the hardest thing to guess, though all 25 read it correctly.

### Constraints

A constraint computes nothing; it holds or it does not. It may stand in a concept's body or a rule's `when`. On a read it filters; on an assert through a concept it refuses the write, and nothing lands. About half of R26's readers could not guess which, so this has to be said. The constraints are:

- **type**: `(: ?x as: :text)`, a value is text, an entity, a number, a case of a variant, or a label that is a `:symbol` or a `:position`;
- **equality**: `(== ?a is: ?b)`, `(!= ?a is: ?b)`, with a variable or a constant;
- **comparison**: less than, greater than, within a range;
- **pick**: `(pick :last from: ?x)`, below.

Negation is not a constraint, since an assert could not say what it enforces, and neither is arithmetic that runs both ways, since it computes. Formulas belong in rules.

The types used so far are `:text`, `:entity`, `:bytes`, `:natural`, `:float`, sized numbers such as `(:float 64)`, variants such as `[:open :done]`, and the label types `:symbol` and `:position`.

`(: ?x as: …)` is ascription, `x : T`. R26 tried five spellings, `(dialog/type ?x is: :text)`, `(: ?x as: :text)`, `(?x as: :text)`, `(:text ?x)` and `(text ?x)`, and every reader read every one as a check that touches no stored facts. `:` is the one taken: it takes no word, where `claim`, `where` and `the` already mean something in dialog.

### Picks

A pick elects among the live claims of one entity and field. `(pick :last from: ?x)` applies to every field whose value position holds `?x`. Without one, a read returns every live claim, `:all`. The picks are `:all`, `:last`, `:max`, `:min`, and `:top`, which ranks a variant's cases with the first listed best, as dialog's `as:` list does today. A pick is named by an entity (`last:`, `top:` …).

- A variable has one pick. To read one field two ways, use two variables.
- A pick on a variable that only appears as an entity is an error: an entity has no claims to elect.
- A pick is per cell, not per row: whether a claim survives depends on the others in its cell, so adding a claim can take away a row. A rule that picks from a field it also writes recurses through an election, and is checked as `unless` is.

Writes normally leave one live claim in a cell, so `:all` usually returns one row, and several only after replicas that wrote concurrently merge. Leaving the pick out shows those conflicts; a concept's picks say how they are elected.

In dialog today a pick is one ordering over a cell's claims, used both ways:

- **On read**, the claims of one entity and field are elected by the ordering, and the winner is returned. `:all` returns every claim.
- **On write**, the claim the ordering elects, the one a read would return, leaves the operational index beside the new value; history keeps every value. Under `:all` a write appends and retracts nothing. Under `:max`, writing 3 over a 5 replaces the 5: the pick decides which claim concurrent writes resolve to, not that values only grow.

The write is the read's shortcut, so a read normally finds one live claim, and elects among several only after replicas that wrote concurrently merge. A write today succeeds only the elected claim and leaves the other concurrent ones live, so under `:max`, `:min` and `:top` a value the write meant to replace can win the next read ([#759](https://github.com/dialog-db/dialog-db/issues/759)). In the notation a write succeeds the claims it read (see [Commands](#writes-succeed-what-they-read)), so a write after a read under `:all` settles every live claim.

**Proposed.** User-defined picks are user-defined orderings: how two values compare, with the shared fallback to standing (version, cause, succession) and then the value's hash, so every ordering is total and every replica elects alike. An ordering runs at read. A write succeeds what its read elected, so it needs no ordering of its own. A pick elects one claim; a sum or a count is a rule over an `:all` field.

### Identity

A concept's identity is its set of fields, each with the type and pick the concept gives it. A field is its domain and its name. A name binds a concept to a signature, meaning the head's labels; two names over the same fields are one concept with two signatures. Redefining a name with a different body rebinds the name. Stored facts belong to fields, not names, so they are not touched.

## Commands

Everything that changes anything is a command, written around a concept or a domain:

```clj
(add (todo list: list:home text: "Buy milk"))
```

- A command's facts are not the concept's facts. They are tagged with the command's name, so `(query (todo …))` never sees them, and they are never carried, stored or synced: rules see them in the tick they are issued, and then they are gone. The `add` above keeps nothing; what lands is what its rules assert. In R25 all 20 readers, shown no documentation, took `add` to write the to-do itself, so this has to be said.
- A command needs no declaration. The rules that read it decide what it takes: any of the concept's parameters, with or without its entity, as in `(remind (todo todo:9c1e))`. Issuing one with a parameter no rule reads is a compile error.
- A command wraps exactly one concept or domain, one deep.
- A command's rows are keyed by its name and everything it passes, so identical commands in one tick are one. A command that must count twice in a tick, such as an increment, carries a field that tells them apart.

### `assert` and `retract`

The two commands that come with the language, and the only ones that change a concept:

| form | does |
|---|---|
| `(assert (todo todo:9c1e list: list:home text: "Buy milk" state: :open))` | names the entity and passes every parameter; leaving any out is a compile error |
| `(assert (xyz.todo todo:9c1e tag: "urgent"))` | a domain takes any of its fields; with nothing read, the value is added beside what is there |
| `(retract (todo todo:9c1e))` | retracts every field of the concept on that entity |
| `(retract (xyz.todo todo:9c1e text: "Buy milk"))` | retracts just that value |

What is asserted holds in this tick and lands after it. What is retracted stops holding now and is gone after it. Every other command changes a concept only through rules that issue `assert` or `retract`.

Leaving a parameter out of an assert through a concept is an error, not "keep what is there", because it is more often a mistake than a choice. A change to some fields goes through the domain, with a `when` (below). An entity is an identity over time, never partial; what can be partial is what is known about it.

### Writes succeed what they read

An assert succeeds the claims its body read for the same entity and field. If it read none, it adds. This is dialog's `cause`: an assertion with one succeeds the claim it names, and one without is additive.

Any top-level command may take a `when`. It runs once, in this document, and is never installed, unlike a top-level `rule` in a transaction:

```clj
; replace: read the winner, succeed it
(assert (xyz.todo ?t state: :done)
  (when (xyz.todo ?t state: ?old)
        (pick :last from: ?old)
        (== ?t is: todo:9c1e)))

; find the entity by what it says
(assert (xyz.todo ?t state: :done)
  (when (todo ?t text: "Call mom")))
```

So writes need no pick of their own. A read under `:last` and a write replace; a read under `:all` and a write settle a conflict; a write with no read of that field appends. An assert through a concept reads first: it reads the concept's fields on the entity under the concept's picks, and succeeds what it read, so `(assert (todo todo:9c1e …))` replaces each field.

Every variable in a command with a `when` is bound by it; a command without one is ground.

There is no content identity: `assert` never derives an entity. In R17 and R18 an entity derived from every field made a changed state a new to-do, and readers tripped on it until it was removed (R19, 0 of 15).

## Creating things

Nothing gets an entity by itself. A new thing is made by a command whose rule computes its entity, with one of two formulas:

- `(id ?t list: ?l text: ?x)` binds `?t` to the entity those labels and values identify. The same labels and values give the same entity, on every device. It derives; it neither mints nor looks up. It runs one way, so the planner binds its fields first.
- `(fresh ?n)` binds `?n` to a new entity, never equal to any other.

```clj
(rule (assert (todo ?t list: ?l text: ?x state: :open))
  (when (add (todo list: ?l text: ?x))
        (id ?t list: ?l text: ?x)))

(rule (assert (note ?n text: ?x))
  (when (jot (note text: ?x))
        (fresh ?n)))
```

Two devices that `add` the same list and text make one to-do; every `jot` makes a new note. The schema's author owns how things are created, so every agent that creates through `add` agrees on identity. In R25, 19 of 20 readers could not tell from the code alone whether `id` mints, derives or looks up, so the spec says it.

## Rules

`(rule head… (when premise…))`. A head is a command the rule issues, or a relation it defines with a domain call. Several heads share one `when`, so one rule can retract and assert:

```clj
(rule (retract (note ?n))
      (assert (archived ?n text: ?x))
  (when (archive (note ?n))
        (note ?n text: ?x)))
```

A premise is one of:

- a call on a concept, a domain or a command;
- `(unless call)`, which holds when the call has no match;
- a constraint: a type, an equality, a comparison or a pick;
- a formula: `id`, `fresh`, `(dialog/position ?b after: ?lo before: ?hi is: ?at)`.

Every variable in a head is bound by a premise. A premise matches what holds now, and `!` never appears in one.

A head without `!` holds in this tick, and rules derive from it to a fixpoint, recursion included. A relation head is never stored; it holds while its premises do:

```clj
(rule (xyz.app.open ?t text: ?x)
  (when (todo ?t text: ?x state: :open)))

(query (xyz.app.open ?t text: ?x))
```

A head with `!` lands after this tick: the rule issues its command again in the next one. This is the only place `!` is written.

```clj
(rule (remind! (todo ?t))
  (when (remind (todo ?t))
        (todo ?t state: :open)))
```

Once `(remind (todo todo:9c1e))` is issued, `remind` is issued again every tick for as long as the to-do is open.

The cost R23's readers named: a head's `assert` lands only when the run is a transaction, and the rule's text does not say which it will be.

## The prelude

**Proposed.** A concept is carried from tick to tick by a rule the language supplies, as persistence is in Dedalus; a command is not carried, because nothing carries it. `?concept` stands for any concept and `… ?params` for its parameters but the primary.

```clj
(rule (?concept ?e … ?params)             ; what is asserted holds now
  (when (assert (?concept ?e … ?params))))

(rule (?concept! ?e … ?params)            ; what holds is carried, unless retracted
  (when (?concept ?e … ?params)
        (unless (retract (?concept ?e)))))
```

They act on each field a concept reads, so an assert or retract through a domain is the same rules over one field. They do not yet say that a retract also stops a fact holding in its own tick; that, and an assert and a retract of one fact in one tick, are open. In R17 the prelude did not help readers; it is the definition, not the teaching.

## Conflicts

A conflict is never an error. It is elected, as concurrent writes are, comparing version first, then cause, then succession, and value only as the last resort.

- Conflicting asserts in one tick are concurrent candidates.
- An assert in a later transaction succeeds the claim it read.
- A value a rule derives under `:last` succeeds the claim its body read (dialog #724, fixed by #725), so a rule that moves a status rewrites it instead of standing beside it.
- A cycle of rewrites converges, with the rewrites as concurrent candidates. A counter (`?n + 1`) does not converge.

A pick is a choice of merge, which Bloom^L would call a lattice. `:all` is a set union and needs no order. `:top` over a variant's ranked cases only moves up the ranking, so two devices that never synced agree once they do. `:last` needs an order across devices, which seventeen of nineteen readers in R15 asked about.

## Sending

**Proposed.** Sending to a later tick or to a peer was spelled `ensue!` before commands, which now collides with `!`. Its semantics stay: where a fact goes is data on it, a `to:` field the runtime routes on, the way Bloom routes a channel row by its `@` column. The facts the receiver needs go with the message, in the same tick: the sender says which, rather than the receiver fetching them. On arrival they hold for one tick at the receiver, as a command's facts do, where its rules join them with its own facts. Its spelling is open.

A remote call, Unison's `fork` and `await`, is a layer on top: a request sent, a rule at the peer that answers, and a rule that matches the reply to its request by the request's entity.

## Views, events and routes

**Proposed.** `view`, `event` and `route` are top-level forms like `rule` and `concept`: installed by a transaction, and named by their first argument.

A view attaches templates to a concept by facet:

```clj
(view block
  show: {:directory "<div class='notebook-block-row' subject={this} data-source={source}></div>"})
```

An event says how a platform event fills a command. Its `where` keys name command parameters, and its values are paths into the event:

```clj
(event block-remove
  type: "remove"
  where: {:block ".currentTarget.dataset.subject"})
```

A template binds an event to a command, and a command filled from an element's event is still a command. Elements never write facts directly. How a template names a command that wraps a concept, `(remove (block …))`, is open.

A route maps a path to a route model:

```clj
(route path: "/notebook/{*entity}" concept: notebook-route)
```

## Prior art

Comparing the design with [Bloom](http://bloom-lang.net), Dedalus and LogiQL settled most of the choices above.

| there | here |
|---|---|
| Bloom: a `table` persists; a `scratch` or `channel` lasts one tick. | A concept is carried until retracted; a command lasts one tick. Lifetime belongs to what is called, not to the call. |
| Bloom: tables change only through `<+` and `<-`. | Concepts change only through `assert` and `retract`, which land after the tick. |
| Bloom: `<=` holds now, `<~` arrives later. | A head without `!` holds now; sending is open. |
| Dedalus: persistence is a rule, and induction is the successor tick. | The [prelude](#the-prelude) carries concepts; `!` on a head is induction. |
| LogiQL: pulse predicates hold for one transaction. | Commands. |
| LogiQL: delta predicates `+R`, `-R`, `^R`. | `assert` and `retract`. |
| LogiQL: constructor predicates make entities from a key. | `id` and `fresh` in a command's rule. |
| LogiQL: `exec` runs a block once, `addblock` installs it. | A query runs a document's rules once; a transaction installs them. |
| Bloom: a row's fields stay together because a row is one tuple. | A command's rows are keyed by its name and content. |
| Bloom^L: lattices merge without coordination. | Picks are merges; `:last` is the one that needs a clock. |

## Open

- Under whose authority an installed rule writes: the transaction that installed it, or the one that set it off. Dialog's induction today runs as the operator committing.
- What a retract hides in its own tick when a rule also derives the fact, and what an assert and a retract of one fact in one tick do.
- A command in a premise and an issued command look alike: `(add (todo list: ?l text: ?x))` matches in a `when` and issues at the top. Readers proposed `(do add …)`.
- What drives a tick. Dialog runs induction inside one commit and fails it after 16 rounds, so a `remind!` that re-issues itself while a to-do is open fails the transaction that issues it. Whether `!` heads must quiesce within a run, or a command can be carried to a later transaction or a clock.
- Which case of a variant ranks best. With `[:open :done]` and the first case best, a to-do done on one device and reopened on another resolves to open.
- How commands are defined for discovery: derived from the rules that read them, with metadata as facts ([implementation](implementation.md#commands)).
- What a view is: data on a concept, a relation head with a template, or a concept that can be written through ([implementation](implementation.md#views)).
- Whether `id` names a domain or a concept, so two concepts with the same labels and values do not share an entity.
- Whether a rule that creates should only create. The `add` rule asserts `state: :open`, so adding a done to-do's list and text again reopens it; `(unless (todo ?t))` in its `when` would stop that.
- Whether a type filters before a pick elects or after. With two text claims, `"Buy milk"` and a newer `42`, under `(: ?x as: :text)` and `:last`, typing first gives `"Buy milk"` and electing first gives nothing.
- What retract takes for a keyed concept: `(retract (notebook-entry notebook:scratch))` names every entry; it likely needs the entity and the key.
- What two concurrent blind `:last` writes leave: each succeeds what was live where it landed, so both stay live for the next read to elect.
- Optional parameters. `(option ?x)` in a head read correctly once in six (R2).
- Whether a query holds several calls answered jointly, and whether questions stay calls or become rules into a relation head.
- How text spans lines or holds `"`. The sketches write templates as `"""…"""`, which is not part of the notation.
- Record types: there is no type for a record yet.
- Concurrent claims under `:last` are ordered by standing and last by the value's hash, so every replica elects alike (#752). A write after them settles them only once [#759](https://github.com/dialog-db/dialog-db/issues/759) lands.
- Whether a command's parameters are a conduct record type, one field whose value is a record. Dialog's `Record` value is opaque to queries today ([record-value](../../notes/record-value.md)).
- How an entity is named across the rules and commands of a document, and what a bare symbol such as `scratch` means.
- Whether metadata (`description:`, `action:`, `role:`) is labeled entries kept as data.
- Whether templates name models by concept name rather than by pinned URI.
- The sketches, which predate commands.
- The YAML spelling.
