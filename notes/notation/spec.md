# Dialog notation

A single notation for dialog queries, rules and transactions. Nothing is implemented yet. Everything here is agreed unless it is marked **Proposed**, and the questions still open are listed at the end. The forms are spelled as Lisp while the semantics settle. The YAML spelling comes after.

[`sketches/`](sketches) has tonk's `core.yaml` and `notebook.yaml` redone in this notation.

## Model

A replica holds facts. Each fact is an entity, an attribute and a value, with the cause and version it was written at. A document is evaluated against two states:

- **`now`** holds the facts as they stand when the document starts, plus whatever the document's facts and deductive rules derive from them.
- **`next`** is the state the document commits: `now` with the document's writes applied.

This is Dedalus's split. Deduction happens within `now`, and induction carries `now` into `next`.

## Terms

| Term | Spelling |
|---|---|
| Variable | `?b`, `?status`. It belongs to the one rule or question it appears in. |
| Text | `"triage"`, or `"""…"""` for text that holds `"` or spans lines |
| Number | `16`, `0.5` |
| Keyword | `:done`, `:text`, `:last`. Types, picks and enumerated values are keywords. |
| Entity | a URI: `id:notebook/scratch`, `did:key:z6Mk…`, `tonk:notebook/edge` |
| Map | `{"N1" id:notebook/scratch/1}`, `{block/notebook ?n block/source ?s}` |
| Vector | `["expel" "remove member"]` |
| Name | `notebook/block`, the name of a concept, view or event |

`;` starts a comment.

## Calls

A call names a concept and passes its parameters. The first parameter is positional, and the rest are labeled and may come in any order, as in [conduct](https://github.com/Gozala/conduct).

```clj
(bug/status ?b is: :triage)
(bug/summary ?b title: ?t assignee: ?who)
```

A `!` after the name makes the call a write: `(bug/status! bug:01 is: :done)`.

## Documents

A document is a set of top-level forms evaluated as one tick. The order of its forms does not matter. Every premise and every question reads `now`, so nothing in a document sees that document's own writes. A chain, where one write depends on reading another, takes a second document.

The top-level forms are:

- **a question**, a bare call such as `(bug/status ?b is: :done)`;
- **a write**, a bare call with `!`;
- **a rule**, `assert` or `retract`;
- **a definition**: `concept!`, `view!` or `event!`, or a rule installed with `assert!` or `retract!`.

## Questions

A bare call at the top of a document is a question. Its answer is the rows of variable bindings that match `now`. A parameter the question leaves out matches any value.

## Writes

A bare call with `!` writes into `next`. It must be ground. The one exception is an entity parameter with a [default](#defaults).

```clj
(bug/status! bug:01 is: :done)
```

A write means the same as an `assert` with a `!` head and no premises, so that longer form is never written.

**Proposed.** A write through a concept that leaves out a value parameter leaves that attribute as it is. `(notebook/block! id:b1 source: "x")` changes the block's source and keeps its notebook.

**Proposed.** A retract through a concept that leaves out a parameter retracts every value of it.

## Rules

A rule is `(assert head premises…)` or `(retract head premises…)`, with exactly one head. A premise is one of:
- a call;
- `(unless call)`, which holds when the call has no match;
- a formula, such as `(== ?a ?b)`, `(!= ?old ?key)` or `(dialog/position ?b after: ?lo before: ?hi is: ?at)`.

Every variable in the head must be bound by the premises.

| | no premises | premises |
|---|---|---|
| head without `!` | a fact in `now` | a deductive rule: it derives into `now` and settles to a fixpoint |
| head with `!` | a write into `next`, spelled as a bare call | an inductive rule: it reads `now` and writes `next` |

```clj
(assert (bug/review ?b reviewer: ?who)
  (bug/status ?b is: :review)
  (bug/assignee ?b is: ?who))

(assert (bug/status! ?b is: :done)
  (bug/status ?b is: :review))

(retract (bug/assignee! ?b is: ?who)
  (bug/status ?b is: :review))
```

When several rules share a body, factor the body into a concept that each of them reads.

**Proposed.** A deductive `retract` is stratified the way `unless` is.

## Installing

A `!` on the keyword installs a definition in the replica: `concept!`, `assert!`, `retract!`, `view!`, `event!`. Without the `!`, the definition belongs to the document it is written in.

An installed inductive rule fires whenever its premises match, which is how a command takes effect.

**Proposed.** Installed rules induce after the document's tick, so the document's questions do not see what they induce. Their edits appear in the report and in the commit.

## Concepts

`concept!` declares a concept. Its head is a call made of variables, and its body is a conjunction. A concept can be read with a query and written with a `!` call.

### Attributes

An attribute is a concept whose body is a single `the` line. The line names the stored attribute, labels every parameter, and declares the value type (`as:`) and the pick.

```clj
(concept! (notebook/title ?of is: ?is)
  (the xyz.tonk.notebook/title
      of: ?of
      is: ?is
      as: :text
      pick: :last))
```

The types used so far are `:text`, `:entity`, `:bytes`, `:natural`, `:float`, sized numbers such as `(:float 64)`, and the collections `(:sequence T)` and `(:dictionary T)`. A collection value is written as a map from key to element: `(notebook/entry! id:notebook/scratch is: {"N1" id:notebook/scratch/1})`.

The picks are `:last`, which keeps the latest claim, and `:all`, which keeps every claim.

A `the` line may also carry `transient: true`, which marks the attribute as never stored (see [Commands](#commands)), and `role:`, the palette slot that a command's field fills.

### Composite concepts

A composite concept's body is a conjunction of attribute calls, concept calls and constants.

```clj
(concept! (notebook/block ?block notebook: ?n source: ?s)
  (block/notebook ?block is: ?n)
  (block/source ?block is: ?s))
```

A concept body holds no formulas, so every concept can be read and written. Formulas belong in rule bodies. A concept may not contain itself.

**Proposed.** Any concept may carry `the` lines, so that a command can declare its fields in the same form.

### Optional parameters

A parameter marked `(option ?x)` in the head may stay unbound in a match.

```clj
(concept! (tonk/repository ?r name: ?name description: (option ?d))
  (repo/name ?r is: ?name)
  (repo/description ?r is: ?d))
```

### Defaults

A `default` line in a concept's body gives an entity parameter the value that a write gets when it leaves that parameter out.

```clj
(concept! (notebook/block ?block notebook: ?n source: ?s)
  (default ?block is: {block/notebook ?n block/source ?s})
  (block/notebook ?block is: ?n)
  (block/source ?block is: ?s))

(notebook/block! notebook: id:notebook/scratch source: "# Scratch")
```

A write that leaves `?block` out writes to the entity hashed from the map. Each key in the map is an attribute concept, or a stored attribute name. Keys are resolved to their stored names before hashing, so renaming the local `block/source` concept never changes an id. The write above stores two facts on that entity.

A query skips the `default` line. `(notebook/block ?b notebook: ?n source: ?s)` matches every block, however its entity came to be.

Entities with the same content are the same entity: writing the block above twice gives one block. That is the point for value-like things such as routes. For things that need to stay distinct even when their content is equal, a rule can lend them another entity. In `notebook.clj`, each new block takes the entity of the insert command that made it.

**Proposed:**
- A map in a slot declared `:entity` stands for the hashed entity. In a `(:sequence …)` or `(:dictionary …)` slot, a map holds the collection's entries.
- Only an entity parameter may have a default. If a value parameter had one, every partial write would reset that value to its default.
- A write that leaves the entity out must bind every variable in the default. An `(option …)` parameter left unbound drops out of the map.
- Each key is an attribute that the body stores, paired with the variable the body stores under it.
- A concept with no `default` refuses a write that leaves its entity out.
- A retract that leaves the entity out targets the default, so it undoes the matching write.

### Identity

**Proposed.** A concept's identity is its set of attributes. A name binds a concept to a signature, meaning the head's labels. Two names over the same attributes are one concept with two signatures. Redefining a name with a different body rebinds the name. Stored facts belong to attributes, not names, so they are not touched.

**Proposed.** Views and routes refer to names, so two route models with the same fields keep separate templates.

## Commands

A command is a concept whose attributes are transient. Writing one triggers installed rules and stores nothing. The command's entity is usually its default, hashed from its fields.

```clj
(concept! (tonk/rename-repository ?cmd repository: ?r name: ?name)
  (default ?cmd is: {xyz.tonk.rename-repository/subject ?r
                     xyz.tonk.rename-repository/name ?name})
  (the xyz.tonk.rename-repository/subject of: ?cmd is: ?r as: :entity pick: :last transient: true)
  (the xyz.tonk.rename-repository/name of: ?cmd is: ?name as: :text pick: :last transient: true))

(assert! (repo/name! ?r is: ?name)
  (tonk/rename-repository ?cmd repository: ?r name: ?name))

(tonk/rename-repository! repository: did:key:z6Mk… name: "Plans")
```

## Conflicts

A conflict is never an error. It is elected, as concurrent writes are, comparing version first, then cause, then succession, and value only as the last resort.

- Conflicting writes in one document are concurrent candidates.
- A write in a later document succeeds the claim it read.
- A deductive rule's value under `:last` succeeds the claim its body read (dialog #724, fixed by #725), so `(assert (bug/status ?b is: :todo) (bug/status ?b is: :triage))` rewrites the status instead of standing beside it.
- A cycle of rewrites converges, with the rewrites as concurrent candidates. A counter (`?n + 1`) does not converge.

## Views, events and routes

**Proposed.** `view!` and `event!` name what they define by their first argument, the way `concept!` does.

A view attaches templates to a concept by facet:

```clj
(view! notebook/block
  show: {:directory """
    <div class="notebook-block-row" subject={this} data-source={source}></div>
    """})
```

An event says how a platform event fills a command. Its `where` keys name command parameters, and its values are paths into the event:

```clj
(event! on/block-remove
  type: "remove"
  where: {:block ".currentTarget.dataset.subject"})
```

A template binds an event to a command with `on:block-remove=block/remove`. A command filled from an element's event is still a command. Elements never write facts directly.

A route maps a path to a route model:

```clj
(route! path: "/notebook/{*entity}" concept: notebook/route)
```

## Preview

**Proposed.** A preview is the same document, evaluated without committing.

## Open

- How an entity is named across the rules and writes of a document, and what a bare symbol such as `scratch` or `tab-title` means.
- How a concept declares that it is transient. Today each `the` line says `transient: true`.
- Whether a document's questions are answered jointly or one at a time.
- Whether a rule's head must be a declared attribute or concept.
- Whether every concept's first parameter is named `of`.
- Whether metadata (`description:`, `action:`, `role:`) is labeled entries kept as data.
- Whether templates name models by concept name rather than by pinned URI.
- The YAML spelling.
