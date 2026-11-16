# The notation, in one place

What five threads have settled, put together. Where two of them answered one question two ways, the answer taken is in the table near the end, with why. A proposal for the maintainer: the parts are his decisions, the joining is the agent's.

```
 #728  the design, to R45        concepts as lines, cardinality, folds in a head,
                                 getters, commands, views
 here  #772, #773, #778, #799    the reader, lists, records and patterns, `...`,
                                 macros as rules, forms kept as values
 #793  cardinality               what `:one` and `:many` do to a write
 #795 on  layers                 `in:`
 #748, #812  the record          a record as attribute and value pairs
```

## What is read

The reader knows brackets, atoms, labels and `'`. It gives nothing a meaning.

| written | is |
|---|---|
| `?t`, `_` | a variable, a blank |
| `"Buy milk"`, `16`, `:done`, `todo:9c1e` | text, a number, a keyword, an entity |
| `xyz.todo/title`, `todo` | an attribute by its full name, a name |
| `(a b c)` | a call |
| `[a b c]` | a list |
| `{title: ?t done: false}` | a record |
| `'x` | `(quote x)`: what is quoted is not called |
| `title:` | a label, for the part after it |
| `...` | a name like any other; the analyzer reads it |
| `; …` | a comment; right above a form, it describes the form |

## A call

One part has no label, the primary, which may also be written `of:`. Every other part has its label.

```clj
(todo ?t list: ?l state: :open)      ; is sugar for
(todo {of: ?t list: ?l state: :open})
```

- A part left out of a read matches anything. An `assert` through a concept says every part.
- `... ?more` takes the parts not named: in a head it captures them as a record, in a body it spreads one.

## Concepts

A concept is a set of attributes, a line each, and runs both ways.

```clj
(concept (todo ?i list: ?l title: ?t state: ?s at: ?where)
  (xyz.todo/title of: ?i is: ?t as: :text            cardinality: :one in: ?where)
  (xyz.todo/state of: ?i is: ?s as: [:open :done]    cardinality: :one)
  (xyz.list/task  of: ?l is: ?i as: :entity          cardinality: :many))

(concept (account ?a status: (top ?s))
  (tonk.account/status of: ?a is: ?s as: [:suspended :active :registered] cardinality: :many))
```

- `cardinality:` says what a write does: `:one` replaces, `:many` adds. There are no picks.
- A fold in the head chooses one claim of many: `last`, `max`, `min`, `top`. `top` takes its order from the line's `as:` list.
- `in:` is the layer, a constant or a field.
- Every variable of the body is in the head.

## Rules, commands, writes

```clj
(rule (assert (todo ?t list: ?l title: ?x state: :open))     ; lands after the tick
  (add (todo ?t list: ?l title: ?x)))                        ; a command: what some rule reads

(rule (open-count (list ?l) is: (count ?t))                  ; a getter: a fold that computes
  (list ?l)
  (todo ?t list: ?l state: :open))

(query (todo ?t list: list:home state: :open))
```

A rule has one conclusion. No form carries `!`: `assert` and `retract` are the two that outlast a tick.

## Records and lists

Where a value stands, `{ }` is a record and `[ ]` is a list. `'(a b)` is the same list as `[a b]`.

```
                   written, in an assert or a head     read, in a query or a body
 {title: ?t}       the record of a title alone         any record with a title
 {title: ?t ... ?r}  a title, and what ?r holds        a title; ?r is the rest
 [?a ?b]           the list of two                     a list of exactly two
 [?a ... ?m]       ?a, then what ?m holds              a list of at least one
```

A record read is open, a list read is exact: a list's length is part of what it says.

## Macros, and where views come from

A form the kernel does not know is a macro: a rule over syntax. `concept`, `view` and a getter's rule are macros, in a prelude written in the notation. A form's syntax is itself a value, records and lists, so it is kept and queried like any other.

A view is the design's, and it is the first form that needs all of this at once:

```clj
(view (card (list ?l name: ?name))
  [:article
    [:h2 ?name]
    [:p (open-count (list ?l)) " open"]
    [:ul (row (todo ?t list: ?l))]])
```

```
 [:article …]            a list whose first item is a keyword: markup, in order
 ?name                   a slot the head binds
 (open-count (list ?l))  a call in the template: a join; a getter fills the slot,
                         a view repeats for each match
 the whole view          not run: kept, as the tree it is, in the concept's views
```

So the template is not a new kind of thing. It is a list, and the view macro keeps its tree where the renderer finds it. That needs lists in order and syntax as a value, which is what this branch built them for.

## What had two answers

| | one thread | another | taken | why |
|---|---|---|---|---|
| `!` | here, 2026-11-12: marks what stays | #728: no form carries one | none | `assert` and `retract` are the only forms that stay, so the mark says nothing more; R23 has readers preferring it |
| a part left out of a read | here, 2026-11-13: refused without `...` | #728: matches anything | matches anything | the maintainer, 2026-11-15: "Open" |
| which claim is read | here: a pick in a wrapper, `(:last (:text ?x))` | #728 and #793: cardinality, and a fold in the head | cardinality and folds | a write under `pick: :max` leaves states nobody expects (the maintainer, 2026-11-16) |
| `[ ]` | #728: a variant in `as:`, markup in a view | here: a list | a list, in each place | one meaning in three places. Not tested on readers |
| the rest | #728's prelude: `… ?params` | here: `... ?more` | `...` | three plain dots, and the reader need not know it |
| a record's key | here: text | #748: an attribute | the label, as written | `{geo/lat: ?l}` and `{title: ?t}` are one spelling; what a label means to a record is the record's |
| a record's bytes | here: dag-cbor under a version byte | #812: its own, as a value type | dag-cbor for now | below |

**The record.** The maintainer left this to the agent on 2026-11-16: carry on with dag-cbor, or move onto #812. Carry on. #812 has no list in order, and a view's markup and a kept form are both lists; it has no way to take a record apart yet; and its Perf gate fails. None of that is against its model. The notation writes `{label: value}` whichever record is under it, so the move can come when #812 holds an ordered list. Before both land, one of the two types named `Composite` takes another name.

## Open

- A getter's head, `(open-count (list ?l) is: …)`, has a command's shape, `(rename (todo ?t) title: …)`. #728 has it open too.
- Whether `top` ranks by the `as:` list (#728) or the order is kept apart in `among` (#793).
- How a line names a key of a collection, now that lines are by attribute.
- A read of exactly these entries has no spelling. Met already: `terms: {}` over a kept form used to say "no labelled part" and now says any.
- `assert` without `!` is taken in this build: it is the head of a rule whose conclusion holds while its conditions do. Step 2 has to give that another spelling or none, as the design does (a conclusion with no word is a deduction).
- How a view's template binds events.

## Order of the build

```
 1  this branch on the record as rebuilt (#778): text keys, a record read open     done, 2026-11-16
 2  assert and retract without !
 3  a concept as lines, with as: and cardinality:   (:one and :many on main's engine)
 4  getters
 5  the stored form of a pattern, `match` (#799), so a rule that holds a record is kept
 6  view as a macro that keeps its tree
 7  folds in a head, and in:, as #793 and the layers land
```
