# What a form is to dialog

**Decision:** what a form is in dialog, so that a rule about syntax is a rule dialog stores and evaluates.

**For:** the direction, macros as rules. The [proof of concept](poc.md) runs the kernel on dialog, and holds the rules about syntax in memory and matches patterns itself. Nothing about syntax is stored, named by hash, or synced.

**Decided 2026-11-11, by the maintainer: a form is a record (C).** `entity/derive` takes a record too. The matrix below is how the approaches compared; what follows it is what the decision asks of records.

## What a rule about syntax needs

```
(rule (syntax (xyz.todo ?t list: ?l text: ?x state: ?s))      ; builds a form
  (when (expand (todo ?t list: ?l text: ?x state: ?s))))      ; takes one apart
```

1. A form is something a variable can be bound to.
2. `expand` takes a bound form apart by shape, and binds the parts. A part may be a run: `...` and `...?rest`.
3. `syntax` builds a form from the parts, and names apart every variable the pattern did not bind.
4. The walker asks dialog what a form expands to, a form at a time.

## The approaches

```
A  facts      form ── entity ──┬─ form/head  todo          each subform an entity of its own
                               ├─ N1         ?t
                               └─ text       ?x

B  bytes      form ── record:<bytes only form/* formulas read>

C  record     form ── { form/head: todo, N1: ?t, label/text: ?x }      a record as #748 has it:
                                                                       pairs, sorted, nested

D  data       pattern and template stored; a native expander applies them; no rule takes part
```

| criterion \ approach | Status quo: the crate holds syntax | A, a form is facts | B, a form is opaque bytes, read by formulas | C, a form is a record as #748 has it | D, syntax is data, expanding is native |
|---|---|---|---|---|---|
| a rule about syntax is a stored rule dialog evaluates | 🔴 it is a Rust struct in a walker | 🟢 a rule over `form/*` facts whose head asserts the facts of new forms | 🟢 a deductive rule: `expand` lowers to one `form/match` premise a variable, `syntax` to a chain that fills one variable a link, as `(:id …)` lowers to `entity/derive` | 🟢 a deductive rule: `expand` is destructuring a bound record, `syntax` a term that builds one | 🔴 a pattern and a template are stored; what applies them is not a rule, and a guard such as `(unless (dialog.name _ is: ?command))` is a second language |
| a form is whole: no part of it can be retracted from under it (decided 2026-11-11, full inlining) | holds, in memory | 🔴 a subform is an entity with facts of its own, which is the shared subform that was ruled out | 🟢 one value | 🟢 one value, written and replaced whole | 🟢 one value |
| `...` and `...?rest` | matched natively | 🟡 a run is a scan of a sequence, and the rest of a dictionary is "every entry but these", which is a negation a field | 🟢 a run and a rest are forms too, so a variable holds them | ? #748 leaves the surface of destructuring open, and says nothing of binding what a pattern did not name | matched natively |
| what dialog has to gain | nothing | heads that write several entities; a formula that makes each one (there now: `entity/derive`) | three or four formulas of fixed cells; a layout for a form's bytes, canonical, that nothing else reads | #748 built, plus what it has not decided: destructuring by pattern, a rest, and a term that builds a record in a head | an index of syntax by head name; nothing in the engine |
| one layout for compound values | not a question | not a question | 🟡 a second layout beside #748's, for forms alone | 🟢 the one #748 defines; a form's atoms and attributes are the tokens claims use | 🟡 as B |
| named by hash, with names as facts | 🔴 nothing is stored | 🟡 by an entity derived from its parts | 🟢 by its merkle reference, as a rule is today | 🟢 by its canonical form | 🟢 as B |
| cost of one expansion | a match in memory | ? a join a pattern node | ? one query, in which every rule deriving the expansion is tried | ? as B | 🟢 a lookup by head name and a match in memory |

Sorted by how much a row separates the columns. The last row is unknown for every column that keeps the direction.

## Hypotheses

Each is the agent's, to be decided on what it shows.

| hypothesis | confirmed by | refuted by |
|---|---|---|
| **H1.** A form is a #748 record with three conventions: a head attribute, position-named members for the bare parts, and a domain for labels. | Every form of `prelude.clj` and `examples/todo.clj` round-trips through such a record, with atoms as values dialog has. | An atom with no value to be: a variable, `_`, `...`. Then a form needs a value type #748 does not give it. |
| **H2.** A pattern is a destructuring. | #748's destructuring, as its surface is decided, binds a rest and a run. | It names fields by attributes only, with no way to say "every other field". Then C needs its own premise, and is B with #748's layout. |
| **H3.** Asking dialog for an expansion is cheap enough that expansion can be a query a form. | With 10, 100 and 1000 rules deriving one relation, each guarded by a constant head, a query bound to one form costs about the same. | The cost grows with the number of rules. Then syntax needs to be found by head name before any rule is tried, which is D's index under B or C. |
| **H4.** An expansion is a function of the form, where it stands and the syntax in force, so it is kept by the form's hash. | Submitting `examples/todo.clj` a second time asks dialog nothing. | A guard reads state that changes without syntax changing. `(dialog.rule _ on: ?domain)` in the warning rules does; it is a diagnostic, not an expansion. |

H3 does not depend on what a form is. It can be measured now, with rules over plain facts, and it decides whether B and C need D's index.

## Decided: a form is a record

A is out by full inlining. D gives up the direction, though its index may yet be what makes expansion fast (H3). B would have been a second layout beside #748's. A form is a record as #748 has it, and so is what an entity is derived from.

That puts one thing under three that looked separate:

```
                 ┌── take apart ──▶ (expand pattern)
   a record ─────┼── build ───────▶ (syntax template)
                 └── build ───────▶ (:id …) = entity/derive of a record
```

Finding 3 of [kernel.md](kernel.md) said `expand`, `syntax` and `:id` each take whatever labels they are given, so each needed a premise of a new kind. With records there is one: a record and its fields.

## `entity/derive` takes a record

```
today     seed = derive("command.add.todo", "list of text")     two cells, so a chain:
          ?e1  = derive(seed, ?l)                               the labels in a seed,
          ?e2  = derive(?e1, ?t)                                a link a value
          this = derive(?e2, ?x)

decided   this = derive({ command.add.todo/of: ?t               one cell: the record
                          command.add.todo/list: ?l             of what the entity is
                          command.add.todo/text: ?x })          derived from
```

The record's attributes carry the domain, so there is no seed, and the order of fields is the record's own. The chain in `rust/dialog-query/src/formula/entity.rs` is a stand-in: it exists because a rule cannot build a record. It goes when one can, and the entities it names will not be the entities a record names.

## What this asks of records

#748 has decided what a record is and that a bound one is destructured. A rule about syntax, and a rule that derives an entity, need more:

| need | for | in #748 |
|---|---|---|
| build a record from what a rule has bound | `:id`, `syntax` | not there: it speaks of reading records, not of a rule making one |
| take a bound record apart by pattern | `expand` | decided; the surface is open |
| bind the pairs a pattern did not name, as a record | `...?rest` | not there |
| several values under one field, in no order | the premises of `when`, the heads of a rule | decided |
| a value for what is not one: a variable, `_` | a form holding `?x` as data | not there |

A form's parts fit pairs better than first drawn above. An application's head is the domain its labels are short for: `(xyz.todo ?t text: ?x)` already means `xyz.todo/text`. So a form needs no head field; its head is the namespace of its attributes. A run such as the premises of `when` is one field with several values, and the order they were written in is the source map's to keep, as was agreed for printing.

## A record with any value as its key

Irakli, 2026-11-11: a record is like a Clojure map, and its key may be any value, since anything has a merkle reference. He recalled conduct doing so. Checked:

| | keys | values a key | where |
|---|---|---|---|
| conduct, record | attributes a type declares; an undeclared key is an error | one | `{name: "Ada"}`, `notes/language.md` |
| conduct, dictionary | keywords | one | `{:class "todo"}` |
| conduct, `conduct.map` | any value | one | `lib/map.cdt`; canonical, so its content hash agrees with `=` (`DECISIONS.md`, "Equal maps are the same value") |
| dialog, `Type::map` | any reference | one | `rust/merkle-reference/src/lib.rs`: key and value references, ordered by key |
| #748, record | attributes, which bring a type and a pick | several | decided there |
| as stated today | any value | not said | |

So the identity of a map with any key is already defined in dialog, and conduct has such a map. But conduct keeps it apart from its record, whose keys are attributes, and #748's record is conduct's record. "A record with any value as its key" names the map with the record's word.

What each use here needs:

- **An entity derived from a row**, `:id`: keys that are attributes. #748's record does.
- **A form:** its bare parts in order, so keys that are positions, and labels that belong to no domain where the head is a concept, so keys that are plain names. Neither is an attribute. A map does; #748's record does not, unless H6 holds.

Not decided: whether dialog has one compound value, a map of which #748's record is the case where every key is an attribute, or two, as conduct has. One value is less to build and one layout. It reopens two things #748 has decided: a field is named by an attribute, and a field may hold several values.

## Hypotheses, after the decision

| hypothesis | confirmed by | refuted by |
|---|---|---|
| **H5.** One premise does both: it relates a record to its fields, building the record where the fields are bound and binding the fields where the record is. | The planner already takes a premise whose parameters differ by application: a concept query builds its schema from its own terms (`ConceptQuery::schema`). A record premise planned the same way runs `:id` one way and `expand` the other. | Building and taking apart need different costs or different requirements that one schema cannot say. Then they are two premises, as `syntax` and `expand` are two words. |
| **H6.** Needed only if a record's keys stay attributes. A form is a record whose attributes are its head and labels, with the one bare part of a read as its entity. | `prelude.clj` and `examples/todo.clj` written out as such records, with nothing left over. | A form with two bare parts that are not a run: `(syntax pattern template)` is one, and `(+ ?a : ?b)` is not, since `:` is a label. |
| **H1**, narrowed. Every atom is a value. | Text, numbers, entities and keywords are. | A variable, `_` and `...` are not values today. They need one, or a form is not a #748 record without a change to what a value is. |

H2 is now a requirement in the table above, not a hypothesis. H3 and H4 stand.

## Next

0. One compound value or two: the map and #748's record.
1. How a rule builds a record: the premise of H5, and how it is written.
2. What a variable is as a value (H1).
3. H3, measured, as a research topic.

The first is #748's missing half, and it unblocks `:id` before it unblocks syntax.
