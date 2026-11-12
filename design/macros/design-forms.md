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

## Decided: data at the center, a name over it, a layout under a version

Irakli, 2026-11-11: the ideal cannot be decided now, so go with the advice below and make what is stored versioned, records and syntax trees both, so it can be upgraded. Name things by merkle reference, now, so a later layout does not move a name. Take EDN's lesson: a syntax tree is data in a few types, so the syntax can change and the format does not. What is nearer the center should move less.

```
 moves most    syntax        which forms mean what            nothing stored moves
               ─────────────────────────────────────────────
               layout        bytes, under a version           bytes are rewritten; no name moves
               ─────────────────────────────────────────────
               name          merkle reference of the parts    moves only if a type does
               ─────────────────────────────────────────────
 moves least   data types    atom, list, map, application
```

Built, in `rust/dialog-notation/src/data.rs` and `src/syntax.rs`. The first version of this kept the notation as parsed; it keeps the syntax tree now (see [The syntax tree](#the-syntax-tree)).

| data | dialog value | referenced under |
|---|---|---|
| text, number, truth | as dialog has them | as dialog does |
| entity | `Value::Entity` | `entity:` |
| symbol, of a domain or of none | `Value::Symbol`: `xyz.todo/text`, `/todo` | `symbol:` |
| list | none yet | `Type::LIST` |
| map, from data to data | none yet | `Type::MAP` |

- **The data knows no syntax.** `Data` is a value, a list or a map. A node of the tree is a map that says its `type`, and only `syntax.rs` reads it as one.
- **The name** is the reference of the tree's data, and `form:` with it in base58 is the entity. Two orders of the same labels are one name, and so are `1.5` and `1.50`. The natural 3 and the float 3.0 are one name too, as they are one number to dialog, though they are two byte strings: a name does not follow the layout.
- **The layout** is `Data::encode`: a version byte, then tags, lengths as bijou64, a map's entries in the order of their keys. `Data::decode` refuses another version by its number, and bytes that are not the one way the data is written. It is version 2, and a stand-in for the composite value [patterns](../patterns.md) builds.
- A tree is kept on a real branch as a `record:` value under its `form:` entity and read back (`it_keeps_a_form_on_a_branch_under_the_entity_that_names_it`). Every form of the prelude and the examples has a tree that lays out and reads back (`it_keeps_every_form_of_the_prelude_and_the_examples`).
- The crate hashes no text. The names an expansion gives its own variables, and the relation a query is derived into, come from references.

**The one choice here that is dear to change** is what each thing is referenced under, since every name follows from it. A list and a map are under the types every list and map have. Chosen by the agent, and open until something real is stored.

**A limit.** `Value::Record` is referenced over its bytes today (`RECORD.reference_of`, `rust/dialog-corpus/src/identity.rs`). So a tree kept as a record has two names: its own, which holds across layouts, and the record value's, which does not. Whatever points at a tree has to point at its `form:` entity until a composite value's own name is structural, which is the first step of [patterns](../patterns/plan.md).

## Tested: what the parts of a form are

Two readings of `( )` were on the table on 2026-11-11, both Irakli's. Counted over every application in `prelude.clj` and the two examples, 151 as written and the 70 of the kernel rules they expand to (`cargo run -p dialog-notation --example shapes -- <files>`):

| parts without a label | as written | kernel | which heads |
|---|---|---|---|
| none or one | 113 | 64 | every read, formula, command, `assert!`, `report`, `expand` |
| several | 38 | 6 | `rule` 15, `when` 7, `unless` 2, `query` 2, `all` 1; `syntax` 8, `concept` 3 |
| a run, `p ...` | 12 | 0 | `when`, `unless`, `rule`, `query`, `all`, `concept` |

| | a set of labelled values: the head is `operator:`, the part with no label `operand:` | a tuple: parts in order, the labelled ones a map at its end |
|---|---|---|
| the 113 with none or one | fit | fit |
| `rule`, `when`, `unless`, `query`, `all`: 27 | fit only if `operand:` may be said several times, which makes a form #748's record, pairs and not a map | fit; the order of premises is in the data, though it means nothing |
| `(syntax pattern template)`, `(concept signature body)`: 11 | 🔴 two operands with different jobs: one of them needs a label the notation does not have | 🟢 fit, by position |
| `p ...` | "every other member", as `...?rest` is every other label | 🟢 what it is now |
| one call written two ways, `(todo ?t state: :open)` and `(todo state: :open of: ?t)` | 🟢 one form | two, unless the reader puts `of:` first |
| what data it is | a set of pairs; only #748 has it, and it is not built | a list and a map, which EDN, JSON and IPLD all have |
| forms that have to be written differently | 11, and a new label | 0 |

Found on the way: where the part with no label may also be written `of:` (#728's design), the prelude's own command row says `of:` twice, `(command.add.todo _ of: ?t …)`: 3 forms as written, 17 in the kernel. The row's field for what the command is about needs another name.

The agent's reading of the count: the tuple, with the labelled parts a map at its end.

## Built: `of:` is the label of the part with no label

Irakli, 2026-11-11: `(todo ?t state: :open)` is sugar for `(todo {of: ?t state: :open})`, done by the reader or derived by the syntax. The empty label is to be revisited, with whether `{: 1 : 2}` writes a set.

```
 written                        the tree's parameters
 (todo ?t state: :open)         items [?t]   terms {state: :open}     one tree,
 (todo state: :open of: ?t)     items [?t]   terms {state: :open}     written two ways
 (todo {of: ?t state: :open})   items [{of: ?t state: :open}]         a struct, passed as the primary
```

- **The reader cannot do it.** It does not know a call from `when`, and `when`, `unless`, `query` and `rule` each stand with one part without a label in some places and several in others (10 and 7 for `when`). A reader that put one such part under `of` would give those heads two shapes.
- **So it runs the other way, and where the tree is made.** A part with no label stays an item. Where no part is written without a label, the part labelled `of:` is the first item. One part or several, the shape is one, and nothing has to know the operator.
- **A struct at the end is a struct.** For an hour it was read as the labelled parts written another way. That and "a struct is a value like 5" cannot both hold: `(expand {note: ?n})` has to pass a struct. The reading is gone (`it_reads_a_struct_as_the_part_it_is`, `it_passes_a_struct_as_a_part`).
- `of:` beside a part with no label is a label like any other, and a relation read refuses it as the primary twice. A relation with a field truly named `of` is read by the field's full name.
- The command row said what a command is about under `of:`, beside the row's own entity. It says `about:` now.
- Tested on a branch: one concept and one relation read the same through both spellings and are written through both, and a struct in the primary's place is refused as not supported yet (`it_reads_a_call_the_same_however_its_primary_is_written`).

Open: the empty label and how a set is written. No set is built: nothing in the prelude or the examples writes one.

## The syntax tree

Irakli, 2026-11-11: parsing transforms notation into a syntax tree, and the tree is kept in the data model, in maps where they fit and lists and tuples where those do. His example:

```
(todo ?t state: :open)

{ type: :application
  operator: 'todo
  operand: { type: :parameters
             terms: { of:    { type: :variable name: "?t" }
                      state: { type: :constant value: :open } } } }
```

So there are three things, and the proof of concept has kept the first as if it were the third:

```
 notation ──parse──▶ syntax tree ──expand──▶ kernel rules ──compile──▶ dialog
 text                nodes, by name          what dialog runs
                     kept as maps, lists, tuples
```

Prior art, checked:

- **conduct** has the same three passes: `Sexp` from the reader, each with a `Span`; `Expr`, "expressions that still refer to things by name"; then `Term`, name-free and hashed, where a reference is `Ref(Hash)` (`src/reader.rs`, `src/syntax.rs`, `src/term.rs`). Its call is `App(operator, Vec<Arg>, Span)` with `Arg { label: Option<String>, value }`: arguments in order, the first allowed no label.
- **Racket** makes the implicit explicit while expanding: a pair whose head is not syntax gets `#%app` put in front, a literal gets `#%datum`, an unbound name `#%top`. A syntax object is a value with its scopes, its source location and properties. A fully expanded program has a small fixed grammar. So `:application` and `:constant` are `#%app` and `#%datum`, and where a node was written and what a name resolved to belong on the node.

Nodes the prelude and the examples need, in the shape of the example:

| node | fields | written |
|---|---|---|
| `:application` | `operator`, a node; `operand`, a `:parameters` | `(todo …)`, `(+ …)`, `(when …)`, `(?name …)` |
| `:parameters` | `items`, a list of nodes; `terms`, a map from label to node; `rest`, where one is written | the parts of an application |
| `:variable` | `name`, text | `?t` |
| `:blank` | | `_` |
| `:constant` | `value` | `"text"`, `42`, `true`, `:open`, `todo:1` |
| `:reference` | `name`, a symbol | `todo`, `xyz.todo`, `xyz.todo/text`, `+` |
| `:map` | `entries`, a map from node to node | `{foo bar}`, `{label: bar}` |
| `:tuple` | `items`, a list of nodes | `[a b]` |
| `:repeat`, `:rest` | `of`, a node; `name`, text | `p ...`, `...?fields`, in a pattern |

**Most of these nodes are in dialog already** (Irakli, the same sitting; checked in `rust/dialog-query/src`):

| the example | dialog-query | written as a map today |
|---|---|---|
| `:application`, `operator`, `operand` | `Proposition`: `ConceptQuery { predicate, terms }`, `FormulaQuery`, `Constraint` | `{ "assert": …, "where": … }` |
| `:parameters`, `terms` | `Parameters`, a map from name to `Term` | the `where` map |
| `:variable`, `:blank` | `Term::Variable { name }`, with and without a name | `{ "?": { "name": "t" } }`, `{ "?": {} }` |
| `:constant` | `Term::Constant(Value)` | the value |
| what must and must not hold | `Premise::Assert`, `Premise::Unless(Negation)` | a rule's `when` and `unless` lists |
| a rule | `DeductiveRule`, `InductiveRule` | `{ "deduce" or "assert!": …, "when": […], "unless": […] }` |

They are named and versioned already too. A rule's identity is a merkle reference of its canonical spelling, in which variables are renamed by structure and premises sorted (`rule/canonical.rs`), so neither a name nor the order of premises is in it. A stored rule is `{ "format": 3, "rule": … }`, and a reader refuses a format newer than its own (`rule/body.rs`).

So the tree for what dialog runs exists. `kernel.rs` in the proof of concept is a second copy of it. What dialog's forms do not have is what the notation adds before that point:

- an operator by name, `'todo`: dialog's `ConceptQuery` holds the concept itself, inlined;
- a term that is an application, a map or a tuple: a `Term` is a variable or a constant;
- a rule of several heads, a head that reports, and `unless` over more than one proposition;
- `expand`, `syntax`, `p ...` and `...?rest`: the forms a rule about syntax is made of.

Two things the example does not yet say, both from the count above:

- **An operator is a node.** 20 of the 151 applications have a variable for a head, `(?name ?this ...?fields)`.
- **Where several parts without a label go.** 38 of 151 have several, and `terms` is a map. Whatever the answer, it cannot depend on how many there are: the pattern `(when ?premise ...)` has to meet `(when A)` and `(when A B)` as one shape. Dialog's own answer for a rule is a field a run: `when` and `unless` are lists in a map. That covers `rule`, `when` and `unless`. It leaves `query`, `all`, `syntax`, `concept`, and patterns.

Decided 2026-11-11, by the maintainer, after a day on option B (every part labelled but the primary): no. A rule about syntax reads any form, and a form that must label its parts to be read is a limit on what syntax can be. So parts with no label stay, in order, and `concept` keeps its signature and its body with no label on either.

### Built: `Syntax`

`rust/dialog-notation/src/syntax.rs`. The example above comes out as:

```
{ type: :application
  operator: { type: :reference name: todo }
  operand: { type: :parameters
             items: [{ type: :variable name: "t" }]
             terms: { state: { type: :constant value: :open } } } }
```

It differs from the example in two places, both from the count:

- The operator is a node, since 20 applications have a variable there.
- The part with no label is the first of `items`, and not `of` among the `terms`. `terms` is a map and has no place for the second and third such part of `(when A B C)`, and `(when A)` cannot have another shape than `(when A B)`. `of:` written out lands in the same place, so the two spellings of a call are one tree and one name.

What the tree forgets of the notation: the order labelled parts were written in, a number's digits, and that `...` came after what it repeats. What it refuses that the reader takes: a label twice, a `...` with nothing before it, a `...?rest` anywhere but among the parts of an application.

Expanding and compiling still read the notation as parsed. They are to read the tree, and that waits on the question below.

### Decided: a name resolves to a concept

Irakli, 2026-11-11: `(concept (todo ?t text: ?x) …)` mirrors tonk's `concept! &todo`. It defines a concept and a name: `id:todo` is associated with the concept's entity, and an application of `todo` resolves to that entity. Two concepts may be named `todo` with different signatures, and either should resolve. Two with one signature he expects not to exist: an editor could ask which, an agent would trip. Unison is the prior art to learn from.

```
 (concept (todo ?t text: ?x) …)  ──▶  concept:9f…   { text: xyz.todo/text, last, text }
                                      id:todo ── referent ──▶ concept:9f…

 (todo ?t text: ?x)  ──resolve──▶  (concept:9f… ?t text: ?x)  ──▶  ConceptQuery { predicate, terms }
```

So `concept` is not a rule about syntax, which is how the proof of concept has it. Checked against what exists:

- **Dialog.** A concept's entity is made of its attributes and each field's options. The names of its fields and its description are not in it (`ConceptDescriptor::identify`; `notes/notation.md`: "two concepts with the same attributes but different names are the same concept").
- **Tonk.** `id:<name>` carries `db.name/referent`, cardinality one, so a second concept under a name supersedes the first. Field names are facts on the concept's entity, `db.concept.with/<field>` (`rust/tonk-core/src/meta.rs`, `rust/tonk-schema/src/concept.rs`). A stored rule holds the concept itself, not its name.
- **Unison.** A definition may have several names. A name with two definitions is a conflict: both are kept, it arises by merge as well as by hand, and someone resolves it. A name that fits several definitions resolves if exactly one has the expected type, and is an error otherwise. `name#hash` says which.

What "the same signature" comes to, given dialog's identity:

| two concepts named `todo` | told apart by | so |
|---|---|---|
| the same attributes, whatever the fields are called | nothing: they are one entity | there is nothing to pick. But the entity has two sets of field names, and tonk writes both onto it |
| different labels | the labels an application writes | a write says every field, so a write always resolves. A read that names only labels both have does not |
| the same labels, different attributes | nothing an application writes | only the entity says which |

The proof of concept already behaves as the second and third rows say, by pattern: `it_tells_two_concepts_of_one_name_apart_by_the_labels_written`.

The agent's hypotheses, to decide on:

| hypothesis | confirmed by | refuted by |
|---|---|---|
| **H7.** One concept per name and signature cannot be kept as a rule about definitions. | Two replicas each define `todo` over the same labels and different attributes, offline, and sync. Both definitions exist and nobody installed a conflict. | Something that refuses the merge, which dialog does not have. |
| **H8.** A name's referents are all kept, and an application that fits several is refused with the candidates. Electing one moves a name's meaning for one of two authors with nobody deciding. | An agent given the refusal and the candidates rewrites the application and goes on; an agent given the elected concept writes to the wrong one and is told nothing. | A case where a refused application is worse than a wrong one. |
| **H9.** A concept's field names belong to the name, not to the concept. | Two definitions that differ only in what a field is called give one entity with both names on it in tonk, and a write through either then has to say which names are its own. | Reading such a concept back in tonk gives the field names of the definition that was asked for. Not run. |

Open, and one of them next:

- **What a name with two referents does where an application fits both.** Tonk elects by `last`. Unison keeps both and refuses.
- How an entity is written where a name stands, `name#hash` in Unison. An entity alone has no field names (H9).
- Which of the proof of concept's concepts are dialog's. `todo` and `author` are: every field is one attribute of the one entity. `list-item` is not: its entity is the value of the attribute it reads. It is a derived concept, a rule that concludes one.

## What to store, and how

Superseded for forms: what is kept is the syntax tree, above, in maps and lists, and not a record of positions. What stands of this advice is that no domain is the special domain, and that a name and a label are symbols of none.

Irakli, 2026-11-11: a record is a constrained map, its keys attributes, as an integer is a constrained number. His hope: a label is an attribute by default, of no domain or a fixed one, read `last`, so an attribute is a key like any other, as Clojure's `:name` and `:ns/name` are both keywords. He asked for advice on what to store.

The advice, the agent's:

**Store one thing, the record. Leave the map as the type above it.** Once a label is an attribute, nothing here has a key that is not one. The map already has an identity (`Type::map`) and needs no layout until something has a key that is not an attribute.

**No domain is the special domain.** `text:` in a record is the attribute `/text`.

- `Relation` takes it today: it asks only for a `/`, 64 bytes at most and no NUL, with a note that more is undecided (`rust/dialog-corpus/src/model/relation.rs`).
- A fixed domain would spend its bytes in every key's 64, and anyone could write it by hand. The empty domain cannot collide with a real one: the domain half of an attribute is a `Symbol`, which is never empty.
- A claim under it is refused today, where the attribute descriptor is read. That is the line to keep. A claim is selected by attribute across a replica, where a bare name collides between applications; a key in a record is never selected (#748), so there a bare name is local.
- It reads `last` and has no type. A record written with one value a key is then a Clojure map, and #748's several values a field stay possible without a form using them.

**A form is a record, stored as written.**

```
(xyz.todo ?t text: ?x)   ──▶   { /A0: /xyz.todo      head and bare parts under positions,
                                 /A1: /?t            the name half an attribute already may be
                                 /text: /?x }        labels under their own names
```

Every key is an attribute, so a form is a record in the constrained sense, and H6 is not needed.

**An atom that is not a value is a symbol with no domain.** `/todo`, `/xyz.todo`, `/?x`, `/+`, `/_`, `/assert!`. Datomic and Clojure do the same: a variable in a query that is data is a symbol whose name starts with `?`. `Value::Symbol` holds these today and a branch stores and returns them (`it_holds_a_symbol_with_no_domain_as_a_value_and_not_as_an_attribute`). So H1 needs no new type of value. What it costs: a name is 63 bytes at most, and `Value::Symbol`, which is documented as an attribute, becomes a symbol of which an attribute is the constrained case.

**The layout is #748's candidate, unchanged.** A pair is still a key's tail, the attribute, the value's type byte and its order-preserving encoding (`ordvalue.rs`), because a key is still a `Relation`. So a record's keys are still the tokens claims use, which is what #748's storage hypothesis rests on. A map of any key would have given that up.

Open in this:

- **Positions.** `Position` is made for members inserted concurrently, with a bias from the member's identity. A form is written whole, so its parts need the one position the nth part always gets, or equal forms are not equal bytes.
- **`Relation::split` refuses an empty domain**, and whatever calls it on a symbol that is a value has to stop doing so.
- **A read with no pick.** The proof of concept reads `all`; dialog's descriptor and the hope above say `last`.

## A struct with a variable in it

Decided 2026-11-11, by the maintainer: it is a destructuring pattern, and that dialog has none is a gap to close. It hinges on composite values, which dialog has not had, and closes with them. It is a topic of its own, [patterns](../patterns.md) (#777): a parameter holds a pattern, of which a variable and a constant are the leaves, and the planner and inference look inside one. Its first step is the composite value, `Value::Record` given a structure, laid out as dag-cbor under a version byte until #748 decides the layout to keep.

What the notation needs of it, for whoever builds records:

| need | for |
|---|---|
| a struct with nothing to bind is a value, like 5 | `{text: "hi"}` as a field's value, a constant in a rule |
| a struct with a variable in it, where a bound record stands, takes the record apart | `(expand {note: ?n})`, reading a command's row |
| the same struct, where its variables are bound and a value is wanted, is the record | `(:id {text: ?x})`: `entity/derive` over a record |
| the pairs a pattern did not name, bound as a record | `...?rest` |
| the keys a label writes: an attribute of a domain, and a name of none | `{xyz.todo/text: ?x}`, `{text: ?x}` |

Until then the notation refuses a struct where a term stands, and `entity/derive` stays the two-cell stand-in. H5 below asked whether this is one premise that runs both ways; that it is a pattern in a term is now the maintainer's, and how dialog evaluates one is the patterns topic's to build.

## Hypotheses, after the decision

| hypothesis | confirmed by | refuted by |
|---|---|---|
| **H5.** One premise does both: it relates a record to its fields, building the record where the fields are bound and binding the fields where the record is. | The planner already takes a premise whose parameters differ by application: a concept query builds its schema from its own terms (`ConceptQuery::schema`). A record premise planned the same way runs `:id` one way and `expand` the other. | Building and taking apart need different costs or different requirements that one schema cannot say. Then they are two premises, as `syntax` and `expand` are two words. |
| **H6.** Needed only if a record's keys stay attributes. A form is a record whose attributes are its head and labels, with the one bare part of a read as its entity. | `prelude.clj` and `examples/todo.clj` written out as such records, with nothing left over. | A form with two bare parts that are not a run: `(syntax pattern template)` is one, and `(+ ?a : ?b)` is not, since `:` is a label. |
| **H1**, narrowed. Every atom is a value. | Text, numbers, entities and keywords are. | A variable, `_` and `...` are not values today. They need one, or a form is not a #748 record without a change to what a value is. |

H2 is now a requirement in the table above, not a hypothesis. H3 and H4 stand.

## Next

1. What a name with two referents does, above. Then `concept` defines a concept and a name, an application resolves to it, the expander and the compiler read the tree, and `kernel.rs` gives way to dialog's own forms.
2. A struct as a term and `entity/derive` over a record: with [patterns](../patterns/plan.md).
3. Syntax kept on the branch as trees, and the walker asking the branch.
4. H3, measured, as a research topic.
