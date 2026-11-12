# Macros as rules: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | The kernel and the core forms run on a real branch through `rust/dialog-notation` ([poc.md](poc.md)); the prelude is rules only. The syntax tree is built, apart from the notation as parsed, and kept as data: maps that say their `type` ([design-forms.md](design-forms.md#the-syntax-tree)). A struct is a value and, with a variable in it, a pattern, which is a topic of its own with an agent on it (#777). Rules of several heads are #776. Expanding and compiling still read the notation as parsed, and `kernel.rs` still copies dialog's forms. | **Designing.** Direction decided by the maintainer. A proof of concept runs. A name resolves to a concept, by the maintainer; the pass that does it is not built. |
| **Agenda** | What a name with two referents does where an application fits both: elect, as tonk does, or keep both and refuse, as Unison does (H7, H8). Whether a concept's field names belong to the name (H9). How a query reports. Whether dialog can be asked for an expansion a form at a time (H3). What a read with no pick reads. | Next: that answer, then `concept` defines a dialog concept and a name, and an application resolves to it; the expander and the compiler read the tree; dialog's own forms replace `kernel.rs` once a parameter holds a pattern. Not waiting: a helper rule for a negated conjunction. Waiting on patterns (#777): a struct as a term, `entity/derive` over a record, syntax kept on the branch. Waiting on #776: a rule of several heads. |

## Candidate problems

"Macros as rules" is one possible answer. These are the problems it could be an answer to. Each is a hypothesis in the [ledger](claims.md) until evidence or the maintainer says otherwise.

1. **A form costs a release.** Adding `view` or `event`, or changing what `concept` takes, means changing a parser and an analyzer in Rust and shipping them. A space cannot add a form of its own.
2. **A definition's meaning is in code, not in data.** What `concept` or `rule` takes is known to the analyzer and to an 831-line guide. An agent or a tool cannot ask the database what forms exist and what each one takes.
3. **A definition's meaning can move after it is written.** A stored form that refers to something by name means something else once the name is re-pointed.
4. **Two builds may read one document two ways.** What a stored form means depends on the analyzer that reads it, and replicas do not all run the same build.

## Approaches parked for Direction

Not examined yet. They are columns, not recommendations.

- **Status quo.** A parser and compiler in Rust know every form (tonk-notation today; the `dialog-notation` crate #728 plans).
- **Everything is a rule** (Irakli). Macros are rules, and the compiler is a rule too.
- **Macros are rules, compiling is a formula** (Irakli). Rules turn a surface form into kernel forms; one built-in formula turns a kernel form into what dialog stores.
- **Macros are data, expanding is built in** (the 2026-05-16 journal). A macro is a stored pattern and templates; a native expander applies them. No rule takes part.

## 2026-11-10, Diagnosing: from "macros as rules" to what is in the way

**Asked.**

- Irakli brought the list in the [topic's description](../macros.md#description), and asked: is the compiler itself a rule, or is there a built-in formula that takes syntax and compiles it?
- The agent's answer, as a hypothesis (H1 in the ledger): both, split at the kernel form. Three reasons the compiler cannot be a rule all the way down. Something has to compile the first rule. A rule's identity comes from a canonical labeling that searches (colour refinement, then individualisation), which is not a deduction. And a rule that does not match says nothing, where a compiler has to say why a form was refused.
- Open, asked of Irakli: which of the four candidate problems is in the way today, and what happens because of it.

**Learned.**

- "All primitive forms are expressed through macro rules" and "macros are rules" cannot both hold all the way down: `rule` would be defined by a rule. Irakli's own follow-up, the built-in formula, is where the regress stops. The word "primitive" was carrying two meanings; the [glossary](glossary.md) splits it into surface form and kernel form.
- Earlier readings by the agent in this conversation were off the mark and are withdrawn: conduct as the host language, and a meta-circular evaluator with bindings threaded through premises. Neither is in the maintainer's notes.
- The 2026-05-16 design had no problem assembling a form from rows, because its macros expanded to triples. The problem appeared on #728 when a definition became one value under its hash. H4 says a set-collecting `reduce` closes it.

**Decided.** Not decided. What would decide the phase: Irakli's answer to the open question, written into the Problem section.

## 2026-11-11, Designing: the kernel, and the core forms run in a model

**Asked.**

- Irakli declined to state a problem and asked for a design and a plan, not an implementation. Direction: macros as rules, a minimal kernel, hash-addressed definitions, nothing built on conduct or wasm, and as fast as the system is today.
- He asked that claims be confirmed in the code and not left as hypotheses, and that notes not be trusted. The planner, the rule evaluator and fixpoint, commit-time induction, the value and type model, resolvers and the write instructions were read.
- He set the spellings: a formula is applied by keyword, `(:text ?x)`, or by a bare operator with `:` as the second operand's label, `(+ ?a : ?b is: ?c)`; equality is `(:value ?a is: ?b)`; `when` and `unless` are siblings in a rule; "application", not "call"; `query` is a rule marked for the report; a head that persists is `assert!`.
- He asked for the core forms defined in the slim kernel, to see whether it works before the kernel is built.

**Learned.**

- From the code: `reduce` is refused in deductive rules; a recursive group is evaluated bottom-up in full and non-recursive rules get the caller's bindings; a rule installed in a commit fires in that commit; `Record` is opaque bytes; formulas have fixed cells; a resolver may read content-addressed storage only; the store has two write instructions, `Assert(fact, pick)` and `Retract(fact)`; a rule's kind is its descriptor's head field, `deduce`, `assert!` or `retract!`; `unless` negates one proposition.
- So: `assert!` and `retract!` cannot both be macros; expansion cannot be a resolver, since names are read through the mutable head, and a query at a pinned revision is a `Snapshot`; a read has to be one entity and several fields.
- Withdrawn by the agent: conduct as the host, a meta-circular evaluator with threaded bindings, building forms with `reduce`, dag-cbor for forms, shared subforms, "a macro applies from the next document", and the walker as a resolver.
- The core forms work in the model with one rule applying every macro. `concept` is one line: a macro that defines a macro.
- With `!` marking what stays, issuing a command is a plain `assert`, and the model no longer tells a command by its domain.

**Decided.**

- The direction, by the maintainer, on an opinion. No problem statement.
- The kernel and its spellings as in [kernel.md](kernel.md), except the pick wrapper, which is proposed.
- `!` marks what stays, which departs from #728's design as of R25.
- Forms are fully inlined, with a canonical encoding (bijoux for integers). The #733 tokenization is for storage. A source map keeps, for each reference, its name, the revision and the entity.
- Not decided: findings 3 to 8 of [kernel.md](kernel.md).

## 2026-11-11, Designing: syntax is a rule

**Asked.** Irakli found the first definitions unreadable: a top-level write defined as a macro of itself, and `concept` defined as a macro that writes a macro. He asked for Racket's style, then asked why syntax is not a regular rule, with `(expand pattern)` in the premise and `(syntax template)` in the head, and whether those two should be the quote and unquote. He asked that a misspelled command be a warning and not a refusal, as tonk's language server reports.

**Learned.**

- A macro written as a rule needs no stored pattern and template, no rule that applies them, and no formulas to match, fill, get and bind. The prelude went from 104 lines to 64 and every check holds.
- `expand` and `syntax` are the quoting forms: the walker does not go into them, a variable the pattern binds is replaced, and any other variable in a template is renamed at each expansion.
- A concept definition is one rule: `(todo …)` stands for that read of `xyz.todo`. Shown as that rule it is plain; shown as the syntax that writes the rule it was not.
- A diagnostic is something a rule derives about a form.

**Decided.** Syntax is a rule with `expand` and `syntax`. A top-level write is kernel. A misspelled command is a warning. Not decided: whether `concept` stays a word of its own, since it only shortens a rule.

## 2026-11-11, Designing: the kernel on Dialog-DB

**Asked.** Irakli asked for a proof of concept in dialog itself: concepts, rules and queries defined in the notation and evaluated. He asked that domains be told from other names, since syntax needs them. He corrected a concept the agent wrote with a check in its body: a concept maps facts to itself and back, and a check has no inverse. He then said a concept may read several entities, a backlink being one, and a rule may have several heads of different kinds; what dialog lacks for these it can be given.

**Learned.**

- The kernel compiles to descriptors Dialog-DB already has, and the to-do, the counter, a backlink, a recursive relation and a query of several premises run on a real branch. See [poc.md](poc.md).
- A rule and the command it reads can share a document: the rule fires in the transaction that installs it.
- Dialog-DB has no formula that makes an entity, so a rule cannot issue a command yet.
- `unless` takes one proposition whose own variables are blank. A formula inside it, a concept spanning entities under it, and a variable repeated inside it are each out of reach.
- The rules about syntax are held and matched by the crate, not stored. That part of the direction is not shown.
- A report comes back in relations, not in the words the query used.
- Two commands in one document share a round. Where both write one `last` field, the cell holds both claims.

**Decided.** Nothing new. The proof of concept is evidence for the kernel table, not a decision about it. Open for the maintainer: whether `report` takes a template; whether a write through a concept may leave fields out; what a read with no pick reads.

## 2026-11-11, Designing: what a form is to dialog

**Asked.** Irakli answered the proof of concept's findings: a formula should derive an entity, so a rule can issue a command; a variable in `unless` is one `when` binds; a query reports the relations queried; a write through a concept is not partial; and two writes of one round to a `last` field are decided as concurrent candidates are. He agreed the next step is forms as values.

**Learned.**

- `entity/derive` has two cells, so `(:id …)` is a chain a value long. A rule issues a command, and another reads it the round after.
- A negation requires every variable bound, a blank excepted (`negation.rs`). So a field left out of a concept under `unless` is blank, and a variable the author wrote there is refused.
- The election between two claims of one round is by the hash of the value (#752), and the test finds the same winner in either order.
- #748 is deciding what a record is: an atomic value of attribute and value pairs, nested, destructured in a query. A form is a candidate for exactly that, and #748 leaves open the three things a form needs: destructuring by pattern, binding what a pattern did not name, and building a record in a head.
- A revision is already one record a formula takes apart (`formula/revision.rs`), and a formula may keep its outputs by input (`#[formula(cached)]`).

**Decided.**

- A query reports relations. A write says every field. Both by the maintainer.
- `(:id domain label: term …)` is the domain and the sorted labels as a seed, then each value in turn. On an opinion of the agent's, open to change: the scheme `id:`, and that order of labels does not matter.
- Not decided: what a form is to dialog. [design-forms.md](design-forms.md) has the approaches and four hypotheses. It rests on an open question: whether forms are a use case #748 carries.

## 2026-11-11, Designing: a form is a record

**Asked.** Whether forms are a use case the record values topic (#748) carries. Irakli: he imagined records here, and imagined `entity/derive` taking a record as well.

**Learned.**

- With a record as its input, `(:id …)` has no open labels of its own. `expand`, `syntax` and `:id` were three premises of a new kind; they are one thing, a record and its fields.
- A command's row is then the record of its fields made into an entity: the attributes carry the domain, so the seed the chain needed goes.
- The two-cell `entity/derive` built this morning is the wrong shape. It is a chain because a rule cannot build a record, which is the thing to build.
- #748 decides how a bound record is read. It says nothing of a rule making one, of binding what a pattern did not name, or of a variable as a value.

**Decided.** A form is a record, and `entity/derive` takes one. By the maintainer. The two-cell formula stays as a stand-in, marked so, since replacing it needs a record layout and that is #748's to decide.

**Backtracked.** Not a phase, a build: `entity/derive` was built before what it takes was asked.

## 2026-11-11, Designing: which record

**Asked.** How a record is written in the notation. Irakli: he imagined records like Clojure maps, with any value as a key, since anything has a merkle reference; conduct does something like it.

**Learned.**

- conduct has three compound values, not one: a record keyed by declared attributes, a dictionary keyed by keywords, and `conduct.map`, a library hash map keyed by any value and canonical so its hash agrees with `=`. The any-key one is the map.
- dialog's `Type::map` already gives a map of any keys an identity: key and value references, ordered by key.
- #748's record is conduct's record: attribute keys, typed on read. It also lets a field hold several values, which a Clojure map does not.
- `:id` needs only attribute keys. A form needs positions and plain names as keys, which are not attributes.

**Decided.** Not decided: one compound value or two. Written out in [design-forms.md](design-forms.md), "A record with any value as its key".

## 2026-11-11, Designing: what to store

**Asked.** One compound value or two. Irakli: a record is a constrained map, its keys attributes, as an integer is a constrained number. He hoped a label could be an attribute by default, of no domain or a fixed one, read `last`, and asked for advice on what to store and how.

**Learned.**

- `Relation` already takes `/text`: it asks for a `/` and nothing of the domain. `Relation::split` does not.
- A symbol with no domain is stored and read back as a value today, `/?x` and `/+` included. A claim under one is refused where the descriptor is read.
- With labels as attributes of no domain and bare parts under positions, every key of a form is an attribute. Nothing at hand needs a key that is not.
- Keys staying `Relation`s keeps #748's storage hypothesis whole. The agent's worry of the sitting before, that one value for map and record would cost it, does not arise.

**Decided.** Not decided; advised, in [design-forms.md](design-forms.md), "What to store, and how": store the record, leave the map a type; no domain is the special domain; a form is a record as written; an atom that is not a value is a symbol with no domain.

## 2026-11-11, Designing: plates

**Asked.** Whether the advice on what to store stands. Irakli, by voice: he cannot decide the ideal now; go with the advice and version what is stored, records and syntax trees, so it can be upgraded. Name by merkle reference now, so a layout does not move a name. Take EDN's lesson, a syntax tree as data in a few types, so syntax changes without the format changing. What is nearer the center moves less.

**Learned.**

- Three things were being treated as one. The data types, the name over them and the layout under them move at different rates, and only the types are dear to change.
- With an application as a data type of its own, a form needs no positions. The open point about one position for the nth part goes.
- The data need not know a variable from a name: both are symbols. Only the syntax reads `?`.
- `Value::Record` is referenced over its bytes today, so a record value's own name does follow its layout. A form's name has to be the form's reference, kept beside the bytes, not taken from them.

**Decided.** By the maintainer: go with the advice, versioned; names are merkle references. By the agent, open until something real is stored: a list and a map are referenced under the generic types, an application under `form:`. Built as `rust/dialog-notation/src/data.rs`.

## 2026-11-11, Designing: what the parts of a form are

**Asked.** Irakli asked what an atom is beside a `Value`, what `{k v}` and `{label: v}` differ in, and what an application is as data, since EDN, JSON and IPLD have none. Then, by voice: the part with no label is short for `of:`, and `:` is a label whose name is empty. As provocations: `( )` as a set, its head under `operator:` and the part with no label under `operand:`, so everything has a label; or `( )` as a tuple, which he called the more honest framing. He asked for it to be tested.

**Learned.**

- An atom is a `Value`. `{k v}` and `{label: v}` are one map written two ways. The application type was the reader's shape, not data anything else has.
- #728's design says every definition has exactly one primary, written without a label, which may also be written `of:`. That holds for calls. It does not hold for `rule`, `when`, `unless`, `query`, `all`, `syntax` and `concept`, there or here.
- Counted: 113 of 151 applications as written have at most one part without a label. Of the 38 with several, 27 are runs of forms that say what they are, and 11 are `syntax` and `concept`, whose two parts have different jobs.
- The prelude's command row uses `of:` for what the command is about, beside the entity. Under #728 that is `of:` twice.

**Decided.** Not decided between set and tuple. The count is in [design-forms.md](design-forms.md).

## 2026-11-11, Designing: a tuple, and `of:`

**Asked.** After the count, Irakli: `(todo ?t state: :open)` should be sugar for `(todo {of: ?t state: :open})`, by the reader or derived by the syntax. A set might be written `{: 1 : 2}`, a map whose keys are its values. Told that `:` is already the label of no name, and that with the labelled parts a map `{: ?b}` is a formula's second operand: the empty label will be revisited.

**Learned.**

- With the labelled parts a map at the end of a tuple, the primary under `of:` and the record written out are one list already. Only the primary with no label is another.
- The reader cannot fold it in: `when`, `unless`, `query` and `rule` stand with one unlabelled part and with several.
- `of:` as the primary takes the name from every relation. The proof of concept had used it twice, in the command row and in a test's `fam.parent`.

**Decided.** Built on the maintainer's sugar, with the choice of where left to the agent: `( )` is a tuple, a list with its labelled parts a map at the end; the application type and the `form:` type are gone; the syntax reads `of:` as the primary. Not decided: the empty label, how a set is written, and whether `[ ]` is the same list.

## 2026-11-11, Designing: what is at the center

**Asked.** Irakli, after the tuple was built: `{: 1}` is a set and the empty label goes, since it cannot be a symbol. `( )` is a list, as every lisp calls it, and `[ ]` a tuple. What is being looked for is a textual encoding, not a visual representation of forms. Check what WAT and wasm's binary format have for forms; it may suffice. And `{foo bar}` could be sugar for `(map (foo bar))`, `[a b]` for `(tuple (a b))`.

**Learned.**

- WAT's tokens are a keyword, a number, a string, an id and the two parentheses. `[`, `]`, `{` and `}` are not tokens. A named part is a nested list headed by a keyword, `(param $x i32)`. `(@id …)` is an annotation, read as whitespace. So WAT's forms are atoms and lists and nothing else.
- Wasm's binary format has no encoding for a form in general: each construct has its own bytes. What is general in it is a vector as a count and its members, a name as bytes, and custom sections, where names live apart from the code they name.
- Sugar over headed lists puts map, tuple and set in the syntax, the plate that moves most, and leaves atoms and lists at the center.
- It costs a map its order-free name: `(map (a 1) (b 2))` is a list, so the order of its entries is in its name unless whatever writes it sorts.
- This reverses "a form is a record" of this morning: a form is a list, and a record is something a form may denote.
- Without the empty label a formula's second operand has no spelling: `(+ ?a : ?b)`, `(< ?p : 10)`, and `:command` six times in the prelude.

**Decided.** Not decided. The agent built the tuple and the map as data on a parked question and one unanswered, and three choices in it are now overturned. No more is built until the center is settled: whether forms are atoms and lists only, how a formula's second operand is written, and whether sugar for a map sorts.

## 2026-11-11, Designing: three things, not two

**Asked.** Whether forms are atoms and lists only. Irakli: no, that mixes things up. The parser reads `{foo bar}` as `(map (foo bar))`: going from parsed notation to the syntax tree is a transformation. It does not say the syntax tree is stored as lists. In the data model the syntax uses a map where a map makes sense, and a list or a tuple where those do. And "record" as a value type misled: it meant an atomic composite value, atomic in that it is updated whole, composite in that it is made of other values.

**Learned.**

- There are three things: the notation as parsed, the syntax tree the parser makes of it, and the data the tree is kept in. The agent had the first stored directly as the third.
- `(map (foo bar))` names a node of the syntax tree, a map written out. It is not a map, since `foo` and `bar` may be anything a rule has yet to bind.
- A record is any composite value kept whole: a map, a list, a tuple. #748's pairs keyed by attributes are one kind of it.

**Decided.** By the maintainer: a record is an atomic composite value; the parser transforms notation into a syntax tree; the tree is kept in maps, lists and tuples as each fits. Not decided: what the nodes of the tree are and which of the three each is kept as.

## 2026-11-11, Designing: the syntax tree

**Asked.** Whether a node is a head and one argument. Irakli gave the tree for `(todo ?t state: :open)`: a map of type `:application`, with an `operator` and an `operand` of type `:parameters` whose `terms` map `of` to a `:variable` and `state` to a `:constant`. Racket and conduct should have prior art.

**Learned.**

- conduct has three passes, `Sexp`, `Expr` and `Term`, and its call keeps arguments in order with a label each may lack. Racket puts `#%app` and `#%datum` in while expanding and keeps scopes, source location and properties on a syntax object.
- 20 applications have a variable for a head, so an operator is a node.
- 38 have several parts without a label. A map of terms has no place for them, and a rule that puts one such part under `of` and several elsewhere gives `(when A)` and `(when A B)` two shapes, which the pattern `(when ?premise ...)` cannot meet as one.

- Irakli, the same sitting: most of these forms are in Rust already. They are: `Premise`, `Proposition`, `Parameters`, `Term` and the rule descriptors, each written as a map, named by a canonical reference that ignores variable names and the order of premises, and stored under a format number. `kernel.rs` copies them.

**Decided.** By the maintainer: a node is a map with a `type`; an application has an operator and an operand of parameters. Not decided: where several parts without a label go. What is to be designed is narrower than it looked: the tree before names are resolved and syntax expanded, and nothing after.

## 2026-11-11, Designing: what is left of the gaps

**Asked.** Irakli went through the four things dialog's forms lack. An operator by name: tonk's analyzer parses to an application by reference and then resolves it inline; do the same. A nested application is sugar: where a premise's `is:` variable is used in one other premise, that premise may be written in the variable's place with `is:` left out. A map and a tuple: unsure what is missing. Several heads: close it in dialog. A head that reports: perhaps metadata, perhaps `query` as a command with a built-in rule, perhaps a macro to a rule and an assertion that its deductions be reported. And is `unless` over several propositions real, since several negations read as an or?

**Learned.**

- tonk's analyzer does what he recalled: `tonk_notation::Syntax` goes through *resolve*, which binds every reference and keeps the source shape, then *expand*, which lowers sugar to dialog's forms (`rust/tonk-analyzer/src/analyzer.rs`). Its expand also puts in an omitted `this:` as `id:<body-digest>`: an entity derived from what the application says.
- The proof of concept already lifts a nested application to a premise with a variable of its own for `is:`.
- By the same rule a map or a tuple written with variables is a nested application of what builds it. What dialog lacks is then a composite value and one proposition that relates it to its entries.
- The agent had the `unless` gap wrong. A body with several negated atoms is ordinary datalog and means none of them holds; dialog has it as a list of negations. What no datalog has as a primitive is the negation of a conjunction, which is written as a helper rule and one negated atom. Bloom's `notin` takes one collection (Bud's `PushNotIn`). So nothing is missing in dialog: the notation's compiler writes the helper rule.

**Decided.** By the maintainer: names resolve in a pass of their own, as in tonk; a nested application is sugar; rules of several heads are to be built in dialog. Not decided: how a query reports; where parts without a label go in the tree.

## 2026-11-11, Designing: a struct is a value

**Asked.** How a map with variables is built, for which the agent had proposed a `map` application lifted to a premise. Irakli: the notation should support structs and need no map form. A struct is `Value::Record`, or a `Value::Map` is added; the point is that it is a value, just like 5.

**Learned.**

- A struct with nothing to bind in it is a constant, and needs only a composite value dialog understands. `Value::Record` is opaque bytes today, written by revision records in a layout that is the reader's own (`history/revision_record.rs`).
- A struct with a variable in it is a value once the variable is bound. Until then it is a term with terms inside, which `Term` has no case for. A `where` slot already holds two terms for a keyed field (`Bound::Entry { the, is }`).
- Nothing about that has to show in the notation or the syntax tree: `{text: ?x}` is written and kept as a struct either way.

**Decided.** By the maintainer: a struct is a value in the notation, written as one, with no form to build it. Not decided: whether the value is `Record` given a structure or a new `Map`, and how dialog evaluates a struct that holds a variable.

## 2026-11-11, Designing: several heads leave this topic

**Asked.** Irakli: rules of several heads can be handled by an agent working on its own, and belong in the next rule body format.

**Decided.** It is a topic of its own, `design/rule-heads.md` on #776, in the Describing phase and not picked up. This topic keeps compiling a rule of several heads to a rule a head until that lands.

## 2026-11-11, Designing: `unless` inside `when`, and rules for syntax

**Asked.** Where parts without a label go. Irakli: option B is fine, or stick to rules and drop the `syntax` macro. A macro should read any form, not an application only. And since several negated atoms mean none holds, `unless` nested in `when` reads correctly and should be supported as well as beside it.

**Learned.**

- The walker already asks about any form that is not kernel, so a pattern in brackets at the top level expands with no change.
- A struct as a pattern does not: `(expand {note: ?n})` is read as `(expand note: ?n)`, by the reading of a trailing record as labelled parts built earlier the same day. That reading and "a struct is a value like 5" contradict each other.

**Decided.** By the maintainer: `unless` may stand inside `when`; a pattern is any form; the `syntax` of two parts goes and the prelude is rules. Built, with tests. Not decided: the label for a concept's body, and which of the two readings of a trailing struct stays.

## 2026-11-11, Designing: a struct with a variable is a pattern

**Asked.** How dialog evaluates a struct that holds a variable: a third case of `Term`, or a premise made when lowering. Irakli: it is a destructuring pattern, and a gap in dialog to close. It hinges on record values, which dialog did not have, and closes with them.

**Decided.** By the maintainer: a struct with a variable in it is a destructuring pattern, to be had in dialog with records as values. It is #748's work, not this topic's. What the notation needs of it is listed in [design-forms.md](design-forms.md). This topic refuses a struct as a term until records land.

## 2026-11-11, Designing: a parameter holds a pattern, and a part needs no label

**Asked.** The plan for records and destructuring, since everything ties to them. Irakli, by voice:

- Option B is not what he wants. A rule about syntax that needs a label on every part is inconvenient to write and limits what syntax can be.
- `Term`, a variable or a constant, becomes the leaf of a pattern: a value pattern, a map pattern, a list pattern, a tuple pattern. It is a requirement. The planner and inference are affected, so it is a large piece of work, for an agent of its own.
- Only the new notation needs it. The stored rule format and the wire can wait, so long as the query engine and inference take a pattern.
- The records work is stalled on a binary layout. Use CBOR now, under a version, and change it later. Asked how revision records are laid out, and whether to reuse `Value::Record` or add a variant: he follows the agent's advice.
- #748's thread is to be left alone. He is the one arguing in it.

**Learned.**

- A revision record is dag-cbor of a serde struct under `Value::Record`: a CBOR map with nothing in front of it. A CBOR map starts at byte `0xa0` or above, so a small version byte in front of a composite value is never how a revision record starts.
- `Value::Record` is referenced over its bytes. A composite value named by its parts is the first thing the pattern work has to build, since a tree, a derived entity and a pattern's constant all need a name no layout moves.

**Decided.** By the maintainer: parts with no label stay; a parameter holds a pattern; the composite value is `Value::Record` given a structure, dag-cbor under a version byte for now; no stored format or wire change in this step. It is a topic of its own, [patterns](../patterns.md) (#777), with a [plan](../patterns/plan.md) of four steps and an agent building it. #776 is left for his workers. The reading of a struct at the end of an application as its labelled parts goes.

## 2026-11-11, Designing: the tree, built

**Asked.** Nothing of the maintainer. The agent built what was decided and stopped at what was not.

**Learned.**

- The tree is `Syntax` and what it is kept in is `Data`. The reader's `Form` is no longer what is named or stored.
- **`of:` runs the other way from the example.** The example keeps the part with no label under `of` among the terms. Kept there, `(when A)` and `(when A B)` have two shapes. Kept as an item, with `of:` written out landing in the same place, every application has one shape and nothing has to know the operator. The two spellings of a call are one tree and one name, which the first build of this had as two.
- A map read back from bytes was taken in the order it was laid out, so bytes with their entries swapped read as a map and wrote back as themselves. The check that there is one way to write a thing passed for them. Found by the test written for it; a map read back is now put in order first.
- **Resolving has nothing to resolve to.** Tonk resolves a name to a concept and then expands. Here `concept` expands to a rule about syntax whose pattern is the signature, so a name is met by patterns and points at nothing; and after expansion only domains, built-in formulas and entities are left, which need no resolving. So "resolve, then expand" does not carry over as two passes. Either a name is a fact that points at a definition and patterns meet the definition, or patterns meet names and nothing is resolved. The first is what "definitions by hash, names as facts" says; it needs a definition to be a thing, and today it is whichever rules came out of one `concept`.

**Decided.** Not decided: what a name resolves to. The expander and the compiler stay on the notation as parsed until it is, since the answer decides what stands between them and the tree. Built, with tests: `rust/dialog-notation/src/syntax.rs`, `src/data.rs`.

## 2026-11-11, Designing: a name resolves to a concept

**Asked.** What a name resolves to, where the proof of concept has a concept as a rule about syntax. Irakli: `concept` mirrors tonk's `concept! &todo`. It defines a concept and a name, `id:todo` associated with the concept's entity, and an application resolves to the entity. One nuance he wants: two concepts named `todo` with different signatures, either resolvable. With the same parameters over the same attributes he does not see how to choose; an editor could ask, an agent would trip, so he expects one concept per name and signature. Learn from Unison.

**Learned.**

- The case he could not choose in does not exist. A concept's entity is its attributes; field names are not in it. The same parameters over the same attributes are one concept.
- The case that does exist is the same labels over different attributes. Nothing an application writes tells those apart.
- Different labels are told apart by the labels written. A write says every field, so it always resolves; a read naming only shared labels does not. The proof of concept does this today, by pattern, and refuses what fits both (`it_tells_two_concepts_of_one_name_apart_by_the_labels_written`).
- Tonk cannot hold two concepts under one name: `db.name/referent` has cardinality one, so the second supersedes. Its field names are facts on the concept's entity, which two definitions over the same attributes share.
- Unison keeps a name's two definitions as a conflict and elects neither. It arises by merge. A name that fits several resolves when exactly one has the expected type and is an error otherwise; `name#hash` says which.
- "One concept per name and signature" is a rule about definitions, and replicas define apart and then sync. So it cannot be kept where a definition is made (H7), only noticed where a name is used.

**Decided.** By the maintainer: a name resolves to a concept's entity, as in tonk; several concepts may share a name. Not decided: what an application does where it fits two (H8), and where field names live (H9). The agent's reading is Unison's: keep both, refuse, list the candidates.

## 2026-11-11, Designing: replace, add, or conflict

**Asked.** Irakli gave the case: two concepts named `todo` with the same labels and types over two domains, and a rule that reads and writes through `todo`. Which does the rule mean? Then: perhaps both get asserted, which is not unreasonable, but it has to be clear.

**Learned.**

- The agent had proposed refusing, not asserting both. There are three behaviours and they had not been set side by side.
- Which applies turns on something the question left out: whether the second definition was written after the first or apart from it. Dialog tells these apart already. Under `last`, a referent written after another replaces it; two written in one round both stand, one elected on a `last` read and both shown with no pick.
- Two top-level `assert!` in one document are in order, not concurrent: the later replaces. Two rules fired in one round are concurrent. So the example, in one document, is a replacement under tonk's rule: `todo` means `io.gozala.task`.
- Replacing everywhere loses his nuance, two `todo`s of different labels. Adding everywhere loses correcting a definition by evaluating it again. Naming a name with its labels keeps both (H10).
- Adding has costs that can be counted: his one rule is four, a write lands in both domains, and a query reports each todo twice, since a report shows relations.

**Decided.** Not decided: what a conflict does. The agent's reading: replace per name and labels, refuse a conflict with its candidates, and say which by writing the relation. Refusing is the one that can later become either other.

