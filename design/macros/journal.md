# Macros as rules: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | The kernel and the core forms run on a real branch through `rust/dialog-notation` ([`design/macros/poc.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/poc.md) on `research-data`); the prelude is rules only. The syntax tree is built, apart from the notation as parsed, and kept as data: maps that say their `type` ([design-forms.md](design-forms.md#the-syntax-tree)). A struct is a value and, with a variable in it, a pattern, which is a topic of its own with an agent on it (#777). Rules of several heads are #776. Expanding and compiling still read the notation as parsed, and `kernel.rs` still copies dialog's forms. | **Designing** again from 2026-11-12, for the three phases ([design-pipeline.md](design-pipeline.md)); how the phases stand behind the query engine is still Directing. Direction decided by the maintainer: phases as formulas and a resolver, one rule over them. A proof of concept runs. A name is kept as a name, what is named and a signature; a definition under it is read `last`; an application resolves by the labels written and a read that fits several is of all. Built and tested, by pattern, in the expander. |
| **Agenda** | **One at a time, in the order of the queue at the end of this journal** (the maintainer, 2026-11-13: the threads were more than he could follow). The rest of this cell is what the queue was drawn from. Answered since, 2026-11-14: descriptions stay under `syntax.meta` and `dialog.` stays closed; a stored rule is written in the new format always (#799); `[ ]` where a value stands is a list, as `'( )` is. #791 is a bug (the maintainer), fixed in #802. Next: what a map's key is. Still to build: a reported row in the order asked; printing from the analyzed tree. Parked by the maintainer: authority, for two definitions of one signature written apart; `last` elects one until then. How a query reports. Decided 2026-11-13: a rule has one head and `(all …)` is not written; what that does to #776 and to several heads in #779 is the maintainer's to say. Whether a template says several things with `(all …)` or a spread. Whether a command takes fields beside what it is about, and a slash in its name. The prelude written without `when`. Parked by the maintainer: `update`, and with it what it does with a dependent that does not fit or arrives by sync. Whether the name as written is part of what names a macro. Whether the analyzer, a resolver by the maintainer's word, gives back the table of what each name meant. Whether dialog can be asked for an expansion a form at a time (H3). What a read with no pick reads. | The notation is on top of patterns (#778, merged in, its perf gate failing): a struct is a value in a write and a pattern in a query of one read. Next: a struct in a rule, which needs the compiler to build dialog's forms in Rust, since a pattern has no wire form; `entity/derive` over a record; then an application lowers to a `ConceptQuery` of the definition's concept. Not waiting: a helper rule for a negated conjunction. Waiting on patterns (#777, #778): a struct as a term, `entity/derive` over a record, syntax kept on the branch. |

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

- The kernel compiles to descriptors Dialog-DB already has, and the to-do, the counter, a backlink, a recursive relation and a query of several premises run on a real branch. See [`design/macros/poc.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/poc.md) on `research-data`.
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
- Different labels are told apart by the labels written. A write says every field, so it always resolves; a read naming only shared labels does not. The proof of concept does this today, by pattern, and refuses what fits both (the test since renamed `it_resolves_a_name_by_the_labels_written`).
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

## 2026-11-11, Designing: the last write wins

**Asked.** Whether defining `todo` again replaces or adds. Irakli: a name using last write wins is fine, and overloads need not be supported, since commands more or less offer a solution for that.

**Learned.**

- With one referent a name, H7, H8 and H10 have nothing left to decide. They are closed by the decision, not by evidence.
- The agent's reading of the remark on commands: a command is defined by the rules that read it, and several rules read one command by different shapes. That is overloading where behaviour differs, without two concepts under one name. Not confirmed with him.
- What the decision leaves is where field names live. A concept's entity does not say them, and tonk reads a concept back from every `db.concept.with/*` claim on its entity, so two names over the same attributes get each other's field names (H9, read from the code, not run).

**Decided.** By the maintainer: a name has one referent under `last`; no overloads. Not decided: whether the referent is the concept's entity or the definition. The patterns agent, which had stopped for lack of usage credits in its second step, was restarted at his word.

## 2026-11-11, Designing: a name, what is named, and a signature

**Asked.** Whether a name points at the concept's entity or at the definition. Irakli: he is fine testing several definitions resolved by signature; where the signature is not enough, by the authority of the definition, or to start with by showing all. It means signatures have to be captured with their parameter names. And then: so maybe it is not a name to an entity only, but also what the named subject is, and a signature.

**Learned.**

- This is H10 with a kind added, and it takes H9: the field names are kept with the name. A definition is kept under the entity that the name, the kind and the parameter names make, read `last`. So the last write wins where a definition is corrected, and definitions of other parameters stand side by side, which the decision an hour before had given up.
- Showing all has to choose once a written form, not once a place the form ends up. The prelude copies a query's head into a premise, and choosing in each place gave four rules for two definitions, two of them reading one relation and reporting the other. One choice a form in a rule gives two.
- A write never needs showing all: it says every field, so the labels written are a signature. A write that fits several and is none of them is a mistake, and is refused.
- What must not hold cannot be shown all by expanding the rule several times. Not any of them is one rule with several negations. Refused for now.
- Two definitions of one concept that call its field by other names give answers that differ only in the names of their own variables. Told as one answer, nothing is chosen.
- On a branch a read by the name alone finds every signature of it, and a corrected definition has replaced the one it corrects.

**Decided.** By the maintainer: a name is a name, what is named, and a signature with parameter names; several definitions may stand; an application resolves by signature, then by authority, and to start with shows all. Built to "shows all". Not decided: what authority is and where it is read from.

## 2026-11-11, Designing: the simple version first

**Asked.** What authority is read from. Irakli: forget authority for now and get the simple version working. What is the status of records, and of things working end to end?

**Learned.**

- Patterns has all four steps of its plan committed on #778: the composite value, the pattern, the planner, inference. Its lints and the replication gate pass. Its perf gate fails: seven scenarios, point reads and rules, cost 8% to 12% more instructions with counters unchanged, against a bar of 3%. A query that holds no pattern pays for patterns.
- A rule that holds a pattern runs and is refused when installed, as the plan said: format 3 cannot write one. So his own example, a rule that derives an entity from `{title: ?t author: ?a}`, cannot be installed on a branch until a stored rule can hold a pattern. The decision to leave the format alone was right for building patterns and is now what stands between them and a rule that uses one.
- A pattern has no wire form above a leaf. The notation's compiler writes descriptors as JSON and reads them back, so it cannot say a pattern until it builds dialog's forms directly.

**Decided.** By the maintainer: authority is parked. Not decided: whether a stored rule may hold a pattern now, which is a new rule body format.

## 2026-11-11, Designing: on top of patterns

**Asked.** Irakli: the overhead on queries that use no pattern is too much, and that is the patterns work's to fix. The notation can be built on top of patterns meanwhile. And how is the notation evaluated today?

**Learned.**

- It is evaluated by `cargo run -p dialog-notation --example notebook`, over files or standard input, on a repository kept in memory: a document is one transaction, its queries print rows and its warnings print as comments. Nothing else runs it: no session that stays open, no repository on disk, nothing in tonk.
- Patterns merged into the notation's branch with two conflicts, both lists that each side added to. Every test passed as it was.
- A struct written as a field's value is a composite kept as a record, and reads back as the struct. With a variable in it, a query of one read takes the kept struct apart by it. The pattern is put into the `ConceptQuery` after dialog has read the rest from JSON, since a pattern has no written form.
- A blank inside a struct is given a variable of its own, so the row can say what stood there. A pattern that holds a blank cannot be built, and its whole is not under a name the row can ask for.
- A rule is handed to dialog whole, as JSON, so a struct in a rule is refused as not supported yet. That is the compiler's limit, not dialog's: the same rule built in Rust runs.

**Decided.** By the maintainer: build on patterns now. Built: a struct as a value and as a pattern in a query of one read, `it_keeps_a_struct_and_takes_it_apart_by_a_pattern`.

## 2026-11-11, Designing: a struct in a rule, and what stops it

**Asked.** Irakli: get structs and macros and the rest.

**Learned.**

- The compiler no longer needs dialog's forms built wholly in Rust to put a pattern in a rule. It reads the rule's descriptor from JSON as before and adds one premise a pattern, an equality between the pattern and the variable written in its place. Dialog builds the value where the pattern's variables are bound and takes it apart where the variable is.
- Such a rule compiles. It cannot be installed, which was known, and it cannot be laid over a branch for one query either, which was not: `Branch::with` names a rule by its stored form and panics where that form cannot be written. So a struct works in a write and in a query of one read, and nowhere a rule is.
- **`entity/derive` over a record was built and taken back out.** With it, every rule that derives an entity from what it binds holds a pattern, so none can be installed, and two things that ran stopped: a rule and a top-level write naming one entity, and a rule issuing a command another rule reads. It cannot be done by halves, since a rule and a top-level write have to name the same entity by the same function.
- The notebook asks dialog whether a rule can be stored before installing it, since an assertion panics where it cannot.

**Decided.** Not decided, and now the only thing between structs and rules: whether a stored rule may hold a pattern, which is a new rule body format. Asked of the patterns work: that a rule holding a pattern can be laid over a branch without one.

## 2026-11-11, Designing: format 4 is its own topic

**Asked.** Whether a stored rule may hold a pattern, as a new rule body format. Irakli: yes, format 4 is the natural next step, and it depends on patterns, records, syntax forms and rules of several heads.

**Learned.** A rule is named by its canonical spelling and since format 3 its body is that spelling, so a format that spells a rule another way names it another entity. That is a reason for one format and not four.

**Decided.** By the maintainer: one format 4, for all four. It is a topic of its own, [rule-format](https://github.com/dialog-db/dialog-db/pull/779), in the Describing phase. What this topic asks of it: a head that is not a concept and a premise that takes a form apart, so a rule about syntax can be stored. Until it lands a struct stays out of rules here.

## 2026-11-12, Designing: a struct in a query of several premises and in a rule that derives

**Asked.** Irakli: where the primary work stands, with #782 merged and patterns green.

**Learned.**

- Patterns (#778) lays a rule that holds a pattern over a branch and names it by its parts, with no stored form. That was the one thing asked of it here, and with it a query of several premises takes a struct apart.
- A rule that derives is only ever read by queries, so the notebook can hold one dialog cannot keep and lay it over each query. A rule that writes cannot be held that way: it runs when a transaction commits.
- So what is left behind format 4 is narrower than "structs in rules": a rule that writes, and `entity/derive` over a record, which makes every rule that derives an entity one that writes.

**Decided.** Built: the notebook holds a deductive rule dialog cannot store (`Notebook::held`). It is a stand-in for format 4 and goes when a stored rule can hold a pattern. Not decided: the stamping policy of format 4, which is the maintainer's and is what #779 waits on.

## 2026-11-12, Directing: every phase where the query engine can reach it

**Asked.** Irakli: all of it should be available to the query engine. A formula that parses source into a syntax tree. The analyzer the same way, over the parsed tree as a record, doing substitutions. Macro expansion "can't be a formula but can be a resolver". Then evaluating forms, so that a command `(dialog/evaluate (source "…"))` has a rule that does all the phases. An LSP server follows from being able to ask.

**Learned.** Read in the code, not built:

```
 source text ──parse──▶ tree ──resolve names──▶ definitions ──expand──▶ kernel ──compile──▶ rule facts
              formula          a read (a scan)    by hash        resolver         formula      written by a rule
              pure             moves, so it                      pure                          that answers the
                               records demand                                                  command
```

- Formulas and resolvers are closed sets inside `dialog-query`, and the notation crate depends on it. The parser, the tree and its data depend on `dialog-corpus` alone, so they can move below the engine. Expanding and compiling cannot as they are.
- A resolver may read only what is addressed by content. A name moves, so looking one up is a scan. Expansion fits a resolver once the definition is given by hash, which is the split already chosen for names: a name is a fact, a definition is named by what it is.
- Commit-time induction re-reads its rules each round so that a rule installed by a rule fires, by its own comment. That is what `dialog/evaluate` as a command needs. No test of it was found.
- A command reports nothing back. A query inside an evaluated source has nowhere to put its rows.
- Reading for an editor and evaluating are two uses of the same formulas: the first writes nothing and has to work on text that is not committed and does not parse; the second is a commit.

**Decided.** The direction is the maintainer's: phases as formulas and a resolver, one rule over them. Not decided: what evaluating a query produces; how a source that defines a form and uses it gets its order; whether expansion splits at the name as above. Each is a hypothesis in the [ledger](claims.md). Nothing is built on this yet.

**Backtracked.** From Designing to Directing: "macros are rules, compiling is a formula" was a column parked for Direction, and this is that column said in full.

## 2026-11-12, Directing: what a phase that reads the database is

**Asked.** Irakli, on the entry above: formulas could be installable instead of moving the parser; names are facts in a database backed by content-addressed storage and resolving one mutates nothing, so it is a resolver; the analyzer probably is one too, since it looks up references.

**Learned.**

- He is right about the resolver, and "cannot" was too strong. A resolver given the tree's root reads a name as a pure function of its inputs, as the tree resolvers do. The rule is not about mutation. It is about what the engine keeps to know an answer is stale: a scan keeps the range it read, a resolver keeps only the head.
- So there are three things a phase that reads the database can be, and they differ in when an answer goes stale and in where a form's meaning is kept:

| | reads a name by | stale after | what a form means is in |
|---|---|---|---|
| a resolver given the root | walking the tree by hash | every commit | Rust |
| a native premise that scans (there is none today) | scans that record their ranges | a commit that touches what it read | Rust |
| rules over native primitives | ordinary premises | a commit that touches what it read | rules, which are data |

- The first two put the analyzer and the expander behind a query, and leave them what they are today: Rust that knows every form. That is candidate problem 2 unsolved, and it is not "macros as rules".

**Decided.** Not decided: which of the three. Installable formulas are a change to the engine of their own, with three things to settle first: what a build does with a rule that names a formula it does not have, what a registry costs where an enum dispatches today, and what keeps one name meaning one function across builds.


## 2026-11-12, Directing: an analyzer given a context

**Asked.** Irakli, on the three kinds: is the point that the analyzer need not be a resolver and could be rules? Staleness after every commit should not be perceivable, since `evaluate` is a command and lasts a tick. Then: a formula could give the names to resolve, a query could resolve them and aggregate them into a context, and the analyzer could be a formula over the form and the context; but aggregation is not allowed inside rules. "Having a formula that can analyze given a context does seem kind a neat."

**Learned.**

- He is right about staleness for a command: it runs once, in a commit, and nothing is kept to go stale. Staleness matters for the other use, a standing query an editor keeps open. What a resolver given the root does miss for a command is state that is not committed: the definitions of the commit in progress, and a rule laid over a branch. A source that defines a form and uses it is exactly that.
- Aggregation is allowed in a deductive rule: a `reduce` clause folds a group with `count`, `count-distinct`, `sum`, `min`, `max` or `avg`. What is in the way is narrower, and it is three things:

```
 names of a form ──▶ read each ──▶ fold into a map ──▶ analyze(form, context)
     formula          premises       no such fold          formula, pure
                                     there is today
```

  - no fold gathers a group into one value, since there was no composite to gather into before #778;
  - a fold inside a dependency cycle is a stratification violation, and expanding can introduce names the source did not have, which is a loop through the fold;
  - a rule that writes has no `reduce`, so a deductive rule gathers the context and the rule that answers `evaluate` reads it.
- Given a context, analysis is a pure function of two values. It sees whatever the premises that gathered the context saw, uncommitted or not. It works on text that is not committed. And an answer is as fresh as its context: a commit that leaves the context the same leaves the analysis valid, with nothing asked of the engine.
- The loop goes away if the context is every name the space defines, gathered once. The cost is the size of that value and that any change of a name is a new context.

**Decided.** Not decided. The maintainer likes an analyzer that is a formula over a form and a context; how the context is gathered is open. Three hypotheses are in the [ledger](claims.md). The test of the first waits on which value a composite is (`Value::Record` on #778, `Value::Composite` in #748's journal).

## 2026-11-12, Directing: the parse names what to resolve

**Asked.** Irakli, correcting the entry above: the parse gives the tree and a context that names the local definitions and the references. A resolver resolves the references. The local definitions and what was resolved go to the analyzer, a formula. And: none of it need be complicated, the analyzer as a resolver is fine too.

**Learned.**

- His shape needs no fold and no loop of mine. A resolver takes the references whole and gives one value back, so nothing is gathered by a rule. What a source defines comes from the source, so a definition used in the source it is written in needs nothing committed.

```
 source ──parse──▶ tree, every name written
                        │
                        ▼ resolver, the one step that reads state that moves
                   name ──▶ hash
                        │
                        ▼ analyze: a function of the tree, the hashes and what they name
                   kernel forms, what each name meant, what did not resolve
```

- The parse can name every reference and cannot name a definition. Every symbol is read as a `:reference` (`syntax.rs`). Which of them a form defines is said by the rule about that form: `concept` is a rule of the prelude, and its pattern is what finds the name. So what a source defines is something analysis finds, in order, and gives back; it is not an input to it. Resolving every name written and letting the misses be locals, binders or errors costs a lookup a name and loses nothing.
- Whether one resolve is enough rests on what a kept rule about syntax holds where its template names something: the name, read where the form is used, or the hash of what the name meant where the rule was written. With hashes an expansion brings no name to resolve. Nothing about syntax is kept yet, so this is open, and it is the same question as whether a macro is hygienic.
- Either way analysis reads what a hash names, to expand by a rule it was not handed. That is a read by content, which a formula cannot do and a resolver can. So "the analyzer is a resolver" and "a resolver, then a formula" are one program with a boundary shown or not. What showing it buys is analysis over a context that is not a committed root.

**Decided.** By the maintainer: do not complicate it, the analyzer as a resolver is fine. Proposed by the agent, not decided: it resolves names first, as one step, and gives the table back with the kernel forms and the diagnostics, so an editor can ask what a name meant and the boundary can be shown later. Open, and his: what a kept template holds where it names something.

## 2026-11-12, Directing: a macro resolves its names when it is defined

**Asked.** What a kept rule about syntax holds where its template names something. The agent's first example used `xyz.todo`, which is a domain and stands for itself; Irakli asked whether it was meant to be a name. It was not one. The name is `todo`, what `concept` defines.

**Learned.**

- A template that names only domains and kernel words has nothing to resolve. Every rule of the prelude is such a template, so the proof of concept never met the question.
- A concept's entity is made of its attributes and does not hold what its fields are called (`Signature`, `syntax.rs`). So the entity of `todo` alone does not say which attribute `state:` is. A template resolved when defined has to hold the attribute of each labelled part as well, or the labels still read the name where the form is used.
- A concept that applies its own name in its body can be resolved before its body is: its entity is its attributes. A rule about syntax whose template applies itself, or one defined later in the source, has no entity yet when the template is resolved.

```
 as written      (todo ?t state: :done)
 as kept         concept:…  this: ?t   xyz.todo/state: :done      what it means, named by content
 beside it       todo, state:, where in the source                what a source map reads
```

**Decided.** By the maintainer: a macro resolves every name when it is defined and holds the entity of what was named, the concept `todo` here. It also holds what the name was, so source maps work. So analysis resolves the names a source writes, once, and expanding brings none. Proposed by the agent, not decided: the name and the labels as written are kept beside the template and are no part of what names the macro, as a description is no part of what names a rule. Open: what a template holds where it applies itself or what is defined after it.

## 2026-11-12, Directing: whether a rule needs `when`

**Asked.** Irakli: use haiku readers to find out whether `when` is needed at all, or whether premises as plain clauses suffice. Run as #728's rounds were. Two rounds, ten readers each, five a spelling: K1 ([`design/macros/rounds/k01.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/rounds/k01.md) on `research-data`), told how a rule is written; K2 ([`design/macros/rounds/k02.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/rounds/k02.md) on `research-data`), shown three rules and told nothing.

```
 when    (rule head… (when premise…) (unless premise…))
 bare    (rule head premise… (unless premise…))          several heads as (all head head)
```

**Learned.**

- For a rule of one head `when` carried nothing the rounds could find. 20 of 20 read and wrote such rules rightly in both spellings, told or not, a rule that reads what it derives among them.
- The difference is in several heads. Untold, three of five `bare` readers wrote `(rule h1 h2 p)`, which is one head and two premises; three of five `when` readers wrote two heads before `when`, which is what it means. Told to use `(all …)`, five of five `bare` readers did.
- A second write after the head is read by its word by some and by its place by others. A parser can go by the word, since a write is never a premise. A head that derives has no word, so there only `when` or a wrapper tells a second head from a first premise.
- The write that stands alone was named confusing by two `when` readers and no `bare` reader.
- The agent's predictions that `bare` readers would take a command for something the rule issues, and a lone write for two writes, were wrong.
- Not about `when`: nine of ten K1 readers saw that a head's variable bound only by a premise makes a rule of two heads give nothing when that premise has no match. Four of ten asked whether `assert!` replaces what is stored.

**Decided.** Not decided; the maintainer's. What the rounds say: `when` is not needed to read or write a rule of one head, and what it buys is that several heads need no wrapper and no teaching. Not run: a rule about syntax, where `when` is also what a pattern over a rule matches on; and readers less capable or more hurried than these, since both rounds were at the ceiling on everything but several heads.

## 2026-11-12, Directing: a group that refers to itself, and a change that reaches what depends on it

**Asked.** Irakli, on what Unison does. Hashing a definition that refers to itself, or a group that refer to each other, as one unit: "sounds like what we should do". On a change not reaching what depends on it: something like Unison's `update`, perhaps `(update my-concept)`, whose handler finds every reference and updates it. And from that: a macro should have relations to what it references, so they can be traced.

**Learned.**

- Dialog keeps such relations for a rule already: `dialog.rule/reads` and `dialog.rule/derives`, a fact for each attribute, are how a rule is found by what it reads. A fact for each entity a kept form holds is the same thing for definitions, and makes "what depends on this" a read by value.
- What goes stale when `todo` changes is not the macros alone. Every kept rule that came of a form applying `todo` holds the old entity too. Storing those again means expanding their forms again, so the form each came of has to be kept, with its references. Syntax kept on the branch is not built yet.
- A change reaches what depends on what depends on it: each thing stored again has a new entity, so what holds it is stale in turn. As rules at commit that is a round a link, and a commit has sixteen.
- Unison stores a dependent again only when it still type-checks. The like of that here is the signature: a `todo` that lost a field a dependent names cannot be stored again for it, and has to be reported.

```
 (update todo)

   name todo ──▶ concept:new          re-pointed by the definition
        │
        ▼ read by value: which kept forms reference concept:old
   finished, pending, …               each stored again against concept:new, its name re-pointed
        │
        ▼ and what references those, a round later
```

**Decided.** By the maintainer: a group that refers to itself is named as one unit, as Unison does. A kept form has relations to what it references. A change reaches dependents by an `update` that is asked for, not by a name read late. The relations are on every kept form and not on macros alone, which he meant and confirmed. Proposed by the agent, not decided: `update` refuses, by name, a dependent whose labels the new definition does not take.

**Scope.** Irakli, later the same day: `update` is something we may want in the future, not now. What is kept now is what would let it be built: every kept form has relations to what it references. What `update` does with a dependent that does not fit, with a chain longer than a commit's rounds, and with one that arrives by sync still naming the old, are parked with it.

## 2026-11-12, Directing: a rule without `when`, and what several heads are

**Asked.** Irakli, on the reader rounds: he would drop `when`, and then has to say what a rule of several heads looks like. The only use he has is to assert and retract on one condition, and that could be the condition written down and reacted to, which the command forms make easy. On cost: not sure, fine if it can be optimized.

**Learned.**

- Several heads do not come from that use alone. Writing a concept whose body is several reads is one head a read ([kernel](kernel.md), "one head each"), so expansion makes rules of several heads whether or not anyone writes one. `(all …)` in a head's place has to mean something either way.
- A rule of several heads is already one Dialog-DB rule a head, each with the whole body (`compile.rs`). So today it is a way of writing, and not a thing dialog runs.
- The move of a to-do, run three ways with the same result (`tests/notation.rs`):

```
 one rule, two heads             each head with the whole body         as today
 two rules, one head each        each with the premises it needs       no round more
 a command the condition issues  two rules read it a round later       one commit, one round more
```

- They differ in one case: a head that needs a variable only one premise binds. With the whole body under every head, nothing is written when that premise has no match. Split by hand, the other head still writes: a to-do on no list is put on one. Nine of ten K1 readers took the first for a trap.
- Cost, not measured: the body is evaluated once a head where it is written again, and once where a command names it, at a round of the sixteen a commit has.
- On 2026-11-11 the maintainer decided rules of several heads are to be built in dialog (#776), and that one head was an accident of tonk's notation. Several heads are also the one addition to a stored rule that might respell a rule of one head (#779). A kernel rule of one head would need neither.

**Decided.** By the maintainer, as a lean: drop `when`. Proposed by the agent, not decided: a kernel rule has one head, `(rule head premise… (unless premise…))`; `(all h1 h2)` in the head's place is one rule a head with the premises again, as it is compiled today; the condition named by a command is a way of writing that needs nothing new. Open, and his: whether a head that cannot be bound holds the others back, which is what decides whether dialog needs a rule of several heads at all.

## 2026-11-12, Directing: several heads as a collection, or not at all

**Asked.** Irakli: several heads could be a tuple or a set in the head's place, `[h1 h2]` or `{: h1 : h2}`, or a map if the parts want names. But rules of several heads are not needed at all: one rule gives the condition a name, `(todo/update-title (todo ?todo title: ?old) title: ?new)`, and a rule a write reads it. A little more to write, and he is not sure it matters.

**Learned.**

- His three rules are the command a rule issues, tested earlier today. Run as he wrote them, with the slips mended, the notebook refuses two things, and neither is the idea:

```
 (retitle (xyz.todo ?todo title: ?new before: ?old))        runs: "Old" is taken back, "New" is kept
 (retitle (xyz.todo ?todo title: ?old) title: ?new)         refused: a command takes one application and nothing beside it
 (todo/update-title (xyz.todo ?todo …))                     refused: the command's domain is made of its name, and a slash in a domain reads as an attribute
```

- This settles what was left open: whether a head that cannot be bound holds the others back is now the author's to say. A condition given a name holds its writes together, and says so in one place. Writes in rules of their own do not wait on each other. Neither needs a rule of several heads.
- A collection in the head's place is told from one head by a parser, since a head is an application. Of the three a set is what heads are: no order, none twice. A tuple has an order that means nothing, and a map has names nothing reads.
- `(all …)` is still there for a template to say several things, so a collection of heads would be a third way to write what two ways already write.
- How much there is to write is what a macro is for: a form that expands to the three rules is a rule about syntax, once `(all …)` stands at the top level.

**Decided.** By the maintainer: rules of several heads are not needed; a condition is given a name and reacted to. Proposed by the agent, not decided: no collection of heads; `(all …)` in a head's place stays as what templates and a concept of several reads expand to, one rule a head. Open: whether a command takes fields beside what it is about, as he wrote it, and whether its name may hold a slash. And what this does to #776 and to several heads in #779, which is his to say.

## 2026-11-12, Directing: what several heads would save, measured

**Asked.** Irakli, by voice: list the trade-offs of having rules of several heads; if it can be done as efficiently without them there is no point in them, and the rest is macros; if they are needed to do it efficiently, have them. And why `all` is needed. Then: "let's measure".

**Learned.** The measurement is a research topic of its own, [rule-heads](https://github.com/dialog-db/dialog-db/pull/788) (#788). One commit of 500 `move` commands, instructions under callgrind:

```
 one rule, one wide head     the stand-in for several heads     900 to 1562 million
 a named condition           22% to 39% more
 the body again a head       61% to 148% more, and what a rule of several heads is compiled to today
```

- The gap of a named condition is about 120 million instructions a head for 500 commands, whatever the body. Most of it is evaluating a rule of one read that a fact has already bound, about 128 thousand instructions a time.
- The agent's guess that a named condition would be within a tenth of several heads was wrong.
- `all` is a splice: a template that has to say several things where one stood. The maintainer: a splice is what macros usually have, perhaps as a spread. The notation has `...` already, and `all` is what the spread sits in where no form encloses it.

**Decided.** Not decided. The bars set before the runs put the result between them: several heads would save at most a fifth to a quarter of a commit's instructions over a named condition, on a stand-in that is a floor. Decided by nobody yet but following from the numbers: `(all …)` in a head's place should compile to a named condition and not to the body again.

## 2026-11-12, Designing: the parser, the analyzer and the compiler as three phases

**Asked.** Irakli: put another agent on the measurement and on how far the cost could be optimized (#788), and meanwhile get the parser, the analyzer and the compiler done.

**Learned.** What is built is one walk: `Expander::submit` reads text, expands by rules about syntax held in memory, and gives kernel rules as the notation as parsed, which `compile` reads. The tree is built beside it and nothing downstream reads it. A reference resolved to an entity has no spelling in the notation as parsed, so what was decided today cannot be built until the phases read the tree.

**Decided.** By the agent, as the reading of "done" to build to, in [design-pipeline.md](design-pipeline.md): three phases that take and give trees, in four stages, the last a name table the analysis gives back. What is left out of the stages is listed there with what each waits on.

## 2026-11-12, Designing: the boundary between the phases

**Asked.** Irakli: what analysis gives should be called the analyzed tree. And: where does macro expansion take place, or type inference?

**Learned.**

- Stage 1 is built: `parse`, `Analyzer::analyze` and `compile` take and give trees, and the prelude itself goes through the tree. Nothing the prelude or the tests write is lost by the tree but one thing: the order of labels. A reported row lists its fields by label now, where a test had it in the order the concept gave them.
- Expansion is inside analysis. Which definition takes a form is found by resolving its head and its labels, and which names in it are references is known only once it is expanded, so the two alternate and there is no tree between them. What can be asked for apart is one step: this form, expanded once.
- Type inference is in the engine, after `compile`: `TypeEnv::infer` when a compiled rule is planned. So a contradiction comes back with no place in the source, and nothing says what type a variable has without compiling and planning.

```
 text ─parse─▶ syntax tree ─analyze─▶ analyzed tree ─compile─▶ rules, queries ─plan─▶ run
                            resolve ⇄ expand                                   types inferred
```

**Decided.** By the maintainer: "analyzed tree". Open, and asked of him: whether types are inferred once, by the engine, and asked for by the pipeline, or by analysis itself; and whether a row keeps the order its fields were written in.

## 2026-11-12, Designing: what was written, kept beside what was said

**Asked.** Irakli, as the stages went by. Inference should happen when compiling: why would it be at run time, and is there a reason the compiled form is not complete? A type error with no place in the source will not do: tonk's notation and its language server say where an error is, and this cannot be worse. Look at conduct, which captured a mapping so the source could be shown as written, and at Unison. Then: a form that takes the analyzed tree and gives the source, tested by printing the original, and the same from the compiled form, which carries none of the mapping. And: holding the source is cheating; keep the mapping only, which attribute was named what, needed for invocations anyway, and the order, so it is easy to arrange back.

**Learned.**

- The agent had inference in the wrong place. The engine infers when a rule value is built (`DeductiveRule::new`), and `compile` is what builds it, so inference is in the compile phase already and planning does not infer again. What is left for later is the order of joins, and analyzing a kept rule again each time it is read from the branch.
- Stages 2 and 3 are built: the compiler and the analyzer read trees, and nothing after `parse` names the notation as parsed. Every test passes with expected text reordered by label and nothing else changed.
- Conduct made most of today's choices already: a range on every node; a template's name with a pin, the hash it meant where the macro was defined, and the name as written got back from it; labels kept by the definition and a call's arguments arranged into its order; a cycle hashed as a unit; a definition checked once, ever.
- Unison's reference: a hash is of a term's structure "excluding all names", and a member of a cycle is the cycle's hash and its place in it.
- A macro call is not in the analyzed tree, and does not have to be held beside it. A definition run backwards gives the call back: the template taken off what the call became gives what the pattern's variables took, and the pattern filled with them is the call. Tested for one step, three shapes of definition. What does not come back is a labelled part the call left out: the template's own variable stands there.

**Decided.** By the maintainer: inference belongs to compiling; an error says where it is; each phase's result can be printed back to the source with a mapping the result does not carry; the mapping holds names and order and never the source. Proposed by the agent, in [design-pipeline.md](design-pipeline.md), not decided: what is held at each phase; that space and comments are not held, so the test is the same tokens in the same order; that a range is on the node, as in conduct and tonk.

**Corrected, later the same day.** Irakli: comments are needed "probably more than anything else", to document source and to find things by documentation. So a comment is held, as a name is: it is what someone said about a node, and nothing gives it back. Today `;` is dropped by the reader, and `description:` on a concept or a rule is the only documentation that survives, inside the kept body where a query cannot reach it.

**Decided, later still.** Irakli does not like `description:` in a concept. He has written a Lisp where `;; something` was read as `(comment "something")`: it worked reasonably well, and was a problem inside a record and the like, where it took a place. Attaching a comment as metadata to the form below it was the better alternative. So a comment is metadata on the node under it, and takes no place among the parts. Proposed by the agent, not decided: `description:` goes from `rule` too, and what a comment above a concept, a rule or a labelled part says reaches Dialog-DB's descriptions through the mapping; a comment with a blank line under it is about the document and not the form after it.

## 2026-11-13, Directing: what several heads would save, with the engine's costs taken apart

**Asked.** Irakli had another agent measure one command a commit and explore how far the cost could be optimized (#788).

**Learned.** From the agent's results on #788, measured under callgrind, three runs within 0.35%:

```
                                   wide    named            the body again
 500 commands a commit, as it is    1     +27% to +33%     +61% to +149%
 one command a commit, as it is     1     +17% to +28%     +27% to +87%
 with three prototypes in the engine 1    +21% to +43%     +1% to +7%
```

- The difference holds a command at a time.
- With a rule planned once, a seed binding its entity and one body evaluated once for the rules that share it, writing the body again a head comes to within 1% to 7% of the stand-in for several heads. A named condition does not: what it has left is its second round.
- The prototypes are not fit to land, by the agent's own account; one is unsound. They say what could be recovered and nothing about how.
- Found on the way: a rule that leaves a premise's subject unnamed fires for entities a commit did not touch. Its own design topic, #789.
- A rule is not analyzed again in a commit: the analyzed rule is kept by entity. It is planned once a seed.

**Decided.** Not decided. What the numbers now say: dialog does not need a rule of several heads to write on one condition cheaply, if the engine evaluates a body once for the rules that share it. That turns over what was said yesterday: `(all …)` in a head's place can go on compiling to the body again a head, and a named condition is the dearer of the two once the engine does that.

## 2026-11-13, Designing: a rule without `when`

**Asked.** Irakli: "we decided to drop when wrapper remember". He had said he would on 2026-11-12, with what several heads look like left to settle; the agent had it down as a lean and kept `when`.

**Learned.** Built. A rule's first part is its head and every part after it is a premise; `(unless …)` stands among them. Several heads are `(all h1 h2)` in the head's place, which is the splice a template already needs. A write written after the first part is refused, and the refusal says to write `(all …)`: three of five readers in round K2 wrote `(rule h1 h2 p)` untold.

```
 (rule (assert! (todo ?t list: ?l text: ?x state: :open))
   (add (todo ?t list: ?l text: ?x)))

 (rule (all (assert! (xyz.todo ?t list: ?to))
            (retract! (xyz.todo ?t list: ?from)))
   (move (xyz.todo ?t list: ?to))
   (xyz.todo ?t list: ?from))
```

- The prelude is shorter: a query is two rules where it was three, since `(unless …)` is one more premise and needs no pattern of its own.
- A query of several heads is `(query (all h1 h2) premise…)`.
- A rule of several heads is still compiled to a rule a head with the body again, which #788 says is the cheap way once the engine evaluates a shared body once.

**Decided.** By the maintainer: no `when`. By the agent, open to change: several heads as `(all …)` and not a tuple or a set, both of which he floated and then set aside; the refusal of a head among the premises.

## 2026-11-13, Designing: a rule has one head

**Asked.** Irakli, after the agent wrote "no rule of several heads" and then showed rules with `(all h1 h2)` as their head: "which one is it?" Then: "I think we can get by without all and do that thing where you can write three rules instead and then we optimize it if we find we need (all ...) we can always add it later".

**Learned.** The two had been one word for two layers. The engine has no rule of several heads and never had; `(all h1 h2)` was the notation writing two engine rules as one. Built without it:

```
 written                                    analyzed
 (rule (assert! A) p q)              ──▶    (rule (assert! A) p q)
 (rule (retract! B) p q)             ──▶    (rule (retract! B) p q)
 (rule (all (assert! A) (retract! B)) p q)  refused: write a rule for each
 (assert! (author ?p name: ?n email: ?e))   one rule a read of the concept's body,
   p q                                ──▶   each with p q again
```

- A kernel rule has one head: `kernel::Rule` holds `head`, not `heads`, and `compile` gives the steps of one rule.
- The split a concept of several reads needs moved from the compiler to the analyzer, which is where it belongs: an analyzed tree is now one engine rule, where it was one or several.
- A report of a read the query named no field of got no step before; now it gets no rule, decided in the analyzer by the same test (`Read::named`), which moved to `kernel.rs`.
- `(all …)` is still what a template answers for several: the concept rule in the prelude is the one use. Written in a rule it is refused, in the head's place and among the premises.
- Lost: `(query (all h1 h2) premise…)`. A query of two relations is two queries.
- Not the same as two heads on one rule: each rule waits only on its own premises. The test of the move shows it: a to-do on no list is still put on one, since the write does not wait on what only the retraction needs. Whoever wants both or neither writes the premises again, or names the condition.

**Decided.** By the maintainer: no `(all …)` for several heads; write a rule for each; add it later if it is found to be needed. He decided knowing the cost as measured: today the body again is 27% to 87% over a wide head at one command a commit (#788), and within 1% to 7% only once the engine evaluates a shared body once, which is prototyped and not sound. By the agent, open to change: `(all …)` refused among the premises too, where it only ever said what writing them one after another says; `(all …)` kept as a template's answer until a spread is designed. Open, and his: #776 and several heads in #779 have nothing left asking for them here.

## 2026-11-13, Designing: what was written, beside the tree

**Asked.** Nothing new of the maintainer. Built from what he said on 2026-11-12: a form that takes the tree and gives the source, tested by printing the original; the mapping holds no source, it holds what was named what and the order; comments are metadata on the form below.

**Learned.** Built, the parser's part of stage 4.

```
 text ──parse_written──▶ trees + Written ──print──▶ text
                                  │
                                  └── a note a node, by the way to it:
                                      span        where it starts and ends, line and column
                                      order       its parts as written: item, label and where, ...?rest
                                      spelling    its token, only where a printer would spell it otherwise
                                      comments    the ones above it
                                      closing     the ones inside it that no part is below
```

- The prelude and both examples print back byte for byte. So do labels in any order, `of:` written or not, `007`, `1.50`, a text with escapes, a struct's key written either way, blank lines and columns.
- Space is not held and did not need a layout: once every token has a place, the space is what lies between two places. Lost: space at the end of a line, and a tab.
- The mapping is beside the tree and keyed by the way to a node, so the tree is what it was: two texts that say the same thing still parse to equal trees.
- A tree changed after parsing still prints. What the mapping knows stays where it was written and what it does not know follows.
- A warning now says the way to its form, found by looking for the form in the tree it was asked for. That is a search and not a record: it is right for a form as written and falls back to the whole tree for a form a definition made. The record is the analyzer's part of the mapping, not built.
- An analyzed tree says which tree it came of. A refusal by the compiler is said with that tree's place.
- A refusal by the analyzer has no place at all. An error is a text, and nothing in it says which node it is about.
- Tonk's language server is the bar he set. There the parser gives a list of diagnostics, the analyzer runs when the parser gives none, and an analyzer error carries a span (`tonk-language-server/src/server.rs`, `tonk-notation/src/syntax.rs`).

**Decided.** Not decided, and next to ask: what a refusal is. Today analysis stops at the first one and says a text. The agent's hypothesis: a refusal is a diagnostic like a warning, with a severity and the way to its form; analysis goes on to the end of the document and gives them all; nothing is compiled while one of them is an error. Refuted if a later refusal in a document is usually noise caused by the first.

## 2026-11-13, Designing: a comment at the end of a line, and refusals as diagnostics

**Asked.** Irakli, on a comment that ends a line being about the node before it: "your suggestion makes sense". On refusals: "Analyzer should offer diagnostics so they could be used by lsp client to communicate with user what needs to happen". And: have an agent do the optimizations #788 found plausible, soundly.

**Learned.** Built, both.

- A comment that ends a line is about the form that ends before it on that line: an atom, or what a closing bracket closes. After an opening bracket or a label no form has ended, so it stays about what is below. `Written::comments` gives what is said about a node: above it, then at the end of its line.
- One case reads oddly: a comment after an operator, `(rule ; fires on add`, is about the name `rule` and not about the rule. Left as it is.
- The analyzer gives diagnostics and does not stop:

```
 trees ──analyze──▶ analyzed trees, origin
                    diagnostics: severity   error, for a refusal; warning, for what a rule said
                                 message
                                 path       the way to the form, which the parser's mapping turns into a range
                                 refusal    the error itself, for a refusal
```

- What a refusal is about is the innermost part being worked on when it happened that is a part of the tree as written: the unknown premise, the second head, the `(all …)`. A form a definition made falls back to the part it came of, and then to the whole tree.
- A document with a refusal in it is refused whole. Nothing of it comes back analyzed, and none of its definitions is kept: before, a definition above a refusal stayed in the analyzer though nothing of the document was submitted. So an editor can analyze the same text again and again.
- Not covered: the parser's refusals (a form that never closes, a label said twice) and the compiler's are still errors with no path, and a refusal of a definition is followed by a refusal of every use of it, which is the noise the hypothesis said would refute it. One document so far; not enough to say.
- The engine work is with an agent, a draft pull request an optimization, measured against main on #788's scenarios. Nothing of it is reported yet.

**Decided.** By the maintainer: a comment that ends a line is about the node before it; the analyzer offers diagnostics for a language server. By the agent, open to change: a refused document is refused whole and keeps no definition; refusals come before warnings; a refusal holds the error it was.

## 2026-11-13, Designing: every phase says what it refuses, and where

**Asked.** Irakli: "Why did you stop?" The agent had reported and waited where it had already named what was next: the parser's refusals and the compiler's had no place.

**Learned.** Built.

```
 text ──check──▶ steps          what the text compiles to, or nothing
                 written        the mapping
                 diagnostics    of every phase, each with a span

   parse      says every refusal, and goes on with the next top-level form
   analyze    runs when the parser refused nothing; says every refusal and warning
   compile    runs when the analyzer refused nothing; a refusal is placed at the tree it came of
```

- One kind of diagnostic for the three phases: a message, a severity, the refusal when it is one, and where. The parser knows the text and says a span. The analyzer and the compiler know trees and say a path, which the mapping turns into a span.
- The parser's refusals are placed where they are: the bracket that closes nothing, the bracket of a form that never closes, the quote of a text that never ends, the `...` that repeats nothing, the form that says a label twice. After a refusal reading goes on at the next top-level form.
- A later phase does not run on what an earlier one refused, as tonk's language server does. That keeps a refusal from being followed by the refusals it causes downstream.
- A document the compiler refuses now keeps no definition either. Before, the analyzer had kept them by the time the compiler ran.
- The notebook is `check` and then the steps.
- Not covered: the compiler says which tree and not which part. Its errors are texts, and the analyzed tree has no way back to the written one yet: that is the analyzer's half of the mapping.

**Decided.** By the agent, open to change: one diagnostic type for the three phases; a later phase waits on the one before; `check` as the one call an editor makes.

## 2026-11-13, Designing: a comment as a description, and where patterns stand

**Asked.** Irakli: "where's pattern support workstream or composite value support? We need both here".

**Learned.** Two things, one built and one found.

- Built: `description:` is out of `concept`. A comment right above a form, with no blank line between, and one that ends its line, say what the form is for. A comment with a blank line under it is about no form. `check` puts what they say on each analyzed rule as its `description:`, which the compiler hands to Dialog-DB.
- Dialog-DB is given a rule's description and keeps none. The descriptor reads one, the rule built from it gives none back (`DeductiveRule::descriptor`, `InductiveRule::descriptor`), and format 3 of a stored rule writes no description (`dialog-query/src/rule/body.rs`). The [kernel note](kernel.md#describing-things) had this already and the proof of concept had not built it: a description is a fact about the rule's entity. The ledger's hypothesis said "the description Dialog-DB keeps", which was loose.
- Built: a described rule compiles to the rule and one more assertion, the description as a fact about the rule's entity. `(query (syntax.meta ?rule description: ?text))` finds a rule by what the comment above it says.
- The kernel note names the relation `dialog.meta/description`. Dialog-DB refuses that from a transaction: `dialog.` is kept for what it writes itself, but for `dialog.rule/` and `dialog.concept/` (`dialog-corpus/src/tree.rs`). So the fact is kept under `syntax.meta`, beside `syntax.name` and `syntax.form`, which the tests already keep names and forms under.
- A concept has no entity yet, since a definition is held by the analyzer and not kept on a branch. Its comment is in the mapping and nowhere a query reaches.
- Where patterns and the composite value stand:

```
 #777  design: a parameter holds a pattern        open, waiting on review
 #778  build: composite value + patterns          draft; merged into this branch
 #779  design: a stored rule says less than a rule can   draft, Diagnosing; stamping not decided
 #748  research: record values                    draft; its variant question was decided 2026-11-11

 in the notation today                       needs
 a struct as a value in a write       ✓
 a struct as a pattern in a query     ✓
 a struct in a rule that derives      held by the notebook, not stored     a stored rule that holds a pattern (#779)
 a struct in a rule that writes       refused                              the same
 entity/derive over a record          taken back out                       the same
 a tree kept as a composite value     a stand-in layout (data.rs)          nothing: #778 is merged in
```

- Neither #773 nor #778 had run a check since main was merged in on 2026-11-12: each conflicted with its base in `design/README.md`, and a pull request that conflicts is not checked. Both are merged with their bases now. Every commit of today was pushed unchecked by CI; the tests were run here.

**Decided.** By the agent, open to change: descriptions under `syntax.meta` and not `dialog.meta`, until the maintainer says whether dialog lets `dialog.meta/` through; a comment block joined by lines. Not decided: what stands between the notation and structs in rules is one decision of the maintainer's, the stamping of a format that can hold a pattern, and then a build. With one head a rule, several heads are no longer one of the additions that format has to carry.

## 2026-11-13, Designing: a tree kept as dialog's composite value, and which patterns the notation writes

**Asked.** "Also where's pattern support workstream or composite value support? We need both here" (the maintainer). The engine agent's report came in the same sitting.

**Learned.**

- Built: a syntax tree is kept as the composite value of #778. `data.rs` was a layout of the notation's own, version 2, marked a stand-in from the day it was written. It is 215 lines where it was 472: what is left is how a name is spelled as a symbol and a map whose keys are names. `Syntax::value` is the tree as a value, and `Syntax::try_from(&Value)` reads one. `bijoux` is no longer a dependency.
- No name moved. The names of three trees and a signature under the old layout are pinned in `it_names_a_tree_apart_from_how_it_is_kept`, recorded before the change and equal after. A name follows nesting 128 composites deep (`NAMED_DEPTH`), about 40 forms nested in forms, and past that a part is named by its bytes.
- What did move: one number is kept one way. A constant written `2.0` reads back from a kept tree as `2`. The old layout kept the two apart in bytes under one name.
- A record of the old layout starts with the byte 2, which dialog now reads as a composite of a version it does not know, and refuses by that number. Nothing real was stored under it.
- Found by keeping trees that way: a query takes a kept form apart by a pattern, with no code that knows what a form is.

```
 (query (syntax.form ?f is: {type: :application
                             operator: {type: :reference name: /todo}
                             operand: _}))

 every kept form that applies `todo`
```

- For that, a name with a `/` in it is a symbol where a struct holds a value (`/todo`, `xyz.todo/text`), as it already was where a field does. A symbol was printed that way and could not be written back.
- The notation writes two of the patterns dialog has:

```
 dialog (#778)                    the notation
 Map   { key: Pattern … }         {author: ?who}            ✓
 Map   { … , rest }               {author: ?who ...?rest}   ✓ built this sitting, below
 List  [ Pattern … ]              no spelling
 List  [ … , run ]                no spelling
 Tuple [ Pattern … ]              [?first ?second]          ✓
```

- So a pattern cannot reach into the parts of a kept node: they are a list, `[ ]` is a tuple, and a list is not a tuple (`it_finds_a_kept_form_by_a_pattern_over_its_tree` pins the empty answer).
- A list a query gives back prints as `[ ]`, which reads as a tuple. What is printed does not read back as the value it was.
- The engine agent's three pull requests are open as drafts, each green: #790 (one body evaluated once), #791 (a seed binds its entity, breaking), #792 (a rule planned once). With #790 alone, a rule each with the body again is 0.8% to 5.7% over one wide head, where it was 27% to 148% on main: inside the bar of 10% set on 2026-11-12. One head a rule costs nothing several heads would have saved. Numbers: `research/rule-heads/results/2026-11-13-sound.md` (#788).

- Built after: `...?rest` after a struct's entries is the entries not named, as a struct. It is a variable of the tree: kept, printed where it was written, renamed like a variable when a definition is expanded, and bound in a query. `{author: ?who ...?others}` is every note with an author, where `{author: ?who}` is the notes that hold nothing else. Over kept forms, `{operator: {name: ?name ...?reference} ...?node}` is every form that applies a name.
- A list with a run is not built. `[?first ...?more]` would have to be a list, since a tuple has no run, which makes `[ ]` a tuple until a rest is written in it. That is the same question as what `[ ]` says, so it waits on it.

**Decided.** Built, needing no decision: the tree over the composite value; a symbol written in a struct; a rest in a struct, since `...?rest` already says a rest in an application. By the agent, open to change: a rest stands after the entries and nowhere else. Not decided, the maintainer's: what `[ ]` says. The agent's hypothesis is a list, in the ledger with what would refute it.

## 2026-11-13, Designing: a list is `( )`, and one thing at a time

**Asked.** "List is ()" (the maintainer), to the agent's question of what `[ ]` says. Then: "This is overwhelming amount of information and threads that I can't possibly navigate myself. So lets tackle one at a time an you pick the order in terms of priorities."

**Learned.**

- The question was already answered. The journal of 2026-11-11 has "`( )` is a list, as every lisp calls it, and `[ ]` a tuple". The agent asked it again, with a hypothesis against it, without reading what was written. The hypothesis is withdrawn in the ledger.
- Built: where a value stands, a `( )` headed by a keyword or an operator is a formula, as it was, and any other is the list of what it holds. `...?run` after its items is the ones after them, as a list. It is read where the kernel reads a term, so the tree has one `( )` node and a pattern over syntax is not changed.

```
 (assert! (xyz.note note:1 tags: ("math" "engines" "looms")))
 (query (xyz.note ?n tags: ("math" ...?more)))        the lists that start with "math"
 (query (xyz.note ?n tags: (?only)))                  the lists of one
 (query (syntax.form ?f is: {type: :tuple items: (_ _)}))    the kept tuples of two parts
```

- A list prints `( )` and a tuple `[ ]`, so what a row prints reads back as the value it was.
- With it the notation writes every pattern dialog has, in a write and in a query. In a stored rule none of them, which is #779.
- Two lists have no spelling: one that starts with a keyword, since `(:open :done)` is a formula, and the list of nothing, since `( )` has a first part.
- The agent ran too many threads at once and reported them all together. From here one question is asked at a time, and nothing new is started beside it.

**Decided.** By the maintainer: `( )` is a list, `[ ]` a tuple; one thing at a time, the agent picks the order. By the agent, open to change: how a list is told from a formula.

## 2026-11-13, Designing: a quoted list, `... ?more`, and the format is the new one

**Asked.** Two things, each answered by the maintainer.

- On lists: "I think it should be ... ?more as in ... is as a special label then space. Also note that if you want to write list you may have to do '(1 2 3) same as in lisp, in other words depends on the the context. I think my assumption is that other then in syntax you're pretty much never be using lists if that does not hold we should discuss".
- On which format a stored rule is stamped: "I don't understand the question. I think we should switch to a new format and maintain old one for backwrards compatibility for one release cycle while offering migration. In version after we'll remove support for old formulas."

**Learned.**

- The agent's question on stamping was badly put: it asked for a column of a matrix and did not say the case the columns differ in. The answer is the first column, "the latest": a release writes the new format always, reads the old one for one release cycle with a migration, and the release after reads the new one only.
- The case the question was about is the other direction, which the answer does not speak of: two peers on one space, one upgraded. Every rule the upgraded one writes is in the new format, a rule of plain terms too, and the older release reads none of them. The notes of #779 record that it skips them and says nothing (`design/rule-format/stamping.md`); not checked again today. It lasts until every peer has upgraded.
- The quote overturns what the agent built an hour before, telling a list from a formula by its head. It is the better rule: it gives the two lists the head rule had no spelling for, `'(:open :done)` and `'()`.
- `... ?more` cannot stand beside `?more ...`. Both are in the ledger, with the agent's hypothesis that the first replaces the second.

**Decided.** By the maintainer: the new format is written always, the old read for one cycle with a migration, then dropped. A list where a value stands is quoted. The parts not named are `... ?more`. Not decided: what becomes of `?more ...`. Not built: any of the three; the reader changes once, after that answer.

## 2026-11-13, Designing: `...` takes a pattern

**Asked.** The maintainer asked to see every `...` there is, then what repeating a pattern of several parts means, then: "Sounds like we could do (expand (labels ... (?key ?text)))) no ?"

**Learned.**

- Yes. `...` followed by a pattern, which each part not named has to match, is the rest and the repeat in one form. `... ?more` is the same with a variable for the pattern. Nothing `p ...` did is lost, so the agent's question, whether to give the repeat up, falls away.

```
 today                                   decided
 (query ?head ?premise ?more ...)        (query ?head ?premise ... ?more)
 (?command (?concept ...?fields))        (?command (?concept ... ?fields))
 (labels (?key ?text) ...)               (labels ... (?key ?text))
```

- What it leaves: an application holds unlabelled and labelled parts in one bracket. `... (?key ?text)` is of the unlabelled ones. A bare `... ?x` could be of either or of both. In the ledger as the agent's hypothesis: both.

**Decided.** By the maintainer: `...` takes a pattern, and `p ...` goes. Not decided: what a bare `... ?x` takes in an application. Not built; the reader changes once, for this and the quoted list.

## 2026-11-13, Designing: a record beside one part, and who gives `...` its meaning

**Asked.** The maintainer: think of `(todo ?t list: ?l text: ?x)` as sugar for `(todo {of: ?t list: ?l text: ?x})`, and `(todo ?t ?extra list: ?l text: ?x)` is not valid. Then, while the agent was building: "I imagine that parser / reader does not give ... any special meaning and it is analyzer that goes from just lists to interpretation I propsoed".

**Learned.**

- The sugar dissolves the agent's question of which parts a bare `... ?x` takes. Beside labels the rest is the record's, and a form with no label is a list, whose rest is a list's. They never share a bracket.
- What does not fold into a record is a form of several parts and no label, `(rule head premise premise)`. The journal of 2026-11-11 had it: "The reader cannot fold it in". So a label written makes the parts a record, and no label leaves a list.
- The agent started building all of it in the reader and the tree: a form for `... p`, the refusal of a second unlabelled part, `of:` said twice. That is the wrong phase by the maintainer's word, and it is where `...` already was before today. The change was set aside before it compiled; nothing of it was pushed.
- With a reader that gives `...` no meaning, `{author: ?who ... ?others}` is two entries, the second under the key `...`. That is what "a special label" is: nothing in the reader, and a key the analyzer knows.

```
 the reader                                    the analyzer
 (query ?head ?premise ... ?more)   a list     ... then a part: the parts not named
 (todo ?t list: ?l ... ?fields)     a list     labels: one record, ?t its of:, ... its rest
 (todo ?t ?extra list: ?l)          a list     refused, with where: two parts beside labels
 {author: ?who ... ?others}         a struct   the entry under ... : the entries not named
```

**Decided.** By the maintainer: a form with a label is sugar for one record; the reader gives `...` no meaning and the analyzer does. Not built. To confirm before building: that the split in the diagram is the one meant, and that `'` is the reader's as in lisp, `'(1 2 3)` read as `(quote (1 2 3))`.

## 2026-11-13, Designing: built, the reader that knows nothing of `...`

**Asked.** The maintainer: yes, `'` is the reader's as in lisp. And a trailing `...` in a rule body, `(todo ?t list: ?l ...)`, is sugar for `... _`: the form may have other labels and they are not cared about. "Again something for analyzer not reader."

**Learned.**

- Built. The reader knows brackets, atoms, labels and `'`. `...` is a symbol, a part among parts and a key among keys, and `'x` is `(quote x)`. The tree lost its repeat node and its rest, and the mapping lost what it kept for them: `...` is a part with a place like any other, so the text prints back with nothing special.
- The analyzer reads `...` where a pattern is matched and a template filled, and refuses what a record may not say, at the form that says it, with line and column.

```
 text             reader          tree                    analyzer
 ... ?more        two parts       a name, a variable      the parts not named, bound
 ... (?k ?t)      two parts       a name, an application  each part not named, taken apart
 ...              one part        a name                  ... _
 '(1 2 3)         (quote (1 2 3)) an application          where a value stands: a list
 (f ?a ?b k: 1)   a list          an application          refused: two parts beside a label
```

- One thing the single `...` forced. The prelude says a command twice, about nothing and about a subject:

```
 (?command (?concept ... ?fields))             was ...?fields: labelled parts only
 (?command (?concept ?subject ... ?fields))
```

  With one `...` both match a call that has a part with no label, and three tests failed as ambiguous. The agent's answer, in the ledger as an opinion: of the rules that apply, the ones that leave the least to `...` are taken. It is a rule about which rule wins, which there was none of before.
- Not built: the list of nothing, `'()`; and the trailing `...` in a use, which waits on what leaving it off means.
- 90 unit, 25 integration and 2 doc tests pass here; clippy and the docs build are clean.

**Decided.** By the maintainer: the split, the quote, the trailing `...`. By the agent, to ask: the rule that names the most wins.

## 2026-11-13, Designing: a read says every field or says `...`

**Asked.** Whether a concept read that leaves fields out without `...` is refused. The maintainer: "Yes it should. Also reader of the { foo: ?a … } should read it as { foo: ?a … _ } or any other trailing key should assume value _".

**Learned.**

- Built. A read of a concept says every field, as a write does, or ends in `...`. Left off, the refusal names what is missing, where the read is.

```
 (query (todo ?t text: ?x))        refused: leaves out `list:`, `state:`
 (query (todo ?t text: ?x ...))    every todo, whatever its list and state
 (query (xyz.todo ?t text: ?x))    a relation: nothing lists its fields, so it reads as before
```

- It changes which definition a read is of. Without `...` a read is of the definition whose fields are the labels written; with it, of every definition that has them. Two definitions of one name are told apart by a read the way they were by a write.
- Seventeen reads in the tests and the example left fields out. Each took a `...`.
- Built, in the reader: a key a struct ends on has `_` for its value. It is the one place the reader fills something in, and it is not about `...`: any key. The mapping notes the blank as written with nothing, so the text prints back as it was.
- What that costs: the reader took a struct of an odd number of parts for a mistake, and no longer does.
- Found on the way: `(todo text: ?x of: ?t ...)` lost its `of:`, since the parts with no label replaced it. Mended, with the test that found it.

**Decided.** By the maintainer: both. Still to ask: whether, of the rules that apply to a form, the ones that name the most win.

## 2026-11-14, Designing: the more specific rule wins

**Asked.** Whether, of the rules that apply to a form, the ones that name the most win, or the two command rules of the prelude are written so that they cannot both match. Shown as a picture, since the question did not read in words.

**Learned.** The maintainer: "More specific rule matching is what I'd expect also so it makes sense to me". The other way needed a pattern to say that a form has no part without a label, which the record has no words for: `{... ?fields}` takes every entry, `of:` too.

**Decided.** The more specific rule wins: of the rules that apply, the ones whose pattern leaves the least to `...`. It is the one rule there is about which rule applies. CI is green on the head that has all of 2026-11-13, on native, web and across the two.

## 2026-11-14, Designing: the data model has no tuple, the notation does

**Asked.** Where a tuple and a list part ways once they are values. The agent had built the two as two values (#778) and proposed, when told the data model has no tuple, to drop the tuple from the notation too.

**Learned.**

- The maintainer: "in the dialog's Value:: type there in no tuple there are maps and lists like in IPLD data model but tuple is not a thing. In our notation however tuple is the thing, but that does not mean it should have a representation in the data model". The agent's proposal mixed the two and is withdrawn.
- The two meet in one place, where a value stands. There `[a b]` and `'(a b)` are one list:

```
 written              the tree (syntax)            the value kept     read back
 [1 2 3]              :tuple, items                list               [1 2 3]
 '(1 2 3)             :application of quote        list               [1 2 3]
 [?a ?b ... ?more]    :tuple                       a list pattern     takes apart either
 '(?a ?b ... ?more)   :application of quote        a list pattern     takes apart either
```

- That is lossy in one spot: `'(1 2 3)` kept as a value comes back `[1 2 3]`. The agent offered to keep the node in the value so that it would not be; the maintainer: "I'm ok with stored lists coming out as tuples I suppose", and "I'd imagine you can destructure stored tuple either with list or tuple". Taken.
- The syntax tree loses nothing: a `:tuple` node and an application of `quote` are told apart by `type`, which is what a macro and the printer of source read. Only a value, once kept, does not say how it was spelled.
- Built. #778 loses the tuple value, the tuple pattern and the type `tuple:`. Here `[ ]` where a value stands compiles to a list pattern, with `... ?run` after its items as the quoted list had, and a kept list prints `[ ]`. The printing of `'( )` for a value, and its stand-in for the list of nothing, are gone with it; `[]` is the list of nothing.

**Decided.** By the maintainer: one sequence in the data model, two spellings of it in the notation, printed back as `[ ]`.

## 2026-11-14, Designing: descriptions stay outside `dialog.`

**Asked.** By voice, whether descriptions are `dialog.meta/description`, as the kernel note said, which Dialog-DB refuses from a transaction. The maintainer: the prefix is reserved "so that others cannot mess up internal indexes the database uses"; updating a description might be fine or even useful, but any other metadata can be associated too, so maybe it does not need to be there. He asked for a recommendation.

**Learned.** The agent's recommendation, which he took: keep `dialog.` closed. A description is text a user writes, and letting one relation through is a special case in the writer or a hole in the rule. A description is also not special: it is one thing said about a form beside who wrote it and when, and those belong in one place.

**Decided.** By the maintainer: `dialog.` stays closed, a description is ordinary metadata outside it. By the agent, his to change: the domain is `syntax`, which the kept form is already under (`syntax.form`), so the relation stays `syntax.meta/description` and nothing is renamed. The kernel note no longer says `dialog.meta`.

In the same conversation he settled how a pattern is written in a stored rule (`match`; [rule-format](https://github.com/dialog-db/dialog-db/pull/779)), which turned up a question of its own, now in the queue.

## 2026-11-15, Designing: a struct read is open, and what goes with it

**Asked.** The maintainer had said a map pattern with no rest should match a map of at least its entries. Put to him against three of his decisions of 2026-11-13 that give a trailing `...` its meaning, with exact as the other choice: "Open".

**Learned.**

- Open cannot be the meaning of dialog's own pattern: built, `{title: ?t}` is the map of a title alone, and taken apart it is any map with a title, so a rule means two things by which premise runs first (#778, `design/patterns/design-pattern-meaning.md`). That note leaves the open reading to the notation.
- So here: where a struct is read, `{title: ?t}` is lowered to `{title: ?t ... _}`. A struct read with no variable in it is a pattern too, and no longer the value. Where a struct is written, an assert or a rule's head, it is the value it says.
- What that takes back, of 2026-11-13: a trailing `...` in a read is allowed and adds nothing; a concept read that leaves fields out is no longer refused. `... ?rest` keeps its meaning.
- A list stays exact: `[?a ?b]` is two items. He spoke of maps.
- Lost for now: a way to say exactly these entries in a read.
- A key in a struct is text, since a map's key is (#778, rebuilt the same day as plain dag-cbor): `{author: ?who}` keys by the text `author`. A struct node of a kept tree keeps its entries in a map keyed by nodes today, and becomes a list of pairs.

**Decided.** By the maintainer: open. Not built here yet: this branch has the exact reading, the refusal, and symbol keys, until #778 and #799 are merged in.

## 2026-11-16, Building: the design moved, and this branch had not followed

**Asked.** The maintainer relayed a note his notation thread (#728) wrote for conduct: concepts as attribute lines, `cardinality:` for picks, electors in a concept's head, getters, views.

**Learned.**

- #728's `research/notation/design.md` is where the notation is designed, and it says so: "everything here is agreed unless it is marked Proposed". This branch took its spellings from it as of R25. It is at R45.
- The two have drifted both ways: [`design/macros/alignment.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/alignment.md) on `research-data`. The design has what this branch lacks (attribute lines, cardinality, electors, getters, layers, views, no `!`). This branch has what the design lacks (lists, the quote, structs, `...`, macros as rules).
- The agent asked the maintainer here for decisions on the notation's surface that he was also making there, and did not read #728 again after the first sitting. A parameter left out of a read is the plainest: the design had "matches anything" all along; here it was refused on 2026-11-13 and opened again on 2026-11-15.
- Three threads now hold decisions of his on overlapping ground, each in its own files: the notation's design (#728), this build (#772, #773), and the record (#748, #812). He carries messages between them by hand.

**Decided.** By the agent, his to overrule: the surface of the notation is #728's to say. This branch builds it, and what it settles on the way goes back there or is not settled. The order is in [`design/macros/alignment.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/alignment.md) on `research-data`; nothing of it is started.

## 2026-11-16, Designing: the maintainer asks for one notation of it all

**Asked.** What to do with the three threads' decisions. The maintainer:

- of the record: "I said lets go with CBOR here to avoid blocking on while that other thread was working out records stuff. At this point it felt like maybe it's worth considering what's there already, but if you feel it's not ready or is more of a [distraction] feel free to carry on with CBOR stuff."
- of #728: it refined how concepts are defined, since "doing it through domains had flaws"; picks went because "with pick: :max write can lead to very surprising states", back to cardinality as in Datomic, with picks said "through selective aggregation in the concept heads".
- of `in:`: "a whole another thread of work that is close to landing in dialog so we need to consider it seriously."
- of views: "never felt right with this notation, but I'm feeling much better about it in the new one."
- "Lists and spreads and our agreements here are something we should keep just need to synthesize it all. Please lead the way".

**Learned.**

- A view is the first form that needs both halves: the design's template is a list in order, kept as data, and that is a list and a form kept as a value, which this branch has.
- #812 is not where the notation can stand yet: no list in order, nothing to take a record apart, its Perf gate failing. The notation's spelling of a record does not depend on which is under it.

**Decided.** By the agent, as he left it: carry on with dag-cbor; the move to #812's record waits on a list in order there. The synthesis is [`design/macros/synthesis.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/synthesis.md) on `research-data`, a proposal: its "what had two answers" table is where he can disagree row by row. The build goes in the order it ends on.

## 2026-11-16, Building: the first step, on the record as rebuilt

**Learned.**

- Merged #778 as rebuilt. A key is text: a label as written, or text. `{1 ?x}` is refused.
- A struct read is open: `shape` in `compile.rs` gives a struct in a premise a blank rest, at every depth, so it is never the value looked up. `{author: "Ada"}` in a query is every note whose record has that author, and the row says the whole record. A struct written is the value, as before.
- A concept read says the fields it wants. The refusal of 2026-11-13 is gone; a trailing `...` is taken off and adds nothing.
- A kept struct node keeps its entries as pairs, in the order of their keys, since a node is no text and cannot be a key. A key twice is still refused there.
- A record is printed by its keys in the order of the alphabet, not the order it is kept in (the shorter key first).
- Names of kept forms moved once, with the data they are names of: three are pinned again.
- The cost said in the synthesis showed in a test: `terms: {}` over a kept form meant "no labelled part" and now means any.
- `assert` without `!` already means something in this build, a deduction's head, so dropping `!` is not a renaming.
- The notebook and the tests publish with the call main has deprecated for `Branch::transact`; allowed there with the reason, since each is the one writer of its branch.

**Decided.** Nothing new. 92 unit, 25 integration and 2 doc tests pass; the lint is clean but for main's own future size on this container's toolchain.

## 2026-11-16, Designing: where the order of `top` lives

**Asked.** Whether `top` ranks by the line's `as:` list, as #728 has it, or by an order kept apart in `among`, as the cardinality branch (#793) has it. It bears on #799, which fixes how a rule is written.

**Decided.** By the maintainer: "It lives on the type but top takes optional among: parameter that can specify it. That can come later". So the `as:` list is the order, and `among:` is a later, optional parameter of `top`, not a second list on the attribute. Relayed to the cardinality thread in the dialog-db space, under the notation thread's memo on #793.

Found on the way to the second step: this build's `assert` with no `!` is the word for issuing what holds in the tick (`Head::Hold`), so `assert!` cannot take its place until that has another spelling. The design leaves the same spot open. Not started.

## 2026-11-16, Building: the writes without their mark

The maintainer was away and asked for the build to go on without him.

**Asked.** With `!` gone (#728), `assert` is the write that stays. The kernel already had `assert` with no mark: the head that holds one round, which the prelude answers for a command in a head's place. What does that head become?

**Learned.**

- Nobody writes the held head. In the crate, the examples and the tests it appears only in the prelude's two templates and in two tests of what they answer.
- A rule with the head `(assert (todo ?t))`, one character from the write, marked `xyz.todo` itself as holding one round, with no diagnostic: any relation under that word was `Head::Hold`, and a held head with premises compiles to `Step::Transient` of its concept. Read off the code, not run. The mark was carrying a hazard as well as a meaning.
- A word for the held head moves the hazard and takes a name: `issue`, `hold`, `dispatch` and `do` are each a likely command.
- With `assert!` no longer a head word, `(assert! (todo …))` is read as a command named `assert!`: issued, kept by nothing, a warning and no refusal. Probed with `hold!`.

**Decided.** By the agent, in [design-issue.md](design-issue.md): the held head has no word. A head with no word on a relation under `command.` is issued for the round; on any other it is derived. `assert` and `retract` are the kept writes, and keeping a command is refused. `assert!` and `retract!` are refused with the word to write. It rests on the maintainer's opinion on #728 that no form carries `!`, against his own of 2026-11-11 here; what he wanted then, that a count persists, holds.

Built: `kernel.rs` (`Head::Hold` by the relation), `expand.rs` (three head words, a command at the top level is a rule with that head), `syntax.rs` (`COMMAND`, `is_command`), the prelude's two templates. 94 unit tests, 25 on a branch, 2 doc tests; lint clean. The stored rule's own keys, `assert!` and `retract!` in `dialog-query`, are what a replica holds and did not move.

Not done: a command at the top level with premises, which #728 writes with a `when`. Posted to #728: that the kernel has no word for issuing, so `(do add …)` would be a prelude rule if taken.

## 2026-11-16, Building: a concept as lines

**Asked.** What a line is under the walker: a new kernel read, or the read the kernel has.

**Learned.**

- The kernel's read is one entity and the fields named, each under a type and a pick. A line says the same of one field, so it needs nothing new below: `(xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)` is `(xyz.todo ?i title: (:last (:text ?t)))`.
- A name cannot be taken apart by a pattern, so the split of `xyz.todo/title` into its domain and its field is in the walker, not in a rule of the prelude.
- A concept was already checked for a variable its head does not hold, since the first build (`cc2448b`, 2026-11-11), which the alignment table had as "not checked". The check asked whether the body is reads, which lines were not, so lines went past it until a concept of lines was kept as its reads.
- Kept as a read a line, a query through a concept would report a row an attribute. Joined where one entity is read under one domain, a concept of lines reports what the same concept written as a read does.
- The reader already refuses two parts with no label beside labelled ones, so a line with two entities never reaches the walker.

**Decided.** By the agent, on #728's design: a line is surface, written by the walker as the kernel read. `:one` is `last` and `:many` is `all`. A variant is read as `:entity` and not checked. `in:` and a sized number are refused. The spelling with wrappers stays, as what the kernel is written in.

Built: `line` and `lines` in `expand.rs`; the to-do example is in lines. 98 unit tests, 26 on a branch, 2 doc tests; lint clean.

Not looked at: what a write under `:one` retires. It is what the engine does for the pick `last`, and it is where the cardinality work (#793) changes things.

## 2026-11-16, Building: a rule that holds a struct is kept

**Asked.** Nothing. The fifth step of the order was taken before the fourth: the stored form of a pattern was decided by the maintainer and waited only on a key being text, where a getter's spelling is still open on #728.

**Learned.**

- `match` is built on #799 ([rule-format journal](../rule-format/journal.md), 2026-11-16), and #799 is merged in here. `DeductiveRule::storable` is gone with what it refused, so the notebook's list of rules held aside went with it.
- A rule that writes is run when it is installed, over what is already there: installed after two notes, `(rule (assert (xyz.signed ?n by: …)) (xyz.note ?n about: {author: ?who}))` signs both in that transaction, and a third when it is written. The agent expected the third alone, and the test now pins all of it.

**Decided.** Nothing new. `it_keeps_a_struct_and_takes_it_apart_by_a_pattern` pins that a rule holding a struct derives from the branch and that one writes through it.

## 2026-11-16, Building: getters, and what stopped them

**Asked.** What a getter compiles to.

**Learned.**

- The engine has the fold a getter needs, `reduce` on a rule that derives, with `count`, `sum`, `min`, `max`, `avg` (`notes/aggregation.md`).
- And refuses it there. `DeductiveRule::with_reduce` returns `ReduceInOpenRule` for any rule with a fold in it. `notes/attribute-heads.md` argues it: a fold has no reading over a set still growing, and belongs to a query, a subscription or a rule that writes. The agent's row in the alignment table, "over the engine's folds", was written without looking.
- So the design's getter and the engine's rule cannot both stand ([`design/macros/alignment.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/alignment.md) on `research-data`). Both are the maintainer's.
- A pattern cannot require a labelled part: one the form leaves out leaves its variable unbound. So the getter's rule matched a command, `(increment (xyz.counter counter:a))`, with `is:` taken as left out, and the two were ambiguous. The rule now asks that `is:` be said, with a formula for that, `(:said ?value)`.

**Decided.** Not decided: where a fold lives, which is his. By the agent, in the build: the surface is the design's, and the refusal is the engine's, said as unsupported. A getter is read and expanded to a relation of its own, `getter.<name>.<concept>`; one that folds nothing runs; one that folds is refused with where the engine admits a fold. `is:` written is what makes the words a getter.

Built: `Term::Fold` and `FOLDS` in `kernel.rs`; the refusal in `compile.rs`; `(:getter …)` and `(:said …)` in `expand.rs`; the getter's rule in the prelude. 101 unit tests, 27 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Designing: the design moved, and a label as a keyword

**Asked.** The maintainer: to start closing the gaps, to read #728's design again as "some things have shifted", and "perhaps instead of my special labels we should just use keyword based labeling".

**Learned.** The design was read at `31929a6f`, eight commits past the last reading. What moved is a table at the top of [`design/macros/alignment.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/alignment.md) on `research-data`. The largest is the command: it is now a declared concept whose head names no entity, on a control layer, issued with `assert`. That reverses the build's choice of 2026-11-16, which issued with no word and refused `assert`; the design said its own a day later, and it is the maintainer's, so the build follows. Getters are gone for attributes in a concept's own domain, which fold, and so is `view`, which is now a rule that folds: main still refuses both (`ReduceInOpenRule`).

On labels: paired by place, a keyword label cannot be told from a keyword value that has no label, and the design's `view` form has one, `(view todo :row …)`. The design also spells a map's key two ways in one template, which may be what prompted the question. Four ways to spell a label are a matrix in [design-labels.md](design-labels.md).

**Decided.** Not decided: how a label is spelled. It rests on two open hypotheses, both the agent's. The order the gaps are closed in is the alignment table's, commands first, and none of the first four depends on how a label is spelled.

## 2026-11-18, Building: a command is a declared concept on a control layer

**Asked.** Gaps 1 and 2 of [`design/macros/alignment.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/alignment.md) on `research-data`, together: a command declared as a concept whose head names no entity, its lines `in: :control`, issued with `assert`; with no word, a command in a head is a deduction. The old form, `(add (todo …))` under `command.`, goes.

**Learned.**

- The entity derived from content and the one-tick lifetime were there, as `:id` and `Head::Hold`. What moved is what tells a command apart. The kernel now reads a layer off a field as it reads a type and a pick: a line `in: :control` is the field `(:control …)`. `assert` of a read whose every field is on the control layer is `Head::Hold`; the same read with no word is `Head::Derive`. The domain's name says nothing.
- A command's head names no entity, so its expansion holds a variable of its own there. Where no premise binds it, in a write and in a deduction alike, the kernel puts `(:id domain label: term …)` over the fields in its place. Identical commands in a tick are one, as before.
- A name with a `/` reads as an attribute. A declared concept's name wins, so `todo/add` is the command where it is declared and an attribute anywhere else. A misspelled command is therefore no command: it is refused as an attribute with no entity, and the refusal says no concept is declared under the name. Finding 9 of [kernel.md](kernel.md), a misspelling as a warning, is overtaken by commands being declared.
- A query of a command expands its head and its premise at two sites, which name the entity two ways. The report takes the entity the premise read.
- A document is still expanded top to bottom: a command used above its declaration in the same document is read as an attribute and refused (`it_finds_a_rule_by_what_the_comment_above_it_says` moved its declaration up). The design says the order of a document's forms does not matter.
- `(id ?t list: ?l text: ?x)` as a premise is not built, so the examples carry the to-do's entity in a `todo:` field of the command.
- A command a rule deduces runs end to end: two rules of one head read it in the tick, and it is not stored (`it_asserts_and_retracts_on_a_command_a_rule_deduces`).

Not decided by the design, and refused as unsupported, each pinned by a test:

- a concept whose head names no entity and whose lines are not on the control layer;
- a concept that mixes control lines with stored ones, and a head that concludes both;
- a concept that names its entity and holds control lines;
- a command whose lines are about more than one entity it does not name;
- retracting a command.

**Decided.** By the agent, each an opinion in [claims.md](claims.md): a declared command that no rule reads is issued with the warning the old form had. A call alone at the top level is refused, a command's included. A command's lines are in one domain, since this build derives its entity from one domain's claims; more than one is unsupported, which is a limit of the build and not a question of the design. A layer other than `:control` stays unsupported.

Built: `kernel.rs` (`LAYERS`, `Field::layer`, the head by layer, the derived entity), `expand.rs` (`in: :control`, the command checked where it is declared, a declared name before an attribute, the top level), `compile.rs` (the reported entity), the prelude's concept with no primary and its warning, the examples and every test. 105 unit tests, 27 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: an attribute's two terms unlabeled

**Asked.** Gap 4: `(xyz.todo/title ?t "Buy milk")` is `(xyz.todo/title of: ?t is: "Buy milk")`, wherever an attribute stands.

**Learned.**

- One place reads an attribute, the walker's `line`, so the spelling is one match there: two parts with no label are the entity and the value. A premise, a head, a line of a concept and a write all pass through it.
- The reader already refuses two parts with no label beside a labelled one, so `(xyz.todo/title ?t "x" as: :text)` is refused before the walker sees it. With `as:`, `cardinality:` or `in:`, the terms are labelled.

**Decided.** Nothing new. Built in `expand.rs`; pinned by `it_reads_an_attribute_of_two_unlabeled_terms_as_of_and_is` and, on a branch, `it_reads_and_writes_an_attribute_by_its_two_terms_unlabeled`. 106 unit tests, 28 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: a derived attribute replaces the getter

**Asked.** Gap 3: `(rule (list/count-open ?l (count ?t)) …)`, read `(list/count-open list:home ?n)`, an attribute in the domain of the concept `list`, concluded only. The getter goes.

**Learned.**

- The design names the relation by the concept's hash and the name. The expander has no hash of a concept: a concept is a rule about syntax, named by its signature, and its identity as a set of attributes exists only in the compiler. So the relation is named by the concept's name: `concept.list.count-open`. Unlabeled, its value is under `is:`; with labels, `(list/count ?l status: ?s count: (count ?t))`, it is the fields the labels name.
- A name with a `/` now resolves three ways, in order: a concept declared under it (`todo/add`), an attribute in the domain of a concept declared under what is before the `/` (`list/count-open`), an attribute (`xyz.todo/title`). A misspelled command under a declared concept's name is read as a derived attribute and refused for having no entity, which the refusal says.
- Writes of the concept's domain are refused in the kernel, by the domain's prefix, so a kernel rule written by hand is refused too.
- A fold in such a head is the engine's refusal, as it was for the getter. One that folds nothing runs on a branch.
- `:getter` and `:said` had nothing else to serve, and went with the getter's rule.

**Decided.** By the agent: retracting a derived attribute is refused as asserting one is; the design names only the assert. The relation's name, from the concept's name. Both opinions in [claims.md](claims.md). Not looked at: a concept's name declared twice under other signatures, which the design lists as open.

Built: `derived` and the resolution order in `expand.rs`, `DERIVED` and `is_derived` in `syntax.rs`, the write refused in `kernel.rs`; the getter's rule out of the prelude. 107 unit tests, 28 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Designing: a struct's keys are keywords

**Asked.** The maintainer: "I think we should just treat (foo ?bar baz: ?beep) as sugar for (foo {:of ?bar :baz ?beep})".

**Learned.** The red cell of [design-labels.md](design-labels.md): paired by place, `(view todo :row …)` gives the template the label `row`. He agreed, and kept the colon label in calls. A call was already read as a struct, so only the struct's spelling moves: the reader takes a key written `:list` as the name `list`, as it took `list:`, and the printer writes it so. A text key, `{"N1" …}`, and `... ?rest` are what they were.

**Decided.** By him: the fourth column, keywords in a map only, a call sugar for the struct. By the managing agent: the old spelling inside a struct, `{of: ?t}`, is refused with the spelling to write, not read as well, and so is a bare name as a key. Not decided: what a key is, a name or an entity. A keyword is an entity in the design (`:done` is `done:`), and dialog-query's `MapPattern::new` refuses a key that ends in `:` (`PatternError::ReservedKey`), so taking a key as the keyword's entity would change what a stored map holds. It stays the name.

Built: the reader's `{` and `former_key` in `form.rs`, the two printers, every test text. 108 unit tests, 28 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: a document's forms in any order

**Asked.** Step E of the tonk order ([`design/macros/tonk.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/tonk.md) on `research-data`): the design says the order of a document's forms does not matter, and the build expanded them top to bottom, so a command used above its declaration was read as an attribute.

**Learned.**

- What declares a name is a `concept`, or a `rule` whose head is `syntax` or `warning`. Each is known by its first word before it is expanded, so the walker expands those first, in the order written, and the rest after them.
- A macro's expansion may also declare: `(note-kind memo)` expands to a concept. That is known only once it is expanded, so what failed before it is expanded again whenever a pass declared something, until a pass declares nothing. A name two such expansions need of each other is not a cycle the walker can break: it is refused with whatever the first one met, an unknown form or an attribute with no entity. Built and pinned, `it_expands_again_what_a_later_expansion_declares_a_name_for`; the mutual case is not pinned, since no core form makes one.
- A document's kernel rules come back in the order of the trees they came of, whatever order they were expanded in, and so do its refusals.
- A concept installed by one document is known to the next one on the same branch, since the walker keeps it (`it_runs_a_document_whose_uses_stand_above_its_declarations`). Nothing stores it on the branch: a new walker over the same branch does not know it ([`design/macros/poc.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/poc.md) on `research-data`, syntax is not stored).

**Decided.** Nothing new. 110 unit tests, 29 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: creating things, `id` and `fresh`

**Asked.** Step F: the design's two formulas that make a new thing, as a rule's premises. `(id ?t list: ?l text: ?x)` binds `?t` to the entity those labels and values identify; `(fresh ?n)` binds a new one.

**Learned.**

- `(:id domain label: term …)` stood only where a term stands, a chain of `entity/derive` from the domain and the sorted labels through each value. As a premise it is the same chain, ending in the variable `id` binds. So `id` names exactly what `(:id …)` names, given a domain.
- `(id ?t …)` writes no domain. The design lists open whether `id` names a domain or a concept. The build takes the domain of the head `?t` is the entity of: in the to-do's rule, `xyz.todo`. An entity of no head, and one of heads of two domains (a concept over two), would each need the question answered, and are unsupported.
- dialog-query has no formula that mints. Its formulas are functions of their inputs and are cached as such (`formula/cache.rs`); the only entity formula is `entity/derive`. `(fresh ?n)` is unsupported. What the engine would need: a formula that is not a function, run once per firing of an inductive rule, its entity drawn from what that firing has that no other has (its cause, or a random value it records), so a rule that fires again is not given a second entity for the same firing.
- The to-do example now adds by list and text, and its rule makes the entity with `id`. Completing finds the to-do by its list and text, since the entity is no longer one an author writes.

**Decided.** Nothing the design leaves open: the domain an `id` names is the head's, said in the refusals, and the question stays the maintainer's. 111 unit tests, 30 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: collections

**Asked.** Step G: the design's collections. A line over one leaves the key a variable in the attribute's name and says what a key is, `(xyz.tonk.notebook/?p of: ?b is: ?x … by: :position)`. Build the read, the assert of one entry, and the check that the key is a head field and that a concept holds one collection.

**Learned.**

- Dialog keeps a collection as the attributes of one domain whose name is the key: `xyz.tonk.notebook/N1`. A descriptor reads every entry as `{the: {domain, keyed: sequence | dictionary}}` (`The::Collection` in `rust/dialog-query/src/attribute/descriptor.rs`), and a query binds an entry under its field as `{the: key, is: value}`. A rule's head writes an entry under the key its body binds to `<field>/key`. Both kinds run from a rule, read and written.
- The kernel had nowhere to keep a key, since a field is one attribute. A line over a collection is now the field `entry:` of its domain, its value `(:entry value key: ?p by: :position)`, and the kernel reads the key and what it is off that wrapper as it reads a type. The name `entry` is the build's, internal to the kernel read.
- A key a position is spelled uppercase, `N1`, and a relation written as text takes no uppercase name (`Relation::validate`). So a ground write of one entry is resolved under a name of its own and given the entry's attribute after, through `The::entry`, which also refuses a key of the wrong shape. For the same reason an entry cannot be read by its attribute written out, `(xyz.tonk.notebook/N2 notebook:1 ?x)`: Dialog-DB refuses the name. Through the concept, `(notebook-entry notebook:1 at: "N2" block: ?x)`, it reads.
- A row says an entry under its key, as the attribute it is: `(xyz.tonk.notebook notebook:1 N1: block:a)`.
- Two entries of one sequence, their keys compared with `<`, run in one rule: text keys compare in the order the index sorts them (`constraint/compare.rs`).

Not decided, unsupported: what a retract of a collection concept takes, every entry or the entity and the key.

**Decided.** By the agent: a key is written as text, `at: "N1"`. The concept's check refuses a key that is no head field and a second collection, as the design says. 113 unit tests, 31 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: a text that spans lines

**Asked.** Step H: tonk's documents hold view templates and method bodies as long texts. The design lists open how a text spans lines or holds `"`. The managing agent chose, as an opinion the maintainer can overturn: a text spans lines as written, newlines kept, `\"` a quote and `\\` a backslash, as a Clojure string. No triple quotes, no indentation taken off, no file included.

**Learned.**

- The reader already took a text over lines, newlines kept, and `\"`, `\\` and `\n`. The printer already wrote a backslash and a quote escaped and a newline as it is, so what it prints reads back to the same text, and a document printed from its mapping is the text written.
- One thing was not so: any other escape lost its backslash, `"\d"` read as `d`. A method's JavaScript holds such escapes, so a backslash would have gone missing without a word. That is refused now, saying to write `\\`. `\t` and `\r` are taken, as Clojure takes them.

**Decided.** The managing agent's choice, built. By the agent: an escape other than `\" \\ \n \t \r` is refused rather than kept as written. 115 unit tests, 32 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: a write at the top level with `when`

**Asked.** Step I: "Writes succeed what they read". `(assert (xyz.todo/state of: ?t is: :done) (when (todo ?t text: "Call mom")))` runs once in its document and is never installed; every variable is bound by the `when`; without one a write is ground. For `assert` and `retract`.

**Learned.**

- A write at the top level with premises after it was a rule with that head, installed. The design has no such form: a top-level write is ground or has a `when`. So premises with no `when` are refused now, naming both forms to write, and a `when` makes a kernel rule that says `once: true`.
- Dialog-DB has no way to run a rule once without installing it. So a write that runs once compiles to a query and a template: a query whose row is every variable the write holds, and the write, which each match fills and makes. The notebook runs the query over the branch as it stood before the transaction and adds each write to it (`Step::Write`).
- What it reads is therefore the branch before the document, not "the facts as they stood when the tick began, plus whatever is asserted or deduced in it": a `when` does not see what its own document asserts, nor the rules it installs. Not built: reading the transaction as it is being made.
- Under `:one`, the write succeeds the claim the cell holds, which is what main does for any `last` write (`Attribution::pick`): after it, every claim of the to-do's state is `:done` alone. It does not use the claim the `when` read: the succession is the cell's, whatever was read. Under `:many` a write adds.
- A command with a `when` would be the design's open "Whether a top-level command keeps `when`", and is unsupported.

**Decided.** By the agent: premises after a top-level write with no `when` are refused, where they made an installed rule before. 116 unit tests, 33 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: a bar for the notebook

**Asked.** Step O: measure each step by tonk's notebook. [`design/macros/sketches/notebook.clj`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/sketches/notebook.clj) on `research-data` is `notebook.yaml` in the notation as designed. For each top-level form, how far it gets: read, expanded, compiled, and the first error of each that stops.

**Learned.**

- The sketch has 56 top-level forms: the YAML's 53, with the element's entries written one attribute each. All 56 are read. 34 expand and 32 compile, on a walker that knows only the prelude.
- What stops them, grouped by what it says (`cargo run -p dialog-notation --example bar`):
  - 9: an element's entries written by attribute with `by: :name` and the key in the name, `(xyz.tonk.element.method/connected of: … by: :name)`. The build took `by:` only with the key a variable. The design puts `by:` on the line as part of the attribute's identity, so a constant key with it is a line the build should read.
  - 5: `view`.
  - 2: `!=`. 2: `dialog/position`, read as an attribute line.
  - 2: `route`, 1: `intent`. Both are core.yaml's concepts, which the sketch reads and does not declare.
  - 1: `id` binding an entity that is no head's entity, `?b` of an entry of `notebook/blocks`: the command's own entity, step M.
  - 1: a retract of an entry of a collection, not decided.
  - 1: `(retract (notebook/block ?b))`, a retract through a concept by its entity alone. The design's table says it retracts every field on the entity; the build refuses it as a write that leaves fields out. Not in this batch.
- The bar measures the three phases with no branch. What a branch refuses on commit it does not see.

**Decided.** Nothing. The numbers are pinned in `it_measures_how_far_the_notebook_gets`, so a step that moves them says so.

## 2026-11-18, Building: one entry written by its attribute, `by:` on the line

**Asked.** Nothing: the notebook's bar found it. Nine of the sketch's forms write an element's methods one entry each, the key in the attribute's name and `by: :name` on the line, and the build refused `by:` without a key variable.

**Learned.** The design puts `by:` on the line because it is part of the attribute's identity, so a line of one entry says it too. Such a line is now the entry with that key, written and read as step G's entries are.

**Decided.** Nothing new. The bar moves from 34 expanded and 32 compiled to 43 and 41, of 56.

## 2026-11-18, Building: a parameter left out of a read

**Asked.** Step J: `(todo ?t text: ?x)` with `state:` left out reads as if `...` were written, in a query and a premise; in an assert through a concept a missing field stays an error. alignment.md had it "refused since 2026-11-13; open again on 2026-11-15, not built".

**Learned.**

- It was built, and the row was stale. Since the read became open (2026-11-15), the walker takes a trailing `...` off a read before it asks the rules, so a read with `...` and one without are one read (`ask` in `expand.rs`). The field left out is still read under a variable of the expansion's own, so it matches any value.
- So an entity that holds no claim of the field is no instance: `todo:3`, with a text and no state, is not a `todo`. That is what a concept is in the design, a set of attributes, and what tonk's YAML says of a block written with only its source (notebook.yaml line 467). Whether "matches anything" also takes in a field with no claim, the design does not say in so many words.
- A write through the concept that leaves a field out is refused by the compiler, as before.

**Decided.** Nothing. Pinned on a branch, `it_reads_a_concept_with_a_parameter_left_out`. The bar does not move: no form of the notebook stopped on it. 116 unit tests, 35 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: equality and inequality as premises

**Asked.** Step K: `(== ?a is: ?b)` and `(!= ?a is: ?b)` as premises.

**Learned.**

- The design spells both, among its constraints: "equality: `(== ?a is: ?b)`, `(!= ?a is: ?b)`". So the spelling is the design's. `==` was the build's `:value` under another name; `!` was no operator character, so `!=` read as a name nothing defined. It is one now.
- Dialog-DB has equality as a constraint and no inequality (`Constraint` in `rust/dialog-query/src/constraint.rs`). It negates a proposition, and a constraint is one, so `!=` compiles to the absence of `==`, as tonk's YAML writes it (`unless: ==`). `!=` inside `unless` would be a negation of a negation, and is unsupported.
- A constraint binds nothing on a read and refuses on an assert, the design says. As a premise here it filters; neither is installed as a check on writes.

**Decided.** Nothing: the design gives the spelling. The bar moves to 45 expanded, 41 compiled: the notebook's two `!=` rules expand, and stop at the retract of a collection entry, which is not decided. 116 unit tests, 36 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: `dialog/position`

**Asked.** Step L: `(dialog/position ?b after: ?lo before: ?hi is: ?at)`, how tonk writes a position key for a `by: :position` collection.

**Learned.**

- The design spells it so, among a rule's formulas. dialog-query has it, `dialog/position` (`rust/dialog-query/src/formula/position.rs`): cells `member`, `after`, `before` and `is`, `""` an open bound, the member's entity biasing the position so the same insert on two devices lands alike. The subject with no label is the member.
- Its name holds a `/`, so the walker read it as an attribute line. It is the one formula named so, and is taken before a line is.
- On a branch an entry written between two others sorts between them under `<`, which compares positions as the index sorts them (`it_places_an_entry_between_two_with_dialog_position`).

**Decided.** Nothing: the design and the engine agree. The bar moves to 47 expanded and 43 compiled: the notebook's two position rules expand and compile. 116 unit tests, 37 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: a rule reads a command's own entity

**Asked.** Step M: tonk's rules need the entity of the command that fired, to attach a cause or to key what it makes. The design does not say how. The managing agent's opinion, to keep reversible: `of: ?c` in the premise that reads the command, `(todo/add of: ?c list: ?l text: ?x)`, binds the entity the command's content derived.

**Learned.**

- It falls out of how a command lowers. A command's expansion holds a variable of its own for the entity its lines are of; a write derives it, and a read leaves it unbound. A command's declaration now adds a second definition for a premise only, whose pattern takes one part with no label: what `of:` gives fills that variable. The read binds it to the entity the command's claims were written under.
- The entity is what the content derives, `(:id xyz.todo.add list: … text: …)` (`it_reads_the_entity_of_the_command_that_fired`).
- In a head, or at the top level, `of:` on a command is unknown: an entity is derived when a command is asserted, never given.
- The notebook sketch recomputes the entity with `id` over every field and does not use `of:`, so the bar does not move. Whether `id` over the same fields names the same entity: it does not. `id` in a rule names the head's domain (`xyz.tonk.notebook.block` for a block), and a command's entity is its own domain's (`xyz.tonk.notebook.insert`).

**Decided.** The managing agent's opinion, built. 116 unit tests, 38 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: a view, to a description

**Asked.** Step N: `(view concept :name template condition…)`, events inline as `:on/<type>`, as the design proposes and the sketch writes. Read it, expand it to a kernel form that is a plain description, check it, and give the description to another back end as data. Dialog-DB refuses it, pointing at #830. No renderer.

**Learned.**

- The kernel form is `(view premise… concept: todo name: :row entity: ?t template: […])`: the conditions expanded as a rule's premises are, and the rest beside them unchanged. `View::describe` reads it into `View`, `Node`, `Element`, `Attribute`, `Event`, `Input` and `Nested`, exported from the crate. Nothing in it is a Dialog-DB rule.
- What the walker checks, as the design says: the conditions apply the view's concept once, and that application's entity is the one rendered; every variable of the template, events included, is one a condition binds; every event posts a declared command with every field of it and no other; every nested view names a view of the document or of one before it.
- Unsupported, as the design leaves them open: a valueless attribute, `:hidden true` (the design has no boolean term); an attribute built of several parts, `:href ["/notebook/" ?n]`; a nested view by anything but its name and entity, `:model`, `:with`; a nested view of no entity.
- The design's own example nests a view as the map right after the tag, `[:ul {view: :row of: ?t}]`, where Hiccup puts an element's attributes. The description reads a map there as attributes unless it has a `:view` key, so an element has no attribute named `view`.
- No event of the sketch needs a flag: none prevents a default or stops propagation (as [`design/macros/tonk-port.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/tonk-port.md) on `research-data` counts it), so none is read and none was needed.

**Decided.** By the agent: a map with a `:view` key is a nested view wherever it stands. The bar moves to 48 expanded, 43 compiled: one of the notebook's five views, `notebook/block :directory`, expands, and stops at Dialog-DB; the other four stop at what is unsupported in their templates. 118 unit tests, 39 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: a declaration as data

**Asked.** Step P, from [`design/macros/tonk-claims-diff.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/tonk-claims-diff.md) on `research-data`: a tonk back end writes tonk's own declaration claims for each concept and command, and the crate gave no public way to get what a document declares. It read a concept's descriptor back by compiling a query over it. Expose, for each declared concept and command, its name, its description, whether it is a command, its lines with theirs, and the Dialog-DB descriptor it compiles to, in one list with the views.

**Learned.**

- A concept is a rule about syntax once installed, and compiles to nothing, so the declaration is read off the form as written: `Checked::declarations`, a `Declaration` each, `Concept` or `View`, in the order written. A line keeps what was written: the attribute's full name, `of:`, `is:`, `as:`, `cardinality:` or none, `in:`, the key over a collection, and the comment above it. A body written as wrapped reads gives a line a field.
- The descriptor is still what a read compiles to: the declaration's head as a query, compiled through the analyzer that holds the document's declarations, one `ConceptDescriptor` a domain it reads.
- `check` refuses a document whole on any refusal, Dialog-DB's compiler included, and refuses every view (#830). So declarations are given whenever analysis refused nothing, whatever the compiler says: a back end that renders views reads them from a document the Dialog-DB back end refuses.
- A concept declared by a macro's expansion, not by `concept`, is not in the list.

**Decided.** By the agent: declarations are given when analysis passes, even if compiling does not. 118 unit tests, 40 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: the retracts the design decides

**Asked.** Step Q. (1) `(retract (notebook/block ?b))`, a retract through a concept by its entity alone, which the design's table says "retracts every field of the concept on that entity", in a rule's head and at the top level. (2) A retract of one entry of a collection with the entity, the key and the value written, which retracts that entry, as `(retract (xyz.todo/text of: todo:9c1e is: "Buy milk"))` "retracts just that value": the managing agent's reading.

**Learned.**

- A retract by the entity alone names no field, so every field of the expansion is a variable of its own. The compiler reads the fields on the entity and retracts what it read: the head's read is added to the premises. At the top level the read is a `when`, run once (step I). Under `:one` it reads the newest claim and retracts that one.
- The design says nothing of a retract through a concept that names some fields and not others. It stays refused, as a write that leaves fields out.
- A retract of an entry with its key and value written compiles to what an assert of it does, with `retract!`: a ground one to a statement under the entry's attribute, one in a rule to an inductive rule whose head binds the entry's key. On a branch the entry goes and its neighbours stay, and the notebook's placement rule takes every other entry of its block out.
- A retract of a collection that leaves out the key or the value is unsupported, not decided. The refusal moved from the kernel, which cannot tell a field left out from one a premise binds, to the compiler, which can.

**Decided.** The managing agent's reading of (2), built. The bar moves from 43 compiled to 47, of 56: the notebook's three retracts of an entry and its retract of a block by its entity compile. 119 unit tests, 41 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: the notebook read with core

**Asked.** Step R: three forms of the notebook stopped on `route` and `intent`, which tonk's `core.yaml` declares, and tonk reads a component joined with core. Write the concepts of core the notebook uses in the notation, and have the bar read several documents in order as one.

**Learned.**

- The notebook uses four of core's concepts: `site`, `route`, `intent/expression` and `intent`. [`design/macros/sketches/core.clj`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/sketches/core.clj) on `research-data` holds them, from `core.yaml` at tonk `c006b50`, each line `cardinality: :one` since core's fields give no pick and tonk reads the newest.
- What has no spelling, as `; GAP:` comments: a concept pinned to an entity (`this: tonk:site`, `this: tonk:route`), an optional field (`maybe: selection`, on `site` and on `intent/expression`), and a layer core names only in prose ("Session facts").
- With core in front, the forms that stopped on `route` and `intent` expand and compile: the two route asserts with `when (id …)` and the palette's retitle rule. None stops on anything new.
- The bar reads several files, joined in order (`cargo run -p dialog-notation --example bar -- core.clj notebook.clj`, the two sketches by default).

**Decided.** Nothing. The bar of core and the notebook: 60 forms, 60 read, 55 expanded, 54 compiled, pinned beside the notebook's own. 119 unit tests, 41 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: three gaps in a view's template

**Asked.** Step S, three readings of the managing agent's, each kept reversible. (1) An attribute written as a list is its parts joined, as text children are: `:href ["/notebook/" ?n]`. (2) `true` as an attribute's value is the attribute present with no value, `:draft true`, there only; `false` is refused, saying to leave the attribute out. (3) A nested view: `:view` names it, `:of` may be left out (the view of every instance), `:model` names the concept whose view it is, and any other entry, the sketch's `:with`, is kept with its parts for the back end.

**Learned.**

- The description grows: `Attribute::Parts(Vec<Piece>)` and `Attribute::Present`, `Piece` of text, values and variables, and `Nested` with `of` optional, `model`, and `entries` of named parts. Every variable in a part or an entry is one a condition must bind.
- With `:model`, the view looked up is that concept's view of that name, so the walker keeps each view as its concept and its name, and refuses a `:model` that the documents read hold no such view of.
- `true` stays no term anywhere but an attribute's value: as a child of an element it is unsupported, as before.
- All five of the notebook's views are described now. Each stops only at Dialog-DB's compiler, which refuses a view (#830).

**Decided.** The managing agent's three readings, built. The bar: the notebook alone 52 expanded and 47 compiled, of 56; with core 59 and 54, of 60. 120 unit tests, 41 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: a fourth number, described

**Asked.** Step T: count a view that reaches `View::describe` as described, though Dialog-DB's compiler refuses it, and pin four numbers.

**Learned.** A view is described during expansion, so a view that expands is one that describes, and the count is the views among the kernel trees that `View::describe` reads. Before step S one view was described, `notebook/block :directory`; after it all five.

**Decided.** Nothing. The bar, read / expanded / compiled / described: the notebook alone 56 / 52 / 47 / 5, of 56; with core 60 / 59 / 54 / 5, of 60. 120 unit tests, 41 on a branch, 2 doc tests; lint clean.

## 2026-11-18, Building: an expansion's variable used once

**Asked.** Step U: write a variable the expander introduced, and the rule uses once, as `_`. A variable the author wrote is kept. The managing agent's opinion, reversible.

**Learned.** The variables the expander introduces are the ones named with a `.` in them, `?c.<site>` and the like, since the reader refuses an author's variable named so. Lowering them to a blank in a rule's premises and its `unless` changes no answer: a blank and a variable read once match the same claims. The stored body of a rule over a command premise now has `{"?":{}}` where it had a named local.

**Decided.** Built in `blanked` in `expand.rs`, for every rule but a `syntax`, `warning` or `report` one, and never in a head. Whether Dialog's canonical form treats any variable used once as a blank stays open and the maintainer's. The bar is unchanged: the notebook alone 56 / 52 / 47 / 5, of 56; with core 60 / 59 / 54 / 5, of 60.

## 2026-11-18, Building: the notebook reads an insert's entity

**Asked.** Step V: rewrite the sketch's five rules marked `; GAP: a command's own entity` to read the entity with `of:` on the command premise, as the YAML does with `this:`, and keep each GAP comment, saying the spelling is the managing agent's opinion.

**Learned.** Each rule now reads `(block/insert of: ?b ...)` and has no `id` premise. The rule tonk's note calls rule 8, YAML 835 and form 41, placed an inserted block in the sequence and was refused as "?b is the entity of no head"; it now expands and compiles. The other four expanded before and still do. The insert's own fields the rules no longer use are kept as written, so the rules match the YAML's shape.

**Decided.** Nothing beyond the sketch. The bar, read / expanded / compiled / described: before, the notebook alone 56 / 52 / 47 / 5, of 56, with core 60 / 59 / 54 / 5, of 60; after, 56 / 53 / 48 / 5 and 60 / 60 / 55 / 5. With core every form expands, and what does not compile is the five views.

## 2026-11-18, Building: what a back end reads, form by form

**Asked.** Step W, additive to what `Checked` gives a back end: each line's label and attribute descriptor, the form each declaration and step came of, and a way to check a document form by form, so that one refused form does not empty the rest. The tonk test reimplemented the loop and copied `described` to get this.

**Learned.** A line's descriptor is what `attribute` in `compile.rs` writes for the kernel field the line reads as, which is the descriptor a concept's read holds under that field's label. A line's label is the head's label whose variable is the line's value; a key's is the head's label whose variable is the key. The expander already keeps every form it can expand when another is refused, and says the way to the refused one; only `Analyzer::analyze` and `check` threw that away. Dialog-DB's `AttributeDescriptor` has no setter for a description, so a line's descriptor carries none; that is dialog-query's to add.

**Decided.** Built, all additive. `Line::label`, `Line::descriptor` and `Key::label`; `Checked::declaration_forms` and `Checked::step_forms`, parallel to `declarations` and `steps`, so `Declaration` keeps its shape; `check_forms` beside `check`, with `Analyzer::analyze_forms` under it. Through `check_forms` every step and analyzed tree carries its description, so a back end has no need of `described`. `check` is as it was, and fills the two new vectors too. The bar is unchanged: 56 / 53 / 48 / 5 and 60 / 60 / 55 / 5.

## 2026-11-18, Building: a second document, the table

**Asked.** Steps X, Y and Z: write tonk's `table.yaml` in the notation as designed, measure it on the bar after core, and sort every stopping form and every GAP comment by whether the notebook met its kind. The managing agent's hypothesis: no new kind of gap outside views.

**Learned.** Read after core, every form of the table reads and expands, and the six that do not compile are its views, which Dialog-DB refuses (#830): 54 / 54 / 54 / 48 / 6, of 54. Nothing the build accepts was spelled to suit it: the table's commands, rules, retract and seeds are the notebook's shapes again, a facet concept a field, a create that takes the command's entity, a retract through a concept with its field written. Of nine kinds of GAP, eight are the notebook's or core's. The new one is a value the host element gives a template, `model={dom.host/model}` (YAML 548), and the design does not decide it. The notebook met a nested view with no view named (YAML 327, 434, 463) and named one with no GAP comment; the table marks it.

**Decided.** The hypothesis holds: no new kind of gap outside views, one inside a view, undecided, pinned refused by `it_refuses_a_nested_view_whose_model_the_host_gives`. Nothing is built for it. The notebook's bar is unchanged: 56 / 53 / 48 / 5, and with core 60 / 60 / 55 / 5.

## 2026-11-18, Building: the table run on a branch, and its sketch read again

**Asked.** The rest of steps Y and Z: report the table's numbers from the bar, sort every stopping form and GAP comment, build what is new and decided, pin what is new and undecided, and read the sketch again against the YAML and the design, restoring any spelling bent to suit the build.

**Learned.**

- The bar, core in front: 54 / 54 / 54 / 48 / 6. The six that stop are the views, each with "not supported yet: the view ... is a description, read by `View::describe`; Dialog-DB has no form for a view" (#830). Nothing else stops.
- Of the GAP comments, every kind but one is the notebook's or core's, as the entry above sorts them. None is new and decided, so nothing is built.
- The one new kind, a value the host element gives, is already pinned refused. In the table it does not stop a form: a `<tonk-display>` with no entity renders its model's own `directory`, so the host of `table :directory` renders `table`, and `:model table` is that value written out. It stops core's view of any model, `tonk:_` (core.yaml 48, 966).
- The sketch has no spelling bent to the build. `cardinality: :one` on every line is the notebook's and core's reading of a field with no pick. `:view :ui` for a display with an entity and `:directory` for one without is tonk's own default, as the notebook wrote it for YAML 327, 434 and 463. A model named by its concept's name and not its pinned entity is the notebook's kind, the design's open question.
- Compiled is not run, so the table's rules were run on a branch from the sketch's own forms, its views left out. They do what the YAML's rules do. The clear rule leaves the cell's address, as the YAML's comment says its style may linger.

**Decided.** Nothing new. The GAP comment at YAML 548 says why the model written out is the same model there. `it_runs_the_table_on_a_branch` added. The bar is unchanged: the table 54 / 54 / 54 / 48 / 6; the notebook 56 / 53 / 48 / 5, with core 60 / 60 / 55 / 5.

## The queue

**Where the threads talk.** Since 2026-11-16: the `dialog-db` space on Tonk. Each thread is the same agent there, `id:daneel`, so a memo says which thread it is for in its first words. Read `inbox: { this: id:daneel, memo: ?m }` at the start of a sitting; the space's skills (`skill:`) say how to post. Posted from here: a memo for the conduct thread, in reply to the notation thread's, and notes on #728 and #812 on GitHub. The notation thread's memo on #793 says the order of `as: [..]` against `among` wants settling "before format 4 fixes it", which is #799.


**Answered 2026-11-16:** which composite is the record was left to the agent: dag-cbor for now ([`design/macros/synthesis.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/synthesis.md) on `research-data`). The order of the build is there.


In order. One is asked, answered and written down before the next.


**Diagnostics through the query engine: decided 2026-11-12, not built.** Asked again on 2026-11-14, the agent answered with options and a question back, without reading the ledger, which has it: every phase is reachable from the query engine, parsing and analysis as formulas, expansion as a resolver, "an LSP server is then queries" ([design-pipeline.md](design-pipeline.md)). The maintainer, 2026-11-15: a resolver takes a syntax form, or source text, and gives diagnostics; the LSP protocol is exposed over dialog, "so that it can then be exposed over HTTP or without". What stands before it: the analyzer reads its definitions from memory, and as a resolver it reads them from the branch, where a rule about syntax is not stored until format 4 (#799). Parsing alone reads no database and could be a formula now, but formulas are a closed set compiled into `dialog-query`.

1. Decided 2026-11-14: the more specific rule wins. Built 2026-11-13: the reader that gives `...` no meaning, the quoted list, the record's refusals, a read that says every field or `...`, a struct's last key.
1. Decided 2026-11-13: the new format always, the old read for one cycle. Built 2026-11-14 as #799, on #778, a draft: every rule body is written in format 4, a rule that holds a pattern is kept, and the bytes format 3 wrote are pinned as reading back. Merged in here on 2026-11-16, with a pattern written under `match`: the notebook holds no rule aside, and a rule that writes may hold a struct.
2. Decided 2026-11-14, by the maintainer, shown #791's cases: it is a bug. Filed as #801 and fixed apart from everything here in #802, merged 2026-11-15 (b8b8ba3), on main: the tests alone are its first commit and four of five fail there; with the fix the crate's 711 pass. #802 is the first of #791's two changes. The second, a written attribute that a deductive rule also derives from itself, is not in it and is filed as #804: main misses the derived fact with the subject named, and #802 makes the unnamed spelling miss it too, where main found it by the bug. Not checked by anyone: whether a tonk rule relies on the old firing.
2. Read off what the maintainer had said, 2026-11-15, after the agent asked three times: a key in a pattern is text. His `match` form has text keys and the data model is IPLD's. `{author: ?who}` keys by the text `author`; a pattern refuses the keys `?`, `...` and one ending in `:`. To build: `match` on #799, text keys here. What a record is written as is its own question now, on #778: he would write dialog's own types as inline CIDs.
3. Answered 2026-11-13 by the quote, and 2026-11-14 by the list: `'(:open :done)` and `[:open :done]` are one list, and `[]` is the list of nothing.
4. Decided 2026-11-14: `dialog.` stays closed, descriptions stay under `syntax.meta`.
5. Review of #777 and the open questions of #778.
6. The agent's choices standing until said otherwise: the blank-line rule for comments; a refused document keeps no definition; a rest stands after a struct's entries; `(all …)` or a spread in a template.
7. Housekeeping: #776 and the several-heads part of #779 after one head a rule; the Perf gate on #788; the sweep output under `/home/claude/heads/target`; the branch `wip/patterns-step-2`.

## 2026-11-18, Designing: what a view takes from where it is rendered

**Asked.** Porting `table.yaml` found one kind of gap the notebook had not: `model={dom.host/model}`, a template value read off the host element. The maintainer: "I think this is a wart we need to consider alternatives for."

**Learned.** tonk's library reads the host five times, and the one spelling carries two things: the model that was asked for (four), and the branch being read (one). The first is there because an instance does not say what model it is of; the second because an element that builds a URL inherits nothing. And a designed view is a rule, which has no host to read, so the question is not how to spell the read but where what it carried comes from.

**Decided.** Not decided. Four approaches are in [design-view-context.md](design-view-context.md). The agent's hypothesis is in the ledger: for a view of a named concept the nested reference can default to that concept. What it leaves is the one view of any model, and whether a space ever replaces it.

## 2026-11-18, Designing: a view is maintained, and is no concept

**Asked.** What a view is, with the fold in a rule that derives still refused (#830). The maintainer, spoken: a view is not a concept, and need not compete with stored values as a concept does; queries and getters are that same separate kind; they derive and are not stored; they could be something with no domain. The more important part to him: a view is not generated whole at every change, it is maintained, as tonk's are. He asked for a prototype.

**Learned.** The design's view (a rule that folds a template into a markup value) braids two things: what a view shows, and how it is kept up to date. A folded value can only be made again. Read as a template whose elements each have their own conditions, a view has no fold in it, and its upkeep is a subscription an element. That also answers something the design's one query does not: a list with no to-dos has no row, so it would have no card.

**Decided.** The direction is the maintainer's. The definition in [design-view-maintenance.md](design-view-maintenance.md) is the agent's, a hypothesis. A prototype in this crate will say. The fold spike of this morning is beside the point for views.


## 2026-11-18, Building: a view maintained from its layers

**Asked.** Whether the reading in [design-view-maintenance.md](design-view-maintenance.md) holds: a view with no fold, each element the conditions that bind what it holds, each change a patch of the smallest part. Built as a prototype in this crate: `view/plan.rs` lays a view out, `view/tree.rs` keeps its instances, `tests/view.rs` runs it on a branch.

**Learned.** It holds for the to-do card, with one change to the rule. An instance told apart by the values its element holds, as written, makes two to-dos of one title one item, and a retitle a remove and an insert. Told apart instead by what those values are fields of (the to-do for an item, nothing for a heading, whose name is one value of the list), every change is the patch of one instance: adding is one insert, renaming the list one text, retitling one text, completing one attribute, removing one remove, and an emptied list keeps its card. After every step the tree the patches make is the tree rendered afresh. Each layer is a subscription evaluated whole once and maintained after. One query over every condition renders no card for a list with no to-dos, as the ledger supposed. A nested view is a view of its own, mounted where its reference is, with subscriptions of its own: a retitle changes its row and nothing of the card.

The rule needed readings the specification leaves open: siblings that both hold what a condition binds take it at their common ancestor; a join on a variable no element holds is pulled in, or is a filter where its variables are bound; the entity rendered is bound above the root, which takes the concept applied to it alone. On the eleven views of the notebook and the table nothing was left unplaced and every layer compiles; the index route's site is shared at its root, the table's unshown name is the root's, and a `:directory` view is a root an instance where tonk renders one index. `Branch::subscribe` takes no rule beside its query, so a layer of several reads has its rule put in the branch's session overlay.

**Decided.** The managing agent's, until the maintainer says otherwise: the key rule above, the order of siblings by key with a collection's position first, no root for an entity that is no instance, a patch naming an instance by its address, and a view compiling to its plan (`Step::View`) when every layer's query compiles. The pinned numbers move on purpose: the notebook compiles 53 of 56 forms (was 48), 60 of 60 with core (was 55), the table 54 of 54 (was 48).

## 2026-11-18, Designing: a container is a view, and a nested view is a call

**Asked.** With the prototype in hand: what a directory is, what stands when nothing matches, and how a nested view is written.

**Learned.** A directory is one more view, by name. What the layered reading infers (the part of a template that stays) the maintainer wants written: a container is a view of its own, and within a view everything repeats. He found `{:view :row}` not obvious as a nested view; a call is, and its labels can narrow what it shows; and it should say `view`. A view that is not found shows as its source, which is the renderer's.

The agent had pressed the layered rule as the answer. It keeps the card, and it hides which part stays. His form says it.

**Decided.** The form, his: `(view concept :name label: term …)` in a template, containers as views, view source for a view not found. Open, his: whether a view and a concept are two things. To prototype: the notebook and table views restated in that form, and what each shows with nothing to show.

## 2026-11-18, Designing: tonk's rule for what repeats

**Asked.** Where repetition happens inside a view, once containers and calls are in. The maintainer saw that a call says it between views and nothing says it within one.

**Learned.** tonk already has the answer he wants, and it says what stays without a container: the element marked with the entity alone in an attribute repeats a match, what is outside it renders once whether or not anything matches, and a field of many values repeats the lowest element that holds it. The agent offered a split (written between entities, declared within one) and an inline nested view; neither was needed.

**Decided.** tonk's rule, the maintainer's. The container as a view of its own is set aside. A nested view stays a call. Open: a view whose conditions read a second entity, which tonk's views cannot.


## 2026-11-18, Building: a view repeats as tonk's do

**Asked.** First a container as a view of its own, then, the same afternoon, tonk's rule instead: a marker attribute on the repeating element, and a field of many values cloned where it is shown. A nested view a call, and a view not found shown as its source.

**Learned.** The call form needed one change to the reader, which refused two parts with no label beside labels; a view's call names its concept and its view first. tonk's rule, built as `Plan::repeating` from its code: with the marker the index stands empty and repeats its links; without it the whole template repeats, which is what tonk's own notebook directory does. Tags are cloned at the span in both readings, by the concept's cardinality, and moving the title changes nothing. A task with no tag is no task at all. Of the eleven restated views, ten repeat the element tonk's planner repeats; `table :directory` does not, as a call has no attribute to carry a marker. Two faults surfaced and were fixed in the build: a query of several reads crossed a row's values when its head was the list, and entries sorted by block, not position. One was sidestepped and is not understood: a rule with a blank and a bound read of one collection does not hydrate.

**Decided.** The maintainer's: tonk's rule is the default and the layered plan stays beside it. Open and his: a view over a second entity, and whether a call may carry a marker.

## 2026-11-18, Building: the rule that did not hydrate

**Asked.** Why a rule that compiled and stored could not be read back (`an empty map names no type`), and at which layer.

**Learned.** The fault is dialog-query's, not the notation's, and the ledger's first account of it was wrong on one point: one read is enough, two reads of the collection are not needed. A collection entry leaves a blank half out when written, so an entry with both halves blank is written `{}`; the reader only took an object holding `the` or `is` for an entry, and read `{}` as a constant. The same lines are on `main`. The fix (`8261ebd`) is on the read side, one condition, so no rule's bytes or identity change and bodies already stored become readable. Writing the blank out instead was set aside: it changes the bytes of these rules and leaves stored ones unreadable.

Two things it turned up and did not settle. The view plan's sidestep cannot come out: with the fix the rules load, and without the sidestep every block is paired with every key, which is a different fault, not looked into. And a rule built through dialog-query's own API with a collection field and no key seems to change identity across a decode, the bytes the same; a hypothesis in the ledger, since only a probe saw it.

**Decided.** The managing agent's: the read-side fix stays on #773 until the maintainer says which pull request below it should carry it. Not decided, the maintainer's: the identity of a missing key against a blank one, since either fix changes identities.

## 2026-11-18, Building: the blank entry goes to main

**Asked.** The maintainer: a fault on `main` is addressed on `main`, as a report with a test that reproduces it and then a fix, after checking it is not fixed or open already. Then: are explicit blanks the better format, and should such a read match at all where there is no entry.

**Learned.** Nothing reports or fixes it: no issue or pull request names it, and of the open pull requests that touch the file (#827, #825, #806, #784) none changes the entry's reader or writer. Built on `main` at `86bd0c2`, both tests fail with the error. The agent's case for leaving blanks out, made by voice, was the wrong way round: a blank in a field that is no collection is written out, and only an entry leaves a blank half out, so writing them out is the consistent spelling. It still changes the bytes of every rule that reads an entry with a blank half, which on `main` is a released format.

His second question is #712, his own: a collection premise is a positive join, so no entry gives no row, and he expected the row. That is also what makes a to-do with no tag no to-do in a view. If #712 goes its first way, a read with both halves blank says nothing, and "has an entry" needs a spelling.

**Decided.** The maintainer's: report and fix on `main`. Done as #834 and #835 (draft), the read-side fix, and taken off #773 by a revert. Not decided, his: whether an entry's blanks are written out, with format 4; and #712.

## 2026-11-18, Designing: the canonical form of a read

**Asked.** Why the build still writes `(:entry (:all …))`, when the design's full spelling was agreed. Then, by the maintainer: the line should be canonical, the expansion is confusing and misleading, and a `claim` form he had considered.

**Learned.** The form he met is the kernel's read, which nobody writes: no sketch, example or prelude rule holds one. The agent had shown it as if it were notation, and the reader does take it from a document. It contradicts the design in one place the agent had not seen: it writes picks, which `cardinality:` replaced. The pick-wraps-a-term form was the agent's proposal in kernel.md, never the maintainer's decision. What it is, plainly, is the engine's descriptor shown as syntax.

**Decided.** Not decided. [design-canonical-read.md](design-canonical-read.md) sets the three side by side. The agent's hypothesis is the line, with the descriptor as compile output; `claim` is the simpler form and has met no reader.

## 2026-11-18, Designing: no entry, no row

**Asked.** Whether a field of many values is satisfied by none, which the agent had asked twice, and which #712 asks of a collection.

**Learned.** The maintainer's answer is no: the key and the value are fields of the head and neither is optional, so a notebook with no block is no `notebook` of that shape. Two concepts, the title in one and the blocks in the other, and the first always matches; Datomic does the same. A reader who wants the blocks with the notebook wants them collected, and that is an aggregate.

It does not sit with #712 as written, which expected the row. The agent's reading: the YAML shows a collection as one field holding a map, where the notation shows an entry as a row, so #712 is two questions and he has answered the first. The second, the map as a whole, is a fold, and no query can state one yet (#830).

It leaves the view where it was. Splitting the concept does not keep a to-do's `li` when it has no tag, since two reads conjoined still give no row.

**Decided.** The maintainer's: no entry, no row; split the concept. So a read of an entry with both halves blank says there is one, and the fix in #835 stands as it is. Not decided: how a view reads the second concept without losing the first, and the aggregate.

## 2026-11-18, Designing: a query nests

**Asked.** What a view does with a to-do that has no tag, which the agent put as a yes or no about one inferred rule. The maintainer could not read the example, which was written from memory in no form the design has, and answered the question behind it.

**Learned.** His: the view says what it needs by its shape. One query needs everything; a nested part or an aggregate may be empty. A nested part is passed its parent and repeats n times, none included. Aggregation belongs at the outside, where nothing can read it back, which is why he kept it out of rule heads; and that outside thing and a rendered view are about the same thing. He asked for a recommendation and for prior art, Datomic's pull and GraphQL.

The prior art draws his line already: Datomic folds in `:find` only and trees in `pull`. The agent had two things wrong by voice: pull does not give an empty list for a missing attribute, it leaves the key out; and `query` is not a free word, it is the design's question form, which makes it the unnamed case of the same thing and not a rival to `view`.

The agent's inferred rule is withdrawn. So, if the sketch holds, are the design's fold over an entity and tonk's marker, the second of which was taken this morning.

**Backtracked.** From Building to Designing on views: what repeats is being decided a third time today.

**Decided.** Not decided. [design-queries.md](design-queries.md) is the sketch to align on; nothing is built from it. Open and his: whether a concept is needed at all once a condition may be a line.

## 2026-11-18, Designing: a head, a body, and where a collection gets its rows

**Asked.** In the first sketch, which part is the parameters, which the answer and which the body. By voice the maintainer and the agent went through four forms: the answer in the head under `is:`, too big there; a marked form between head and body, which muddies conclusion and conditions; the shape as a head field, which hides the parameters; and the one he took: the head a concept's, the answer a variable under `is:`, and `(is ?x shape)` in the body. A view says `(view ?x template)` there.

**Learned.** Written out with its answers, the form as agreed does not give the empty case. The tags' read is a condition of the query that collects them, so a to-do with no tag has no `?tags`, and the card that calls it has no row. That is the design's own rule for a fold and his rule from this afternoon, and nobody said it on the call. Three ways out are in the note; the agent proposes that a collection carry its conditions.

`query` cannot be both the word for what is defined and the design's word for asking.

**Decided.** The form, his. Not decided: where a collection gets its rows, and the word for asking. Nothing is built.

## 2026-11-18, Designing: the read says it collects

**Asked.** Which of the ways to the empty list. The maintainer gave a fourth: mark the collected term on the read, `(todo/tag ?t tag: [?g])`, and refuse a query that has only such reads, since nothing would say which entities.

**Learned.** His is better than the agent's B on three counts: no clause with conditions inside it, a fold becomes a function of a list, and the refusal gives one meaning however the query is asked, which A lacked. It collects one read, so an item built from a join (a block with its source, a chip) is a query of its own whose field is collected: his "subview passed its parent". The brackets are taken, though: `tags: [?only]` is a list pattern in the build, tested. `all` is the design's word.

**Decided.** The maintainer's: the read says it collects, and a query needs a required condition. Not decided: the marker, proposed as `(all ?g)`; and the word for asking.

## 2026-11-18, Designing: collect

**Asked.** The marker. `all` does not say a list comes out. The agent offered `list`; the maintainer prefers `collect`, and likes a spread too.

**Learned.** A spread is the right idea with no free spelling: three dots after a term open an application, inside brackets they are a pattern over a stored list, and after a label they need the reader changed and are a template's own rest inside a rule about syntax. The notebook's page restated as queries is three where it was two views, and it shows whole for a notebook with no block, which the marker form did not.

**Decided.** The maintainer's: `(collect ?g)`. Not decided: the word for asking. Next to test: the table's views.

## 2026-11-18, Designing: an entry is one term, and the spread takes the whole

**Asked.** By voice: how Datomic and Datalogs aggregate; what stratifying folds in rules would cost; whether the marker could be a clause of its own; how a collection becomes a map and not a list. The agent went through `many` on two terms, which do not say how they pair, and `many` round a read, which the maintainer did not like.

**Learned.** His way out was one he had raised in the afternoon and the agent had let drop: a collection's field is one entry, key and value together, `{?k ?b}`. Then nothing needs pairing, the whole of it is a map, many plain values are a set and never a list, and the spread says "the whole" on both. The agent's table against the spread had judged it with key and value apart and outside brackets; inside them it is the pattern's own rest, already in the tests. What it costs is that on a stored record the same shape gates, and that a set has neither a spelling nor a value yet.

A separate clause for collecting needs inference or its own conditions, and inference has a case with no right answer: a variable read in two places.

**Backtracked.** `collect`, taken an hour before, to the spread.

**Decided.** The maintainer's: an entry is one term; a set, not a list. Proposed: `{... ?m}` and `#{... ?s}`. Not decided: the set's spelling and carrier, and the word for asking. Nothing is built.

## 2026-11-18, Designing: a set is a map with no names

**Asked.** What a set is, as a spelling and a value. The agent had written `#{`.

**Learned.** The maintainer: a set is a map with no names, `{: 1 : 2}`. So one spread, `{... ?x}`, takes the whole of a keyed collection and of many plain values alike, and the empty set and the empty map are one thing. The reader has the empty label and refuses it as a key today. His own doubt and its answer: `{... ?r}` is destructuring, which reads as taking; and destructuring a map into a map has no point, so on a stored record nobody writes it.

**Decided.** The maintainer's: the set's spelling. Proposed still: the spread for the whole. Nothing is built.

## 2026-11-18, Designing: a word after all

**Asked.** Whether `{... ?x}` reads as taking a value apart or putting rows together.

**Learned.** The maintainer does not find it obvious, and that is reason enough: they are different acts, and the notation's braces in a condition have only ever meant the first. The agent had argued position tells them apart; position tells taking from putting in a template, not one value from many rows in a condition. `collect`, dropped an hour before for promising a list and for not pairing keys, has neither fault now: it always makes a map, and an entry carries its key.

**Backtracked.** From the spread to `collect`, the second change of marker this evening.

**Decided.** The maintainer's: not the spread; a record stores a set as a list. The marker is `(collect ?x)`. Nothing is built.

## 2026-11-18, Designing: or a spelling of the variable

**Asked.** The maintainer, once `collect` was back: maybe `?x…`, `…?x`, `?..x` or `{?..x}`.

**Learned.** The reader takes anything after `?` as a variable's name, so two of these are plain variables today and say nothing of it. Three of the four sit one space from a form that means something else. `?..x` does not.

**Decided.** Not decided: `(collect ?x)` or `?..x`. The agent's hypothesis is that the word reads right unaided more often, which a reader round would settle.

**Decided, later the same evening.** The maintainer's: `(collect ?x)`. What stands of the sketch: the two layers, the head with `is:`, `is` and `view` in the body, an entry as one term, a set as a map with no names stored as a list, `collect` for the whole of a field, and the refusal of a query that only collects. What is open first: the word for asking, since `query` now names what is defined.

## 2026-11-18, Designing: select, and an index

**Asked.** The word for asking, now that `query` names what is defined.

**Learned.** The maintainer: `select`, or the head applied with nothing round it. The second goes against a rule the design wrote down on purpose, that every top-level form starts with its verb, and against the build, which refuses a call there. A command is why: `(todo/add list: … text: …)` bare could be asking or doing. `select` was read as well as `query` in R24 and R25, and unlike `query` it does not clash with running a document as a query.

Restating the table's views turned up what a directory is: a query whose head has no entity. It only collects, answers once, and stands empty. So the refusal is of a query of an entity that only collects.

**Decided.** The maintainer's: `select`. design.md on #728 still says `query` for asking, and still has the fold in a rule's head, the view form and two fields for a collection; none of it is changed yet.


## 2026-11-18, Building: queries, stage 1

**Asked.** Build what design-queries.md decided, `select` answering once on a branch: `query` and `is`, `collect`, the entry as one term, the set as a map with no names, and the refusals. Not views.

**Learned.** Every table of the note answers as written. The notation evaluates a query: its required conditions are one Dialog-DB query, each collecting read is one more, grouped in memory, and a call takes the called query's rows where its fields agree. A query of several reads read back through its head's entity crosses one entity's values, as the view plan found; a row named by `(id …)` of all its values does not. Two things in the way: the sketch says a field passed bound is a parameter, and a query here is answered whole and then narrowed, so a field only a caller gives is refused; and `notebook-views.clj`, left for stage 2, still writes `key: ?k block: ?b`, so a read written that way of an entry is read as the entry, while a concept head written that way is refused. The pinned numbers of the notebook did not move.

**Decided.** Not decided by the maintainer: the agent's defaults, `count` the one function, a query that calls itself refused, a set merging equal answers and printing in the order of its values, a stored set reading back as a list. Next: views as queries, stage 2.

## 2026-11-18, Building: views as queries, stage 2

**Asked.** Build views as queries: `(view ?x template)` in a query's body, what it collects put in place, maintained through subscriptions; remove the view form, its call, the marker rule and the read of two fields as an entry.

**Learned.** The layered maintainer needed little: a query's markup is one template once each query it collects is put in its place, and each query's required conditions are a layer. What the old plan worked out by rule, which element takes which condition, the query now says. Three things needed a rule of the agent's: what tells an answer from another (its head, a hypothesis), a count (a layer counted and not shown), and a layer that reads with `_` (it takes the conditions above it). An index stands as one container, where tonk's notebook directory was one a notebook.

**Decided.** The maintainer's, from the brief: the view form goes, and the views are restated as the note writes them. The agent's: the rules above, refusing a required call in markup, and taking the view forms out of `notebook.clj` and `table.clj` so the measures read the views' files.

## 2026-11-18, Building: what it costs is not known

**Asked.** The maintainer, on the stage 2 report: several of these sound serious, cost above all, and if it cannot be lightning fast it will not fly. On rows of one head: aggregation makes more sense to him than several rows, though he was not sure he had the issue.

**Learned.** Nothing about cost is measured. The report's account of it (a layer is read again when a change touches it, and a layer joined to it too) is a reading of the code. The hypothesis and its bars are in the ledger, written before any number: one change costs the same at 10, 100 and 1000.

**Decided.** Not decided. The agent's: measure before anything is made faster or built further on it.


## 2026-11-18, Measuring: what one change costs as the page grows

**Question.** Does the work one change causes stay in proportion to what it touches, the same among 10, 100 and 1000 to-dos (the ledger's hypothesis and bars)?

**Method.** `rust/dialog-notation/examples/cost.rs`, run as `cargo run -p dialog-notation --example cost -- 5` (5 runs, N = 10, 100, 1000 interleaved, the notation and the baseline alternated, a fresh branch each). One list of N open to-dos, two tags each. The card of the design note: rows with collected chips, the open ones counted. Reads are tree node reads through dialog-repository's `Interposing` env; time is from after the write lands to the patches. Dev profile: the workspace's crates at opt-level 1, dependencies at 3; nothing else built during the runs. Blob loads were 0 throughout.

**Results.** Medians of 5; spread in the harness's output, under 15% but for single outliers.

| measurement | N=10 | N=100 | N=1000 | ×time | ×nodes | bar |
|---|---|---|---|---|---|---|
| 1 first render, card | 13.1 ms, 491 nodes | 29.6 ms, 1767 | 232 ms, 16202 | 17.7 | 33.0 | refuted |
| 2 rename one to-do | 6.7 ms, 715, 1 patch | 16.4 ms, 2302 | 140 ms, 20332 | 21.0 | 28.4 | refuted |
| 3 tag added | 4.8 ms, 116, 1 patch | 14.7 ms, 144 | 145 ms, 166 | 30.0 | 1.4 | refuted on time; reads within |
| 4 to-do added | 4.9 ms, 511, 2 patches | 12.4 ms, 1803 | 111 ms, 16234 | 22.5 | 31.8 | refuted |
| 5 select one card | 10.0 ms | 86 ms | 57.1 s | 5759 | 0.5 | refuted |
| 5 select all cards | 2.7 ms | 79 ms | 57.8 s | 21484 | 0.5 | refuted |
| B rename, plain subscription | 0.85 ms, 17 | 0.45 ms, 21 | 0.68 ms, 19 | 0.8 | 1.1 | within |
| B tag added | 0.56 ms, 4 | 0.16 ms, 10 | 0.29 ms, 6 | 0.5 | 1.5 | within |
| B to-do added | 0.86 ms, 17 | 0.45 ms, 23 | 0.64 ms, 25 | 0.7 | 1.5 | within |
| B one title by entity | 0.40 ms, 13 | 0.42 ms, 13 | 0.47 ms, 13 | 1.2 | 1.0 | within |

**Where.**

| step | N=10 | N=1000 | what shows it |
|---|---|---|---|
| rename, layer 1 (the rows) poll | 3.4 ms, 507 nodes | 98.7 ms, 12207 | the row-identity rule |
| rename, layer 3 (the open ones counted) poll | 2.1 ms, 200 | 40.4 ms, 8111 | the same rule; the count's read keeps `title: _` |
| tag added, layer 2 (the chips) poll | 3.1 ms, 104 | 140.5 ms, 144 | the same rule, in CPU, not reads |
| to-do added, layers 1 and 3 | 2.4 and 1.4 ms | 67.2 and 41.5 ms | the same rule |
| to-do added, maintainer apply | 0.05 ms | 1.37 ms | the notation's own tree |
| select, answer in memory | 0.34 ms | 57.7 s | the whole-then-narrow and its joins |
| select, the queries | 2.4 ms | 127 ms | the required group's rows, through the same rule |
| the same reads as rules keyed by the to-do (B6 rename, B7 tag) | 0.95 and 1.9 ms, 17 and 82 nodes | 2.2 and 4.6 ms, 23 and 86 | what the layers would read without it |

- **The row-identity rule.** Every layer reports its row under `(:id view.layer c0: … )`, an entity made of the row's values, so a row's values stay together. The subscription maintains a rule per entity it touches. The changed fact's subject is a to-do, the rule's head is a row id, so the rule is not entity-local and the discovery joins the change sideways through the layer's premises: thousands of node reads for the rows and the count, and for the chips 140 ms of work with almost no reads. The same premises under a rule whose head is the to-do read 23 and 86 nodes at N=1000, flat.
- **The joined layers** are not in this card: no layer here reads with `_` without binding the list.
- **`select`'s whole-then-narrow.** The queries grow 53 times, and answering in memory grows 168,000 times: `Evaluation::whole` and `called` filter every row of a collected read for every row of the query, and `rows()` builds the read's rows again each time it is asked, deduplicating by `contains`. Asking one card answers every card first.
- **Not the notation's tree.** The maintainer's apply is under 1.4 ms at N=1000 in every case.

**Could not measure.** Rows read is node reads, not rows: counting the rows a scan visits needs a counter in dialog-query or dialog-repository, which this measurement does not change. Time is the dev build, not release.

**Decision.** Not decided. Nothing is made faster.

## 2026-11-18, Designing: rows or one answer, and what Datomic does

**Asked.** Stage 2 keyed a row of markup by its head, so two rows of one head showed as one. The agent proposed refusing such a query; the maintainer leaned to aggregating whatever is many, unasked, with a variable moved into the head to get rows. Then: compare them, suggest something for pairs, and take Datomic seriously, since each time he thought he had a better idea he regretted it. Then: does Datomic write `[?g ...]`, and `(distinct ?tags)` reads well to him.

**Learned.** Datomic does neither: rows by default, aggregates written in `:find`, trees from `pull`. Unasked aggregation loses which value goes with which when two of them are many, which is the maintainer's own question about pairing from the afternoon. Datomic pairs by collecting one value, a tuple; here the entry is that value. `[?a ...]` exists in Datomic, with both directions told apart by where it stands.

The measurement that came in after (next entry but one above) bears on this: what costs is a row keyed by something other than an entity, which is exactly what several rows of one head need.

**Decided.** Not decided: rows or one answer for a head; `distinct` for `collect`; a set always, with `merge` for a map.

## 2026-11-19, Designing: definitions on the branch, and a question asked twice

**Asked.** How concepts, macros and queries reach the branch. The maintainer: names are attributes pointing at a definition's entity, as in tonk and roughly as in Unison; starting with option A of [`design/macros/definitions-on-the-branch.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/definitions-on-the-branch.md) on `research-data` is fine. The agent then asked whether a stored form may refer to what it uses by name.

**Learned.** That was decided on 2026-11-12 (above: a macro resolves its names when it is defined) and the agent asked it again, having taken the research report's wording of option A, which keeps source forms, without checking the ledger. The maintainer: all references are resolved at compile time.

**Decided.** A, with two corrections to the report's wording: a definition of any kind is keyed by its content and the name points at it; and what is kept is the resolved definition, each reference holding the entity of what was named with the name beside it. Rests on no open hypothesis.

## 2026-11-19, Designing: resolved and inlined, corrected

**Learned.** The entry above says a kept reference holds the entity of what was named. That is not the plan. The maintainer: every reference is resolved and inlined at compile time, and the source map captures both the name it was referenced under and the entity it resolved to. It is what the entries of 2026-11-11 ("Forms are fully inlined") and 2026-11-12 (the drawing of as written, as kept, beside it) already say.

```
 as written    (todo ?t state: :done)
 as kept       this: ?t   xyz.todo/state: :done          what it meant, written out
 source map    todo -> concept:…, state: -> xyz.todo/state, where in the source
```

**Decided.** What reaches the branch for a definition is its compiled form, inlined, and a source map beside it. Nothing stored follows a name.


## 2026-11-19, Measuring: rows under their entity, and the fold under a subscription

**Question.** The ledger's second hypothesis: with rows reported under the entity they are about and `select` pushed into its reads, does one change cost the same at 10 and 1000? And what does the engine's `Fold` cost under a subscription?

**Method.** `cargo run -p dialog-notation --example cost -- 5` on `wip/engine-fold` at `144ddf8`, dev profile. Output ([`design/macros/rounds/cost-2026-11-19.txt`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/rounds/cost-2026-11-19.txt) on `research-data`).

| one change | N=10 | N=100 | N=1000 | growth | plain at 1000 | times plain | bar |
|---|---|---|---|---|---|---|---|
| rename | 2.5 ms | 1.1 | 4.5 | 1.8 | 0.73 ms | 6.1 | between |
| tag added | 2.6 | 1.4 | 4.3 | 1.7 | 0.24 | 18 | between |
| to-do added | 2.4 | 1.3 | 5.4 | 2.2 | 0.61 | 8.8 | between |
| `select` one card | 3.7 | 3.7 | 4.4 | 1.2 | | | confirmed |
| tags folded by the engine, tag added (B8) | 2.5 | 11.7 | 138 | 55 | 0.24 | 575 | grows with every group |

**Learned.** The 140 ms was the row's made-up entity; keyed by the entity it is 4.5 ms. Several rows of one head are not slow when carried as one record under their entity, so cost does not decide rows against one answer for a head. The fold is the cost that remains, and it now has a number. Reads are flat in the three changes; time is not, from 100 to 1000.

**Decided.** Not decided. A release build and N=10000 are needed before the remaining growth is called anything.

## 2026-11-19, Delimiting again: what views as queries are for

**Asked.** What the maintainer saw in tonk that made him want views kept current by the engine: cost, parts out of step, authoring, or two engines. He: all of them, and a view written as a query fits better than a custom element at each step. He had hoped views could go as deep as needed with the engine's changes keeping them current, and the first prototype fell short.

**Learned.** He did not rank the four, and a statement cannot be four things. The agent's reading is that they share a cause: a view is asked for piecewise. If so, "a view is one query" is the remedy and what it needs of the engine is #837: a query that nests and folds, kept at the cost of the change. He called a view a terminal deductive rule; decided earlier is that a query is not a rule and no rule reads it. The agent takes "terminal" as the same thing said loosely, and records it so it can be corrected.

**Backtracked.** From Building to Delimiting for views: the prototype was built with no problem written.

**Decided.** Not decided. The statement is put to him.

## 2026-11-20, Designing: many definitions under a name, told apart by hash for now

**Asked.** What had been agreed about names. The maintainer: tonk's `name` is of cardinality one. An agent pointed out a merge could then point a name elsewhere unseen, and he wants overloading, so one referent is not kept. Petnames make most sense to him: the name I gave, else the name someone I name uses. That is too much to build now, so a `#hash` qualifier tells several apart in the meantime.

**Learned.** "By authority" in the entry of 2026-11-11 was petnames, not yet named so. The overnight choice of a name's entity from its signature with one definition is overruled ([`design/macros/decisions-overnight.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/decisions-overnight.md) on `research-data`, row 4). The worry about a merge is half answered already: what is compiled holds what its names meant, so a merge that adds a definition under a name is met only at the next compile.

**Decided.** By the maintainer: many definitions under one name; a signature tells them apart; where it does not, `#hash`, until petnames. Not decided: the qualifier's spelling, and what an unqualified reference that fits several does.

## 2026-11-20, Designing, backtracking: whose a bare name is

**Asked.** Nothing; the maintainer came back to it. An hour after deciding that a bare name is mine alone, he thought it should be the repository's, the consensus of the space, with mine written `~/todo` or `@/todo`; then doubted the `@` prefix, which runs into `todo/model`; and saw that if a bare name is the writer's, source cannot be copied between people, which an editor might mend and an agent might not.

**Learned.** Three positions in one day: resolved over every namespace and refused when several fit; mine alone; the space's. What moved him each time was a different criterion: a merge repointing a name unseen, then meaning what I wrote, then text that means the same to whoever reads it. The first is already softened, since nothing compiled follows a name. The third is the one the notation's own method cares most about, agents writing it.

**Backtracked.** From built to Designing, for what a bare name means. The build's other parts stand: content entities, source maps, bindings, the walk.

**Decided.** Not decided. The agent's hypothesis: a bare name is the space's and "mine" is not in the language; a person's namespace is a name in the space like any other. Spellings by a reader round.

## 2026-11-24, A paginated view has to stay expressible

**Asked.** The maintainer, after talking with Chris about virtualized lists: no hurry to port sapling's pagination, but our views must not make it impossible to say.

**Learned.** sapling pages by segments, ranges of an order key, each a live query, with layout kept in the front end and the next bound found by a read of the greatest keys below a key. A segment is a query with two parameters in our notation, and the window is session facts a collecting query reads, which is the maintainer's model and Chris's at once.

**Decided.** Five things a view must keep true are written in [paginated-views.md](paginated-views.md). Two are facts today, two are hypotheses about the engine (a bound from a join reaching the scan, many small live queries being cheap), one is a read main does not have. Nothing is built.

## 2026-11-24, Removing and inserting an element is not always the same as changing it

**The maintainer:** naively removing an element and inserting one may not work well, above all for text inputs, where focus and selection are finicky. React goes to some length there, and so did dominion (gozala/dominion), which also used logic inspired by edit distance to avoid needless changes to the document.

**Fact.** `Maintainer::apply` takes the rows that left and arrived of every layer as one change and looks at the touched instances once, so a row replaced within one change is a text or an attribute set on the same instance, not an instance removed and inserted. The patches are `Insert`, `Remove`, `SetText`, `SetAttribute` and what the list below it holds; none moves an instance.

**Open, for the renderer and the patches, not decided:**

- An instance whose place in its key order changes is a `Remove` and an `Insert` today. A renderer has to move the same node, or focus, selection, scroll and a playing video are lost. Either a `Move` patch, or the renderer pairing a remove and an insert of one key within a change.
- An instance whose key changes is another instance. So a key must not hold what a person edits in place.
- An input a person is typing in: its value arrives back as a patch after a round trip, later than keys typed since. Setting it moves the caret and can undo what was typed. The renderer must not write a value the element already holds, and has to know which change a value answers.
- Changes should reach the renderer as one batch for one commit, so it can do the least to the document, as dominion did by edit distance and others do by the longest run already in order.

## 2026-11-24, Building: the notation on the engine as it lands

**Asked.** By the coordinating session for the maintainer: bring #773 onto the engine on main, quickly and with no new design.

**Learned.** The branch's engine was an older history of #778 with #799 on it, both since rewritten, so it was moved onto `feat/patterns` by taking its engine files whole. Against it the notation broke in one place, the composite's API, and #827 broke it at run time only. A query's row of several columns is concluded as one record by a pattern (decision 15), and a branch refuses to store such a rule until format 5: asked once it runs, kept current it does not. 14 of 31 view layers in the sketches and tests are such rows, and so is every layer nested two levels deep.

**Interpreted, for the maintainer to correct.**

- A line with no `cardinality:` is read as `many`, which is what `all` did before.
- A variant's cases, `as: [:open :done]`, are always the attribute's listed values, so its identity holds them; before, they were dropped.
- An elector is taken in a concept's head and on a line's value, `is: (top ?s)`, both of which the note of 2026-11-20 named.
- A write and a rule's conclusion go by the attribute and carry no elector; only a read's field does.

**Decided.** By the coordinating session: format 5 is ported onto #778 as `feat/rule-format-5` and merged here when ready, and the measurements wait for it; `Aggregator::Distinct` stays, apart from the notation's commits.

## 2026-11-24, Measuring: one change at a thousand and ten thousand, on the engine

**Question.** The ledger's second hypothesis (claims.md, "the cost measured is of two choices in the build"), on the engine as it lands, release build, and at ten thousand.

**Method.** `cargo run --release -p dialog-notation --example cost -- 5 10 1000 10000`, with format 5 (`feat/rule-format-5` at `618233f`) merged locally, since the card's rows need it. Output ([`design/macros/rounds/cost-2026-11-24.txt`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/rounds/cost-2026-11-24.txt) on `research-data`). The 2026-11-19 run was the dev build, so its column is not the same build.

| one change | N=10 | N=1000 | N=10000 | growth to 1000 | growth to 10000 | times plain at 1000 | at 10000 | 2026-11-19, 1000 | bar |
|---|---|---|---|---|---|---|---|---|---|
| rename | 1.6 ms | 3.3 | 56 | 2.1 | 35 | 8.3 | 61 | 4.5 | between at 1000; refuted at 10000 |
| tag added | 1.9 | 1.3 | 5.1 | 0.7 | 2.6 | 8.2 | 7.1 | 4.3 | between |
| to-do added | 1.6 | 3.7 | 62 | 2.3 | 39 | 11.2 | 65 | 5.4 | between at 1000; refuted at 10000 |
| `select` one card | 1.9 | 2.1 | 2.8 | 1.1 | 1.5 | | | 4.4 | confirmed |
| tags folded by the engine (B8) | 0.86 | 0.48 | 1.2 | 0.6 | 1.4 | 1.5 of its inner subscriptions | 1.7 | 138 | within |

Node reads stay between 46 and 100 for every change, and no layer is evaluated whole again.

**Where.** Driving the card's four layers by hand on the same seed, at ten thousand: the four polls take 3 to 5 ms together, each maintained, and the two whose cover grows with the page (10011 and 20010 ranges) take 1.2 to 1.6 ms each. `Maintainer::apply` takes 16 to 24 ms, against 0.4 to 0.6 ms at a thousand: it is the notation's own tree that does not scale. For a tag added, the chips' poll is the cost, 9 to 11 ms.

**Learned.** The engine's fold does not grow with the page and costs 1.5 to 1.7 times its own inner subscriptions. No change through the notation meets the bar the maintainer was offered (not growing, within 2 times plain), and at ten thousand the cost is the notation's own.

**Decided.** Not decided. Nothing is made faster.

## 2026-11-24, Building: what one change cost the notation, made to cost what it touches

**Asked.** By the coordinating session, on the measurement above: fix what is the notation's, report what is the engine's.

**Learned.**

- `Maintainer::apply` counted a counted place and found an inserted instance's index by collecting every instance under the parent again, on every change. With each parent's instances kept in key order, one changed row costs 0.05 to 0.11 ms at a thousand and at ten thousand, where it cost 0.4 to 0.6 and 16 to 24 ms.
- A view's count was folded by the selection's fold, which also builds the distinct set of the group's values. A count reads none of it; folded alone, a view needs no `distinct`.
- What is left is the engine's: a fold's subscription folds a touched group again over all its kept rows, 28 to 34 ms for the one list of ten thousand. A running count for a group would remove it (decision 2 of [`design/macros/decisions-overnight.md`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/decisions-overnight.md) on `research-data`).
- The cover of a row layer holds a range of the entity index for each to-do and attribute read, 10011 and 20010 at ten thousand, where one range of the attribute index would do. The engine's, reported on 2026-11-19.

| one change, through `Live::watch` | N=1000 before | after | N=10000 before | after |
|---|---|---|---|---|
| rename | 3.3 ms | 2.9 | 56 | 33 |
| to-do added | 3.7 | 3.0 | 62 | 28 |
| tag added | 1.3 | 1.5 | 5.1 | 5.0 |

Output ([`design/macros/rounds/cost-2026-11-24-after.txt`](https://github.com/dialog-db/dialog-db/blob/research-data/design/macros/rounds/cost-2026-11-24-after.txt) on `research-data`).

**Decided.** Not decided: whether a view's count stays on the fold while the fold refolds the group, or is counted from the layer's rows, which now costs one lookup.

## 2026-11-24, Archived: the exploration, so what lands is what main needs

**Asked.** The maintainer: no Python or other exploratory code reaches main.

**Done.** Moved to the `research-data` branch at the paths they had here at `79979af`, as research artifacts are kept off `main` (the maintainer, 2026-11-24): the Python model (`model/`), the rounds and cost dumps (`rounds/`), the design's sketches (`sketches/`), the crate's examples (the cost harness, the notebook bar, the shapes count, the notebook runner), and the status reports, plans and comparisons: `alignment.md`, `composite-next.md`, `composites-compared.md`, `decisions-overnight.md`, `definitions-on-the-branch.md`, `definitions-plan.md`, `poc.md`, `slotting-cardinality.md`, `synthesis.md`, `tonk.md`, `tonk-claims-diff.md`, `tonk-dialog-move.md`, `tonk-integration.md`, `tonk-notebook-bar.md`, `tonk-notebook-runs.md`, `tonk-port.md`, `where-things-stand.md`. A file this journal or the ledger names and this tree does not hold is on `research-data`, at the path named; the history is on `archive/notation-poc-2026-11-24`.

The maintainer's decisions those files held and nothing else did are rows of the ledger now, each with its date: what the notation is for first, tonk's events, elements and routes, a query's definition synced, the fold landed without `distinct`, a name's entity and the canonical form hashed, formats 4 and 5 and the listed `as`, and the one composite. Also his, 2026-11-19, and not a design matter: commits are authored by the environment's identity. The grouping of what `Unsupported` refuses moved to [gaps.md](gaps.md).

The sketches of tonk's notebook and table, and the two to-do documents, stay as the crate's test fixtures, under `rust/dialog-notation/tests/fixtures/`, read by tests that run them. The two tests that counted how far the sketches get are gone with the bar that counted.

## 2026-11-24, Measuring: one change, with a fold that keeps a running value

**Question.** With the running value of a standing fold (`perf/fold-running-value` at `63957c3`, on main as #856) merged, a fold whose entries are `count`, `count-distinct`, `sum`, `min` or `max` keeps a running value for each group. Is one change to a view through `Live::watch` now flat from a thousand to ten thousand?

**Method.** The cost harness of [`rust/dialog-notation/examples/cost.rs`](https://github.com/dialog-db/dialog-db/blob/research-data/rust/dialog-notation/examples/cost.rs) on `research-data`, run on this branch untracked, release build, medians of 5, the changes in its order: a rename, a tag added, a to-do added. A view's count folds `count-distinct` alone, so it runs; `distinct` keeps no running value.

| one change | N=1000 before | after | N=10000 before | after | plain at 10000 |
|---|---|---|---|---|---|
| rename | 2.4 ms | 2.4 | 35 | 25 | 0.95 |
| tag added | 1.2 | 1.6 | 4.5 | 5.2 | 0.77 |
| to-do added | 2.4 | 1.1 | 25 | 4.6 | 0.89 |
| `select` one card | 2.1 | 2.3 | 2.5 | 3.1 | |

**Learned.** A to-do added is flat now: the count's fold steps its running value, 1.8 ms at ten thousand where it folded the list's one group again for 28 to 34 ms. A rename is not, and the cause is the harness's order and the running value's start: a group's running value is made by folding it whole the first time a row of it moves, and the rename is the first change the list's group sees after the first render. The fold's subscription says so: `refolds` rises on the rename, `steps` on the to-do added after it. Every later change to the group steps. What is left of a change at ten thousand is the four layers' polls, 3 to 5 ms in all, about 5 times a plain subscription.

**Decided.** Not decided: whether a group's running value is made when the group is first folded, so its first change steps too.

## 2026-11-25, Decided: a name of words joined by `/` is never a stored attribute

**Found.** The second independent check at `ff19a1fed`: with no concept `foo`, `(foo/x ?x ?n)` was read as `(foo ?x x: ?n)`, a relation named by the bare word `foo`. A head on it said "?x applies no relation", a premise "(foo ?x x: ?n) is not a kernel premise", and under a word of the notation, `select/x`, `rule/x`, `unless/x`, the rewrite was that form, `(rule ?x x: ?n)`. The two-term line sugar asked the reader's `Kind::of`, which calls any name with a `/` an attribute, and not `syntax::named`, which tells a dotted attribute from a concept's name.

**Decided.** By the coordinator, (a) of two: the sugar is a stored attribute's only, its domain dotted. An undotted `word/field` under no concept of its name, nor one of its first part, is refused naming what is missing, in a head, a premise, `unless` and a select alike. A consequence: a read above the macro use that declares its concept was refused, saying to declare the concept above, because its first try read the stored relation; now its first try reads nothing, and it is expanded again once the use declares the concept, as a read above a `(concept …)` already was. Both orders answer the same. Still refused: a derived attribute of a declared concept read before a macro's use declares a concept of that name.

## 2026-11-25, Decided: a derived attribute is one value

**Reversed.** The entry at line 1263 above: "with labels, `(list/count ?l status: ?s count: (count ?t))`, it is the fields the labels name". Decided by the coordinating session under the maintainer's instruction to proceed without blocking on him, and listed for his check-in. A derived attribute's head and its reads take the entity and one value, unlabeled or under `is:`; any other label is refused at the head and at a read, in documents and in trees built by hand, with the rewrite shown: a rule that concludes a domain, `(rule (xyz.list.count ?l status: ?s n: ?n) …)`, read through a concept, `(concept (list/count ?l status: ?s n: ?n) (xyz.list.count ?l status: ?s n: ?n))`.

**Why.** Three reasons, each run: the labelled form's own example cannot compile, since a fold in a rule that derives is refused by Dialog-DB and a fold in one that writes by the notation; no fixture and no tonk form uses a label; and the form took what says nothing, a head that writes `is:` beside another label and a read of a label no rule concludes, which answered nothing.

**What brings it back.** Folds admitted in rules that derive, so that a derivation of several fields at once, a count beside the status it counts, says something a domain and a concept cannot. Reversing this is one step: let `derived` in `src/expand.rs` and `read` in `src/kernel.rs` take the labels again (the check is `syntax::one_value`), and turn `it_refuses_a_derived_attribute_with_a_label_other_than_is` back into a test of what they answer.

## 2026-11-25, Decided: the marker is distinct

**Found.** The record of 2026-11-18 conflicts. The ledger row, `claims.md` 440: "the marker of a read that collects is `(distinct ?x)`, Datomic's word, in place of `collect`; it always gives a set, of values or of whole entries | opinion | Irakli, 2026-11-18: "I like (distinct ?tags) it makes sense", and of the set with a map made from it, "I like it" | decides; ours does not gate and Datomic's does. Not built: the build says `collect`". The journal, the same day: "Designing: a word after all": "The marker is `(collect ?x)`. Nothing is built."; "Designing: or a spelling of the variable": "**Decided, later the same evening.** The maintainer's: `(collect ?x)`."; and "Designing: rows or one answer, and what Datomic does", where he says `(distinct ?tags)` reads well: "**Decided.** Not decided: rows or one answer for a head; `distinct` for `collect`; a set always, with `merge` for a map." Gaps row 14, of 2026-11-24, says "decided and not built". Which came last is not in the files.

**Asked.** The maintainer, today, of count and collect: "Wait I thought we had no collect and copied Datomic here. So count is on distinct entries correct?"

**Decided.** By the coordinating session, on that reading, under the maintainer's instruction to proceed without blocking on him: the marker is `(distinct ?x)`, and `(collect ?x)` is refused as a former word, saying to write `(distinct ?x)`. The analyzed tree still names the gathering `collect`, a word of the notation's own. Reversing it is one step: `select::DISTINCT` back to the marker `collect` in `collects_in` and `collected`, `collected_as_before` in `src/expand.rs` removed, and the documents' `(distinct ?x)` written `(collect ?x)` again.

**Confirmed.** By the maintainer, 2026-11-25: "We should also use distinct not collect, walkthrough even did that". The marker is `(distinct ?x)`, and the one-step reversal above is not taken.

## 2026-11-25, Reversed: a derived attribute concludes its labels

**Asked.** The maintainer, of the entry above, "a derived attribute is one value": "Rule without labels is just sugar for (rule (list/label of: ?l is: ?n) ...). I think it also ok to have other labeled parameters."

**Decided.** By the maintainer, the same day, reversing the coordinating session's decision of that morning: a derived attribute's head and reads carry labels again, each the field it names, and its two terms unlabeled are sugar for `of:` and `is:`, `(list/label ?l ?n)` being `(list/label of: ?l is: ?n)`, which is now read so. What did not hang on the reversed decision stays: `in:` and a layer on a derived attribute are refused, its declared labels are spelled by the name rule and held to the room a relation leaves, a line of a concept or a command naming a derived attribute is refused, and so is a name that is both a concept and a derived attribute. Pinned by `it_concludes_and_reads_a_derived_attribute_with_labels` (`tests/notation.rs`) and the expansion test in `src/expand.rs`.

## 2026-11-25, Decided: a document writes no wrapper

**Asked.** The maintainer, of gaps row 56, why the wrappers are back, `(:integer ?n)` and `(:one …)` written where nothing else says a type or a cardinality.

**Decided.** By the maintainer: "I think we should remove it and potentially add formula per type or something like that. More forms add things to keep in mind/context." A document writes no wrapper. A type, a pick, a cardinality, a listed type's cases or a layer written in a document, anywhere it stands, is refused as `Error::Refused`, naming the line to write in its place. The walker still writes them for a line and the kernel still reads them; a kernel tree built through the Rust API is no document and compiles with them.

**Built.** Every position row 56 listed has a line spelling: four already worked, and two were added, `as:` a map of a label to its type on a derived attribute's labelled fields, `as: {:status :text}`, and an elector on a derived attribute's value, `is: (max ?n)`, as on a line. A sixth position turned up while building, a listed type on a derived attribute, which the kernel's `(:among …)` said and nothing else did: `as: [:open :done]` on a derived attribute, as on a line, is added too. What a document gives up is one form for a write of several fields of one domain under `:one`: it is a line a form, or a concept.

**Open.** "A formula per type" is asked of the maintainer and not built.

