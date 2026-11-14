# Macros as rules: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | The kernel and the core forms run on a real branch through `rust/dialog-notation` ([poc.md](poc.md)); the prelude is rules only. The syntax tree is built, apart from the notation as parsed, and kept as data: maps that say their `type` ([design-forms.md](design-forms.md#the-syntax-tree)). A struct is a value and, with a variable in it, a pattern, which is a topic of its own with an agent on it (#777). Rules of several heads are #776. Expanding and compiling still read the notation as parsed, and `kernel.rs` still copies dialog's forms. | **Designing** again from 2026-11-12, for the three phases ([design-pipeline.md](design-pipeline.md)); how the phases stand behind the query engine is still Directing. Direction decided by the maintainer: phases as formulas and a resolver, one rule over them. A proof of concept runs. A name is kept as a name, what is named and a signature; a definition under it is read `last`; an application resolves by the labels written and a read that fits several is of all. Built and tested, by pattern, in the expander. |
| **Agenda** | **One at a time, in the order of the queue at the end of this journal** (the maintainer, 2026-11-13: the threads were more than he could follow). The rest of this cell is what the queue was drawn from. To ask: whether descriptions are `dialog.meta/description`, which Dialog-DB refuses from a transaction today, or stay under the notation's own `syntax.meta`. Which format a stored rule that holds a pattern is stamped (#779), which is what stands between the notation and structs in rules that write. What `[ ]` says, a list or a tuple: it is a tuple today, a list has no spelling, and the choice is dear to change once values are stored. Next: a list and a list with a run, which dialog has and the notation does not write, once `[ ]` is decided; a reported row in the order asked; printing from the analyzed tree. Parked by the maintainer: authority, for two definitions of one signature written apart; `last` elects one until then. How a query reports. Decided 2026-11-13: a rule has one head and `(all …)` is not written; what that does to #776 and to several heads in #779 is the maintainer's to say. Whether a template says several things with `(all …)` or a spread. Whether a command takes fields beside what it is about, and a slash in its name. The prelude written without `when`. Parked by the maintainer: `update`, and with it what it does with a dependent that does not fit or arrives by sync. Whether the name as written is part of what names a macro. Whether the analyzer, a resolver by the maintainer's word, gives back the table of what each name meant. Whether dialog can be asked for an expansion a form at a time (H3). What a read with no pick reads. | The notation is on top of patterns (#778, merged in, its perf gate failing): a struct is a value in a write and a pattern in a query of one read. Next: a struct in a rule, which needs the compiler to build dialog's forms in Rust, since a pattern has no wire form; `entity/derive` over a record; then an application lowers to a `ConceptQuery` of the definition's concept. Not waiting: a helper rule for a negated conjunction. Waiting on patterns (#777, #778): a struct as a term, `entity/derive` over a record, syntax kept on the branch. |

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

**Asked.** Irakli: use haiku readers to find out whether `when` is needed at all, or whether premises as plain clauses suffice. Run as #728's rounds were. Two rounds, ten readers each, five a spelling: [K1](rounds/k01.md), told how a rule is written; [K2](rounds/k02.md), shown three rules and told nothing.

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

## The queue

In order. One is asked, answered and written down before the next.

1. To ask: whether, of the rules that apply to a form, the ones that name the most win (the agent's, built to make two rules of the prelude one answer). Built 2026-11-13: the reader that gives `...` no meaning, the quoted list, the record's refusals, a read that says every field or `...`, a struct's last key.
1. Decided 2026-11-13: the new format always, the old read for one cycle. To build next: format 4 on #778, with the migration.
2. Whether a rule that leaves a premise's subject unnamed fires only for the entity written (#789, #791). It changes what a commit writes on main.
3. Answered 2026-11-13 by the quote: `'(:open :done)`, `'()`.
4. Whether descriptions are `dialog.meta/description` or stay under `syntax.meta`.
5. Review of #777 and the open questions of #778.
6. The agent's choices standing until said otherwise: the blank-line rule for comments; a refused document keeps no definition; a rest stands after a struct's entries; `(all …)` or a spread in a template.
7. Housekeeping: #776 and the several-heads part of #779 after one head a rule; the Perf gate on #788; the sweep output under `/home/claude/heads/target`; the branch `wip/patterns-step-2`.
