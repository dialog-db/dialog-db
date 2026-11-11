# Macros as rules: claims

| claim | kind | backing | status |
|---|---|---|---|
| tonk-notation parses `when:` and `unless:` as premise lists, and the analyzer lifts a `rule!` body's fields into a rule mutation | fact | tonk `rust/tonk-notation/README.md` | holds |
| a rule's identity is the hash of its canonical spelling: variables renamed by colour refinement and individualisation, premises sorted | fact | `rust/dialog-query/src/rule/canonical.rs` | holds |
| a rule is stored as `dialog.rule/*` facts with its body as bytes; an entry whose bytes do not hash to its entity is ignored | fact | `rust/dialog-repository/src/rules.rs`, `notes/inductive-rules.md` | holds |
| concepts and names are not stored; a query carries descriptors inline and the engine never sees a name | fact | `research/notation/implementation.md` on #728 | holds |
| a deductive rule refuses a `reduce` clause; only a query, a subscription and an inductive rule fold | fact | `TypeError::ReduceInOpenRule`, `rust/dialog-query/src/rule.rs` | holds; `notes/aggregation.md` and the design on #728 say otherwise |
| a formula takes bound inputs and answers zero, one or many rows, and is registered in Rust | fact | `notes/formula.md` | holds |
| the 2026-05-16 macros expand to triples; #728's macros store a definition as one value under its hash | fact | tonk journal 2026-05-16; `research/notation/macros.md` on #728 | holds |
| a form costs a release, and that is what is in the way | hypothesis | confirmed if the maintainer names a form that waited on a dialog or tonk release, or one a space wanted and could not add; refuted if every form added so far was wanted by dialog itself | open |
| a definition's meaning being in code keeps agents from writing valid forms | hypothesis | confirmed if an agent given only a space's stored definitions writes valid forms less often than one given the guide (H2 of `research/notation/macros.md`); a research topic | open |
| a stored form's meaning moves when a name is re-pointed | hypothesis | confirmed by one tonk rule or view whose behaviour changed after an anchor was re-pointed; refuted if every stored form already pins what it names | open |
| two builds read one document two ways | hypothesis | confirmed by one stored definition that two released analyzers lower differently | open |
| **H1.** Rules turn surface forms into kernel forms, and native code turns a kernel form into a descriptor | hypothesis | the prelude in `model/prelude.clj` defines `query`, top-level writes, `concept` and commands as rules ending in kernel forms | confirmed in the model for those forms; `compile` is not modelled |
| **H2.** Resolving a name is an expansion: a rule joins the name with the fact that points it at a hash, and what is stored holds no names | hypothesis | install a rule that names a concept, rename the concept, and find the rule's identity and answers unchanged and its printed form showing the new name. Refuted if two definitions under one name cannot be told apart without asking the author | open |
| **H3.** A definition's identity is its compiled descriptor's, and the surface form is a fact beside it | hypothesis | two spellings of one concept, with variables renamed and fields reordered, give one entity; a replica that holds no macros runs a rule it pulled. Refuted if printing a definition back needs something neither the descriptor nor the surface form holds | open |
| **H4.** A set-collecting `reduce` is all a macro needs to build one form from several rows | hypothesis | - | refuted: deductive rules refuse `reduce`, and no core form needed rows assembled, since a stored pattern and template carry the per-field information in one value |
| **H5.** One rule reports every refused form | hypothesis | issue a misspelled `concept` and get a problem naming the form and the shapes its macro takes | open; in the model a refused form is found by the walker, with the wrong reason (kernel.md, finding 6) |
| the store has two write instructions, `Assert(fact, pick)` and `Retract(fact)` | fact | `rust/dialog-corpus/src/model/instruction.rs` | holds |
| a rule's kind is its descriptor's head field: `deduce`, `assert!` or `retract!` | fact | `rust/dialog-query/src/rule/inductive/descriptor.rs`, `rule/statement.rs` | holds |
| installing a rule is asserting it, and a rule installed in a commit fires in that commit | fact | `rust/dialog-query/src/rule/statement.rs`, `rust/dialog-repository/src/repository/branch/transaction/induce.rs` | holds |
| `unless` negates one proposition, and an application that leaves a field out still requires it | fact | `rust/dialog-query/src/negation.rs`, `concept/query.rs` | holds |
| a recursive group of rules is evaluated bottom-up in full; a non-recursive rule gets the caller's bindings | fact | `rust/dialog-query/src/concept/query/fixpoint.rs`, `adornment.rs` | holds |
| a formula has fixed cells and no environment; a resolver gets the evaluation scope and may read content-addressed storage only | fact | `rust/dialog-query/src/formula.rs`, `resolver.rs` | holds |
| a query can run at one revision | fact | `Snapshot`, `rust/dialog-repository/src/repository/snapshot.rs` | holds |
| the core forms expand and run as written | fact | `design/macros/model`; `rust/dialog-notation/tests/notation.rs` on a real branch | holds, except a rule that issues a command |
| the kernel compiles to descriptors Dialog-DB already has | fact | `rust/dialog-notation/src/compile.rs`, [poc.md](poc.md) | holds for reads, writes, the three rule kinds and queries; four refusals listed |
| a rule about syntax can be a stored rule Dialog-DB evaluates | hypothesis | H1 to H3 of [design-forms.md](design-forms.md) | open; the crate holds syntax in memory |
| a rule can name an entity it did not read | fact | `entity/derive`, `rust/dialog-query/src/formula/entity.rs`; `it_lets_a_rule_issue_a_command_another_rule_reads` | holds |
| a negation requires every variable in it bound, a blank excepted | fact | `Negation::schema`, `rust/dialog-query/src/negation.rs` | holds |
| two claims one round wrote to a `last` field elect by the hash of the value, whatever order they were written in | fact | CHANGELOG, #752; `it_elects_one_of_two_writes_of_one_round_whatever_their_order` | holds |
| a record is taken apart by a formula today | fact | `dialog/revision`, `rust/dialog-query/src/formula/revision.rs` | holds, for one record type with fixed fields |
| a form is a record as #748 has it, and `entity/derive` takes a record | opinion | Irakli, 2026-11-11 | decides |
| a record's key may be any value, as a Clojure map's | opinion | Irakli, 2026-11-11 | conflicts with #748, where a field is named by an attribute and may hold several values; not reconciled |
| conduct's records take any value as a key | hypothesis | `notes/language.md` and `DECISIONS.md` in Gozala/conduct | refuted for its record and dictionary; holds for `conduct.map` |
| a map of any keys has an identity in dialog today | fact | `Type::map`, `rust/merkle-reference/src/lib.rs` | holds |
| a record is a constrained map, its keys attributes | opinion | Irakli, 2026-11-11 | decides: one compound value, the record a case of it |
| an attribute of no domain is a `Relation` today, and a value a branch stores | fact | `Relation::try_from`; `it_holds_a_symbol_with_no_domain_as_a_value_and_not_as_an_attribute` | holds; `Relation::split` refuses it |
| a claim under an attribute of no domain is refused | fact | the same test | holds where the descriptor is read; not checked below the query layer |
| every key of a form can be an attribute | hypothesis | confirmed by `prelude.clj` written as records under positions and labels of no domain | open; needs one position for the nth part |
| what is stored is versioned, and named by merkle reference so a layout does not move a name | opinion | Irakli, 2026-11-11 | decides |
| every form of the prelude and the examples is data: it lays out as bytes and reads back | fact | `it_lays_out_every_form_of_the_prelude_and_the_examples`, `rust/dialog-notation/src/data.rs` | holds; H1 confirmed with variables as symbols |
| a form's name does not depend on its layout | fact | `Form::reference` is made of its parts' references; `it_names_apart_from_the_layout` | holds for the form's own reference |
| a record value's name does not depend on its layout | hypothesis | `Value::reference`, `rust/dialog-corpus/src/identity.rs` | refuted today: `RECORD.reference_of(bytes)` |
| a list and a map are referenced under the generic types, and `( )` is a list | opinion | the agent, 2026-11-11, on Irakli's tuple | open until something real is stored; every name follows from it |
| `(todo ?t state: :open)` is sugar for `(todo {of: ?t state: :open})` | opinion | Irakli, 2026-11-11 | decides; built as the syntax's reading, `Form::bare` |
| the reader can fold the primary into the record | hypothesis | the count in `examples/shapes.rs` | refuted: four heads stand with one unlabelled part and with several |
| one call reads the same however its primary is written | fact | `it_reads_a_call_the_same_however_its_primary_is_written` | holds |
| a branch keeps a form as a record under its `form:` entity | fact | `it_keeps_a_form_on_a_branch_under_the_entity_that_names_it` | holds |
| every application has at most one part without a label | hypothesis | `examples/shapes.rs` over the prelude and the examples | refuted: 38 of 151 as written have several; holds for calls |
| the part with no label is `of:`, and `:` is the label of no name | opinion | Irakli, 2026-11-11; `research/notation/design.md` in #728 | decides; the prelude's command row breaks it |
| a record is an atomic composite value: updated whole, made of other values | opinion | Irakli, 2026-11-11 | decides; a map, a list and a tuple are each one |
| parsing transforms notation into a syntax tree, which is kept in maps, lists and tuples as each fits | opinion | Irakli, 2026-11-11 | decides; the crate keeps parsed notation as data directly, which is not this |
| the syntax tree of what dialog runs exists in Rust, as maps, named by reference and versioned | fact | `Premise`, `Proposition`, `Parameters`, `Term` in `rust/dialog-query/src`; `rule/canonical.rs`; `FORMAT` in `rule/body.rs` | holds |
| a rule's identity ignores the order of its premises and the names of its variables | fact | `rust/dialog-query/src/rule/canonical.rs` | holds |
| a rule can build a record from what it has bound | hypothesis | H5 of [design-forms.md](design-forms.md): confirmed by a record premise planned as a concept query is, running both ways | open; #748 speaks only of reading one |
| a premise may take parameters that differ by application | fact | `ConceptQuery::schema`, `rust/dialog-query/src/concept/query.rs` | holds |
| the two-cell `entity/derive` names the entities a record will | hypothesis | refuted by its own definition: a chain of pairs under a seed is not a record's canonical form | refuted; it is a stand-in |
| a query reports the relations queried, and a write says every field | opinion | Irakli, 2026-11-11 | decides |
| a rule and the command it reads can share a document | fact | `it_fires_a_rule_in_the_document_that_installs_it` | holds |
| what runs after expansion costs what it costs today | hypothesis | a perf scenario over a document expanded cold and warm, with its bar set first | open; cold expansion is not measured |
| nothing in dialog may depend on conduct or on a wasm runtime | opinion | Irakli, 2026-11-10 | decides |
| the notation should be minimal and defined in itself, as McCarthy's evaluator is | opinion | Irakli, 2026-11-10 | - |
| macros as rules is the direction, without a problem statement | opinion | Irakli, 2026-11-10 | decides |
| whatever is built is as fast to use as the system is today | opinion | Irakli, 2026-11-11 | decides |
