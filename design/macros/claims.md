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
| the core forms expand and run as written | fact | `design/macros/model`, `python3 -I model.py . todo.clj` and `checks.py` | holds in the model; nothing is built in dialog |
| what runs after expansion costs what it costs today | hypothesis | a perf scenario over a document expanded cold and warm, with its bar set first | open; cold expansion is not measured |
| nothing in dialog may depend on conduct or on a wasm runtime | opinion | Irakli, 2026-11-10 | decides |
| the notation should be minimal and defined in itself, as McCarthy's evaluator is | opinion | Irakli, 2026-11-10 | - |
| macros as rules is the direction, without a problem statement | opinion | Irakli, 2026-11-10 | decides |
| whatever is built is as fast to use as the system is today | opinion | Irakli, 2026-11-11 | decides |
