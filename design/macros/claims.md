# Macros as rules: claims

| claim | kind | backing | status |
|---|---|---|---|
| tonk-notation parses `when:` and `unless:` as premise lists, and the analyzer lifts a `rule!` body's fields into a rule mutation | fact | tonk `rust/tonk-notation/README.md` | holds |
| a rule's identity is the hash of its canonical spelling: variables renamed by colour refinement and individualisation, premises sorted | fact | `rust/dialog-query/src/rule/canonical.rs` | holds |
| a rule is stored as `dialog.rule/*` facts with its body as bytes; an entry whose bytes do not hash to its entity is ignored | fact | `rust/dialog-repository/src/rules.rs`, `notes/inductive-rules.md` | holds |
| concepts and names are not stored; a query carries descriptors inline and the engine never sees a name | fact | `research/notation/implementation.md` on #728 | holds |
| a deductive rule can fold rows with a `reduce` clause | fact | `notes/aggregation.md` | holds |
| a formula takes bound inputs and answers zero, one or many rows, and is registered in Rust | fact | `notes/formula.md` | holds |
| the 2026-05-16 macros expand to triples; #728's macros store a definition as one value under its hash | fact | tonk journal 2026-05-16; `research/notation/macros.md` on #728 | holds |
| a form costs a release, and that is what is in the way | hypothesis | confirmed if the maintainer names a form that waited on a dialog or tonk release, or one a space wanted and could not add; refuted if every form added so far was wanted by dialog itself | open |
| a definition's meaning being in code keeps agents from writing valid forms | hypothesis | confirmed if an agent given only a space's stored definitions writes valid forms less often than one given the guide (H2 of `research/notation/macros.md`); a research topic | open |
| a stored form's meaning moves when a name is re-pointed | hypothesis | confirmed by one tonk rule or view whose behaviour changed after an anchor was re-pointed; refuted if every stored form already pins what it names | open |
| two builds read one document two ways | hypothesis | confirmed by one stored definition that two released analyzers lower differently | open |
| **H1.** The compiler splits at the kernel form: rules turn surface forms into kernel forms, and one built-in formula turns a kernel form into the descriptor dialog stores | hypothesis | write `concept` and a rule with several heads as rules that end in the formula. Refuted if a surface form needs something of the descriptor no kernel form can spell, or if a macro needs what only the compiler knows, such as an inferred type | open |
| **H2.** Resolving a name is an expansion: a rule joins the name with the fact that points it at a hash, and what is stored holds no names | hypothesis | install a rule that names a concept, rename the concept, and find the rule's identity and answers unchanged and its printed form showing the new name. Refuted if two definitions under one name cannot be told apart without asking the author | open |
| **H3.** A definition's identity is its compiled descriptor's, and the surface form is a fact beside it | hypothesis | two spellings of one concept, with variables renamed and fields reordered, give one entity; a replica that holds no macros runs a rule it pulled. Refuted if printing a definition back needs something neither the descriptor nor the surface form holds | open |
| **H4.** A set-collecting `reduce` is all a macro needs to build one form from several rows, because labelled arguments and rule bodies are sets | hypothesis | expand a concept of N fields into one rule. Refuted by a template that needs an order, such as a variant's ranked cases or a text template | open |
| **H5.** One rule reports every refused form: a form that was issued and that no macro and no kernel compile accepted | hypothesis | issue a misspelled `concept` and get a problem naming the form and the shapes its macro takes. Refuted if the report cannot say which part did not match | open |
| nothing in dialog may depend on conduct or on a wasm runtime | opinion | Irakli, 2026-11-10 | decides |
| the notation should be minimal and defined in itself, as McCarthy's evaluator is | opinion | Irakli, 2026-11-10 | - |
