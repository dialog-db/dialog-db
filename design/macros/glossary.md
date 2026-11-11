# Macros as rules: glossary

- **Form.** A piece of notation as a value: an atom, or a call with a head, one unlabelled argument and labelled ones. It is never evaluated by being read.
- **Surface form.** A form a person or an agent writes: `concept`, `query`, `view`, a command around a concept. Until 2026-11-10 this topic called these "primitive forms", which is the word that broke: a form defined by a macro is not primitive.
- **Kernel form.** A form dialog accepts with no expansion: `rule` with `when` and `unless`, an application of a relation, an application of a formula, and `assert!`, `retract!`, `assert` and `report` as a rule's head.
- **Application.** A head applied to parts: one part with no label, the others labelled. The engine's own word. Not "call".
- **Read.** An application of a relation: one entity and the fields named, together.
- **Hole.** A variable in a pattern or a template. What it took is kept in one value, apart from the rule's variables.
- **Walker.** The native loop that rewrites a form until it is all kernel, asking the rules at each step.
- **Macro.** What gives a surface form its meaning in terms of other forms. Whether it is a rule, or data a native expander applies, is a column in Direction.
- **Compile.** To turn a kernel form into what dialog stores and runs: a descriptor, its identity and its index facts. In this conversation "compiler" has also meant the whole path from text; here it means only this last step.
- **Name.** A fact that associates a word with the hash of a definition. Not part of the definition.
