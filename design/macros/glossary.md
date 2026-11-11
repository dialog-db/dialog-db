# Macros as rules: glossary

- **Form.** A piece of notation as a value: an atom, or a call with a head, one unlabelled argument and labelled ones. It is never evaluated by being read.
- **Surface form.** A form a person or an agent writes: `concept`, `query`, `view`, a command around a concept. Until 2026-11-10 this topic called these "primitive forms", which is the word that broke: a form defined by a macro is not primitive.
- **Kernel form.** A form dialog accepts with no expansion. Which forms these are is not decided; `rule` over attribute calls, constraints and formulas is the candidate.
- **Macro.** What gives a surface form its meaning in terms of other forms. Whether it is a rule, or data a native expander applies, is a column in Direction.
- **Compile.** To turn a kernel form into what dialog stores and runs: a descriptor, its identity and its index facts. In this conversation "compiler" has also meant the whole path from text; here it means only this last step.
- **Name.** A fact that associates a word with the hash of a definition. Not part of the definition.
