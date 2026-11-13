# The three phases, and what passes between them

**Decision:** what the parser, the analyzer and the compiler each take and give, so each can be asked for apart and later stand behind a formula or a resolver. **For:** every phase reachable from the query engine (maintainer, 2026-11-12), and an editor that asks what a name meant.

Decided by the maintainer: there are three phases and a command over them; the analyzer may be a resolver; a macro resolves its names when it is defined. The shapes below are the agent's, built in `rust/dialog-notation` and open to change.

```
 text ──parse──▶ syntax tree ──analyze──▶ analyzed tree ──compile──▶ what Dialog-DB runs
                 as written,              every form a rule          statements, rules,
                 names unresolved         of the kernel, names       queries
                                          resolved, and a table
                                          of what each name meant
```

| phase | takes | gives | reads the database |
|---|---|---|---|
| `parse` | text | a list of trees: applications, variables, constants, references, structs, tuples | no |
| `analyze` | trees, and the definitions in force | the analyzed trees, each a rule of the kernel; the name table: each name written and what it resolved to, or that it did not; diagnostics | yes: the definitions. Today they are held by the analyzer in memory |
| `compile` | an analyzed tree | steps: assert, retract, issue, deduce, induce, query | no |

Named by the maintainer: what analysis gives is the **analyzed tree**. What is the same in every phase is the tree. The notation as parsed (`Form`) is the parser's own and what a tree is printed as; nothing after `parse` reads it.

## Stages

Each ends with the tests passing.

1. **The boundary.** `parse`, `Analyzer::analyze` and `compile` take and give trees. Done. Inside, expanding and compiling still go through the notation as parsed.
2. **The compiler reads the tree.** `kernel.rs` and `compile.rs`.
3. **The analyzer reads the tree.** Patterns and templates match and fill trees; the walker walks trees.
4. **The name table.** Names are resolved as a step of their own, and the analysis gives the table back.

## Not in these stages

- A template that holds the entity of what it names, with the name beside it (decided 2026-11-12): needs a definition to have an entity, and a group that refers to itself named as one unit.
- Where in the text a node came from. A node moves when a template is filled, so it cannot be a table by path beside the tree.
- A kept form's relations to what it references: needs syntax kept on the branch, which waits on the composite value (#778, #748).
- The phases behind a formula and a resolver in `dialog-query`, and `(dialog/evaluate (source "…"))`.
- A rule without `when`: the maintainer's lean, waiting on what several heads are (#788).
- An application lowered to a query of the definition's concept, in place of the reads it stands for.
