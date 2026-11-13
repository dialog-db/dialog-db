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

1. **The boundary.** `parse`, `Analyzer::analyze` and `compile` take and give trees. Done.
2. **The compiler reads the tree.** `kernel.rs` and `compile.rs`. Done: neither names the notation as parsed, and a reported row is a tree.
3. **The analyzer reads the tree.** Patterns and templates match and fill trees; the walker walks trees. Done: nothing after `parse` names the notation as parsed.
4. **What was written, beside what was said.** Each phase gives its result and a mapping of what the result left out, and a printer takes both back to the source. See below.
   - Done, the parser's part: `parse_written` gives the trees and `Written`, the mapping; `print(trees, written)` gives the text, the same bytes for the prelude and both examples. A warning says the way to its form and an analyzed tree says which tree it came of, so the notebook says `5:1: warning: …` and where a refusal by the compiler is.
   - Done, diagnostics: the analyzer does not stop at a refusal. It goes to the end of the document and gives every refusal and every warning as a diagnostic, each with a severity and the way to the form it is about, for a language server to show. A document with a refusal in it is refused whole: nothing of it comes back analyzed and none of its definitions is kept.
   - Done, the other two phases: the parser goes on past a refusal and says each with its span; `check(analyzer, text)` runs the three phases and gives what the text compiles to and every diagnostic with a span. A later phase does not run on what an earlier one refused.
   - Not done: a refusal by the compiler is placed at the tree it came of and no closer; a reported row is still printed by label and not in the order asked; `description:` is still in `concept`; printing from the analyzed tree and from the compiled form.
5. **A pinned reference and the name table.** A resolved reference is the name and the entity it meant where it was written.

## The mapping, and printing back

Decided by the maintainer, 2026-11-12: there is a form that takes the analyzed tree and gives the source, and the test is that it prints the original; the same from the compiled form. The compiled form carries none of the mapping. The mapping does not hold the source: that is cheating. It holds what was named what, which invocation needs anyway, and the order things were written in, so they are easy to arrange back.

```
 text ──parse──▶ syntax tree   + written   each label's place in the order written, each
                                           name and number as spelled, where each node is
      ──analyze─▶ analyzed tree + origin    which definition was applied at which node, and
                                           what each name resolved to
      ──compile─▶ compiled form + sites     which analyzed node each rule, premise and
                                           variable came of

 print(syntax tree, written)                    says the text
 print(analyzed tree, origin, written)          says the text
 print(compiled form, sites, origin, written)   says the text
```

A macro call is not in the analyzed tree: after `(query (todo ?t text: ?x))` only `?t` and `?x` are left. It does not have to be held either. A definition is a pattern and a template, and both run both ways: matching the template against the analyzed node gives back what the pattern's variables took, and filling the pattern with them gives back the call. So `origin` holds which definition was applied where, and beside it only what the template threw away: a part the pattern bound and the template did not use.

| lost at | what | held as |
|---|---|---|
| parse | the order of labelled parts | a permutation a node |
| parse | `of:` written or left out | where `of:` is, in the node's order |
| parse | a number's digits, a text's escapes, a struct's key written as a label or not | the token as spelled, a node, only where a printer would spell it otherwise |
| parse | where a node is in the text | a range a node |
| parse | a comment | its text, on the node it is about. Decided by the maintainer: comments matter more than anything else here, to document source and to find things by their documentation. A comment is not derived from anything, as a name is not, so holding it is not holding the source |
| parse | space between tokens | not held, and not needed: it is what lies between where one token ends and the next starts. So the test is the same bytes. Two things are lost: space at the end of a line, and a tab, which is written as a space |
| analyze | the call a definition was applied to | the definition, by entity, at the node; what its template dropped |
| analyze | the name a reference was written as | the name, beside the entity |
| analyze | the names a template's own variables were given | derived from the site, as now |
| compile | which rule, premise and variable is which node | a path a compiled part |

Built as the maintainer decided it:

```
 ; about the concept                      above a form: about the form below
 (concept (todo ?t
            ; about text                  above a label: about the part under it
            text: ?x  ; about text too    ends a line a form ends on: about that form
            list: ?l)                     
   (xyz.todo ?t text: ?x list: ?l))  ; about the concept too
```

A comment after a bracket that opens or after a label ends no form, so it is about what is below. A comment no form is below and none ends before, before a closing bracket or at the end of the text, is kept on what it is inside.

Proposed by the agent, not decided: this table; that order is held a node and not once a definition, since a call may write its labels in another order than the definition does; that a comment with a blank line under it is about the document; that what a comment says of a concept, a rule or a labelled part is what Dialog-DB keeps as its description, and is kept as a fact so it can be found. Decided by the maintainer: a comment is metadata on the form below it and takes no place among the parts, and `description:` does not belong in a concept.

## Not in these stages

- A template that holds the entity of what it names, with the name beside it (decided 2026-11-12): needs a definition to have an entity, and a group that refers to itself named as one unit.
- A kept form's relations to what it references: needs syntax kept on the branch, which waits on the composite value (#778, #748).
- The phases behind a formula and a resolver in `dialog-query`, and `(dialog/evaluate (source "…"))`.
- An application lowered to a query of the definition's concept, in place of the reads it stands for.
