# Definitions on the branch: as built, the count, and the plan

Read from the code at `bf4e157` by a thread of its own, 2026-11-19; nothing was run. Decided by the maintainer and not reopened: every kind is stored, keyed by its content, a name points at the definition's entity, every reference is resolved and inlined when compiled, a source map beside it holds the name written and the entity it resolved to.

## As built

| kind | written to the branch | bound |
|---|---|---|
| concept | nothing; a `Definition` in `Expander.syntax` (`expand.rs` 1059-1069) | never applies another concept |
| rule | `dialog.rule/*` under `rule:<hash>`, its comment in `syntax.meta/description` | carries each concept's attributes written out: resolved |
| query, view | nothing; kept as written in `Expander.queries` (`expand/query.rs` 287) | 🔴 late: each `select` reads its callees by name (`query.rs` 590-629), so a callee defined again changes what its caller answers |
| macro | nothing; in memory | 🔴 late: its output is expanded again against the table as it is at each use (`expand.rs` 1325) |

Nothing is loaded from a branch (`analyze.rs` 9-11).

## Definitions that refer to each other: none

Counted in the prelude, the model and every sketch and example. None refers to itself and no two refer to each other. A rule refers to a concept's attributes and never to the rules that conclude it, so recursion through a concept is through data and is no cycle of definitions. A query that calls itself is refused, and a callee must be defined first (`query.rs` 286-293, 370-375). The hypothesis in the ledger is refuted for these files. tonk's library was not counted. One latent case: a field's `conforms` holds a whole concept inline, so a concept conforming to itself could not be written; the notation emits no `conforms`.

## Plan

```
 name entity ── syntax.name/is "todo/card"
     └─ syntax.name/definition ──▶ definition:<hash of the compiled form>
                                      ├ syntax.definition/is    the compiled form, inlined
                                      └ syntax.definition/kind  concept | query | macro
 source:<ref> ── syntax.source/definition ──▶ the definition
                 syntax.source/form        the form as written
                 syntax.source/references  [{name, entity, path}]
```

Corrected by the maintainer, 2026-11-20:

- The form hashed is canonical: conditions and anything else whose order does not matter are put in the order the rule's canonical labelling gives, and variables named by it. Two spellings of one definition are one entity.
- A name's entity is made of the name alone and points at every definition standing under it. A definition holds its signature, and an application is resolved by the labels it writes. What an application that fits several does is open: by authority and showing all to start (the ledger), or qualified by hash as in Unison (his recollection).

The name and the description are not part of what is hashed.

1. A compiled form and an entity for each kind (`src/definition.rs`). A concept: its labels with attribute, type, cardinality, layer, key. A macro: its resolved rule. A query: its analyzed tree without a call's constants.
2. Queries resolved when defined: `define` keeps the analyzed tree with callees inlined, `reach` reads trees and not names.
3. Macros resolved when defined: the template expanded to a fixpoint with the pattern's variables as holes.
4. `Step::Define`: the facts above written for each declaring form; a rule writes only its source map.
5. Loading: `Analyzer::load` takes decoded definitions; the crate gives the queries to run on open, so it need not depend on dialog-repository.
6. Tests, on the filesystem provider and the volatile one: a fresh analyzer on the same branch answers; a name bound again leaves a stored dependent's entity and answers; two branches defining one thing get one entity; a pull brings a definition the analyzer then sees.

Not known: whether every macro in the sketches expands to a fixpoint with holes, which step 3 assumes.
