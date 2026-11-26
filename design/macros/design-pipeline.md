# The three phases, and what passes between them

**Decision:** what the parser, the analyzer and the compiler each take and give, so each can be asked for apart and later stand behind a formula or a resolver. **For:** every phase reachable from the query engine (maintainer, 2026-11-12), and an editor that asks what a name meant.

Decided by the maintainer: there are three phases and a command over them; the analyzer may be a resolver; a macro resolves its names when it is defined. The shapes below are the agent's, built in `rust/dialog-notation` and open to change.

```
 text ──parse──▶ syntax tree ──analyze──▶ analyzed tree ──compile──▶ what Dialog-DB runs
                 as written,              every form a rule          statements, rules,
                 names unresolved         of the primitive forms, names       queries
                                          resolved, and a table
                                          of what each name meant
```

| phase | takes | gives | reads the database |
|---|---|---|---|
| `parse` | text | a list of trees: applications, variables, constants, references, structs, tuples | no |
| `analyze` | trees, and the definitions in force | the analyzed trees, each a rule of the primitive forms; the name table: each name written and what it resolved to, or that it did not; diagnostics | yes: the definitions. Today they are held by the analyzer in memory |
| `compile` | an analyzed tree | steps: assert, retract, issue, deduce, induce, query | no |

Named by the maintainer: what analysis gives is the **analyzed tree**. What is the same in every phase is the tree. The notation as parsed (`Form`) is the parser's own and what a tree is printed as; nothing after `parse` reads it.

## Stages

Each ends with the tests passing.

1. **The boundary.** `parse`, `Analyzer::analyze` and `compile` take and give trees. Done.
2. **The compiler reads the tree.** `primitive.rs` and `compile.rs`. Done: neither names the notation as parsed, and a reported row is a tree.
3. **The analyzer reads the tree.** Patterns and templates match and fill trees; the walker walks trees. Done: nothing after `parse` names the notation as parsed.
4. **What was written, beside what was said.** Each phase gives its result and a mapping of what the result left out, and a printer takes both back to the source. See below.
   - Done, the parser's part: `parse_written` gives the trees and `Written`, the mapping; `print(trees, written)` gives the text, the same bytes for the prelude and both examples. A warning says the way to its form and an analyzed tree says which tree it came of, so the notebook says `5:1: warning: …` and where a refusal by the compiler is.
   - Done, diagnostics: the analyzer does not stop at a refusal. It goes to the end of the document and gives every refusal and every warning as a diagnostic, each with a severity and the way to the form it is about, for a language server to show. A document with a refusal in it is refused whole: nothing of it comes back analyzed and none of its definitions is kept.
   - Done, the other two phases: the parser goes on past a refusal and says each with its span; `check(analyzer, text)` runs the three phases and gives what the text compiles to and every diagnostic with a span. A later phase does not run on what an earlier one refused.
   - Not done: a refusal by the compiler is placed at the tree it came of and no closer; a reported row is still printed by label and not in the order asked; printing from the analyzed tree and from the compiled form.
   - Done, descriptions: `description:` is out of `concept`. What the comments about a form say is the `description:` of each analyzed rule that came of it. Dialog-DB keeps none of it, so nothing can be found by it yet.
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

## Every phase as a query

**Decision:** how a notebook cell parses a text, gets its diagnostics and gets completions with ordinary `select`s, and what dialog-query needs for that. **For:** the maintainer, 2026-11-26: "I also want us to manifest LSP we talked about through query engine … it would not be a separate thing but rather leverage regular queries utilizing formulas and resolvers to do it." It is his decision of 2026-11-12 (claims: "every phase is available to the query engine"), decided and not built. **Not decided:** the matrix below, for the maintainer.

### What the code is today

Read at `87e56fdc0`; facts unless marked.

- Formulas are a closed set: `define_formulas!` in `dialog-query/src/formula/query.rs` lists every one, and `FormulaQuery` is an enum of them. `dialog-notation` depends on `dialog-query`, so it cannot add one without a change below it.
- A formula yields any number of rows for one row in: `Formula::compute` returns a `Vec` (`formula.rs`). A diagnostic a row, a completion a row, needs nothing new of the shape.
- A pure formula may keep its outputs: `#[formula(cached)]` keeps them by its input values in a cache the environment holds (`formula/cache.rs`).
- A resolver, by its admission rule, selects only from content-addressed storage (`resolver.rs`): what is read from the branch head is a scan, which records demand. So nothing that reads the definitions in force is a resolver; it is a scan, and a formula fed by one.
- The definitions a document's forms are checked against are held by `Analyzer`, in memory, across the documents it has checked (`expand.rs`). The driver installs compiled rules and keeps no concept, query or macro on the branch (`apply.rs`). The 2026-11-12 blocker, "the analyzer reads definitions from memory, not the branch", holds.
- The parser and the analyzer (`form.rs`, `syntax.rs`, `written.rs`, `expand.rs`) import nothing from `dialog-query`; the analyzer calls the crate's own `compile` module (`write_refusal`, `DESCRIBED`), which does. Diagnostics are `Diagnostic { severity, message, path, span, refusal }`, a span's column counted in characters (`written.rs`); an editor speaking LSP counts UTF-16 units by default.

### How a cell writes it

Proposed spellings, the agent's; the names are open.

```clj
; (a) parse: a row a form, its tree a composite value a pattern can read
(select (cell/form of: ?f at: ?n tree: ?tree)
  (xyz.cell/source of: cell:1 is: ?src cardinality: :one)
  (notation/parse text: ?src at: ?n tree: ?tree form: ?f))

; (b) diagnostics: a row a diagnostic
(select (cell/diagnostic of: cell:1 from-line: ?fl from-column: ?fc
                         to-line: ?tl to-column: ?tc severity: ?s code: ?k message: ?m)
  (xyz.cell/source of: cell:1 is: ?src cardinality: :one)
  (notation/check text: ?src from-line: ?fl from-column: ?fc to-line: ?tl
                  to-column: ?tc severity: ?s code: ?k message: ?m))

; (c) completions at a place: what the text expects there, from the formula,
;     and the candidates the branch holds, joined
(select (cell/completion label: ?l kind: ?k detail: ?d)
  (xyz.cell/source of: cell:1 is: ?src cardinality: :one)
  (notation/complete text: ?src line: 3 column: 7 label: ?l kind: ?k detail: ?d))
```

What each reads. (a) is the text alone. (b) and (c) are the text against the prelude alone, as the formula is pure: refusals of the text as written (names, wrappers, shapes, a read's `...`, a view's markup) and the words of the notation. What a document checks against the branch, the concepts, queries and macros it uses, is not in the text and is not a formula's to read. Where it is needed, an unknown concept's diagnostic or a concept's labels as completions, the formula yields what the text uses at a place (`notation/use`: a name, a kind, a span) and the query joins it with a scan of the definitions on the branch: unknown is `unless` a definition holds; candidates are the definitions that do. That needs the definitions on the branch, which the driver does not write today.

### Approaches

| Decision: how `notation/*` reaches the query engine, for: a notebook cell parses, checks and completes with `select`s |
|---|---|---|---|---|
| criterion \ approach | A. Status quo: `check(analyzer, text)` from Rust, beside the queries | B. An extension point: dialog-query evaluates a formula an embedder registers by name, in queries only | C. Below the engine: the parser and the analyzer move into a crate dialog-query depends on, their formulas built in | D. The notation's own driver evaluates `notation/*` premises and lays their rows for the engine, as it lays a write's rule (`TransactionQuery::with_rule`) |
| a cell asks with a `select`, a view and a subscription alike | 🔴 a separate API | 🟢 a formula like any other; `Live` re-asks it | 🟢 | 🟡 a `select` and a view through the notation's driver; an engine query or subscription that did not come from the notation cannot |
| a replica or a rule that does not have the notation | | 🟢 a stored rule holding one is refused, so no replica holds a rule it cannot run | 🟢 every replica has them | 🟡 only the notation's driver runs them |
| what changes in dialog-query | none | 🟡 a `FormulaQuery` variant evaluated through a registry the embedder fills, with each formula's cells known when a query is planned | 🔴 a new crate below the engine, and the analyzer's calls into `compile` cut first | none |
| sound: rows are a function of the inputs, demand recorded for what is read from the branch | | 🟢 pure formulas; branch reads stay scans in the same query | 🟢 | 🟢 |
| wasm, and the engine with no notation | | ? a registry filled at start in a browser worker: not tried | 🟢 | 🟢 |
| cost of a keystroke | each cell's `check` is the caller's | 🟢 the changed cell's formula runs again; the others hit the formula cache | 🟢 the same | 🟡 the driver computes again what it laid, a cell at a time |

Recommendation, the agent's: **B**, queries only, the formulas pure. It is the smallest change that makes `notation/*` an ordinary formula wherever a query runs; C is B with a crate split it does not need; D answers only what comes through the notation's driver, which is not "not a separate thing". The branch-dependent half (definitions on the branch, joined) is a later step either way.

**Corrected, 2026-11-26.** B as written above rested on a claim the engine contradicts: that an embedder's formula can stand in any query and be refused only in a stored rule. In dialog-query every query of several premises is a rule (`Application` is implemented for a concept, attribute queries, a fold and a resolver only), and even a rule laid over one query only is written as facts and read back from its body (`TransactionQuery::with_rule` asserts it into the query's changes). So a query's rule and a stored rule are one mechanism. Two ways to make B sound were put to the coordinator: B1, bodies that name an embedder's formula, with the registry passed to every place a query reads a rule back and a refusal at commit; B2, a query of premises that is no rule, an embedder's formula allowed there alone and refused at rule compile.

**Decided, 2026-11-26.** B2, by the coordinating session on the agent's recommendation: the refusal sits at rule compile, where no stored or laid rule slips past; no registry is threaded through rule hydration, where a missed site would be a silent no-rule; nothing about the parser's behaviour is frozen into what replicas store. Built in dialog-query as `Extension`, `Extensions` (a value the embedder owns, no global) and `PremiseQuery`, on its own pull request. **What B2 gives up:** a `notation/*` formula can never stand in a rule, so a command a rule answers, the maintainer's `(dialog/evaluate (source "…"))`, cannot reach the notation this way. That would take a formula every replica has, with its behaviour versioned in what a replica stores: the door is described here, not built, and not closed. A select or a view over a premise query is evaluated again in full when what it reads changes, not maintained incrementally.

### Rows

Proposed, to be checked against what tonk's plugin consumes (forwarded by the coordinator):

- **diagnostic:** `from-line`, `from-column`, `to-line`, `to-column` (from zero), `severity` (`:error`, `:warning`), `code` (a stable word for the kind, `refused`, `reserved`, `unknown`, ...; today `Error`'s variant), `message`. Columns in characters, as `Span` counts them; if the plugin speaks LSP's default, UTF-16, the formula converts, and the rows say which.
- **completion:** `label`, `kind` (`:concept`, `:field`, `:form`, `:type`, `:attribute`), `detail` (a concept's head, a form's shape), and the span it replaces.
- **form:** `at` (its place among the forms), `tree` (the tree, `Syntax::encode`, a composite value), and its span.

### A keystroke

A cell's text is a fact (`xyz.cell/source`) or a constant in the query. A keystroke changes it, so the query is asked again: under a subscription only the row of that cell's text changes, and the formula runs again on that text alone, parse and analysis whole, as `check` does today; there is no incremental parser. Every other cell's text is unchanged, so its formula is a cache hit when the formula is marked cached. What a keystroke costs is one `check` of one cell; how long that is for a cell of tonk's size is not measured (hypothesis: under a frame for a cell of 50 lines, release build, the prelude analyzed once and kept, not per call; `Analyzer::new` analyzes the prelude every time today).

### What a query left out

Since #889 a query's `perform` returns `Answered`, its rows and a `Report` of the rules its derivation reached and did not evaluate here (`Entry::RuleSkipped { rule, concludes, reason }`), filled as the query runs. The notation drains every `perform` with `.try_vec()` (`live.rs`, `apply.rs`), which reads the rows and drops the report. Surfacing it, in this order, none of it built (agreed with the coordinator, 2026-11-26):

1. **A write.** The read inside a write (`stage` in `apply.rs`) is a transaction's `perform`, also `Answered`. A write whose read left a rule out writes fewer statements than the document says, and nothing says so. It takes `.try_report()` in place of `.try_vec()`, the entries carried on the step's `Did`, and `DocumentReport` gathering them. **Open, the maintainer's:** whether such a write is refused, nothing of the document applied, or applied and reported. "A write does what it says" argues for refusing; the notation does not decide it.
2. **A one-shot answer** (`answer`, `Live::select`, a view's layers). One form runs several `perform`s, its query, one per fold, one per layer of a view, so the entries are gathered per form and deduplicated by rule. They are said in the author's terms: `concludes` is the `on:` entity a rule is indexed under, mapped back to the concept or attribute name the form read, a mapping the compiler has when it lowers the form and keeps nowhere today; `rule` is an entity, shown by its `dialog.rule/source`. Where they go is a change to `Answer` (or `Applied`); cheaper once diagnostics are rows, where a rule left out is a warning at the form's span.
3. **Live.** `Branch::subscribe` returns a `Subscription` with no report. `Live` could report only what a separate first `perform` left out, and a rule that compiles later (after `upgrade_rules`, under a newer release) would stay reported, as a `Report` only grows. A subscription's report per evaluation, one that can say a rule is no longer left out, is a change to the engine, and `Live` does not stand one in.

What reaches a left-out rule from a document: not the rules the notation lays itself (`layer.with`), but a rule the branch holds that this replica does not compile, a newer release's formula or another embedder's. No notation test produces one yet; the first would be a branch holding a rule of a formula this release does not know, read by a document.

### Not in this step

An HTTP endpoint, the LSP wire protocol, `(dialog/evaluate …)`, the definitions on the branch, and completions (after parse and diagnostics).
