## What

An embedder can give the engine a formula of its own and ask it in a query.

- `dialog_query::formula::Extension`: a pure function of its input cells, declared as a derived formula's cells are, giving any number of rows for a row in.
- `Extensions`: the extensions an embedder holds, by name. It is a value the embedder owns, not a registry: a premise built through `Extensions::apply` carries the extension it was built of, so two embedders, or two tests, never share or race over one.
- `FormulaQuery::Extension`: the premise. It has no serialized form.
- `dialog_query::query::PremiseQuery`: a query of premises, planned as a rule's body is and asked with no rule. Each row (`PremiseRow`) is the entity `this` binds and the values of the variables it selects. `select` and `subscribe` take it as any query.

## Where an extension may stand, and why

Only in a premise query. A rule holding one is refused when it is compiled.

A rule is stored and read back from its body, and even a rule laid over one query only is written as facts (`TransactionQuery::with_rule` asserts it into the query's changes) and read back from those. A body cannot say an embedder's code, so a rule holding one would run only where that embedder is, or not at all. Keeping extensions out of every rule makes that one check at rule compile, where no stored or laid rule can slip past, and freezes nothing about the embedder's code into what a replica stores.

What this gives up: a rule, and so a command a rule answers (the notation's `(dialog/evaluate (source "…"))`), cannot ask an extension. That would take a formula every replica has, with its behaviour versioned in what is stored. This pull request does not close that door; it does not open it either.

## Refusals, each before any evaluation

| what | refused as | when |
|---|---|---|
| a premise naming an extension the `Extensions` does not hold | `TypeError::UnknownFormula` | `Extensions::apply` |
| a query or rule written as data naming an extension | a deserialization error | reading it |
| a deductive rule, built or compiled from its descriptor, or an inductive rule, holding an extension | `TypeError::EmbedderFormula` (from `AnalysisError::EmbedderFormula`) | rule compile |
| a premise query selecting a variable no premise binds | `TypeError::UnselectedVariable` | `PremiseQuery::new` |

An extension's own faults fail the query, naming the extension and the cell, and are never a row that did not match. A row is held to the extension's declaration (`Extension::outputs`, its cells' types) before it is written:

| what the extension's row does | fails as |
|---|---|
| gives a value for a cell it does not declare as an output | `EvaluationError::UndeclaredOutput` |
| leaves out an output it declares | `EvaluationError::MissingOutput` |
| gives a value its cell's declared type does not admit | `EvaluationError::MistypedOutput` |

Only a well-typed value that disagrees with what the row already binds (`Bindings::write`'s `Conflict`) is a non-match. `Bindings::write` reports every failure to bind as `Conflict`, which is why the declaration is checked first, apart from it. A value read from the row that an input cell does not admit stays a non-match, as for every formula. The built-in formulas write fields typed in Rust, so a mistyped output cannot arise from them; their swallowing of `Conflict`, `Absent` and `TypeMismatch` in `emit` and `expand_with` is left as it is.

## Subscriptions

A subscription over a premise query is never restricted to one entity (`Restriction::Unsupported`): when what its premises read changes, it evaluates the query again in full. Pinned by a commit that changes a text and the subscription retracting the old row and asserting the new.

## Tests

`rust/dialog-repository/src/repository/branch/extension_tests.rs`, written first (the first commit, failing):

- the refusals above, each asserting its variant with `matches!`;
- a rule holding an extension, refused as deductive, from a descriptor and as inductive, with no `dialog.rule/source` on the branch after, on the volatile and the filesystem providers;
- a premise query over a branch's facts with a one-row and a many-row extension;
- a subscription kept current through a change;
- an extension whose row mistypes an output, gives an undeclared cell or leaves an output out failing the query with the variant naming it (a commit of its own, failing, before the fix).

On wasm: the nine tests that are not filesystem-only pass in headless Chrome (`cargo test --target wasm32-unknown-unknown -p dialog-repository --lib extension_tests`). An `Extension` is `Send + Sync` on every target, the browser's too, because premises are held in statics (the rules `#[derive(Concept)]` writes), and a static is `Sync` wherever it is compiled.

## Breaking

`FormulaQuery`, `TypeError` and `AnalysisError` have new variants: an exhaustive `match` on them outside this crate needs an arm. Nothing a replica stores or syncs changes.

## Gate

Run locally on `fc949277c` (main `2316db9c0` merged in):

- `cargo fmt --all -- --check`; `cargo clippy --all --all-targets --all-features -- -D warnings`; `cargo +1.93.0 check --workspace --all-targets`; `cargo check --target wasm32-unknown-unknown -p dialog-query -p dialog-repository --tests`: clean.
- `cargo test -p dialog-query -p dialog-repository`: 1147 and 950 passed, and the smaller targets, none failed.
- `cargo test -p dialog-baseline --release`: passed.
- `cargo test --target wasm32-unknown-unknown -p dialog-repository --lib extension_tests` (wbg-pool, headless Chrome): 9 passed.
- Perf: `dialog-perf sweep` of main `a79de3d2f` and of this branch at `0746922b0` on one machine, then `dialog-perf compare`: 65 scenarios, "no regressions", every `instructions.outside_allocator` within 0.2% of main's and every counter unchanged. Not run again since: the commits after it change `ExtensionQuery::resolve` only, which no scenario reaches, and merge main. The Perf gate on this pull request compares it with main as it is.
- `RUSTDOCFLAGS="-D warnings" cargo doc -p dialog-query` fails on 101 broken links already on main; none is in a line this pull request adds.
