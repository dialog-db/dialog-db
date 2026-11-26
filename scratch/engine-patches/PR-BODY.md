Replaces #894.

## What

The engine declares two more by name and by their cells, and their code comes from a crate of dialog's own, the notation:

| declared | kind | in | a row for, and its cells |
|---|---|---|---|
| `notation/read` | formula | `text` | each form read: `at`, `tree`, `start-line`, `start-character`, `end-line`, `end-character` |
| `notation/analyze` | resolver | `form` | each thing said of the form: `severity`, `code`, `message`, `path` |

- `dialog_query::supplied`: the traits `NotationRead` and `NotationAnalyze`, and `supply_read` / `supply_analyze`, which hold an implementation in the environment a query runs in (`Holds`).
- `FormulaQuery::NotationRead(NotationReadQuery)` and `ResolverQuery::NotationAnalyze(NotationAnalyzeQuery)`: the premises. Each has a name, so each reads and writes as data.
- `Facts`: the reads an analyzer makes, through the query's own environment, at the query's revision.
- `query::PremiseQuery` / `PremiseRow`: a query of premises planned as a rule's body is and asked with no rule. It is where these two are asked, since no rule holds them.

`notation/analyze`'s real implementation waits for definitions on the branch. Here it is declared with its trait and tested against a stub.

## What is locked, and what is not

- **Locked:** the names and the cells are the engine's. No crate adds a formula. A premise names one of these or a built-in, and nothing else deserializes.
- **Not locked:** `Holds` is open, so any code holding the environment can supply an implementation under these names. That is how the notation's code reaches the engine without the engine depending on it. It is not offered as an extension point, and the module docs say so.

## Why no rule holds them yet

A built-in formula is on every replica. One whose implementation comes from another crate is only where that crate is linked, and what it does would become part of what a stored rule means. So a deductive or inductive rule holding either is refused when it is compiled, built directly or read from data, with `TypeError::SuppliedInRule` (from `AnalysisError::SuppliedInRule`).

Opening rules to them later is that one refusal removed, once two things are true:

- what the implementation does is versioned in what a replica stores, as a rule body's format is;
- a replica holding no implementation judges such a rule when its rule set is assembled, as a `RuleError` in quarantine, not as an `EvaluationError` when a query asks it.

The note sits beside the refusal (`rule/analyzer.rs`) and in the module docs (`supplied.rs`).

## Refusals and faults, each named

| what | fails as | when |
|---|---|---|
| a premise asking either where the environment holds no implementation | `EvaluationError::Unsupplied { formula }` | at evaluation, before any row is read: with no row in as well, never a query that answers nothing |
| a rule holding either | `TypeError::SuppliedInRule` | rule compile |
| a premise query selecting a variable no premise binds | `TypeError::UnselectedVariable` | `PremiseQuery::new` |
| a row giving a cell not declared as an output | `EvaluationError::UndeclaredOutput` | at evaluation |
| a row leaving out a declared output | `EvaluationError::MissingOutput` | at evaluation |
| a row giving a value its cell's type does not admit | `EvaluationError::MistypedOutput` | at evaluation |

A missing implementation fails at evaluation, not when the query is built: a query is built with no environment, and the implementation is the environment's.

## What an analyzer reads is the query's demand

Checked, not assumed: `Facts::select` runs through the query environment's own `Select`, which records demand (`QueryEnv::record_demand`). `it_follows_the_definitions_a_resolver_reads` pins what follows:

- a subscription over a premise query asking `notation/analyze` is left alone by a commit of a fact the analyzer did not read: no rows move, and the analyzer is not called again;
- it is evaluated again when a definition it read changes.

A subscription over a premise query is otherwise never restricted to one entity: when what it read changes, it evaluates the query again in full.

## Known cost

On #773's local build of `notation/check` (release, one machine), one cell's edit re-checked every cell: about 9 ms per cell of 57 lines, 440 ms for 50 cells. What would fix it, in this crate, later: a supplied formula's rows kept by their input values, as a `#[formula(cached)]` formula's are, so an edit reads only the cell edited.

## Of #894

Kept: `PremiseQuery` and its refusal, the three output checks, the rule-compile refusal (renamed).

Gone: `Extension`, `Extensions`, `Extensions::apply`, `TypeError::UnknownFormula`, and the `extensions` feature. The wasm finding goes too: a premise carries no implementation, so nothing `Send + Sync` sits in a static, and both traits are `ConditionalSend + ConditionalSync`.

## Tests

`rust/dialog-repository/src/repository/branch/supplied_tests.rs`, written first and failing, in their own commit:

- the reader the environment holds, asked in a premise query;
- `Unsupplied` for read and analyze, with a text and with none;
- both declared names read from data;
- a rule holding either refused as deductive, from a descriptor read from data and as inductive, with no `dialog.rule/source` after, on the volatile and the filesystem providers;
- `UnselectedVariable`;
- a subscription kept current through a change;
- the resolver's reads followed as demand;
- the three output faults.

Each asserts its variant with `matches!`.

## Breaking

`FormulaQuery`, `ResolverQuery`, `TypeError`, `AnalysisError` and `EvaluationError` have new variants, and none is `non_exhaustive`, so an exhaustive `match` on them outside this crate needs the arms. Nothing a replica stores or syncs changes: a rule naming either is refused, so none is stored.

## Gate

Run locally on `785c4d0e2`, from main `5e382e63b`:

- `cargo fmt --all -- --check`, `cargo clippy --all --all-targets --all-features -- -D warnings`, `cargo +1.93.0 check --workspace --all-targets`, `cargo check --target wasm32-unknown-unknown -p dialog-query -p dialog-repository --tests`: clean.
- `cargo test --workspace --lib --tests --bins`: 4302 passed in 73 targets, none failed, 20 ignored. dialog-common's convention test is among them, and passes on its own too. Example targets were left out after a full run filled this machine's disk; CI's archive builds tests, not examples.
- `cargo test -p dialog-query -p dialog-repository -p dialog-common --doc`: passed.
- `cargo test -p dialog-baseline --release --lib --tests`: 13 passed.
- `supplied_tests` in headless Chrome (wbg-pool): 10 passed. The filesystem test is native only.
- Perf: `dialog-perf sweep` of main `5e382e63b` and of this branch on one machine, then `dialog-perf compare`: 31 scenarios, "no regressions". Every counter is unchanged, and every `instructions.outside_allocator` is within -0.2% to +0.3% of main's.
- `RUSTDOCFLAGS="-D warnings" cargo doc -p dialog-query` fails on 101 broken links already on main. Each one's line blames to a commit on main; none is in a line this pull request adds.
