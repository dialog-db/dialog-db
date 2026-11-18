# Moving tonk onto the dialog notation stack

tonk `c006b50` on a local branch, `poc/dialog-notation`, not pushed, checked against this branch (`feat/notation-poc`) with `cargo check`, native, on stable. Measured on 2026-11-18. That checkout moved from `be983b4` to `2ea1f8c` during the run; every commit in that span touched `rust/dialog-notation` only. The last change under `dialog-query`, `dialog-corpus` and `dialog-repository` is `1559eda`.

Every tonk workspace crate checked compiles against the new Dialog with no source change. The one port is a registry entry for the new formula, which no compiler error points to.

## Crates

All checks are native `cargo check` on stable through the wrapper.

| crate | checks | errors before | errors after | causes |
|---|---|---|---|---|
| tonk-notation (lib, all targets) | yes | 0 | 0 | none; uses only `dialog-common` |
| tonk-schema (lib, all targets) | yes | 0 | 0 | none |
| tonk-analyzer (lib, all targets) | yes | 0 | 0 | formula registry lacks `entity/derive` (silent, not a compile error) |
| tonk-library, tonk-template, tonk-evaluator, dialog-reactor (lib, tests) | yes | 0 | 0 | none |
| tonk-worker (lib, tests) | yes | 0 | 0 | none |
| tonk-cli, tonk-display, tonk-macros, tonk-language-server, tonk-intent, tonk-inspector, tonk-render, tonk-agent-peer, tonk-guest, tonk-ui, tonk-portal, tonk-fab (lib) | yes | 0 | 0 | none |

Verified by running: `cargo test -p tonk-analyzer --lib formula`, 6 passed after the port.

Not checked: the `wasm32-unknown-unknown` target, all-targets for crates other than the ones marked, `tonk-host-worker` and `tonk-view-peer` (they have directories but are not workspace members).

## Resolution

The `[patch."https://github.com/dialog-db/dialog-db.git"]` section points 19 crates at `rust/<crate>`. The versions match (`0.5.0-rc.6` on both sides), so no version change was needed. `cargo update -p dialog-query` needed network access to fetch the `v0.5.0-rc.6` tag, because a patch resolves against the source it replaces, and the fetch succeeded. `Cargo.lock` moved 25 dialog packages to path sources and added nothing third-party. `dialog-stack` is patched but unused in tonk's graph, so cargo warns about it. Nothing depends on `dialog-notation` yet.

## Pattern construction

Dialog side: `rust/dialog-query/src/parameters.rs`, `rust/dialog-query/src/pattern.rs`. `Parameters` now holds a `Held` (terms plus optional `Pattern` shapes). Its whole rc.6 surface is unchanged (`new`, `get`, `insert(String, Term<Any>)`, `remove`, `contains`, `iter`, `keys`), and the additions are `set`, `shape`, `pattern`, `slot`, `shapes`, `leaves`, `lowered` and others. tonk's 92 `Parameters::new` / `ConceptQuery {` sites (most in `rust/tonk-schema/src/concept.rs`, `rust/tonk-schema/src/transact.rs`, `rust/tonk-analyzer/src/analyzer/*.rs`) compile as they are. They only need to change when tonk builds map or list patterns. The count of about 62 affected sites names places that could adopt patterns, not places that break.

## Stored rule format 4

Dialog side: `rust/dialog-query/src/rule/body.rs` (`FORMAT` 3 to 4), `rust/dialog-query/src/rule/reference.rs`. The format only changes how rules are stored, so tonk's code is not affected. Formats 0 to 3 are still read. From reading the code: a rule with no map or list pattern keeps its identity, because `pattern_reference` is only used for composite sides, and its body is the format 3 body with a new number. tonk's `Branch::upgrade_rules` call in `rust/dialog-reactor/src/branch/reference.rs` is gated on `needs_migration` (the storage layout), so existing format 3 bodies stay as they are, and they remain readable. rc.6 refuses a body whose format number is later than its own, so an rc.6 tonk cannot read rules this stack writes.

## Records named by their parts

Dialog side: `rust/dialog-corpus/src/model/composite.rs`, `rust/dialog-corpus/src/identity.rs` (`Value::reference` to `reference_within`). Change `033cac1` is breaking on the Dialog side. Under it, a `Value::Record` whose bytes are `0x01` followed by one dag-cbor map or list is named by its parts, and any other record is still named by its bytes. tonk's records (`rust/tonk-template/src/bindings.rs`, `rust/tonk-template/src/embed.rs`) are plain `serde_ipld_dagcbor` maps, so they start with a map byte, not `0x01`, and keep their names. This is inferred from reading; no stored space was tested. `rust/tonk-worker/src/router/claim.rs:191` passes through records that come from outside, and any of those that happen to have the composite layout now get a different name.

## New formula entity/derive

Dialog side: `rust/dialog-query/src/formula/entity.rs`, registered as `"entity/derive"` in `rust/dialog-query/src/formula/query.rs`. tonk side: `rust/tonk-analyzer/src/analyzer/formula.rs` mirrors Dialog's formula table by hand. Without an entry, `lookup_formula` does not know the formula. A premise named `entity/derive` then falls through `lift_premise` (`rust/tonk-analyzer/src/analyzer/rule.rs`) and is resolved as a concept name, which no library defines. Ported in `5cea035`: `info::<DeriveEntity>("entity/derive")` plus the expected-name entry. The test `it_lists_every_dialog_query_formula` passed before the port as well. It compares two hand-written lists, so its doc comment's claim that it fails when Dialog adds a formula is false.

## Commits on poc/dialog-notation

- `1845b62` build: patch dialog-db to the local notation checkout
- `5cea035` feat(analyzer): register the entity/derive formula

## Decisions needed

1. Mixed versions. rc.6 refuses format 4 rule bodies. Decide whether the proof of concept has to sync with spaces served by an rc.6 tonk, or whether it accepts a one-way move.
2. The formula registry. tonk keeps a second copy of Dialog's formula table with a test that cannot detect drift. Decide whether Dialog should export the table so tonk can derive the list from it (a Dialog-side change), or whether tonk keeps the hand-written copy.
3. Pinning. The patch uses absolute paths into a checkout another agent is committing to. Anything shared needs the exact revision of a pushed pull request head (`rev =`), per the versioning rule.
4. Adopting patterns. The notation reader will produce map and list values. Decide whether `tonk-schema`'s construction sites move to `Parameters::set` with `Pattern`, or whether the reader lowers to plain terms before handing over. Nothing forces either choice yet.
