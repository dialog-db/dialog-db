# The notebook from YAML and from the notation, run side by side

Run on 2026-11-18 on tonk's local branch `poc/dialog-notation`, against dialog-db `feat/notation-poc` at `c938ec9`. The test is `a_notebook_installed_from_the_notation_reads_as_from_yaml` in `rust/tonk-worker/src/router/repository.rs`, beside `a_component_installs_beside_the_seed`. Native, stable. It ran first against the local checkout and then again, with the same result, against the pin to the pushed revision `c938ec9c78d86512b0ad8c3cff7ec8a2632006aa`.

## Headline

The notebook installs from the notation sketch through the worker's own `install_component`, and for all six steps of the script the two spaces read the same notebook, once position keys are compared by rank. The notebook's views resolve headless in the notation space the same as in the YAML space, except the two route concepts the YAML pins to an entity.

## Setup

Two spaces, each created from core's YAML as every space is (`new_space`). In one, `install_component` installs `/library/notebook.yaml`; in the other, `/library/notebook.clj`. The second is lowered by `tonk_analyzer::notation::notation_library_claims`, with `core.clj` read in front for names only. Both installs are recorded complete.

The sketches are copies in tonk's library directory, `rust/tonk-core/assets/library/notebook.clj` and `core.clj`, each with a header naming the dialog revision it came from.

## The script

Each command is a YAML document posted through `evaluate_body` with `transact` on, the way the notebook's own router tests post them. The YAML analyzer resolves `block/insert`, `block/edit` and the rest by name on the branch, so the same text reaches the notation's concepts in the second space.

1. create: the inspector's `create_notation("notebook:probe", "Groceries", "# Groceries\n\nmilk\n\neggs")`, a `notebook/named` and three `block/insert`s.
2. insert: one `block/insert` of `bread` after `eggs`, `next: case:none`.
3. edit: `block/edit` of `milk` to `oat milk`.
4. move: `block/move` of `bread` from its key to the heading's key with `V` appended, between the heading and the milk.
5. remove: `block/remove` of `eggs` under its key.
6. retitle: `notebook/retitle` to `Shopping`.

After each step both spaces are read with YAML queries: `notebook/named` (titles), `notebook` with `block: {?key: ?block}` (the sequence), `notebook/blocks` (the standalone entries), `notebook/block` of the notebook (sources), `block/position` and `block/chain` of the notebook's blocks. Every block entity is spelled as its block's source, and every position key as its rank among the keys read.

## Results

| step | same | what both read |
|---|---|---|
| create | yes | title Groceries; heading, milk, eggs in that order; four positions with the edge |
| insert | yes | bread last |
| edit | yes | oat milk in milk's place, its key unchanged |
| move | yes | heading, bread, oat milk, eggs; one entry per block |
| remove | yes | heading, bread, oat milk |
| retitle | yes | one title, Shopping |

The test asserts that no step differs.

## Differences found

1. **Position keys are spelled differently.** For example the heading sits at `Myq37xHz` in one space and `Myqg0AqT` in the other. The order is the same at every step. Cause, traced: `dialog/position` biases a key by the block's entity, and the two spaces derive different block entities. A block's entity derives from its `block/insert`, and the notation's command lines have no `cardinality:` and pick `all:` where tonk's pick `last:`, so the command concept, and every entity derived from a command, differs. This is the known pick default, now seen in behaviour: a notebook moved from one install to the other keeps its order but not its keys or block entities.
2. **`block/chain` reads nothing in either space.** The comparison passes on two empty sets, so the chain is not exercised by this test. The cause was not traced. The YAML router tests count chains only for diagnostics and never assert them.

## Views

The test renders routes headless through `tonk_render::outline`, the renderer behind the worker's outline, over each space's content branch. A summary of each node (model, entity, name, facet, whether the concept resolved, records, items, error) is compared, concept entities aside.

| route | from YAML | from the notation |
|---|---|---|
| `notebook:probe@notebook` | resolves, 1 record, nests `notebook/block` with 6 | the same |
| `notebook` (directory) | resolves, 2 records | the same |
| `notebook/block` (directory) | resolves, 6 records | the same |
| `notebook/index-route`, `notebook:probe@notebook/route` by name | resolve, 0 records (no site in a test) | the same |
| `tonk:notebook/index-route`, `notebook:probe@tonk:notebook/route` | resolve | fail: no concept matched |

The last row is the known difference: the YAML pins these two route concepts to entities, the router names the pinned entity as a site's concept, and the notation cannot pin a concept. So in a browser the notebook's two pages would not be found in a notation-installed space. The test pins this.

Not checked: whether the rendered HTML is the same, and whether a browser's events post commands the rules take. The outline proves the views resolve and render the same rows.

## What changed in tonk-worker, and why each change is this small

- `is_component_url` takes `.clj` beside `.yaml`. One `or_else`. Without it a `.clj` source reads as a main library and core is not put in front.
- `embedded_standard_library` has two arms, `/library/notebook.clj` and `/library/core.clj`, each an `include_str!` of the copy, as every YAML component is.
- `install_claims_for` returns `notation_claims` for a `.clj` component. That function fetches `core.clj`, calls the analyzer's back end, refuses a library with any refused form other than a view, and converts instructions to `RawClaim`s as `library_claims_after` does. It installs nothing of `core.clj`.
- `install_claims_for` and `declared_by` take the worker state. The back end runs a write's `when` through a query, and a query needs an operator; nothing on that path had one. This touches five call sites and two test call sites. It is the largest change and the only one forced.

Not touched: `prepare_seed`, the language server, the inspector, `/evaluate`, `tonk-library`'s build script (it bundles `.yaml` only, so the copies are not bundled there).

## The back end's home

`tonk_analyzer::notation`, with `dialog-notation` and `tonk-render` as normal dependencies of `tonk-analyzer`. There is no cycle: `tonk-render` depends on `tonk-template` and `tonk-schema`, not on the analyzer. The wasm build of `tonk-analyzer` with these dependencies was not checked. The opinion calls are small functions there: `event_name`, `slots`, and no event flag read in `view_claims`. How a view is compared, the managing session's opinion, stays in the measuring test, the one place that compares views; its pinned numbers are unchanged.

## Decisions needed

1. The pick default on command lines. It is now the one thing that moves behaviour visibly: block entities and position keys differ between the two installs.
2. Concepts pinned to an entity, for the two route concepts. Without them the notebook's pages are not found in a browser.
3. Whether a notation component should ship. The copies are served natively; whether the service worker serves them in a browser was not checked.
