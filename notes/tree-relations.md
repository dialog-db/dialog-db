# Tree-Inspection Relations — Implementation Plan

Status: **planned, not implemented**. A self-contained specification for
exposing the search tree's structure to the query engine as first-class
relations, so that tonk's tree inspector
(<https://github.com/tonk-labs/tonk/blob/staging/plan/tree-inspector.md>)
can run its `tree/node` / `tree/child` / `tree/entry` / `tree/key`
predicates through ordinary dialog queries — retiring the custom worker
endpoint that currently intercepts those predicates and bypasses the
evaluator.

## Goal and non-negotiable constraint

Tonk's inspector chains point queries: fetch the root, list its
children, descend, decompose keys. Today the tonk worker special-cases
those predicate names and walks the tree itself, "bypassing dialog's
evaluator entirely". The port must make the same queries answerable by
dialog itself — composable with other premises, usable from branch
*and* transaction queries, and visible to standing subscriptions —
**without breaking differential subscriptions**.

The subscription system is sound because every read a query performs
flows through `Provider<Select>`, where the demanded ranges are
recorded (`QueryEnv::record_demand`,
`rust/dialog-repository/src/repository/branch/session.rs`). Any design
that reads the tree behind that funnel (an effectful formula, a
side-channel) makes standing queries silently stale. So the one rule
this design never violates: **every tree read is a selector executed
through `Provider<Select>`.**

## Why this is sound (keep this argument in the module docs)

Two facts make tree relations compatible with differential
subscriptions:

1. **Every tree fact is content-addressed and therefore immutable.**
   `Node(<hash>)`'s kind, size, children, and entries can never change —
   a different tree is a different hash. Rows keyed by node hash are
   permanent; stale demand entries about old hashes can never be wrong,
   only unnecessary. (This is the same argument that makes
   `CausalityCache` in `dialog-artifacts` require no invalidation.)
2. **The only mutable fact in the domain is "what is the current
   root?"** — and that is already an ordinary, tracked fact:
   [`BranchRevision.tree`](../rust/dialog-repository/src/schema.rs)
   (`dialog.branch/tree`, a base58 string), injected into every query's
   metadata overlay. Standing queries that depend on it already
   re-evaluate when the head moves.

Therefore: expose tree relations **only in node-bound form** (the node
reference must be bound), and let queries reach the tree exclusively by
joining through `BranchRevision`:

```text
BranchRevision(branch, tree: ?root58)
  ⋈ dialog/tree-reference(of: ?root58, this: ?root)   ← pure formula (glue)
  ⋈ TreeNode(?root, kind: ?k, size: ?s)               ← synthetic relation
  ⋈ TreeChild(?root, child: ?c) ⋈ TreeNode(?c, …)     ← descend
```

When a commit lands, the `BranchRevision` fact changes (already
tracked), re-evaluation re-binds `?root`, and the join walks the new
tree. Per-hash rows never invalidate. No new invalidation machinery.

## Terminology: two kinds of "formula"

Tonk's plan calls all four predicates "formulas". Implementation-wise
they split, and the split is load-bearing:

- **Pure formulas** (the `Formula` derive,
  `rust/dialog-query/src/formula/`): synchronous functions of their
  bound inputs, no store access. `Formula::compute(Input) -> Vec<Self>`
  cannot read the tree, and must not be made to: the incremental
  maintainer classifies formulas as `Inert`
  (`fixpoint.rs::classify_base`) and demand tracking would not see the
  reads. Only `tree/key` decomposition and the base58↔entity glue are
  pure — those two become real `Formula`s.
- **Synthetic relations**: `tree/node`, `tree/child`, `tree/entry`
  require reading node buffers. They are served as reserved
  *attributes* answered by the `Provider<Select>` implementation itself
  (below), so from the query's point of view they are ordinary
  EAV premises — plannable, joinable, demand-tracked — even though no
  fact is ever stored under them.

## Reserved schema

All synthetic attributes live under `dialog.tree/*`. The `dialog.`
prefix is already write-reserved (user instructions cannot assert it —
see the reserved-domain enforcement added with the revision records),
so no collision with user data is possible.

Node references are `Entity`s with a dedicated scheme:
`tree:z<base58(blake3)>` — the same base58 encoding
`dialog.branch/tree` already uses (verify `"tree:..."` parses as an
`Entity`; see `rust/dialog-artifacts/src/uri.rs`; if the scheme is
constrained, pick any accepted scheme and note it).

Synthetic attributes (all `of` = a `tree:` node entity):

| attribute            | is                                   | cardinality |
|----------------------|--------------------------------------|-------------|
| `dialog.tree/kind`   | `"index"` \| `"segment"` (Text)      | one         |
| `dialog.tree/size`   | serialized byte length (UnsignedInt) | one         |
| `dialog.tree/count`  | children (index) or entries (segment) count (UnsignedInt) | one |
| `dialog.tree/bound`  | the node's upper-bound key (Bytes, 162) | one      |
| `dialog.tree/child`  | child node entity (`tree:` Entity)   | many        |
| `dialog.tree/key`    | an entry key in a segment (Bytes, 162) | many      |

Derived concepts on top (like `schema::Revision` /
`schema::RevisionParent`, via `builtin` in
`rust/dialog-repository/src/rules.rs`): `TreeNode { this, kind, size,
count }`, `TreeChild { this, child }`, `TreeKey { this, key }` — plus
`bound` as a `maybe` field if optional-field support fits, else a field
on `TreeNode`. Follow the existing `revision::*` attribute-newtype
pattern in `rust/dialog-repository/src/schema.rs`.

Note on `level`: tonk surfaces a node level. Check whether
`ArchivedNodeBody` records a rank/level
(`rust/dialog-search-tree/src/node/persistent.rs`,
`distribution.rs`); if it does, add `dialog.tree/level`; if not, omit —
the inspector derives depth from its own descent, and a synthetic
"distance from root" is not a per-node fact (the same node can appear
at different depths under different roots).

Pure formulas (in `rust/dialog-query/src/formula/`, registered in the
`define_formulas!` table in `formula/query.rs` like `dialog/revision`):

1. **`dialog/tree-reference`** — glue between the base58 string the
   `BranchRevision.tree` fact carries and the `tree:` entity the
   relations key on. Input `of: String` (base58 hash), output
   `this: Entity` (`tree:z…`). Malformed base58 → zero rows (mirror the
   forged-record-projects-nothing convention).
2. **`dialog/tree-key`** — decompose a 162-byte index key. Input
   `of: Bytes`; outputs `tag: UnsignedInt`, `entity: Bytes` (64),
   `attribute: Bytes` (64), `value_type: UnsignedInt`,
   `value_reference: Bytes` (32). Layout constants are in
   `rust/dialog-artifacts/src/key.rs` (`TAG_LENGTH`=1,
   `ENTITY_LENGTH`=64, `ATTRIBUTE_LENGTH`=64, value-type=1,
   value-reference=32; `HISTORY_KEY_TAG`=3 — enumerate the other tag
   constants there so the inspector can label regions). Wrong length →
   zero rows. This formula is pure per-row computation — the legitimate
   `Formula` kind, exactly like `dialog/revision-parent`.

## Serving the synthetic relations

### Routing point

`impl Provider<Select> for QueryEnv` in
`rust/dialog-repository/src/repository/branch/session.rs`
(`async fn execute(&self, input: ArtifactSelector<Constrained>)`).
After `self.record_demand(&input)` (recording MUST stay first — that is
the whole point), check whether the selector's `the` is a
`dialog.tree/*` attribute. If so, do **not** union branch/overlay
streams; return the synthesized stream instead. Everything else is
unchanged. Because transaction queries construct the same `QueryEnv`
(see `repository/branch/transaction/query.rs`), the tx view gets tree
relations for free.

### Selector contract

- `of` must be a constant `tree:` entity. If `of` is unconstrained,
  return a stream whose first item is an error
  (`DialogArtifactsError::InvalidSelector` or nearest fit) with a
  message like *"tree relations are node-bound: constrain `of` to a
  tree: entity"*. This is deliberate, twice over: hashes are not
  enumerable, and an unbound scan is the one shape whose demand would
  be "the whole tree" — the shape that would actually degrade
  subscriptions.
- `is` constrained → filter the synthesized artifacts before yielding
  (ordinary post-filter; the engine also re-checks).
- A hash that decodes but whose block is absent everywhere → yield
  nothing (zero rows), consistent with "unreplicated contributes
  nothing". A malformed `tree:` entity → zero rows.

### Reading a node

Given the `Blake3Hash` decoded from the `of` entity:

1. Fetch the buffer through the same path branch reads use so the
   shared node cache and remote fallback apply: per branch in
   `self.branches`, construct the store the way `select_from_branch`
   does (`NetworkedIndex::new(env, branch.claims().select(...).catalog(), remote)`
   — extract/reuse the store-construction part, not the selector part)
   and attempt the content-addressed read
   (`dialog_search_tree::ContentAddressedStorage`; the branch's
   `node_cache()` is threaded the same way `Branch::history()` does it
   in `repository/branch.rs`). First branch that has the block wins —
   content addressing makes them interchangeable.
2. Decode as the artifact tree's node type: `ArtifactTree =
   PersistentTree<KeyBytes, State<Datum>>`
   (`rust/dialog-artifacts/src/tree.rs`), so nodes are
   `PersistentNode<KeyBytes, State<Datum>>` with
   `body() -> ArchivedNodeBody` = `Index` (children as
   `Link { upper_bound, node: Blake3Hash }`,
   `rust/dialog-search-tree/src/link.rs`) or `Segment` (entries with
   162-byte keys). Mirror however the existing read path decodes a
   buffer into a `PersistentNode`
   (`rust/dialog-search-tree/src/node/persistent.rs`,
   `accessor.rs`).
3. Synthesize `Artifact`s for the requested attribute:
   - `kind`: `"index"` / `"segment"`.
   - `size`: buffer byte length.
   - `count`: children/entries length.
   - `bound`: the node's upper-bound key bytes
     (`Node::upper_bound_ref`).
   - `child`: one artifact per `Link`, `is` = `tree:z<base58(link.node)>`
     entity. Yield in child order.
   - `key`: one artifact per segment entry, `is` = `Value::Bytes` of
     the 162-byte key. For an index node, zero rows (not an error —
     lets a query union over mixed levels). Large segments must be
     yielded lazily through the stream, not collected (nodes can be
     ~150 KB).
   - `cause` on synthetic artifacts: `None`.

### Where the mutable world enters

Nothing else. Do not add a "current root" tree attribute — the root
enters queries via `BranchRevision.tree` + `dialog/tree-reference`
only. That keeps the invalidation story exactly as it is today.

## Subscriptions: what to verify, not what to build

No new machinery. The correctness case to encode in tests:

- A standing query shaped `BranchRevision(branch, tree) ⋈
  tree-reference ⋈ TreeNode(root, …)` re-fires after a commit and
  reflects the new root (the `BranchRevision` dependency triggers it —
  that already works for revision-shaped subscriptions).
- Committed facts never carry `dialog.tree/*` attributes, so the
  incremental maintainer (`extend` / `retract` in `fixpoint.rs`) never
  sees them in additions/deletions — nothing to do, but assert the
  assumption: attempting to `assert` a `dialog.tree/*` fact through a
  transaction must be rejected by the existing reserved-domain check
  (add a test; if the reservation currently covers only exact
  `dialog.db`/`dialog.revision`-style prefixes, extend it to
  `dialog.tree`).

## What this unlocks / defers

- Tonk's chained point queries (`node → children → node → entries →
  key decomposition`) work with plain non-recursive queries — no
  fixpoint involved. The worker's predicate interception and the
  custom endpoint can be deleted; `dialog-arboretum` (the UI) stays.
- Declarative subtree traversal ("all nodes under this root", "bytes
  per subtree") is a recursive rule over `TreeChild`. It is **blocked
  on** the goal-directed fixpoint
  (`notes/goal-directed-fixpoint.md`): the full-closure evaluator's
  seed round scans unbound, which the selector contract above rejects
  loudly — the right failure mode. Do not weaken the contract to make
  full-closure traversal pass; implement demand seeding instead.

## Tests

In `dialog-repository` (model fixtures on
`repository.rs::it_projects_revision_attribution_from_the_signed_record`
for the BranchRevision join):

1. `it_reads_the_root_node_through_the_query_engine` — commit a few
   facts; query `BranchRevision` for `tree`, glue through
   `dialog/tree-reference`, select `TreeNode` — kind ∈
   {index, segment}, size > 0, count > 0. Do the whole thing as ONE
   query (joins, not application-side chaining) to prove composition.
2. `it_descends_to_a_leaf_and_decomposes_keys` — commit enough facts to
   force an index root (or accept a segment root and skip descent);
   follow `TreeChild` one level, select `TreeKey`, run
   `dialog/tree-key` over the bytes; assert tags are within the known
   tag set from `key.rs` and entity/attribute segment lengths are
   64/64.
3. `it_rejects_unbound_tree_scans` — `TreeChild` with `this` unbound
   errors with the node-bound message.
4. `it_keeps_old_roots_queryable` — capture root₁, commit again, query
   `TreeNode(root₁)` — still answers (content-addressed history), and
   `TreeNode(root₂)` differs.
5. `it_yields_nothing_for_an_absent_node` — a syntactically valid
   `tree:` entity whose hash is not in the store → zero rows, no error.
6. `it_refuses_committing_tree_facts` — `tx.assert(dialog.tree/kind …)`
   is rejected by the reserved-domain check.
7. `it_refires_a_tree_subscription_on_commit` — standing query over
   `BranchRevision ⋈ tree-reference ⋈ TreeNode`; commit; assert the
   subscription delivers rows for the *new* root. (Anchor on the
   existing subscription tests in
   `repository/branch/subscription.rs` for the harness.)
8. `it_serves_tree_relations_in_transaction_queries` — the same
   root-node query through `tx.query()`.

In `dialog-query`: unit tests for both pure formulas (round-trip
base58 ↔ entity; key decomposition on a synthetic 162-byte key;
wrong-length and malformed inputs → zero rows). Model on
`formula/revision.rs::tests`.

## Acceptance checklist

- [ ] `cargo test --workspace` green (existing suites untouched).
- [ ] `cargo test -p dialog-repository --features integration-tests` green.
- [ ] `cargo clippy --workspace --all-targets --all-features` clean.
- [ ] `cargo check --target wasm32-unknown-unknown -p dialog-query -p dialog-repository -p dialog-artifacts`
      compiles (the inspector's whole point is running against
      IndexedDB-backed wasm builds).
- [ ] Tests 1–8 + formula unit tests present and green.
- [ ] Demand recording precedes routing in `QueryEnv::execute`
      (assert by code review; the subscription test backs it
      behaviorally).
- [ ] Doc comments on the routing carry the soundness argument from
      "Why this is sound" above.

## Out of scope

- Entry *values* (`dialog.tree/entry` as a dag-cbor record of
  key + value): v2 — needs a decision on surfacing `State<Datum>`
  variants; keys alone serve the inspector's size/boundary analysis.
- `dialog.tree/level` if the persisted node turns out not to record it.
- Recursive subtree aggregation (blocked on
  `notes/goal-directed-fixpoint.md`).
- Any write path for tree facts (they are read-only by construction).
- History-region-specific conveniences (the `tag` output of
  `dialog/tree-key` already lets the inspector distinguish regions).

## Gotchas

- **Never bypass `record_demand`.** If a refactor moves routing above
  it, subscriptions rot silently — the exact failure this design
  exists to avoid.
- `Value` byte payloads: keys/bounds go out as `Value::Bytes`; sizes
  and counts as unsigned integers; kind as text. Check the `Value`
  variant set in `dialog-artifacts` before inventing encodings.
- Base58: the workspace already depends on `base58` (see
  `ToBase58` usage in `dialog-query/src/formula/revision.rs` tests);
  encode/decode with it, and match the exact encoding
  `dialog.branch/tree` uses (see where `branch::Tree` values are
  produced in the commit path).
- Large nodes: stream synthesized artifacts; do not collect a 150 KB
  segment's keys into a `Vec` eagerly if the stream machinery allows
  incremental yield.
- Entity scheme: confirm `tree:` survives `Entity::from_str`
  round-trips on both native and wasm; add a unit test.
- wasm: no threads, no native-only sync primitives; reuse the existing
  `Cache`/`ConditionalSync` patterns if any per-env memoization is
  added (none is required for v1 — the branch node cache already
  serves repeat reads).
