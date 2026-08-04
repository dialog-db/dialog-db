# Tree-Inspection Relations — Implementation Strategy

Status: **planned, not implemented**. Revision 2 — re-validated against
the current tree format (separator links, variable-length keys, novelty
buffers, scale) and against what tonk actually ships as of
[tonk#635](https://github.com/tonk-labs/tonk/pull/635). A self-contained
specification for exposing the search tree's structure to the query
engine as first-class relations, so that tonk's tree inspector
(<https://github.com/tonk-labs/tonk/blob/staging/plan/tree-inspector.md>)
can run its `tree/*` predicates through ordinary dialog queries —
retiring the custom worker endpoint that currently intercepts those
predicates and bypasses the evaluator.

## What tonk ships today (and what it teaches)

Tonk's `/query` endpoint special-cases a wire `predicate` that is a bare
*string* (an object is a concept): the worker routes it to its own
resolver (`rust/dialog-reactor/src/formula.rs` in tonk), which walks the
tree itself and emits `Conclusion` rows in the concept shape — dialog's
planner and evaluator are never involved. Four operators:

- `tree/node { hash? }` — one row describing a node (`kind` =
  `index`/`segment`, `size`, `count`, `bound`, `rank`). `hash` absent
  defaults to the branch's current root, so a bare `tree/node` is the
  entry point.
- `tree/child { hash }` — one self-contained row per link of an index
  node: `child`, `at`, the link **separator** (rendered as the row's
  `bound`), `rank`, plus the child's own node fields when its block is
  cached locally, flagged `cached: true/false`.
- `tree/entry { hash }` — one row per segment entry: key bytes, decoded
  `entity`/`attribute`/`type`/`value` for asserted facts,
  `retracted: true` for tombstones.
- `tree/key { key }` — pure decomposition of key bytes into components.

Since the M3 (variable-length key) adaptation in tonk#635, rows also
carry pre-decoded `bound-parts`/`key-parts`: lists of
`{ kind, text, hex }` chips produced server-side via the
`dialog_artifacts` key views (`EntityKey`/`AttributeKey`/`ValueKey`),
with a distinct lenient path for link separators (front-coded *prefixes*
of keys, which do not parse under the full-key schema).

What the interception model cannot do — and why the native port is
worth it: one predicate per request (the client issues one round-trip
per expansion; no joins), no composition with real facts or rules, no
transaction-query view, and no standing subscriptions (the inspector
re-fetches; "re-fetch on commit" is listed as later work in tonk's
plan). Everything else — the operator vocabulary, the moded "input"
discipline, self-contained rows, lazy child loading, separator-aware key
decoding — ports over and is kept by this design.

## Goal and non-negotiable constraint

The port must make the same queries answerable by dialog itself —
composable with other premises, usable from branch *and* transaction
queries, and visible to standing subscriptions — **without breaking
differential subscriptions**.

The subscription system is sound because every read a query performs
flows through `Provider<Select>`, where the demanded ranges are
recorded (`QueryEnv::record_demand`,
`rust/dialog-repository/src/repository/branch/session.rs:322`, invoked
first thing in `execute` at `:411`). Any design that reads the tree
behind that funnel (an effectful formula, a side-channel) makes standing
queries silently stale. So the one rule this design never violates:
**every tree read is a selector executed through `Provider<Select>`.**

## Why this is sound (keep this argument in the module docs)

Two facts make tree relations compatible with differential
subscriptions:

1. **Every tree fact is content-addressed and therefore immutable.**
   `Node(<hash>)`'s kind, size, links, and entries can never change — a
   different tree is a different hash. This now includes novelty: a
   node's hash covers its buffered ops
   (`rust/dialog-artifacts/src/buffered.rs` module doc), so "this
   node's pending novelty" is as immutable as its entries. Rows keyed
   by node hash are permanent; stale demand entries about old hashes
   can never be wrong, only unnecessary. (This is the same argument
   that makes `CausalityCache` in `dialog-artifacts` require no
   invalidation.)
2. **The only mutable fact in the domain is "what is the current
   root?"** — carried by
   [`BranchRevision.tree`](../rust/dialog-repository/src/schema.rs)
   (`dialog.branch/tree`, a base58 string), injected into every query's
   metadata overlay
   (`repository/branch/metadata.rs`, `QueryLayer::metadata` in
   `session.rs`).

Therefore: expose tree relations **only in node-bound form** (the node
reference must be bound), and let queries reach the tree exclusively by
joining through `BranchRevision`:

```text
BranchRevision(branch, tree: ?root58)
  ⋈ dialog/tree-reference(of: ?root58, this: ?root)   ← pure formula (glue)
  ⋈ TreeNode(?root, kind: ?k, size: ?s)               ← synthetic relation
  ⋈ TreeLink(?root, at: ?i, node: ?c) ⋈ TreeNode(?c)  ← descend
```

When a commit lands, re-evaluation re-binds `?root` and the join walks
the new tree. Per-hash rows never invalidate.

## The head-tracking gap (new machinery IS required)

Revision 1 of this note claimed "standing queries that depend on
`BranchRevision` already re-evaluate when the head moves — no new
machinery". **Reading the current poll path says otherwise; treat the
claim as false until test 7 passes.**

The gate in `Subscription::poll`
(`repository/branch/subscription.rs:381`) is: overlay epoch moved ⇒
recompute; else revision moved ⇒ `touched()` diffs the two roots
*scoped to the demand cover*; `Touched::Nothing` ⇒ **the pin advances
silently and no delta is delivered** (`:407-410`). Now trace a
subscription whose only premises are `BranchRevision ⋈ tree-reference ⋈
TreeNode`:

- `BranchRevision` is **overlay-injected metadata**, rebuilt fresh per
  evaluation (`metadata.rs`) — it is never a fact in the tree. Its
  selector records demand over `dialog.branch/*` EAV ranges that **no
  committed fact ever occupies**.
- The `dialog.tree/*` synthetic selectors likewise record ranges no
  commit touches (the namespace is write-reserved).
- A commit does **not** bump the session overlay epoch
  (`overlay.rs` — the epoch moves only on overlay mutation).

So after a commit: revision moved, diff-in-cover finds nothing,
`Touched::Nothing`, pin advances — the subscription keeps reporting the
old root's rows forever. (Subscriptions anchored on the *record-backed*
`Revision` concept do refire — `dialog.db/revision` facts are in-tree
and appended on every commit — but the record set only grows; the
"current head" is not expressible from records without aggregation, so
they are not a substitute anchor.)

The fix is small and principled — a third demand class:

- `Demand` (`subscription.rs:101`) gains a `head: bool` (alongside
  `facts` and `rules`).
- `QueryEnv::record_demand` sets it when a selector's `the` is one of
  the revision-bearing metadata attributes — `dialog.branch/tree`,
  `dialog.branch/edition`, `dialog.branch/revision` (not
  `dialog.branch/name`/`replica`, which are stable per branch, and not
  `dialog.tree/*`, which are per-hash immutable).
- `poll` checks it before the diff gate: epoch unchanged, revision
  moved, `demand.head` ⇒ go straight to full re-evaluation.

This adds no waste: any query that reads `dialog.branch/tree` has a
result that genuinely changes on every commit (the binding itself
changes), so a per-commit recompute is semantically forced, not a
regression of the diff gate's frugality. Data-fact subscriptions that
never read revision metadata are untouched. Encode the gap as a failing
test first (test 7 below), then land the flag.

## Terminology: two kinds of "formula"

Tonk's plan calls all four predicates "formulas". Implementation-wise
they split, and the split is load-bearing:

- **Pure formulas** (the `Formula` derive,
  `rust/dialog-query/src/formula/`): synchronous functions of their
  bound inputs, no store access. `Formula::compute(Input) -> Vec<Self>`
  cannot read the tree, and must not be made to: the incremental
  maintainer classifies formulas as `Inert`
  (`fixpoint.rs::classify_base`) and demand tracking would not see the
  reads. Only key decomposition and the base58↔entity glue are pure —
  those become real `Formula`s.
- **Synthetic relations**: node structure requires reading node
  buffers. They are served as reserved *attributes* answered by the
  `Provider<Select>` implementation itself (below), so from the query's
  point of view they are ordinary EAV premises — plannable, joinable,
  demand-tracked — even though no fact is ever stored under them.

## Reserved schema

All synthetic attributes live under `dialog.tree/*`. The `dialog.`
prefix is already write-reserved (user instructions cannot assert it —
see `it_rejects_writes_to_the_reserved_dialog_namespace`,
`repository/branch/commit.rs`), so no collision with user data is
possible.

Two entity shapes, both under a dedicated scheme (verify both survive
`Entity`/`Uri` round-trips on native and wasm — `uri.rs` accepts any
whitespace-free URL the `url` crate parses; add unit tests):

- **Node**: `tree:z<base58(blake3)>` — same base58 encoding
  `dialog.branch/tree` already uses.
- **Link**: `tree:z<base58(parent)>/<at>` — the `at`-th link of an
  index node. Links need their own entities because their fields
  (separator, scale, novelty) belong to the *(parent, position)* pair,
  not to the child node: the same child hash reappears under other
  roots across history with a different position or siblings.

Node-scoped attributes (`of` = a `tree:` node entity):

| attribute            | is                                            | cardinality |
|----------------------|-----------------------------------------------|-------------|
| `dialog.tree/kind`   | `"index"` \| `"segment"` (Text)               | one         |
| `dialog.tree/size`   | serialized byte length (UnsignedInt)          | one         |
| `dialog.tree/count`  | links (index) or entries (segment) (UnsignedInt) | one      |
| `dialog.tree/bound`  | the node's upper-bound key (Bytes) — absent for an empty node | one |
| `dialog.tree/rank`   | rank of the upper-bound key under the node's own embedded manifest (UnsignedInt) | one |
| `dialog.tree/scale`  | the node's own `Scale` byte (UnsignedInt)     | one         |
| `dialog.tree/link`   | link entity (`tree:z<hash>/<at>`)             | many        |
| `dialog.tree/child`  | child node entity (`tree:`) — convenience projection of links | many |
| `dialog.tree/key`    | an entry key in a segment (Bytes, variable length) | many   |

Link-scoped attributes (`of` = a `tree:` link entity):

| attribute                  | is                                       | cardinality |
|----------------------------|------------------------------------------|-------------|
| `dialog.tree.link/at`      | position among siblings (UnsignedInt)    | one         |
| `dialog.tree.link/node`    | child node entity (`tree:`)              | one         |
| `dialog.tree.link/separator` | the link's separator bytes (Bytes; empty = leftmost/−∞) | one |
| `dialog.tree.link/scale`   | advisory subtree scale (UnsignedInt)     | one         |
| `dialog.tree.link/novelty` | buffered ops pending against this subtree (UnsignedInt) | one |

Format notes (all verified against current code):

- Links are `Link { separator, node, scale }`
  (`rust/dialog-search-tree/src/link.rs`) — **lower-bound separators**,
  not upper bounds as revision 1 assumed. A separator is a front-coded
  *prefix* of the subtree's minimum leaf key; the empty separator is
  the level's global leftmost link. The node's own `upper_bound()`
  still exists (`node/persistent.rs:134`) and is what `dialog.tree/bound`
  surfaces.
- Index nodes carry per-link novelty buffers
  (`PersistentIndex.novelty`, `node/persistent.rs:635`) — hitchhiker
  ops not yet flushed to their destination subtrees. Surface the count
  per link; it is the inspector's window into buffered-vs-canonical
  cost, and it is covered by the node's hash so immutability holds.
- `rank` needs the manifest, which every node embeds
  (`Distribution::rank(key, manifest)`,
  `dialog-search-tree/src/distribution.rs:144`), so it is node-derived
  and belongs here rather than in a pure formula (which would need the
  manifest threaded as input).
- Do **not** add `dialog.tree/level`/depth: the same node can sit at
  different depths under different roots; depth is a property of the
  inspector's descent, not of the node.

Derived concepts on top (like `schema::Revision` /
`schema::RevisionParent`, via `builtin` in
`rust/dialog-repository/src/rules.rs`): `TreeNode { this, kind, size,
count, rank, scale }`, `TreeLink { this, at, node, separator, scale,
novelty }`, `TreeChild { this, child }`, `TreeKey { this, key }` —
`bound` as a `maybe` field if optional-field support fits, else a field
on `TreeNode`. Follow the attribute-newtype pattern in
`rust/dialog-repository/src/schema.rs`.

### What must NOT become a relation

Tonk's `tree/child` rows carry `cached: bool` — whether the child's
block is in the local archive. **This field cannot port.** Locality is
mutable without a commit (a block arrives when someone expands it, or a
replication task lands it), so a `dialog.tree/cached` fact would change
underneath a standing subscription with nothing in the demand/diff
machinery to notice — exactly the staleness this design exists to
prevent. Locality is presentation state: the client can infer it (row
resolved fast vs. slow, or a separate non-subscribable diagnostic
endpoint) but it must not be a queryable fact. The same reasoning
excludes anything else observer-relative: fetch latency, cache
residency, connection state.

## Serving the synthetic relations

### Routing point

`impl Provider<Select> for QueryEnv` in
`rust/dialog-repository/src/repository/branch/session.rs` (`execute`,
`:407`). After `self.record_demand(&input)` (`:411` — recording MUST
stay first; that is the whole point), check whether the selector's
`the` is a `dialog.tree/*` attribute. If so, do **not** union
branch/overlay streams; return the synthesized stream instead.
Everything else is unchanged. Because transaction queries construct the
same `QueryEnv` (see `repository/branch/transaction/query.rs`), the tx
view gets tree relations for free.

### Selector contract

- `of` must be a constant `tree:` entity (node or link form per the
  attribute). If `of` is unconstrained, return a stream whose first
  item is an error (`DialogArtifactsError::InvalidSelector` or nearest
  fit) with a message like *"tree relations are node-bound: constrain
  `of` to a tree: entity"*. This is deliberate, twice over: hashes are
  not enumerable, and an unbound scan is the one shape whose demand
  would be "the whole tree" — the shape that would actually degrade
  subscriptions.
- `is` constrained → filter the synthesized artifacts before yielding
  (ordinary post-filter; the engine also re-checks).
- A hash that decodes but whose block is absent everywhere → yield
  nothing (zero rows), consistent with "unreplicated contributes
  nothing". A malformed `tree:` entity → zero rows.
- Link attributes for an `at` out of range, or link/entry attributes
  asked of the wrong node kind → zero rows (not an error — lets a
  query union over mixed levels).

### Reading a node

Given the `Blake3Hash` decoded from the `of` entity:

1. Fetch the buffer through the same path branch reads use so the
   shared node cache and remote fallback apply: per branch in
   `self.branches`, construct the store the way the branch select does
   (`NetworkedIndex` + the branch's `node_cache()`, as
   `Subscription::touched` also does at `subscription.rs:509-526`) and
   attempt the content-addressed read. First branch that has the block
   wins — content addressing makes them interchangeable. The remote
   fallback means expanding a not-yet-replicated node transparently
   pulls it, same as tonk's resolver.
2. Decode as the artifact tree's node type: `ArtifactTree =
   PersistentTree<Key, State<Datum>>`
   (`rust/dialog-artifacts/src/tree.rs`), so nodes are
   `PersistentNode<Key, State<Datum>>` with `body()` = `Index` (links +
   per-link novelty) or `Segment` (entries with variable-length keys).
3. Synthesize `Artifact`s for the requested attribute, in the shapes
   from the schema table. Yield `child`/`link`/`key` rows in link/entry
   order. Large segments must be yielded lazily through the stream, not
   collected (nodes can be ~150 KB). `cause` on synthetic artifacts:
   `None`.

### Where the mutable world enters

Nothing else. Do not add a "current root" tree attribute — the root
enters queries via `BranchRevision.tree` + `dialog/tree-reference`
only. That keeps invalidation confined to the head flag above.

Caveat worth documenting: "absent everywhere → zero rows" interacts
with offline operation. A subscription that read zero rows for a node
whose block was unreachable will not refire when connectivity returns —
nothing moved the head. That is consistent with how partial replicas
behave elsewhere, and the inspector's usage (descend from a root you
just read) makes it rare; note it in the module docs rather than
engineering around it.

## Pure formulas

In `rust/dialog-query/src/formula/`, registered in the
`define_formulas!` table in `formula/query.rs` (`:93`) beside
`dialog/revision` / `dialog/revision-parent`:

1. **`dialog/tree-reference`** — glue between the base58 string the
   `BranchRevision.tree` fact carries and the `tree:` entity the
   relations key on. Input `of: String` (base58 hash), output
   `this: Entity` (`tree:z…`). Malformed base58 → zero rows (mirror the
   forged-record-projects-nothing convention).
2. **`dialog/key-part`** — decompose a full, variable-length index key.
   Input `of: Bytes`; output **one row per component**:
   `at: UnsignedInt` (position), `kind: Text` (`index` / `entity` /
   `attribute` / `vtype` / `value` / `spill` / `origin` / `edition` /
   `blob`), `text: Text` (human rendering), `bytes: Bytes` (raw).
   Multi-row output is native to `Formula::compute -> Vec<Self>`.
   Build it on the key views (`EntityKey`/`AttributeKey`/`ValueKey`
   over `key/varkey.rs`), dispatching on the tag byte — entity(0),
   attribute(1), value(2), history(3), blob(4), coverage(5), from
   `dialog-artifacts/src/constants.rs` — exactly as tonk's `key_parts`
   does, including the spilled-value arm and the history/coverage
   `origin ‖ edition ‖ fact-tail` shape. Unparseable under its tag's
   schema → a single `kind: "opaque"` row (never zero rows for
   non-empty input: the inspector must always have something to show).
3. **`dialog/separator-part`** — same output shape over a link
   separator. Separators are front-coded *prefixes*: the column framing
   a full-key parse relies on lies past the truncation, so this formula
   is lenient — emit the tag chip and as many leading components as the
   prefix carries, ending with an opaque remainder chip; empty input →
   one `kind: "min"` row (the −∞ separator). Keeping it separate from
   `dialog/key-part` mirrors tonk's `key_parts`/`separator_parts` split
   and keeps the strict/lenient contracts honest.

Both decomposition formulas are pure per-row computation — the
legitimate `Formula` kind, exactly like `dialog/revision-parent`.

## Subscriptions: what to build, and what to verify

Machinery to build: the `Demand::head` flag from "The head-tracking
gap" above. Everything else is verification:

- A standing query shaped `BranchRevision(branch, tree) ⋈
  tree-reference ⋈ TreeNode(root, …)` re-fires after a commit and
  reflects the new root (via the head flag; write the test to fail
  before the flag lands).
- Committed facts never carry `dialog.tree/*` attributes, so the
  incremental maintainer (`extend` / `retract` in `fixpoint.rs`) never
  sees them in additions/deletions — nothing to do, but assert the
  assumption: attempting to `assert` a `dialog.tree/*` fact through a
  transaction must be rejected by the existing reserved-domain check.
- Head-flagged subscriptions take the full-recompute path per commit by
  design; incremental maintenance of mixed fact+tree queries is not a
  goal (the revision binding changes every commit, so every commit is a
  real delta for them).

## What this unlocks / defers

- Tonk's chained point queries (`node → links → node → keys →
  decomposition`) become ONE dialog query — joins, not client
  round-trips per level — usable from branch and transaction queries,
  live under subscriptions. The worker's predicate interception and
  the custom endpoint can be deleted; `dialog-arboretum` (the UI)
  stays, now driven by a subscription instead of re-fetch-on-demand.
- Chips (`{kind, text, hex}`) move client-side or become
  `dialog/key-part` / `dialog/separator-part` rows — either way the
  worker stops hand-rolling key parsing.
- Declarative subtree traversal ("all nodes under this root", "bytes
  per subtree") is a recursive rule over `TreeChild`. It is **blocked
  on** the goal-directed fixpoint
  (`notes/goal-directed-fixpoint.md`): the full-closure evaluator's
  seed round scans unbound, which the selector contract above rejects
  loudly — the right failure mode. Do not weaken the contract to make
  full-closure traversal pass; implement demand seeding instead.

## Tests

In `dialog-repository` (model fixtures on the existing
revision-projection and subscription tests):

1. `it_reads_the_root_node_through_the_query_engine` — commit a few
   facts; query `BranchRevision` for `tree`, glue through
   `dialog/tree-reference`, select `TreeNode` — kind ∈
   {index, segment}, size > 0, count > 0. Do the whole thing as ONE
   query (joins, not application-side chaining) to prove composition.
2. `it_descends_through_links_and_decomposes_keys` — commit enough
   facts to force an index root (or accept a segment root and skip
   descent); follow `TreeLink`/`TreeChild` one level, select `TreeKey`,
   run `dialog/key-part` over the bytes; assert tags are within the
   known tag set and per-tag component kinds match.
3. `it_rejects_unbound_tree_scans` — `TreeNode` with `this` unbound
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
   subscription delivers rows for the *new* root. **Write this first;
   it is expected to fail against today's poll gate** and is the
   evidence for the `Demand::head` flag. (Anchor on the harness in
   `repository/branch/subscription.rs` tests.)
8. `it_serves_tree_relations_in_transaction_queries` — the same
   root-node query through `tx.query()`.
9. `it_surfaces_link_novelty` — commit through the buffered path so an
   index node holds novelty; assert `TreeLink.novelty` > 0 for the
   affected link and 0 elsewhere; `canonicalize()` and assert all
   zeros.

In `dialog-query`: unit tests for all three pure formulas (round-trip
base58 ↔ entity; per-tag decomposition incl. history and blob keys;
spilled values; separator prefixes incl. empty; wrong-length and
malformed inputs). Model on `formula/revision.rs::tests`.

## Acceptance checklist

- [ ] `cargo test --workspace` green (existing suites untouched).
- [ ] `cargo test -p dialog-repository --features integration-tests` green.
- [ ] `cargo clippy --workspace --all-targets --all-features` clean.
- [ ] `cargo check --target wasm32-unknown-unknown -p dialog-query -p dialog-repository -p dialog-artifacts`
      compiles (the inspector's whole point is running against
      IndexedDB-backed wasm builds).
- [ ] Test 7 demonstrated failing before the `Demand::head` fix, green
      after.
- [ ] Tests 1–9 + formula unit tests present and green.
- [ ] Demand recording precedes routing in `QueryEnv::execute`
      (assert by code review; the subscription test backs it
      behaviorally).
- [ ] Doc comments on the routing carry the soundness argument from
      "Why this is sound" above, including the novelty-under-hash
      point and the `cached`-exclusion rationale.

## Out of scope

- Entry *values* / states (`dialog.tree/entry` as a record of
  key + `State<Datum>`): v2 — needs a decision on surfacing tombstones
  and decoded values; keys alone serve the inspector's size/boundary
  analysis, and `dialog/key-part` already exposes the value component
  of the key.
- Manifest fields as relations (`version`, branch factor, …) — the
  inspector can read them from any node row later if needed.
- Node depth/level (path-dependent, see schema notes).
- Locality (`cached`) as a fact — excluded by design, see "What must
  NOT become a relation".
- Recursive subtree aggregation (blocked on
  `notes/goal-directed-fixpoint.md`).
- Any write path for tree facts (they are read-only by construction).

## Gotchas

- **Never bypass `record_demand`.** If a refactor moves routing above
  it, subscriptions rot silently — the exact failure this design
  exists to avoid.
- **Separators are not bounds.** Revision 1 specified `upper_bound`
  per link; the current format stores lower-bound separator prefixes
  (`link.rs`). Bounds exist per *node* (`upper_bound()`), separators
  per *link*; the schema reflects both, and the decomposition formulas
  differ (strict vs. lenient) for exactly this reason.
- `Value` payloads: keys/bounds/separators go out as `Value::Bytes`;
  sizes, counts, ranks, scales, novelty as unsigned integers; kind as
  text. Check the `Value` variant set in `dialog-artifacts` before
  inventing encodings.
- Base58: match the exact encoding `dialog.branch/tree` uses
  (`ToBase58` in `repository/branch/metadata.rs:76-79`).
- `Demand` builds its ranges under the **default** `Manifest`
  (`subscription.rs` doc around `Demand`) — fine for `dialog.tree/*`
  ranges since no committed fact ever occupies them, but keep the
  caveat in mind if the head flag is instead implemented as a fake
  range (don't: the flag is more honest).
- Large nodes: stream synthesized artifacts; do not collect a 150 KB
  segment's keys into a `Vec` eagerly.
- Entity scheme: confirm `tree:z…` and `tree:z…/3` survive
  `Entity::from_str` round-trips on both native and wasm (`uri.rs`
  strips nothing but rejects whitespace; the `url` crate may normalize
  opaque paths — add a unit test).
- wasm: no threads, no native-only sync primitives; reuse the existing
  `Cache`/`ConditionalSync` patterns if any per-env memoization is
  added (none is required for v1 — the branch node cache already
  serves repeat reads).
