# Handoff: `claude/empty-tree-manifest-node`

Status of the stacked branch as of 2026-08-09, written for whoever picks
this up next. The deeper narrative (measurements, dead ends, owner
feedback quotes) is in `notes/tree-research-2026-07.md`; this is the
map.

## Branch topology

- **Base**: `claude/root-frame-write-amp-6wb1z8` — the long-running
  write-amplification / query-performance campaign branch. Its own PR
  (if any) is separate; do not mix the two.
- **This branch**: `claude/empty-tree-manifest-node`, stacked on the
  base at `0ca891e`. Eight commits, all pushed, **no PR opened yet**:
  - `1a6e7c6` Represent the empty tree as a manifest-carrying node
  - `8d48a79` Make the empty node the canonical empty form under every
    manifest
  - `d20e725` Drop the `EMPTY_TREE_HASH` sentinel; absence is Option,
    emptiness is derived
  - `9894a0b` Adjust tests for the sentinel-free empty representation
  - `fb5ace3` Drop `NULL_BLAKE3_HASH` from the tree layer
  - `b715002` (fmt + notes)
  - `a44a338` Drop the legacy zero-hash decode shim from upstream sync
    bases
  - `bce9133` Dissolve `NULL_REVISION_HASH`: Option revisions and an
    empty head pointer

## The invariant this branch establishes

Every layer now agrees on one rule: **a thing either exists as a real,
content-addressed value, or it is absent — never a magic number.**

1. **The empty tree is a real value.** Its persisted form, under EVERY
   manifest (default included), is the canonical zero-entry segment
   node carrying that manifest — "the manifest without children or
   novelty." One fixed encoding per manifest, so
   `(entry set, manifest) → persisted form` is a bijection and
   empty-from-scratch converges byte-for-byte with
   emptied-by-deletes. The node is a pure format marker: load paths
   treat it as "no root, manifest M"; it is never edited in place, so
   the root-is-always-an-index invariant and first-insert shapes are
   untouched.
2. **Absence is `Option`.** A branch with no revision, an upstream
   never synced, a store never committed — all `None` at the API,
   an empty cell value on the wire. No `[0; 32]` constant exists
   anywhere (`git grep` for the old names comes back empty).
3. **Derived, not stored, where a value is needed before persist.**
   `PersistentTree::empty_root(&manifest)` computes the empty root as
   a pure function (no storage). Used for genesis revision minting in
   commit/blob and as the root an unpersisted empty tree reports.

## Key API shapes (what a next agent will actually touch)

dialog-search-tree:
- `PersistentTree` root is an internal enum: `Node(hash)` |
  `Empty { manifest, hash }` (hash = derived empty root).
  - `root() -> &Blake3Hash` — always real; for `Empty` it is the hash
    the first persist will land on.
  - `stored_root() -> Option<&Blake3Hash>` — `None` until something is
    durable. **Read paths gate on this**, not on hash comparison.
  - `empty()` / `empty_with_cache()` / `empty_with_manifest()`.
- `TransientRoot::Empty` / `HitchhikerRoot::Empty` /
  `TransientRootParts::Empty` are explicit states;
  `TransientTree::empty_with_manifest(cache, manifest)` is the
  write-side entry for a fresh tree under a declared format.
- `TreeWalker::new(Option<Blake3Hash>)`; the differential's
  `SparseTree::from_root(Option<&Blake3Hash>, ..)`; settled sparse
  nodes have `hash() -> Option`.
- Persist of an `Empty` root writes the canonical empty node via
  `persist_empty_root` (crate-internal choke point).

dialog-artifacts:
- `TreeReference` has no `Default`; it always names a real tree.
- `TreeHistory::empty_with_cache(store, cache)` for revision-less
  branches.
- `Artifacts::revision() -> Result<Option<Blake3Hash>>`;
  `reset(Option<Blake3Hash>)` where `None` = the empty, no-revision
  state (always writes the pointer); `reload()` = re-read the durable
  head (never writes). Head-pointer cell: empty value = no revision.
- wasm (`web.rs`): `revision()` returns an empty `Uint8Array` for a
  fresh store; `reset()` maps empty array → empty state, missing
  argument → `reload()`. JS-visible types unchanged.

dialog-repository:
- `Upstream::tree() -> Option<&TreeReference>` (`None` = never
  synced); plain `Option` on the CBOR wire.
- `PushError::NonFastForward { expected, actual }` fields are
  `Option<TreeReference>`.
- Commit/blob mint revisions from the tree they started from (base
  revision's root, or the derived empty root for genesis); pull's
  three merge mints start from the pre-record merged root.

## Compatibility decisions (deliberate, owner-approved)

There is **no legacy tolerance** for the old zero-hash encodings —
pre-release format, no deployed data to protect:
- An old upstream cell recording "never synced" as 32 zero bytes now
  decodes as a sync base naming a nonexistent tree and fails loudly on
  the next pull (shim existed briefly, removed in `a44a338`).
- An old store head pointer holding 32 zero bytes errors on
  open/reload ("block not found") instead of opening empty.
- Old software and new software emptying the same default-manifest
  tree land on different roots (null vs empty node) — cross-version
  convergence for the empty state was knowingly broken.

## Verification state

Everything below was green at `bce9133`:
- Full workspace suite: 64 suites, 2,171 tests, 0 failures.
- `cargo clippy --all --all-targets --all-features -- -D warnings`
  and `cargo fmt --all` clean (project rules; always run both).
- `cargo check -p dialog-artifacts --target wasm32-unknown-unknown`.
- Along the way: adversarial manifest soak, program harness, SE
  convergence, differential/validator model tests all adapted and
  passing; the hitchhiker flush path's manifest-continuity bug (first
  batch of a `with_manifest` session replayed under the default) was
  found and fixed as part of `1a6e7c6`.

## Gotchas for future work

- **The migration hazard pattern**: code that did
  `TransientTree::with_manifest(tree.root().clone(), ..)` in a
  build-loop breaks for unpersisted-empty trees (the derived root is
  not in storage). The fix pattern, used everywhere now:
  match `stored_root()` → `with_manifest(root, ..)` /
  `empty_with_manifest(cache, manifest)`. Any new helper that walks
  from `tree.root()` should gate on `stored_root()` first.
- **Stitch semantics**: an unpersisted empty source contributes its
  manifest to the stitch's agreement check but no parts; a stitch of
  empty pieces persists to one marker node (`written == 1`, not 0).
- **A no-op batch over a fresh empty tree persists the marker node**
  and moves the root from "unpersisted" to "persisted empty" — tests
  should assert against `empty_root(..)`/`stored_root()`, not "root
  unchanged".
- **TS experimental session** (`typescript/dialog-experimental`)
  treats "empty byte array" as the empty revision (`GENESIS` is
  `Link.of(null)`); the wasm change aligns with this better than the
  zero hash did, but the `Link.of(empty bytes)` vs `Link.of(null)`
  equality has never been reconciled on the TS side — worth a look if
  TS work resumes.
- **Environment**: the dev container's disk quota is tight; the
  workspace debug build + incremental cache can exhaust it (ENOSPC
  mid-suite looks like a test failure). `rm -rf target/debug/incremental`
  and `CARGO_INCREMENTAL=0` are the remedies.

## Open threads / candidate next steps

1. Open a PR for this branch (stacked on
   `claude/root-frame-write-amp-6wb1z8`) when the owner wants it
   reviewed; none exists yet.
2. `IndexRoot` in `dialog-artifacts` is a one-field struct wrapping
   the tree root — with sentinels gone it is close to redundant with
   `TreeReference`; unifying them would remove a layer.
3. The TS `GENESIS`/empty-revision reconciliation above.
4. The base campaign branch has its own open follow-ups (see the
   research note's earlier sections) — unrelated to this branch's
   scope.
