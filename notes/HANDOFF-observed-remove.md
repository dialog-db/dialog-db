# Handoff — observed-remove merge branch

**Branch:** `claude/observed-remove-merge-ukvjrc` (pushed, `local == remote`).
**Base:** forked from `claude/version-control-repository-ukvjrc` (the main
version-control stack, itself grafted onto `feat/inductive-self-negation`).
The observed-remove work is exactly **one commit** on top of that base:
`457950d feat: observed-remove merge — retire tombstones, deletion via history`.

**Design doc (read first):** `notes/observed-remove-merge.md` — status
line at the top says what landed vs. what's deferred. The convergence
audit that motivated it is in `notes/version-control.md` (retraction /
tombstone section) and the interim it replaces.

**Verification (all green as pushed):** `cargo test --workspace` = 1827
pass; `cargo test -p dialog-repository --features integration-tests` =
189 pass; `cargo clippy --workspace --all-targets --all-features` = 0
warnings; `cargo check --target wasm32-unknown-unknown -p dialog-artifacts
-p dialog-repository -p dialog-query` compiles.

## What this branch does

Retires tombstones as the representation of deletion. **The log (history
region) is the truth; the active index is a cache of the live fold.** A
retraction deletes the fact's EAV/AEV/VAE keys outright and appends a
signed retract record — the record is the durable carrier of the
deletion, and a replica's **causal context** (per-origin watermark of
its head's ancestry) is what stops a stale peer from resurrecting it.
This is an optimized observed-remove set (OR-set); `(Origin, Edition)`
versions are its dots, the revision DAG is its causal context.

### Files changed (diff `43fdb45..457950d`)

- **`rust/dialog-artifacts/src/history/context.rs`** (new) — `Context`
  (a `BTreeMap<Origin, Edition>` watermark) with `observes(v) ⇔
  v.edition ≤ context[v.origin]`, exact by the sequential-origin
  invariant. `context_of(head, history)` derives it by an O(ancestry)
  walk of revision records.
- **`rust/dialog-artifacts/src/merge.rs`** (new) — the merge screen.
  `screen_history` (R3 + record appends) and `screen_data` (R1),
  region-scoped by key tag via `history_scope()` / `data_scope()`. R2
  is the tree's own byte-guarded remove, untouched.
- **`rust/dialog-artifacts/src/tree.rs`** — retract now deletes keys
  unconditionally (removed the tombstone branch and the base snapshot);
  the interim `State<Datum>: prevails_over` deletion-wins override is
  gone (`impl TreeValue for State<Datum> {}` is now empty).
- **`rust/dialog-repository/src/repository/branch/pull.rs`** — merge
  direction **flipped**: integrate the upstream delta onto the *local*
  tree (receiver's context guards its own cache) instead of replaying
  local onto upstream. Two screened differentials (history-scope then
  data-scope) are chained into a **single** integrate pass so coverage
  (R3) is ordered before data adds (R1) contest the same slots. New
  no-op case: `merged == local` → local head stands, advance only the
  sync base.
- **`rust/dialog-artifacts/src/artifacts.rs`** — renamed the obsolete
  `it_tombstones_when_retracting...` test to
  `it_deletes_from_the_indexes_when_retracting_a_committed_fact`.

### The three merge rules (in `merge.rs` module docs)

- **R1** (`screen_data`): an incoming *live* data claim the receiver has
  **observed** (in local head's ancestry) is never re-applied — if still
  live locally it's a no-op, if not it was covered by a local record and
  re-applying would resurrect. Unobserved claims are news → pass through
  (same-value byte-variant contests fall to the tree's deterministic
  hash race).
- **R2**: incoming guarded removes pass through; the tree applies them
  only on exact byte match.
- **R3** (`screen_history`): each incoming *covering* record (retraction
  `cause`, or replace `supersedes`) emits guarded removes for the
  covered versions still live locally. This is how a deletion reaches a
  replica whose sync base never covered the fact (empty-base pull).
  Coverage matches by **version, not value**: a replace supersedes
  claims of *other* values, which live at other keys (keys embed the
  value hash), so the screen scans the record's `(entity, attribute)`
  slot in the receiver's snapshot rather than probing the record's own
  keys. (The original landing probed the record's keys, which silently
  made replace coverage a no-op; fixed with the stale-peer replace
  acceptance test `it_retires_a_replaced_value_on_an_empty_base_pull`.)

## Convergence evidence (tests to keep green)

In `rust/dialog-repository/src/repository/branch/pull.rs`
(`history_tests`):
- `it_does_not_resurrect_a_deleted_fact_on_pull` — now **deterministic**
  (was ~8/20 failures under the interim's hash-race; run it in a 20×
  loop to confirm).
- `it_quiesces_after_concurrent_identical_asserts` — mutual pulls reach
  identical trees in bounded rounds.
- `it_keeps_a_concurrent_assertion_the_retraction_never_observed` — the
  Alice/Bob/Mallory/Jordan OR-set acceptance test (cardinality-many).
- `it_resurrects_a_deleted_fact_and_the_resurrection_survives` —
  re-assert survives an empty-base pull from a stale peer.

`context.rs` has unit tests for `observes` and `merge`.

## Deferred (documented in `notes/observed-remove-merge.md`)

1. **Persist the watermark vector in the tree.** Right now `pull`
   recomputes `context_of` per merge (O(ancestry) walk). Design calls
   for a small `dialog.`-reserved record maintained incrementally
   (`context(commit) = context(parent) + own version`; `context(merge) =
   union(parents) + own`), so fresh partial replicas read it in one
   fetch. This is the main perf follow-up.
2. **Fold `Replace` into R3.** `Replace` still hard-deletes the
   superseded value locally. Its `supersedes` records flow through R3
   for *remote* coverage (the slot-scan fix above is what actually made
   this true), but the local hard-delete is unchanged — folding it in
   would route local and remote supersession through one code path.
3. **`State::Removed` variant** still exists for reading legacy trees;
   the screen drops it on ingest. Fully removing the enum variant is a
   serialization-format change, deliberately not done.

## Gotchas / invariants a new agent must respect

- **Origin-sequential invariant is now safety-critical.** `observes()`
  is exact *only* because one origin = one sequential actor (a
  branch-lineage + issuer). Origin is lineage-scoped
  (`Revision::origin_of(lineage, issuer)`), so in-repo multi-branch
  tests are valid. Two revisions claiming the same version is protocol
  corruption (documented in `notes/version-control.md`).
- **Merge reads only the receiver's own state** (its snapshot +
  context) plus the incoming differential — never the sender's context.
- **Partial replication is unaffected:** "observed" is *logical*
  (ancestry), not "blocks on disk." R1 is an in-memory vector lookup;
  R2/R3 read only the differential with the existing lazy remote
  fallback.
- The screened differentials **must** stay ordered history-before-data
  in the chained integrate pass (R3 before R1). Don't split them into
  two persists — an earlier attempt did and hit "blob not found"
  because the first pass's new nodes live only in the in-memory delta.

## Not done

- **No PR opened** for this branch (or the base). Open one only if asked;
  if you do, it should target `feat/inductive-self-negation` (the graft
  base), and check for a PR template first.
- History has been rewritten once already (authorship reset and GPG
  signing); commits from the graft base onward are authored and signed
  by Irakli.
