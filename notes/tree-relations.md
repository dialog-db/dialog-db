# Tree-Inspection Relations — Implementation Strategy

Status: **core implemented** (see "What landed" below). Revision 4 —
the serving mechanism is a general engine facility, **procedures**
(moded premises over *idempotent effects*; called "operations" in revision 3),
replacing revision 2's reserved `dialog.tree/*` attributes intercepted
inside `Provider<Select>`. The
goal is unchanged: let tonk's tree inspector
(<https://github.com/tonk-labs/tonk/blob/staging/plan/tree-inspector.md>)
run its `tree/*` predicates through ordinary dialog queries — retiring
the custom worker endpoint that currently intercepts those predicates
and bypasses the evaluator — without breaking differential
subscriptions. Validated against the current tree format (separator
links, variable-length keys, novelty buffers, scale) and against what
tonk ships as of [tonk#635](https://github.com/tonk-labs/tonk/pull/635).

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
discipline, self-contained multi-field rows, lazy child loading,
separator-aware key decoding — ports over and is kept by this design.
The flat row shape in particular is kept: procedures return relational
rows natively, which is what revision 2's EAV attribute encoding had to
contort around (synthetic link entities) and now doesn't.

## Goal and non-negotiable constraint

The port must make the same queries answerable by dialog itself —
composable with other premises, usable from branch *and* transaction
queries, and visible to standing subscriptions — **without breaking
differential subscriptions**.

The subscription system is sound because every read a query performs is
visible to the maintenance machinery: selects flow through
`Provider<Select>`, where demanded ranges are recorded
(`QueryEnv::record_demand`,
`rust/dialog-repository/src/repository/branch/session.rs:322`, invoked
first thing in `execute` at `:411`), and the poll gate diffs the tree
within that cover. Any read the machinery cannot account for makes
standing queries silently stale. Procedures extend the accounting with
a second admissible read kind: **an effect whose result can never
change** — for which there is, by construction, nothing to account.
So the generalized rule this design never violates: **every read a
query performs is either a demand-recorded selector or a
certified-idempotent effect performed through the query environment.**

## Why this is sound (keep this argument in the module docs)

Two facts make tree relations compatible with differential
subscriptions:

1. **Every tree fact is content-addressed and therefore immutable.**
   `Node(<hash>)`'s kind, size, links, and entries can never change — a
   different tree is a different hash. This now includes novelty: a
   node's hash covers its buffered ops
   (`rust/dialog-artifacts/src/buffered.rs` module doc), so "this
   node's pending novelty" is as immutable as its entries. Rows derived
   from a node hash are permanent; they can become unnecessary, never
   wrong. (This is the same argument that makes `CausalityCache` in
   `dialog-artifacts` require no invalidation.) In effect terms:
   `archive` reads (`dialog_effects::archive::Get` — content-addressed
   catalog fetch) are **idempotent**.
2. **The only mutable fact in the domain is "what is the current
   root?"** — carried by
   [`BranchRevision.tree`](../rust/dialog-repository/src/schema.rs)
   (`dialog.branch/tree`), injected into every query's metadata overlay
   (`repository/branch/metadata.rs`, `QueryLayer::metadata` in
   `session.rs`). In effect terms this is `memory`-domain territory
   (`Resolve` on a CAS cell) — **not** idempotent, which is exactly why
   it must stay a tracked fact and must never be modeled as a
   procedure.

The capability domains draw the line for free: **archive effects are
idempotent; memory effects are not.** Tree procedures use only archive
effects, and queries reach the mutable world exclusively by joining
through `BranchRevision`:

```text
BranchRevision(branch, tree: ?root)
  ⋈ tree/node(of: ?root, kind: ?k, size: ?s)   ← procedure
  ⋈ tree/link(of: ?root, node: ?c)             ← procedure, descend
  ⋈ tree/node(of: ?c, …)
```

(No glue formula: procedures take the same base58 string the
`BranchRevision.tree` fact carries.)

When a commit lands, re-evaluation re-binds `?root` and the join walks
the new tree. Per-hash rows never invalidate.

## The head-tracking gap (independent of the serving mechanism)

Revision 1 claimed "standing queries that depend on `BranchRevision`
already re-evaluate when the head moves — no new machinery". **Reading
the current poll path says otherwise; treat the claim as false until
test 7 passes.**

The gate in `Subscription::poll`
(`repository/branch/subscription.rs:381`) is: overlay epoch moved ⇒
recompute; else revision moved ⇒ `touched()` diffs the two roots
*scoped to the demand cover*; `Touched::Nothing` ⇒ **the pin advances
silently and no delta is delivered** (`:407-410`). Now trace a
subscription whose only premises are `BranchRevision ⋈ tree-reference ⋈
tree/node`:

- `BranchRevision` is **overlay-injected metadata**, rebuilt fresh per
  evaluation (`metadata.rs`) — never a fact in the tree. Its selector
  records demand over `dialog.branch/*` EAV ranges that **no committed
  fact ever occupies**.
- The tree procedures record no fact demand at all (idempotent — see
  below), correctly.
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
  `dialog.branch/name`/`replica`, which are stable per branch).
- `poll` checks it before the diff gate: epoch unchanged, revision
  moved, `demand.head` ⇒ go straight to full re-evaluation.

This adds no waste: any query that reads `dialog.branch/tree` has a
result that genuinely changes on every commit (the binding itself
changes), so a per-commit recompute is semantically forced, not a
regression of the diff gate's frugality. Data-fact subscriptions that
never read revision metadata are untouched. Encode the gap as a failing
test first (test 7 below), then land the flag.

## Procedures: the general facility

A **procedure** is a premise kind of its own, not a kind of formula —
the resemblance to formulas begins and ends at the parameter
machinery: named slots with `Requirement::Required` input cells, and
the `estimate() → None` protocol that makes an unbound-input premise
non-viable until a join binds it. That machinery (`Cells`,
`Requirement`, the planner's feasibility check) is engine-level and
reused verbatim. Everything past the cells is different: evaluation
performs a capability-authorized effect through the query environment
instead of computing in-process. "Idempotent" here means *replayable
forever*: same bound inputs ⇒ same rows, at any later time, on any
replica that can perform the effect. Content-addressed reads are the
canonical case; anything observer- or time-dependent is not admissible.

Why this beats reserved attributes (revision 2's design), point by
point:

- **No special names.** Nothing is smuggled through the EAV fact
  namespace; no write-path reservation is load-bearing; no interception
  hidden inside `Provider<Select>` behind the planner's back. The
  procedure registry is the same kind of namespace `math/sum` already
  lives in.
- **Capability-gated, not name-gated.** The effect is performed as a
  `Capability` invocation (`Subject → attenuate(Archive) →
  attenuate(Catalog) → invoke(Get)`, per `dialog-effects`), so
  authorization composes: a session whose capability set does not
  include archive reads for the branch's subject cannot run tree
  inspection, locally or remotely, with no extra enforcement code.
- **Relational rows.** A procedure returns multi-field rows
  (`tree/link` → at, node, separator, scale, novelty per row), which
  the EAV encoding could not express without synthetic per-link
  entities.
- **Honest accounting.** Idempotent reads record no fact demand because
  none is needed — rather than recording fake ranges and arguing they
  are harmless.

### Shape

Sketch (final naming open — anything but "formula": "procedure",
"procedure", "derivation" all work; the `dialog-operator` crate is
about authority, not queries, so watch the collision):

```no_run
# use dialog_query::{Cells, EvaluationError};
# struct Bindings; struct Match;
/// A formula-shaped premise resolved by performing an idempotent
/// effect through the query environment.
trait Operation: Sized {
    /// The effect this procedure performs. Must be certified
    /// idempotent (sealed marker trait) — e.g. archive::Get.
    type Effect;

    /// Moded parameter slots, exactly as Formula::cells — inputs are
    /// Required, outputs Optional.
    fn cells() -> &'static Cells;

    /// Build the effect invocation from the bound inputs.
    fn invoke(input: &Bindings) -> Result<Self::Effect, EvaluationError>;

    /// Project rows from the effect's output. Pure.
    fn project(output: <Self::Effect as Returns>::Value, input: &Bindings)
        -> Result<Vec<Match>, EvaluationError>;
}
# trait Returns { type Value; }
```

The split matters: **all mutability lives in the effect, all logic in a
pure projection.** The procedure is idempotent iff its effect kind is,
and effect kinds are certified once, by a sealed marker trait
(`IdempotentEffect`), implemented for `archive::Get` first and nothing
else. `memory::Resolve`/`Publish` must never receive the marker — that
is the crisp boundary between the immutable universe (procedures) and
the mutable world (facts + the head flag).

### Engine integration

- **Premise kind**: a new `Proposition::Operation(OperationQuery)`
  variant (`dialog-query/src/proposition.rs:32`) and matching
  `Plan::Operation` node. Evaluation is async with env access — the
  precedent is `Plan::Scan`, whose stream awaits `Provider<Select>`
  inside `Plan::evaluate<Env: Provider<Select<'_>> + …>`
  (`planner/plan.rs:168`); procedures extend the env bound with the
  provider for their effect (in practice `Provider<archive::Get>`-
  shaped, which `QueryEnv` can supply the same way it supplies
  `Select` — through the branch's `NetworkedIndex` + node cache, so
  remote fallback and caching behave exactly as branch reads do).
- **Modes/planner**: `Requirement::Required` on the input slots means
  `estimate() → None` while the input is unbound — the planner refuses
  to schedule the premise until a join binds it. The "node-bound only"
  contract of revision 2 falls out of modes instead of a runtime
  selector error, and the planner can order procedures after the
  premises that bind their inputs, costed like formulas
  (`PARAM_COST`-style base + a fetch constant).
- **Incremental maintenance**: classify procedures exactly as formulas
  are classified in `fixpoint.rs::classify_base` — `Inert`. Their
  outputs change only when their inputs change; a changed input
  re-derives through the procedure in the delta-join. This is *sound
  only because of the idempotence certificate* — say so in the
  `classify_base` match arm.
- **Demand**: procedures record nothing in the demand cover. The
  mutable anchor (`BranchRevision`) is what re-fires the subscription
  (head flag), and the re-evaluation replays the procedures against the
  new root binding.
- **Serialization**: mirror `FormulaQuery`'s
  `{"assert": "<name>", "where": {…}}` tagged form
  (`formula/query.rs:37-56`) in an `define_procedures!` registry. On
  the wire this is exactly the shape tonk already speaks (a string
  predicate naming the operator, terms in `where`), so the migration
  path for tonk is: same names, same rows, delete the interception.
- **Capability derivation**: the wire query never names a subject; the
  session env already scopes evaluation to the branches in the
  `QueryLayer`, and the procedure's effect is invoked against those
  branches' archive capabilities (first branch whose catalog has the
  block wins — content addressing makes them interchangeable). A
  remote evaluator performs the same invocation under the caller's
  delegated capabilities; attenuation is the authorization story.

### Failure semantics

- Input that decodes but whose block is absent everywhere → zero rows,
  consistent with "unreplicated contributes nothing". (Caveat: a
  subscription that read zero rows for an unreachable block will not
  refire when connectivity returns — nothing moved the head. Same
  behavior partial replicas exhibit elsewhere; document, don't
  engineer around.)
- Malformed input (bad hash string, wrong length) → zero rows,
  mirroring the forged-record-projects-nothing convention.
- Asking `tree/link` of a segment, or `tree/key` of an index → zero
  rows (not an error — lets a query union over mixed levels).
- Effect *transport* failure (storage error mid-stream) → propagate as
  an evaluation error, like a failed select.

## The tree procedure library

Procedures (all inputs `Required`; every row self-contained, in tonk's
proven shape; large segments streamed, not collected — nodes can be
~150 KB):

- **`tree/node`** — input `of` (node reference); one row:
  `kind` (`"index"`/`"segment"`), `size` (encoded byte length),
  `count` (links or entries), `bound` (the node's upper-bound key
  bytes, `PersistentNode::upper_bound`, absent for empty), `rank`
  (rank of the bound under the node's own embedded manifest —
  `Distribution::rank(key, manifest)`,
  `dialog-search-tree/src/distribution.rs:144`), `scale` (the node's
  `Scale` byte).
- **`tree/link`** — input `of`; one row per link of an index node
  (`Link { separator, node, scale }`,
  `dialog-search-tree/src/link.rs`): `at` (position), `node` (child
  reference), `separator` (bytes; empty = the level's leftmost/−∞),
  `scale` (advisory subtree size), `novelty` (count of hitchhiker ops
  buffered against this subtree, from `PersistentIndex.novelty`,
  `node/persistent.rs:635` — the window into buffered-vs-canonical
  cost, covered by the node's hash so immutability holds).
- **`tree/key`** — input `of`; one row per entry key of a segment
  node (bytes, in entry order). Entry *values*/states are v2 (see out
  of scope).

Node references travel as the same base58 string `dialog.branch/tree`
uses (`ToBase58` in `repository/branch/metadata.rs:76-79`) — no new
entity scheme is needed now that rows are not EAV facts; whether to
also mint a `tree:` URI form is a presentation choice, not a
requirement.

Pure formulas (ordinary `Formula`s in `dialog-query/src/formula/`,
registered in `define_formulas!` beside `dialog/revision`):

1. **`dialog/tree-reference`** — glue from the `BranchRevision.tree`
   base58 string to the node-reference value the procedures take (and
   back). Malformed base58 → zero rows.
2. **`dialog/key-part`** — decompose a full, variable-length index key.
   Input `of: Bytes`; output **one row per component**:
   `at` (position), `kind` (`index` / `entity` / `attribute` / `vtype`
   / `value` / `spill` / `origin` / `edition` / `blob`), `text` (human
   rendering), `bytes` (raw). Build on the key views
   (`EntityKey`/`AttributeKey`/`ValueKey` over `key/varkey.rs`),
   dispatching on the tag byte — entity(0), attribute(1), value(2),
   history(3), blob(4), coverage(5), from
   `dialog-artifacts/src/constants.rs` — exactly as tonk's `key_parts`
   does, including the spilled-value arm and the history/coverage
   `origin ‖ edition ‖ fact-tail` shape. Unparseable under its tag's
   schema → a single `kind: "opaque"` row (never zero rows for
   non-empty input: the inspector must always have something to show).
3. **`dialog/separator-part`** — same output shape over a link
   separator. Separators are front-coded *prefixes*: the column framing
   a full-key parse relies on lies past the truncation, so this formula
   is lenient — the tag chip, as many leading components as the prefix
   carries, an opaque remainder chip; empty input → one `kind: "min"`
   row (the −∞ separator). Keeping it separate from `dialog/key-part`
   mirrors tonk's `key_parts`/`separator_parts` split and keeps the
   strict/lenient contracts honest.

### What must NOT become a procedure output

Tonk's `tree/child` rows carry `cached: bool` — whether the child's
block is in the local archive. **This field cannot be a procedure
output.** Locality is mutable without a commit (a block arrives when
someone expands it, or a replication task lands it): a procedure
surfacing it would not be idempotent, and a standing subscription
would go stale with nothing in the machinery to notice. The same
reasoning excludes fetch latency and connection state — and, on the
other side of the boundary, the current head itself
(`memory::Resolve`), which stays a tracked fact. The inspector *does*
want to visualize locality, though — see "Locality as a versioned
source" below for the sound path.

Also excluded outright: node depth/level — the same node can sit at
different depths under different roots; depth is a property of the
inspector's descent, not of the node.

### Locality as a versioned source (v2)

Idempotence is the degenerate case of the invariant that actually
keeps subscriptions sound: **every premise's result is a pure function
of its bound inputs and the pinned versions of the mutable sources it
read.** The gate in `Subscription::poll` is already a pin-comparison
vector — the tree pins the root hash, the session overlay pins its
epoch, and the head fix adds the revision pin. Procedures are sound
with *zero* pins. Locality is inadmissible today only because the
local block set has no version to pin: nothing can tell a poll that a
`resident?` answer moved.

So give it one:

- The branch's local archive maintains a monotone **generation**
  counter, bumped whenever a block lands locally (a storage-layer
  hook; batch writes bump once). Between polls, bumps coalesce — the
  pull-driven model absorbs replication burstiness for free.
- A second premise tier beside procedures — call it an **observation**
  — with the same cells/mode machinery but a different certificate:
  its effect kind (`archive::Contains`-shaped, local-only, *no* remote
  fallback) is not `IdempotentEffect` but `VersionedEffect`: the
  provider returns the answer *and* the source generation it observed.
- `Demand` records the pin: `sources: Vec<(SourceId, Generation)>`
  (the head flag generalizes into this vector — `(branch cell,
  revision)` is the same shape). `poll` re-evaluates when any pinned
  source moved; observation-bearing subscriptions are never
  incrementally maintained, only recomputed — correct, since a
  generation move can flip any `resident` bit.
- The inspector's residency dots then update through ordinary
  subscription deltas as replication lands blocks — which is exactly
  the visualization wanted, live.

Sequencing: land procedures (zero-pin) and the head flag first; the
pin-vector generalization and the archive generation counter are a
separate, later change. Until then locality stays out of the engine
(client-side inference or a non-subscribable diagnostic call).

### Prior art: separating pure/idempotent effects from mutable ones

The pure / read-only / mutable stratification is well-trodden;
the closest neighbors, most relevant first:

- **CALM / Bloom / Dedalus** (Hellerstein, Alvaro): monotone logic
  needs no coordination; non-monotone operators must be stratified
  against an explicit notion of time. Block arrival is monotone
  (content-addressed, append-only — a procedure's world), while
  `resident?` is a **non-monotone observation of a monotone process**
  ("do I have it *yet*"), and Dedalus's answer is exactly the one
  above: stamp the observation with the time (generation) it was made
  at and stratify on it.
- **Materialize / differential dataflow**: every source must carry
  versions (timestamps) for incremental view maintenance to be sound;
  sources that can't are declared `VOLATILE` and excluded from the
  incremental guarantees — the observation tier is precisely a
  volatile source with a coarse (generation-level) timestamp.
- **Nix fixed-output derivations**: a derivation may perform network
  I/O *iff* its output's hash is pinned in advance — idempotence by
  content verification. That is `archive::Get`'s admission certificate
  stated as a build-system rule, and a good intuition for why the
  sealed marker is per effect kind, not per call site.
- **FX / Gifford–Lucassen effect classes** and region systems
  (Tofte–Talpin): the original read/write/alloc-per-region effect
  lattices, with *effect masking* (unobservable effects are pure) —
  the ancestor of "benign effects", the term of art for effects
  invisible to the semantics (memoization, laziness,
  content-addressed reads).
- **Algebraic effect theories** (Plotkin–Power): effects come with
  equations; the state theory literally contains lookup-idempotence
  (`get; get ≡ get`) and read-only state has a comodel/Reader
  presentation. "Which equations does this effect satisfy" is the
  formal version of the `IdempotentEffect` marker.
- **F\*'s effect lattice** (`PURE ≤ DIV ≤ STATE ≤ ALL`) and Koka's
  effect rows: graded effect types where "reads immutable state" is a
  point strictly between pure and stateful — the type-system shape of
  formula < procedure < observation.
- **Datomic**: "the database is a value" — queries run against
  immutable snapshots; the only mutable thing is the connection's
  current-basis pointer, kept outside the query language. That is the
  `BranchRevision`-as-sole-anchor architecture, independently arrived
  at.

## Subscriptions: what to build, and what to verify

Machinery to build: the `Demand::head` flag (above) and the `Inert`
classification for procedure premises. Everything else is verification:

- A standing query shaped `BranchRevision(branch, tree) ⋈
  tree-reference ⋈ tree/node(root, …)` re-fires after a commit and
  reflects the new root (via the head flag; write the test to fail
  before the flag lands).
- Operation rows never appear in the tree, so the incremental
  maintainer never sees them in additions/deletions; their premises
  re-derive from changed inputs like formulas do.
- Head-flagged subscriptions take the full-recompute path per commit by
  design; incremental maintenance of mixed fact+tree queries is not a
  goal (the revision binding changes every commit, so every commit is a
  real delta for them).

## What this unlocks / defers

- Tonk's chained point queries (`node → links → node → keys →
  decomposition`) become ONE dialog query — joins, not client
  round-trips per level — usable from branch and transaction queries
  (`tx.query()` builds the same `QueryEnv`), live under subscriptions.
  The worker's predicate interception and the custom endpoint can be
  deleted; the wire shape (string operator name + `where` terms) is
  what tonk already speaks. `dialog-arboretum` (the UI) stays, driven
  by a subscription instead of re-fetch-on-demand.
- Procedures are a general facility: future idempotent-effect premises
  (blob metadata by digest, record fetch by content address, packfile
  introspection) reuse the same admission rule — certified-idempotent
  effect + pure projection — instead of each inventing a side channel.
- Declarative subtree traversal ("all nodes under this root", "bytes
  per subtree") is a recursive rule over `tree/link`. It is **blocked
  on** the goal-directed fixpoint
  (`notes/goal-directed-fixpoint.md`): the full-closure evaluator's
  seed round evaluates with unbound inputs, which the procedure's
  Required mode refuses — the right failure mode. Do not weaken modes
  to make full-closure traversal pass; implement demand seeding
  instead.

## Tests

In `dialog-query` (engine-level, with a stub idempotent effect +
in-memory provider):

1. Operation premise with unbound input is non-viable
   (`estimate() → None`); binding it through a join schedules it.
2. Multi-row projection joins onward (procedure output feeding a
   second procedure's input — the descent chain).
3. `classify_base` treats a procedure premise as `Inert`.
4. Unit tests for all three pure formulas (round-trip base58; per-tag
   decomposition incl. history and blob keys; spilled values;
   separator prefixes incl. empty; wrong-length and malformed inputs).
   Model on `formula/revision.rs::tests`.

In `dialog-repository` (integration, model fixtures on the existing
revision-projection and subscription tests):

5. `it_reads_the_root_node_through_the_query_engine` — commit a few
   facts; `BranchRevision ⋈ dialog/tree-reference ⋈ tree/node` as ONE
   query — kind ∈ {index, segment}, size > 0, count > 0.
6. `it_descends_through_links_and_decomposes_keys` — follow
   `tree/link` a level, `tree/key` at a segment, `dialog/key-part`
   over the bytes; assert tags within the known set and component
   kinds per tag.
7. `it_refires_a_tree_subscription_on_commit` — standing query over
   `BranchRevision ⋈ tree-reference ⋈ tree/node`; commit; assert the
   subscription delivers rows for the *new* root. **Write this first;
   it is expected to fail against today's poll gate** and is the
   evidence for the `Demand::head` flag.
8. `it_keeps_old_roots_queryable` — capture root₁, commit again;
   `tree/node(root₁)` still answers (content-addressed history) and
   differs from `tree/node(root₂)`.
9. `it_yields_nothing_for_an_absent_node` — a well-formed reference
   whose block is nowhere → zero rows, no error.
10. `it_serves_tree_procedures_in_transaction_queries` — the
    root-node query through `tx.query()`.
11. `it_surfaces_link_novelty` — commit through the buffered path so
    an index node holds novelty; assert `tree/link.novelty` > 0 for
    the affected link and 0 elsewhere; `canonicalize()` and assert all
    zeros.
12. `it_denies_tree_procedures_without_archive_capability` — a session
    whose capability set lacks archive read for the subject gets an
    authorization error, not rows (the capability story is real, so
    test it).

## Acceptance checklist

- [ ] `cargo test --workspace` green (existing suites untouched).
- [ ] `cargo test -p dialog-repository --features integration-tests` green.
- [ ] `cargo clippy --workspace --all-targets --all-features` clean.
- [ ] `cargo check --target wasm32-unknown-unknown -p dialog-query -p dialog-repository -p dialog-artifacts`
      compiles (the inspector's whole point is running against
      IndexedDB-backed wasm builds; procedures must not introduce
      native-only bounds — use `ConditionalSend`/`ConditionalSync`).
- [ ] Test 7 demonstrated failing before the `Demand::head` fix, green
      after.
- [ ] The `IdempotentEffect` marker is sealed, implemented exactly for
      `archive::Get`, with the memory-domain counterexample in its
      docs.
- [ ] Doc comments on the procedure facility carry the soundness
      argument from "Why this is sound", including the
      novelty-under-hash point and the `cached`-exclusion rationale.

## Out of scope

- Entry *values* / states (`tree/entry` as key + `State<Datum>` rows):
  v2 — needs a decision on surfacing tombstones and decoded values;
  keys alone serve the inspector's size/boundary analysis, and
  `dialog/key-part` already exposes the value component of the key.
- Manifest fields as rows (`version`, branch factor, …) — add to
  `tree/node` later if needed.
- The root *default* (tonk's bare `tree/node` = current root): in
  dialog the root is one join away through `BranchRevision`; a
  defaulting convenience premise would re-smuggle the mutable head
  into a procedure. Don't.
- Recursive subtree aggregation (blocked on
  `notes/goal-directed-fixpoint.md`).
- Any write path for tree data through procedures (archive `Put` is
  idempotent too, but procedures are a *read* facility; effects with
  observable external consequences are out until a separate design
  says otherwise).

## Gotchas

- **Idempotence is the whole contract.** The marker trait must stay
  sealed; the moment a non-idempotent effect (a memory resolve, a
  clock, a random source) becomes a procedure, subscriptions rot
  silently and undetectably. This is the procedures-era restatement of
  "never bypass `record_demand`".
- **Separators are not bounds.** Links store lower-bound separator
  prefixes (`link.rs`); nodes have upper bounds (`upper_bound()`). The
  row schema reflects both, and the decomposition formulas differ
  (strict vs. lenient) for exactly this reason.
- Procedures must read through the branch's `NetworkedIndex` + node
  cache (as `Subscription::touched` does, `subscription.rs:509-526`)
  so remote fallback and caching match branch reads; a bespoke fetch
  path would fork behavior.
- Large nodes: stream projected rows; do not collect a 150 KB
  segment's keys into a `Vec` eagerly.
- `Formula::compute` stays sync and storage-free; do not "extend"
  formulas with async instead of adding the procedure kind — the
  maintainer's `Inert` classification and the demand story both lean
  on formulas staying pure.
- wasm: no threads, no native-only sync primitives; the procedure
  evaluation path must be `Send`-general on native and single-threaded
  on wasm exactly as `Plan::evaluate` already is.
