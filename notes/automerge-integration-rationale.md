# Automerge Integration: Design Walkthrough & Rationale

**Companion to**: [`notes/automerge-integration-spec.md`](./automerge-integration-spec.md) (Draft v4)
**Date**: 2026-07-08

The spec is a reference: what changes, where, with code citations. This document is the reasoning: the design retold as a sequence of decisions, each with the alternatives that were on the table and why the chosen one won. Read this to understand *why the design is shaped the way it is*; read the spec to implement it.

## The principles that did the deciding

Four principles recur below. Naming them once keeps each step short.

- **P1 — Minimize system delta.** No new layers, formats, or protocols where existing machinery suffices. The final design changes zero storage, sync, or on-disk format code; its one engine change is a substitution inside machinery that already exists.
- **P2 — Place functionality where knowledge lives.** Merging two document siblings requires three things simultaneously: the concrete format type, the attribute's cardinality, and the sibling set. Only one place in the system has all three — the typed attribute layer (descriptor + derive). Every version of this design that put merge elsewhere had to smuggle knowledge to it, and the smuggling *was* the complexity.
- **P3 — One convention over two.** Where the system already has a deterministic rule, extend it rather than introduce a parallel one. Two rules that answer the same question ("which sibling wins?") are a standing invitation for divergent behavior.
- **P4 — Identity is load-bearing.** A reference certifies its bytes, checkable by anyone without format knowledge. Decisions that would weaken that invariant were rejected (heads-based identity) or pushed below the identity layer (compression).

---

## Step 1 — A document is one value, not many claims

**Decision.** An automerge document lives in a single `{the, of, is}` claim as an atomic `Value::Record`, opaque to the query engine.

**Why.** Dialog's alternative decomposition — model document structure as many claims — gives each fragment independent provenance and independent conflict resolution. That is precisely wrong for a CRDT: automerge *is* its own conflict resolution, applied to its own interior structure. Splitting it would put two conflict-resolution systems in charge of one artifact. The document is a single fact whose internal structure is its type's business (`record-value.md`'s core argument, already a merged decision record). This also keeps the query engine honest: it carries, stores, and compares bytes; it never interprets them.

**Alternative rejected.** Claims-per-paragraph / claims-per-span modeling. Besides the dual-resolution problem, it turns every keystroke-adjacent edit into index churn and makes document identity a query rather than a value.

## Step 2 — Store the format's natural encoding: naked `save()` bytes

**Decision.** The record payload is exactly automerge's `save()` output. No envelope, no format tag, no version byte. `Value::Record(Vec<u8>)`'s storage decode path — today an `unimplemented!()` panic — becomes a pass-through.

**Why.** Three facts align. First, the panic proves the design space is unclaimed: no deployment can hold *readable* record data, so there is nothing to migrate and no compatibility to negotiate — a one-time chance to pick the simplest encoding. Second, naked bytes make the JS story free: the wasm boundary already passes `Uint8Array` through untouched, so `@automerge/automerge.load()` consumes stored bytes directly, no unwrapping layer on the flagship platform. Third, P1: a tag or envelope is a format change, and every consumer — index keys, wasm arms, CSV arms — would need tag awareness forever, to serve a dispatch need that Step 5 shows doesn't exist.

**Alternative rejected.** A byte-prefix format tag (v1 of the spec). It existed only to let *storage* dispatch to the right merge function — a need that evaporates once merge runs where the type is known (Step 5). Removing the tag deleted a format migration, a registry, and a class of "unknown tag" failure modes in one move.

## Step 3 — Identity: canonical uncompressed bytes, hash-certified

**Decision.** Canonical stored form is `save()` with compression off and `retain_orphans: false`, pinned to one automerge version per workspace. The claim key is `blake3` of those bytes, like every other value.

**Why.** Automerge's saved form is canonical per version: replicas holding the same change-set emit byte-identical output regardless of edit order. That single upstream property is what lets documents behave like ordinary values — byte `Eq`, stable keys, and the crucial convergence trick that two replicas independently writing the same merged state mint the *same* key and collide into a no-op. Compression breaks this across builds (deflate output is not specified across library versions), so it is excluded from the identity layer and reclaimed later beneath it — the git arrangement: hash uncompressed, compress at rest (P4, P1).

**Alternative rejected (documented, revisitable).** Heads-based identity — keying a record by a hash of its automerge heads rather than its bytes. It would survive compressor and automerge-major changes, but it breaks "a reference certifies its bytes" for records: the key↔bytes binding becomes writer-asserted, so a write-capable peer could occupy a legitimate key with garbage, detectable only at decode and attributable to no one. Auditability lost silently versus size won incrementally is a bad trade, and the migration is asymmetric: moving *from* strict identity later is a re-key; moving *back* to it would also have to re-establish trust in every existing key. Start strict.

## Step 4 — Merge runs at the typed read boundary

**Decision.** Sibling merge happens when claims materialize into typed attribute values — not in storage, not at sync/pull time.

**Why.** This is P2 applied to the system's actual shape, which the codebase states outright: storage is value-blind and schema-blind *on purpose*. The tree cannot even distinguish "conflict" from "intentional cardinality-many set" — only the schema layer knows the difference, and only the typed layer knows the format. Meanwhile the correctness pressure everyone fears at read time isn't there: automerge's merge is idempotent and monotone, so folding a stale sibling into a fresh one is harmless — **no concurrency detection is required for correctness**, which means no clock, no cause traversal, and no sync-stage bookkeeping are prerequisites.

**History, because it explains the shape.** v1 put reconciliation in the pull path. That forced two inventions: a byte tag (storage had to know *that* something was mergeable) and a merge registry (storage had to find out *how*). Both were symptoms of one mistake — running merge where type knowledge had been erased, then paying to reconstruct it. v2 moved merge to the typed boundary and both inventions disappeared. v3 (this revision) discovered the boundary was even readier than v2 knew — Step 5.

**Alternatives rejected.** Sync-time reconciliation (v1: parses untrusted bytes on the hot replication path, demands schema knowledge of `dialog-repository`, adds a sync stage that every peer must agree on). Storage-side merge-on-write (same erasure problem, plus it would make writes schema-dependent).

## Step 5 — Don't build the fold; substitute into the winner selection that already exists

**Decision.** The merge hook is a format-aware *resolution strategy* slotted into `AttributeQueryOnly` — the engine's existing `Cardinality::One` winner machinery — replacing its `choose()` combining step for record-format attributes. Default stays `choose()`; nothing else moves.

**Why.** The v3 deep pass found that the engine already collapses `Cardinality::One` siblings at read time, with exactly the two strategies a fold needs: a **sliding window** over adjacent same-`(the, of)` siblings (the `SortKey` adjacency invariant guarantees grouping, preserved through the k-way merge of branch scans and overlay), and a **challenge** path that re-verifies value-bound candidates against a secondary lookup. v2 had specified precisely this machinery as new work — "a streaming group-consecutive adapter with O(1) memory" — not knowing it was describing code that already shipped. Given that, building a parallel fold adapter would violate P1 twice over: duplicate machinery, and two code paths answering "which sibling wins?" (P3). A substitution gets both scan strategies, their cost model, and their tests for the price of one seam.

**Why the seam is safe.** The strategy is a pure function of `(attribute, format)` — both already part of attribute identity — so it can ride the descriptor (where cardinality already lives) without entering serialized plans or plan identity. Two candidate wirings (a serde-skipped slot on the dynamic query, or a typed fold evaluator beside `Only` constructed before type erasure) are deliberately left to a design checkpoint: the commitment is the *shape* — one substitution point, both strategies inherit — not the plumbing.

**Alternatives rejected.** A new fold adapter above the scan (duplicates `Only`, and the challenge path's value-position semantics would need reinventing). A merge registry keyed by attribute name at the dynamic layer (v1's ghost: dispatch on erased types). Folding in the JS client only (leaves every native and typed consumer with pick-one data loss).

## Step 6 — The default merge rule is the existing winner rule

**Decision.** `RecordFormat::merge`'s default (for records without CRDT semantics) resolves exactly as `choose()` does today: higher cause wins, fact-hash tiebreak — which, with production causes all `None`, is deterministic content order.

**Why.** P3 directly. v2 specified a separate convention for records (fold in value-hash stream order, last one wins) that was *almost* the same as the scalar rule but not quite — a discrepancy with no benefit, discovered only because v3 read the implementation. Records that don't override `merge` should be indistinguishable from scalars in conflict behavior: least surprise, one set of determinism tests, one thing to explain. And there is a free upgrade built in: when `version-control.md`'s editions make cause comparison meaningful, scalars and default-merge records both graduate from content order to true causal last-write-wins *simultaneously*, because they share the rule.

**What this does not touch.** Real CRDT formats (automerge) ignore fold order entirely — merge is commutative and produces canonical bytes — so this decision only governs the LWW fallback.

## Step 7 — Readers converge immediately; storage converges on the next write

**Decision.** The fold fixes what readers *see*, at read time. Physical storage keeps both siblings until an ordinary write supersedes them — no new sync stage, no write hidden inside the read path (resolution is deliberately unscheduled — Step 8).

**Why.** The write machinery already does the right thing, all the way down: typed `Cardinality::One` asserts emit `Instruction::Replace` (this is the default typed write path, not an opt-in — a v3 discovery); `Replace` deletes *every* different-valued prior from all three indexes; the resulting diff replays exactly on other replicas; and a same-value `Replace` is a no-op, so two replicas that independently write back the same merged state converge without coordination — the canonical-bytes property from Step 3 cashing out. Sibling-until-next-write is also the lifecycle scalars already have; records just upgrade the reader's projection from "pick one" to "merge."

**Alternative rejected.** Eager physical reconciliation at sync time — rejected in Step 4 for knowledge reasons, and unnecessary here because logical convergence (what apps observe) doesn't wait for it.

## Step 8 — Nothing schedules the write-back; the next edit is the only writer

**Decision.** No read-repair, no pull-time reconciliation — removed, not deferred. *(Changed in v5; v4 briefly promoted pull-triggered reconciliation to a default.)* The fold is the only record-specific engine machinery. Storage converges when the next ordinary edit `Replace`s the siblings; documents that diverge and then go quiet keep both variants, while every reader keeps seeing the same merged doc.

**Why both schedules failed review.** Read-repair is a write smuggled into the read path — reads behaving differently by capability, commits attributed to whoever happened to look first. Pull-time reconciliation founders on layering: pull is schema-blind and cannot classify a sibling set as conflict versus cardinality-many, so making pull smart means teaching the repository schema — the v1 mistake wearing a new coat. (Running it *above* pull in the session works, but it is still machinery.)

**Why doing nothing costs less than it looks.** Correctness is complete without any schedule — the fold is deterministic on every replica. No edits are ever lost — the next commit folds-then-replaces by construction. What remains is the standing cost of quiescent diverged docs: the per-read fold tax and doubled storage/sync for that value. That is bounded, prose-scale small, and *measurable* — a diagnostic scan for multi-sibling cardinality-one record groups reports exactly how much standing divergence real usage produces. Ship the floor, measure, decide.

**The recorded evolution.** If measurement ever says otherwise, resolution decomposes coherently as detect / project / resolve: the repository reports which groups a pull touched (derived from the differential it already runs — the `blob_changes` pattern); the session that initiated the pull classifies them with the schema it holds and commits folds; and one rule governs it all — **only information-preserving merges are ever auto-written**. Record folds keep both sides' edits; `choose()` picks are lossy and stay projections forever; cardinality-many is never touched. Step one is pure composition (zero repository changes); step two — the fold landing inside the pull's merge revision, citing both parents — belongs to the version-control chapter. None of it is built; all of it is additive.

## Step 9 — Fix read-your-writes in the merge layer, not with fold policy

**Decision.** Before any record work lands (WS0), fix the transaction overlay: a pending `Replace` must shadow same-`(the, of)`, different-value committed facts at the query-time union, the way a pending `Retract` already tombstones.

**Why.** v2 flagged "uncommitted overlay claims might lose to stale tree siblings" as a fold gate needing policy (fold overlay-last, shadow rules). The deep pass showed it is not a fold problem: it is a live engine quirk *today* — tombstones are lifted from Retracts only, so mid-transaction, a scalar you just replaced can still read back as the old value whenever the old value wins content order. Roughly a coin flip, unexercised by any test. Given that, fold-specific policy would have been a patch over a defect (P1 violation: special case atop a bug). The root fix is smaller than the policy would have been — one change at the single union site where branch streams meet the overlay — and it repairs scalars and records identically, deleting the gate from the record plan entirely.

**Why WS0, first.** It is standalone, it fixes user-visible behavior that exists now, and it removes a confound from every fold test written later.

## Step 10 — Build no cause plumbing; inherit `version-control.md`

**Decision.** This design records nothing about causal parentage itself. The fold write-back is an ordinary `Replace`; whatever cause semantics the repository has, it inherits.

**Why.** Facts first: today, *every* production claim carries `cause: None` — the `Cause` field is aspirational, and the one documented population behavior (`Replace` citing the prior it supersedes) turns out not to be implemented (doc/impl mismatch; WS0 corrects the doc). Meanwhile `version-control.md` has landed on main as the designed future: `Cause(Vec<Version>)` with Lamport editions and two-tier conflict detection. Building parallel cause plumbing in this spec would collide with that design and violate P1. And nothing here needs it: Step 4's monotonicity argument means merge correctness never depends on knowing what's concurrent.

**The convergence that makes this comfortable.** When multi-parent cause lands, `Replace`'s supersession scan — which already visits every prior it deletes — is the natural single place to populate all parents. The fold write-back then automatically records "saw and superseded both siblings" with zero record-specific code. v2 listed "a merged claim can cite only one parent" as a standing risk; v3 reclassifies it as tracked, with a designed owner.

## Step 11 — Lazy decode behind a typed handle

**Decision.** Generated record fields hold `Recorded<F>` — the raw `Record` plus a phantom type — with eager encode on write and lazy, memoized decode on read via `realize()`. No eager-decode option.

**Why, in descending weight.** (a) Decoded documents cannot cross the wasm/worker boundary; bytes ship regardless, so eager decode is pure waste for every JS consumer. (b) Decode cost scales with document *history*, not size — eager decode would tax every row of a list query invisibly; lazy keeps materialization flat. (c) Generated structs keep cheap `Clone` (Arc bump) and byte-wise `Eq`. (d) Failure lands at the accessing field, not as a query-time row error — truthful, since record decode genuinely can fail. (e) The `Record` memo cache was designed for exactly this, and the fold composes with it: folding constructs the merged `Record` via `TryFrom<F>`, pre-populating the cache, so fold work double-serves the document open that usually follows.

**Encode stays eager** because identity is bytes: keys, `Eq`, and dedup all need the canonical encoding to exist at write time (Step 3).

**Residuals owned.** Fallible accessors are asymmetric with scalar fields; a naive main-thread `realize()` remains possible (mitigated by the doc-handle being the paved path); and lazy decode fixes the CPU rung only — row bytes still ship until deferred-from-storage / blob-backing (WS6).

## Step 12 — Sessions: a doc-handle, and merge work at three app-controlled moments

**Decision.** The edge crate ships a doc-handle for open editing sessions: subscribe to the attribute, merge arriving siblings into the live in-memory document, commit from the live doc. Merge work happens at exactly three moments — fold at open, absorption mid-session, optional write-back in the background — none on the sync path, none per keystroke. Threading rules are normative: fold/decode run off the UI thread; divergence adds zero to time-to-first-paint (render the deterministic winner immediately, absorb the other sibling as an ordinary late-sync delta); never block open on a remote fetch.

**Why normative rather than advisory.** The failure modes here are silent. An app that keeps committing from a pre-arrival document does a data-losing `Replace` over the concurrent sibling — the exact loss CRDTs exist to prevent, reintroduced by session mismanagement. An app that folds on the main thread janks in proportion to document history. Neither failure is visible in small tests; both are certain at scale. When correctness and latency depend on usage shape, the shape must be the paved path, so the constraints are part of the design, not a README suggestion.

**Why this costs nothing extra.** Progressive open reuses the mid-session absorption path — to the CRDT, a conflict found at open is indistinguishable from a sync that arrived early in the session. One mechanism, two duties.

## Step 13 — Notation: document what already parses now; mirror the shipped pattern for the rest

**Decision.** Ship `as: Record` immediately — it turns out to *already parse* (the notation's `as` field deserializes straight into `ValueDataType` at runtime; the docs and JSON schema simply omit `Record`), so WS5 is documentation and a conformance test. Format-qualified attributes are deferred and shaped as a **sibling key**: `as: Record` + `format: automerge/Text`. *(Changed in v4: supersedes v3's object form.)*

**Why the shape.** v2's bare qualified string collides with grammar that is already spoken for: concept references use exactly the `domain/PascalCase` shape (`diy.cook/Ingredient`). The collision matters because attribute identity is structural — `(the, type, cardinality)` — and must be computable from schema text alone, by tools that hold no name registry. v3 answered with an object form; then `feat/composite-types` shipped the real precedent: concept-typed fields are **`as: Entity` plus a sibling `conforms:` refinement** carrying the target descriptor — structural, explicitly "without a registry," storage type syntactic in `as`. Record formats mirror it exactly: the storage tag stays in `as` (no resolution needed, no collision possible), and `format:` is a sibling refinement that never touches disk. Unbound format names degrade safely to opaque records, because the storage type was never in question — and whether `format` joins attribute identity is the same question `conforms` already poses for concepts; resolve them together, at WS5, with the composite-types owners.

**Alternatives rejected.** Case or sigil conventions to distinguish formats from concepts (fragile grammar tricks, invisible to readers). A shared namespace disambiguated by a registry at load time (identity becomes registry-dependent — the exact property structural identity exists to avoid). v3's `as: { record: … }` object form — nothing wrong with it, but the codebase picked sibling keys, and two shapes for one idea is a cost with no return.

## Step 14 — An edge crate, and one automerge version

**Decision.** All automerge code lives in an optional `dialog-automerge` workspace crate. Only applications declaring automerge-typed attributes link it; the core wasm blob and native core never do. One automerge version is pinned workspace-wide, and any app running automerge in two bundles (worker + editor thread) must pin them together.

**Why.** The dependency boundary is the goal restated (Goal #3), but the version pin is load-bearing rather than hygienic: canonical bytes are stable *per automerge version* (Step 3), so version skew between two encoders in one system is silent key churn — logically convergent documents landing under different keys. Pinning turns a probabilistic drift into a build-time fact.

## Step 15 — What this design deliberately does not solve

Named so they're chosen, not forgotten:

- **High-frequency collaborative sync.** Dialog ships full canonical snapshots as durable values; automerge-repo syncs deltas. For live many-cursor editing, automerge-repo's protocol wins and this design does not compete — documents-as-durable-values is the intended model, and the spec says so plainly rather than pretending otherwise.
- **Snapshot GC.** Superseded snapshots persist in prior revisions until repository-level pruning exists — a repository concern, not a record concern.
- **Large documents.** Above roughly tens of KB, inline datums bloat tree nodes; the designed escape is a `{hash, size}` reference into the blob layer — landed with #372: a blob ordering in the artifact tree plus an entity-addressed `BlobArchive` with ranged reads and intrinsic size metadata, exactly the hydration surface WS6 needs — deferred until real sizes demand it.
- **Scheduled resolution.** No read-repair, no pull-time reconciliation — the next edit is the only writer (Step 8). The detect/project/resolve path is recorded in the spec (§4.4) for the day measurement demands it.
- **Typed fold parity in TS.** Tracks whatever typed query surface TS grows; until then, the JS discipline is the explicit helper + doc-handle (spec §6.13, §6.15).
- **Cause/CAS semantics.** Owned by `version-control.md` and the transactor work respectively; this design is written to inherit both without modification (Steps 7, 10; spec §6.6).

---

## The shape of the result

Every step above subtracts rather than adds, and the totals show it:

| Layer | Delta |
|---|---|
| On-disk format, index keys, block encoding | **None** |
| Sync / pull / push | **None** |
| Storage engine | **None** |
| Query engine | One overlay-shadowing fix (WS0, fixes scalars today) + one resolution-strategy seam in existing winner selection (WS4) |
| Type/derive layer | `Record` per the merged note; `Recorded<F>` glue (WS1, WS2) |
| New code proper | One optional edge crate: format impl + doc-handle (WS3); JS helper + tests (WS5) |
| Notation | Document `as: Record` (already parses); one deferred `format:` sibling-key decision |

The reason the deltas are small is not restraint for its own sake: at each decision point, the system already contained a mechanism whose *knowledge* matched the problem — winner selection knew cardinality, the descriptor knew the type, `Replace` knew the priors, canonical bytes knew equality. The design's job was to route the new capability through those points instead of building beside them.
