# Observed-Remove Merge — the First-Principles Design

Status: **implemented** on `claude/version-control-review-ukvjrc`
(`history::Context` + `history::context_of`, `crate::merge` screening,
observed-remove retract path, two-pass screened pull). This is the
answer to "forget what we have — what is the *right* solution?", written
after the convergence audit and the retraction-semantics thread. It
replaces tombstones, covered sets, and slot-level conflict rules with a
single reframe.

What landed, mapped to the design below: `Context` is the per-origin
watermark (derived by `context_of`; not yet persisted in the tree — the
pull recomputes it from the local head's ancestry, an O(ancestry) walk
that is the documented optimization target). `merge::screen_history` +
`merge::screen_data` implement R1/R2/R3, run as one chained pass with
history changes ordered before data changes. Retract deletes the fact's
active-index keys and appends its signed history record — no tombstone.
The interim `State<Datum>: prevails_over` deletion-override is removed;
`State::Removed` survives only as a legacy-tree variant the screen drops
on ingest. Deferred: persisting the watermark vector, and folding
`Replace`'s hard-delete into the same coverage machinery (its
`supersedes` records already flow through R3, but the local hard-delete
is unchanged).

## Requirements (accumulated from the thread)

1. **Strong eventual consistency**: replicas that have exchanged the
   same events reach identical states — any merge order, any direction,
   no coin flips.
2. **Observed-remove semantics**: a retraction removes exactly the
   assertions its author had observed. Alice retracts Bob's assertion
   of v; Mallory concurrently asserts v — v stays visible (hers was
   never retracted). Jordan, having seen both, retracts — v is gone
   everywhere. Cardinality-many queries show exactly the unretracted,
   unsuperseded facts.
3. **Resurrectability**: deletion is not forever; a re-assert works and
   is causally expressible.
4. **No storage ballooning**: active-index cost independent of how many
   times a fact was toggled; ideally no tombstones at all.
5. **Cheap reads** (no history walks) and **cheap merges** (no per-key
   history traversals).

## The reframe

Every pathology in this thread came from one architectural habit:
**encoding deletion as state in the active index** and asking a
key-by-key slot join to resolve it. Tombstones, hash races, covered
sets, watermarks-per-slot — all of it is deletion-as-state.

The unconstrained answer inverts this:

- **The log is the truth.** The history region already stores every
  claim, immutable and content-addressed, with cause pointers. The
  union of two logs is a grow-only set — its merge is trivially
  commutative, associative, idempotent. *Nothing at the storage layer
  ever needs conflict resolution.*
- **The active index is a cache of a fold.** Define visibility
  declaratively: *a claim is live iff no record in the log covers it*
  (a retraction naming it in `cause`, a replace naming it in
  `supersedes`). The active index materializes exactly the live set —
  plain data, **no tombstone variant at all**. Deleted and never-existed
  are indistinguishable in the cache, and that is now fine, because the
  cache is not the carrier of deletion; the log is.
- **Observation is state the system already has.** The question a merge
  must answer is "have I *seen* this incoming claim before (and
  dropped it), or is it news?" — and dialog's revision DAG answers it
  exactly. A claim is observed iff its producing revision is an
  ancestor of my head. Because an **origin is a sequential actor**
  (the documented origin invariant) and **edition is a Lamport
  timestamp**, ancestry restricted to one origin is a prefix: the
  entire observation set compresses to a per-origin watermark —

  ```
  context(head) = { origin → max edition of that origin in head's ancestry }
  observed(claim) ⇔ claim.version.edition ≤ context[claim.version.origin]
  ```

  This is *exact*, not approximate — the sequential-origin invariant is
  what makes a version vector over origins a lossless summary of the
  ancestry. It is derivable from the head (log walk with skip links)
  and cacheable by head hash: `#origins × 40 bytes`, growing with
  devices/authors, never with writes.

  In CRDT terms: **the revision DAG is the causal context of an
  optimized OR-set, and `(Origin, Edition)` versions are its dots.**
  The version design built for conflict detection turns out to be
  precisely the structure that makes observed-remove tombstone-free.

## The merge

Pull integrates **the upstream's delta since the sync base onto the
local tree** (note: today's code replays the local delta onto the
upstream tree; this design flips the direction so the receiver's
context guards its own cache). Three rules, all O(1) per changed key:

- **R1 — incoming assertion `c` at key K:**
  - K locally live with the same claim → nothing to do.
  - `¬observed(c)` → news: accept (add to K; if K already holds a
    *different* claim of the same value — same key, differing version
    metadata — keep the deterministic hash winner: both assert the same
    value, so either is semantically identical).
  - `observed(c)` and K not locally live → I have already seen this
    claim and something covered it: **reject**. This is what makes a
    stale peer's copy unable to resurrect a deletion — with no
    tombstone anywhere.

- **R2 — incoming guarded remove `Remove(K, c)`** (upstream dropped a
  key since base): apply iff the local K holds exactly `c`. This is
  the *current* integrate guard, unchanged — it is accidentally the
  correct observed-remove rule: if my K holds something the remover
  never observed (my later re-assert), the remove misses it.

- **R3 — incoming coverage records:** the upstream delta necessarily
  carries the history records of its novel revisions. For each incoming
  covering record (a retraction, or a replace with non-empty
  `supersedes`), scan the record's `(entity, attribute)` slot in the
  local snapshot and drop any live claim whose version appears in the
  record's coverage. The scan is by version, not by the record's own
  value: a replace supersedes claims of *other* values, which live at
  other keys (keys embed the value hash), so probing the record's own
  keys would miss them. This is how *their* deletion reaches *my* copy
  even across an empty base: deletion travels as history (it already
  does), and the merge keeps the cache consistent with the growing log.

R1 handles coverage that happened in my past; R3 handles coverage
arriving in this delta; R2 is the cheap fast-path both directions
already share. Together the cache equals the fold of the union log.

**Convergence argument** (one paragraph, keep with the code): the log
merges as a set union — order-free. Liveness of a claim is a monotone
predicate of the log (once covered, always covered; coverage records
are immutable). R1/R2/R3 maintain `cache = live(log)` across every
merge, so two replicas holding the same log hold the same cache —
regardless of exchange order or direction. No slot algebra, no
tie-breaks (the single hash tie-break chooses among byte-variants of
the *same* fact), no races.

## The thread's pathologies, resolved

| pathology | outcome under this design |
|---|---|
| tombstone/assert hash race (measured 8/20 resurrections) | no tombstones exist; R1 rejects observed-and-dropped claims deterministically |
| deletion is forever (delete-wins 2P-set) | re-assert mints a fresh version above every watermark → live everywhere; R2 can't touch it, R3 never covers it |
| stale tombstone kills a later re-assert | stale peers carry no tombstones; their stale *asserts* are rejected by R1; my re-assert is news to them by R1 |
| Mallory's concurrent assert must survive Jordan-less retraction | her claim is unobserved by the retractor → not covered by any record → live |
| Jordan retracts all observed | his record covers both versions → R3 drops both everywhere |
| toggle storage balloon | slots hold live data only; history grows per toggle (as it always did); watermark is per-origin, not per-toggle |
| version-bookkeeping caveat (duplicate birth certificates) | shrinks to history cosmetics: the never-cited twin is dead by R1 everywhere, only the lineage narrative cites one certificate |
| re-assert not expressible in lineage | assert-over-deletion should record the covered versions as its claim `cause` (adopt regardless — it makes resurrection first-class for `causality()`) |
| Replace's stale-copy survival (guarded remove skips on byte drift) | replace records already carry `supersedes`; R3 applies them — the hole closes with the same rule as retraction |

## Costs and invariants

- **The origin invariant becomes safety-critical.** `observed()` is
  exact only if each origin is sequential. That is already a documented
  protocol requirement with a corruption-detection story; this design
  raises its stakes and its tests should say so.
- **Watermark maintenance**: the vector is a pure function of head
  ancestry, maintained incrementally at commit/pull (my vector ∪
  theirs ∪ the new revision), `O(#origins)` in size. **Persist it in
  the tree** as a small `dialog.`-reserved record rather than deriving
  it by ancestry walk: it then replicates with the tree, fast-forward
  adopters inherit it, and a *fresh partial replica* obtains it with
  one lazy block fetch instead of O(depth) record reads. (Verify it
  against the signed head's ancestry opportunistically; a mismatch is
  the same corruption class as an origin-invariant violation.)
- **Merge direction flip**: integrate their-delta-onto-mine instead of
  mine-onto-theirs. Fast-forward checks become symmetric: merged ==
  upstream → adopt their head; merged == local → keep local head and
  advance only the sync base (new case worth adding; today's
  formulation cannot see it).
- **Partial replication is unaffected.** The load-bearing distinction:
  *observed* means "in my head's ancestry", not "bytes on my disk". A
  replica that adopts head H holds H's fold regardless of which blocks
  it has fetched; laziness changes what is materialized, never what
  the state contains. Concretely: R1 is an in-memory vector lookup (no
  fetches); R2/R3 read only the differential, which walks only
  divergent paths with lazy remote fallback — byte-for-byte today's
  pull access pattern, and the retract record arrives precisely
  because it lies on a divergent path. No rule ever requires holding
  full history bytes. The one thing a partial replica must not do is
  prune history-region entries — already true today; horizon GC
  remains the (unchanged) future story for that.
- **What gets deleted from the current code**: `State::Removed`, the
  `prevails_over` deletion override, and the tombstone branches of the
  retract path (retract then truly deletes keys, exactly as `Replace`
  already does, and writes its history claim, exactly as today).

## Test plan

1. The Alice/Bob/Mallory/Jordan scenario, verbatim, as the acceptance
   test for observed-remove (cardinality-many).
2. The existing resurrection suite re-expressed: deletion survives
   tracked, empty-base, and reverse pulls (20× loop stays in CI spirit:
   no randomness left to flush out, but keep one repetition run while
   the change is fresh).
3. Re-assert-after-deletion propagates everywhere, including through a
   stale peer sandwich (the versioned-tombstone hazard trace from the
   thread).
4. Quiescence: mutual pulls reach identical trees in bounded rounds for
   every scenario above.
5. Replace: superseded values do not survive via stale peers (R3).
6. Origin-invariant violation: two same-version claims surface the
   documented corruption error rather than silent divergence.

## Sequencing

The interim (tombstones + deletion-wins) is live and convergent today —
this design is not urgent, it is *correct*. Natural staging: (1) flip
merge direction + R2 symmetric FF, behavior-neutral; (2) add watermark
cache + R1/R3 behind the existing tests; (3) drop `State::Removed` and
the retract tombstone branch; (4) land the OR-semantics tests; (5) make
assert-over-deletion record its covered causes (lineage expressibility)
— worthwhile independently and first, since it is additive.
