# Tree research: g-trees, bijoux, and the transient question

Status: research digest from the 2026-07 performance campaign, feeding
the group-B design. Owner prompts: read the g-tree paper for
simplification opportunities; consider bijoux as the varint
replacement; reconsider whether transients still earn their keep next
to the hitchhiker design; make caches passable/persistable.

## G-trees (Geometric Search Trees — Meyer & Farmer)

Paper: https://g-trees.github.io/g_trees/ (HN discussion:
https://news.ycombinator.com/item?id=41546874).

The frame: give every item a rank drawn geometrically from a hash of
the item; zip trees, skip lists, Merkle search trees, and prolly trees
are all instances of one construction. Their novel instance, the
k-zip-tree over "k-lists" (sorted linked lists of ≤k items packed
as-early-as-possible), is history-independent with a HARD deterministic
node-capacity bound, and updates are three set-algebra primitives
(`unzip`/`zip2`/`zip3`) whose work is confined to the rank-cut path.

What transfers to dialog's tree:

- **Rank-from-key-hash boundaries.** Deriving a node boundary from the
  key's own hash (e.g. trailing zeros of keyed blake3) instead of a
  content-defined chunking over serialized entries makes boundaries a
  pure function of the KEY: an edit can never move a neighbor's
  boundary, so the boundary-shift cascades our frame-ceiling/election
  machinery manages (and pays for — group 2's election memoization,
  `frame_cut_positions`, widened regroups with provenance tracking)
  structurally cannot happen. This is the strongest simplification
  candidate in the paper: it would retire a whole subsystem rather than
  optimize it.
- **Hard capacity bound** kills the adversarial/heavy-tail chunk-size
  problem and makes node sizing static (helpful for buffer/page
  planning, and for a future mmap story).
- **Batch updates as unzip/merge/zip** fit the deferred-flush batch
  shape we just landed better than per-leaf re-chunking, and canonical
  form is provable rather than asserted.

What does NOT transfer:

- Content addressing still rewrites and re-hashes every dirtied node
  and its ancestors — g-trees bound WHICH nodes are touched, not the
  per-node encode/hash cost. The O(frame)-vs-O(novelty) work (group B)
  is orthogonal and still needed.
- k-list packing has its own bounded ripple (an insert can shift items
  into subsequent k-list nodes: O(k) typical), so "no rebuilds" is not
  literal in a content-addressed setting.
- Rank grinding is possible with public hashes — rank derivation must
  use keyed blake3.
- The paper has no buffered-update story; the hitchhiker layer remains
  ours.

Verdict: worth a serious design spike for the CANONICAL layer — it
attacks the implementation complexity (election, force-splits, regroup
provenance) rather than the constants, and composes with both the
hitchhiker buffers and the group-B delta chain. Not a quick win; a
format + algorithm change on the same scale as B itself.

## Bijoux (varint replacement)

`bijoux` is Ink & Switch's canonical variable-length integer encoding
(the bijou family; absorbed `bijou64`): first byte 0-247 is the value,
248-255 tags an exact 1-8-byte big-endian payload, with bijective
per-tier offsets so every integer has EXACTLY one encoding.

- Decode is O(1): one byte gives the length, payload is a single
  unaligned load + bswap — no continuation-bit scanning. Claimed ~10x
  vs LEB128 on uniform u64, still ahead on realistic mixes.
- Canonical-by-construction is exactly the property a content-addressed
  codec wants (no "canonical varint" validation class of bugs; stable
  hashes).
- Unsigned formats sort bytewise in numeric order — a bonus anywhere
  lengths are compared raw.
- Crate: https://crates.io/crates/bijoux (v0.3.1, MIT/Apache-2.0,
  no_std, u32/u64/u128 + zigzag signed). Young (~190 downloads); the
  format is small enough to vendor if that is a concern.
- Fit: replaces `read_varint`/`encode` in the columnar node codec
  (`node/codec.rs`) — hand-rolled regions only, not rkyv layouts. In
  the current post-6A profile `read_varint` is ~0.7% (it was never the
  wall), so this is a format-migration decision, not a hot fix: adopt
  it WHEN a node-format revision (group B / g-tree spike) forces a
  format break anyway, and get canonicality + the faster decode for
  free at that boundary.

## Transients vs the hitchhiker path (owner direction)

The owner's framing: transients existed to accumulate changes along
updated paths so the same spine is not re-hashed/rebuilt over and over;
copying was never the goal (compare Clojure transients: COW, free
transition, mutation touches only the spine). Post-6A state of the
copies:

- Amending novelty on the live spine copies NOTHING per-child: children
  are `Node::Persistent` links, appends push into a decoded op vec, and
  only the touched link re-encodes at persist.
- The remaining per-commit copies are FRAME-level (rkyv serialize of
  the whole root frame, `Buffer::from`, backend `set`) — Arc-ing
  children does not remove them; the group-B O(novelty) format does.
- The canonical edit path (TransientTree) still fully materializes the
  leaves it reshapes; after deferred flush, the batch regime's cost is
  concentrated exactly there (`replay_ops` -> per-edit reshape).

Direction recorded: let the hitchhiker flush subsume the canonical edit
path — a flush that reaches a leaf applies its WHOLE accumulated batch
to that leaf in one rebuild (one decode, one merge, one encode, one
hash per touched leaf per settle), instead of running per-op canonical
edits through the transient machinery. Combined with rank-cut
boundaries (above) the transient layer could reduce to "the in-memory
working form of a node being rebuilt," which is the Clojure-shaped
role the owner describes.

## Cache lifetime (owner, separate concern)

Caches (node cache, spill cache, record cache) should be passable in
from outside generally — today `Artifacts` and `Branch` construct their
own — so an embedder can persist and recover them across sessions
(e.g. serialize hot node sets, or back the cache with the mmap'd DCAA
file so "cache warm" survives restart for free). Noted as design work
for the group-B/DCAA integration round, where the mmap story makes it
mostly structural.

## Boundary-decision design space: tails, context, and where to pay (2026-07-30)

Owner questions after the widening attribution and the mark fix: is
dolt's approach history-dependent; can widening's cost be fixed while
keeping history independence; and do the previously considered ideas
(synthetic boundary entries, finer rank granularity, second-coin
flips) reopen anything.

### The structural fact all options answer to

Any cut rule in which each seam's decision reads only a bounded local
neighborhood of keys behaves like independent coins, so run lengths
have an exponential tail: unbounded frames occur with positive
probability. Bounding frame size DETERMINISTICALLY therefore requires
reading unbounded context (the frame's membership) somewhere. The
whole design space is where that context is read and how it is
maintained:

1. **At edit time, whole frame** — today's widening: anchors are a
   pure function of frame membership (history-independent), and every
   membership edit re-merges, re-elects, re-splits. Correct (now that
   the marks survive), O(frame) per edit.
2. **Left-to-right recurrence + rightward resync** — dolt's ramp and
   ours: the decision reads run-so-far; maintenance must re-decide
   from the frame start and continue until an emitted cut coincides
   with a stored one. History-INDEPENDENT in structure — the
   boundary set is a deterministic function of the key sequence —
   PROVIDED resync runs to its fixed point. What it gives up is
   worst-case edit locality: boundary shifts can cascade. With a ramp
   the chain dies geometrically (a hop continues only through a frame
   that ends in the ramp zone, ~e^-2 ≈ 13% at threshold 2S; expected
   chain ~1.16 frames), which is exactly why dolt ramps too. Our
   prototype implements the ramp WITHOUT full resync, which is why
   its four adversarial order-convergence fixtures fail — multiple
   locally consistent fixed points. Dolt's approach is not
   history-dependent; half-implemented resync is.
3. **Frame-scoped election + incremental summaries** — the overlooked
   option: keep today's election semantics untouched (anchors remain
   a pure function of membership → same canonical shapes, same
   convergence argument), but maintain the election incrementally.
   Each stored piece caches a summary derivable purely from its own
   content — its election candidate (min hash + candidate class), its
   weight, its count. An edit rebuilds one piece's summary (O(piece))
   and re-runs the election over per-piece summaries (O(#pieces)),
   touching no other piece unless an anchor actually moves (rare:
   only when the argmin changes or weight crosses a spacing
   threshold). Summaries can live in a hash-keyed cache (piece hash →
   summary, recomputed on miss; no format change) or later as a tiny
   stored header field (format change, saves the recompute).

### Where the owner's earlier ideas sit

- **Synthetic boundary entries** (insert a phantom key as boundary,
  evict by policy): whether the phantom exists depends on when the
  run crossed the threshold in that particular history — two replicas
  diverge. Making placement deterministic-from-content turns it into
  exactly the stored form of the anchor election; the min-hash
  election was the right replacement, as concluded at the time.
- **Finer rank granularity ×X** (round down normally, consult higher
  resolution in long runs): "in long runs" is frame context, so the
  refined decision is an election over the frame — and taking the
  best higher-resolution candidate IS a min-hash election. Same
  family as today; the deficiency was never the election's semantics
  but its maintenance cost, which is what option 3 fixes.
- **Second coin from the other end of the hash / deterministic salt
  in long runs**: deciding WHEN to consult the second coin again
  reads frame context; and composing per-key coins only changes the
  rate, never removes the exponential tail. Collapses into the same
  election family.

### Cascade susceptibility, compared

- Today's design: coin cuts are per-key (edits cannot move them);
  the bank is seam-structural (an edit inside a vetoed stretch can
  flip the next accepted seam's cut — one frame boundary); anchors
  re-elect within the frame. Cascades stop at the frame edge BY
  CONSTRUCTION. This is a real property, and it is exactly what the
  widening buys.
- The ramp: geometric cascade chains (short in expectation, unbounded
  in the worst case), plus the resync machinery becomes
  load-bearing for convergence.

### Recommendation

Keep the (now-correct) election design and pursue option 3 —
incremental elections via per-piece summaries — as the widening cost
fix: it removes the O(frame)-per-edit tax and the (32K, 32K)-class
catastrophes while preserving every current invariant, including
frame-bounded cascades and history independence, with no format
change in the cache-keyed variant. Keep the ramp as an experiment arm
only; adopting it means committing to full rightward resync and
accepting dolt-class worst cases. Re-measure the widening share
first: the mark fix means runs now REJOIN and can dissolve when
content shrinks, so the standing pool of forced frames — and the
25%+ edit share measured before the fix — may already be smaller.

## Long-run boundary ideas, unified (2026-07-30 discussion)

Owner raised Carson's (g-tree co-author) nested-g-tree idea for long
runs, the earlier finer-ranks and reverse-hash-flip proposals, keying
on "most distinct" natural boundaries, CDC-within-runs / FastCDC, and
Merkle Search Trees. Where each lands:

- **Nested g-trees / finer ranks**: the limit of finer rank
  granularity is the key's raw hash as a continuous rank; dividing a
  run at the highest refined ranks, recursively until pieces fit, IS
  the recursive min-hash election `choose_cuts` ships. Threshold-k
  selection (cut at the k smallest hashes) is the less stable
  sibling: k changes with run size and can re-derive every anchor,
  where recursive bisection is hierarchically stable.
- **Natural / most-distinct boundaries**: already shipped as the
  hybrid anchor selector (default): shortest-separator class first,
  hash only as tiebreak. Field evidence from the convergence hunt:
  the elected anchors were exactly the region-tag seams. Inserts
  never move them; only deleting the anchor or dropping the class
  floor does.
- **FastCDC**: its normalized chunking is a two-step ramp (stricter
  mask before target, looser after) — evidence a coarse step captures
  most of a continuous ramp's variance win, relevant only to the ramp
  arm. Its max-size hard cut is position-based and strictly worse
  than the election (any upstream insert shifts it — FastCDC's known
  dedup weakness). CDC scoped to runs is still a bounded-window rule
  and re-inherits the exponential tail.
- **Merkle Search Trees**: layer = leading zero digits of hash(key);
  perfectly edit-local (a key's layer is intrinsic) and history
  independent, but NO backstop: a stretch with no layered keys is one
  unbounded leaf. Our coin without the safety net — same pathology,
  fewer defenses. Nothing to borrow for long runs.

Conclusion: every proposal converges on the three-family map
(per-key intrinsic / run-position recurrence / membership election),
and the owner's and Carson's ideas all land in the election family —
whose boundary rule is already what ships (hybrid selector over
recursive bisection). The deficiency is maintenance cost, which the
incremental-summary work addresses; a FastCDC-style step within the
candidate classes remains a cheap later knob if anchor churn ever
measures high.

## Forced-run quiet check: V1 measured (2026-07-30)

Implemented the widening quiet check on this branch: `cut_plan`
extracted from `regroup_entries_reusing` (single source of truth for
the cut-decision pipeline), and `forced_run_quiet` in the edit path —
before merging a forced run, stream the run's keys and weights
read-only, simulate the edit, re-run `cut_plan`, and skip the
widening when the predicted cuts land exactly on the current piece
boundaries with their forced marks intact. Guards: edit strictly
interior to its piece (boundary machinery must not engage), deletes
not beside a vetoed seam, exact plan match. Buffered parent novelty
does NOT block the skip: the merge path itself elects over stored
keys only and re-routes buffers by link boundary, so an unchanged
partition makes that re-route an identity.

SE replay, default config, MeasuredStorage:

- 2,000 txns: 16,252 checks / 7,895 real runs / 6,111 skipped (77%
  of runs). Rejects: 1,617 interior, 167 plan, 0 novelty.
- 6,000 txns: 49,586 checks / 11,905 runs / 7,150 skipped (60%).
  Rejects: 4,524 interior (SE's per-region ascending IDs make
  piece-tail inserts common), 231 plan.
- Election stability on real data: only ~2% of full-run re-elections
  move any anchor. The hybrid selector's anchors are as durable as
  the manifest docs claim.
- Bytes written and read are IDENTICAL with and without the skip
  (961,035,972 B written either way at 6,000) — piece-origin reuse
  was already eliminating the rebuild's writes, so the widening's
  marginal cost is CPU only, and both paths pay the same O(run)
  stream against a warm node cache. Wall time is a wash at 2k and
  6k txns.
- Convergence: per-txn vs by-five vs single-commit replays converge
  at 200/1,000/3,000/10,000 txns with the check active; the full
  suite (293 tests) passes.

Conclusions. (1) V1 is structurally safe and byte-identical but
buys no wall time at the default config — the merge was never the
default-config bottleneck (it was at 32K/32K, unmeasured here).
(2) The 98% election stability is the licence for V2: replace the
O(run) stream with per-piece summaries (piece weight, entry count,
interior-candidate minima per hybrid class, edge keys) combined in
O(pieces), loading pieces only when a cut actually lands inside one.
The quiet check's plan-comparison scaffold is exactly where the
summary combiner plugs in. (3) Cold starts: summaries are pure
functions of piece bytes, so they can be persisted keyed by piece
hash, or — endgame — embedded per-link in the parent index node
(format change), making re-election free even cold since the parent
is already loaded for routing.
