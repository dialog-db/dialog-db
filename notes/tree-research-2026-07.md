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

## V1 verdict: negative; pivot to the root-frame re-encode (2026-07-31)

Corrected A/B at DIALOG_TREE_MAX_SEGMENT=32768 (3,000 txns; the
earlier same-build comparison was invalid): baseline 2416/5475/9061
us per commit windowed vs 2958/5962/9656 with the quiet check — the
check costs a consistent 6-9% and saves nothing, because the O(run)
stream it adds on every membership edit exceeds what the skips
avoid, and the widening's writes were already elided by piece-origin
reuse at this config too. Owner's call: win is negligible, aim for
the higher-price ticket.

The measured higher ticket (from the callgrind decomposition that
motivated measure_se_replay): the commit path re-encodes, re-copies,
and re-hashes the whole root frame — entries plus the full novelty
buffer — on every commit: ~118-160 KB written per commit carrying
~2.3 facts, with memcpy + blake3 + allocator at ~60% of commit
instructions. That is orders of magnitude of write amplification per
fact, dwarfing anything widening-related at any measured config.

Next campaign: incremental root-frame maintenance — keep the sealed
column encoding of the unchanged prefix/suffix and re-encode only
the touched region (or chunk the novelty buffer so a commit appends
instead of rewriting), with the same convergence discipline as this
spike: byte-identical stored form, per-txn vs batched replay roots
equal at every scale.

The quiet check stays on this spike branch as measurement
infrastructure (its audit counters produced the election-stability
numbers; election moved anchors in only ~2% of full-run recomputes,
which remains the licence for parent-embedded summaries if widening
ever becomes the bottleneck at scale), but it should NOT merge to
main as-is: it is a measured 6-9% regression at 32K and neutral at
default.

## Spine-frame write amplification: baseline and design sketch (2026-07-31)

### Measured baseline (default config, SE replay)

`measure_se_replay 3000 1000`, MeasuredStorage over the memory
backend: 3,000 commits carrying 6,991 facts (2.33/commit) write
407,855,156 B total — **135,952 B/commit**, ~58 KB per fact.
Windowed: 167K / 100K / 141K B per commit, 4.7-6.0 sets/commit,
3.3 / 9.9 / 12.7 ms per commit.

`write_attribution 3000 500` splits that volume by block class and
corrects the campaign's framing. Per-commit averages by window:

- **Index frames dominate**: 1.0-2.1 index blocks totalling
  72-162 KB per commit. Leaf writes are 11-60 KB (flush/cascade
  traffic, amortized), revision + pointer blocks ~1 KB.
- **It is the spine, not just the root.** Through txn ~1000 the
  tree is 2 levels and the root IS the big frame (probe: 148 KiB,
  119 links). From ~txn 1000 the tree is 3 levels: the root shrinks
  to 27-42 KiB with 6-17 links, and the dominant term becomes the
  level-2 index frames (~100 KiB each, holding the pushed-down
  novelty), rewritten on most commits (~0.7-1.1 non-root index
  writes/commit). Any fix scoped literally to the root node would
  miss the larger share.
- Root novelty holds 76-167 buffered ops at the probe points; a
  commit adds ~7 ops (2.33 facts x 3 key regions). So a frame
  rewrite moves ~20x more ops than the commit adds, and ~250x more
  bytes than the commit's payload.

Where the cost sits in code (all in `persist_mut` /
`TransientNode::persist_mut`, `node/transient.rs`):

1. `Novelty::persist_buffers` already skips the columnar re-encode
   for Sealed/Cached links — but it **clones** every buffer
   (columns + polarity + values) into the body it hands rkyv.
2. `PersistentIndex::from_links` rebuilds the whole separator/
   hash/scale tables per persist.
3. `body.as_bytes()` = `rkyv::to_bytes` serializes the entire node
   into a fresh AlignedVec — full memcpy of every column and value.
4. `Buffer::from` blake3-hashes the whole frame; `delta.add` →
   `publish_root` writes the whole frame as a new block.

Steps 3-4 are the callgrind 60% (memcpy + blake3 + allocator).

### The structural constraint

Content addressing makes the frame's byte count a floor: any change
to a node's content is a new hash, and a one-block node must then be
written in full. **Within the current format, bytes-written per
commit cannot drop below the frame size** — only CPU can be saved.
Cutting bytes requires the frame to stop containing the novelty
payload: the ops must live in blocks that survive across commits
unchanged (Merkle indirection), which is a deliberate format change.

### Design sketch: per-node novelty delta chunks (the oplog form)

Move a buffered index node's novelty out of its frame into
content-addressed **chunk blocks**, referenced by hash:

- Node body (v-next): links table (prefix/suffixes/ends/hashes/
  scales — the small part, ~1-3 KiB at observed fanouts) plus an
  ordered list of **chunk references** replacing the inline
  `novelty: Vec<NoveltyBuffer>`.
- A chunk is one commit's ops for that node, grouped per link in
  the existing `NoveltyBuffer` encoding (one block per touched
  spine node per commit, NOT per link — keeps block count at ~1-2
  new blocks/commit). Chunks are immutable and shared by every
  subsequent root until compaction/flush.
- Commit writes: new chunk (~7 ops, roughly 0.3-1 KB) + the small
  node frame + revision blocks. Estimated 5-10 KB/commit against
  the measured 136 KB — order 15-25x on the index share; leaf
  flush traffic (11-60 KB/commit, amortized) is untouched and
  becomes the next-largest term.
- Read path: per-link resolve overlays chunks in list order (later
  chunk wins for equal keys — same newest-last rule as today, with
  chunk order as the tiebreak above entry order). `all_novelty` =
  stable merge by key across chunks. Chunks are tiny and cache-hot;
  cold opens pay 1 + C block reads per spine node.
- **Compaction bounds read-amp**: when a node's chunk count exceeds
  C (or the existing weight cap triggers a flush), merge the chunk
  list into one chunk (or drain via the existing cascade). Today's
  every-commit whole-buffer rewrite becomes a 1/C-amortized event.
  C in the 8-32 range; measure.
- Flush interaction: a cascade that drains link k's ops must drop
  them from the chunk set — that persist rewrites the chunk list
  (compact the survivors into one chunk). Cascade commits pay
  roughly today's cost; quiet commits (the vast majority) pay the
  append.

Both spine levels get this: the level-2 frames carry more novelty
bytes than the root at depth 3, and the transient plumbing
(`Novelty`, `LinkNovelty`) is shared, so scoping to the root only
would strand most of the win.

Correctness and discipline:

- The root hash still covers everything (chunk hashes are in the
  frame; chunk bytes are reachable blocks) — replication, diff, and
  pinning keep working once the reachability walker learns to
  traverse chunk refs. The novelty-aware differential must load
  chunks where it read inline buffers.
- Canonical form is untouched: canonicalize drains all novelty, so
  `converge_check` (which compares canonicalized roots) is
  unaffected. The BUFFERED stored form becomes batching-dependent —
  it already is (buffered roots were never canonical).
- Deliberate format change: `Manifest.version` bump; decide
  read-old/write-new migration vs a hard break on the spike branch.

### The byte-identical alternative (rejected as primary)

An incremental assembler — cache the previous frame's bytes, splice
only the touched link buffer's region, resume blake3 from cached
chunk chaining values for the stable prefix — keeps the stored form
byte-identical. But rkyv's layout shifts every byte after a grown
buffer (relative pointers), positional shifts invalidate blake3
chunk CVs even for identical content, and a random touched link
leaves ~half the frame stable in expectation. Complexity high,
savings capped well under the 60%, and **zero** bytes-written
savings. Worth keeping only as: (a) eliminate the per-persist buffer
clones in `persist_buffers` (serialize from borrowed refs — small,
safe, byte-identical), which the chunk design subsumes anyway by
making frames small.

### Measurement plan for the implementation

A/B `measure_se_replay` at 3000/1000 (bytes + us per commit, from a
real pre-change rebuild), `converge_check` at 200/1000/3000/10000,
full suite, plus new counters: chunks written, chunk reads per
resolve, compactions, chunk count at probe. Record the cold-open
read-amp explicitly.

### Owner verdict on chunks: rejected (2026-07-31)

Chunking multiplies network read count and degrades sync: the whole
point of inlining novelty in the root frame is that two replicas
that have not diverged much exchange a SINGLE block. Novelty stays
inline; a spine node stays one block. The accepted direction is
incremental hashing/assembly of that one block, with padding or
alignment changes on the table if they make it work.

## Design v2: containerized frame, incremental assembly + hash (2026-07-31)

Sync constraint restated: one block per spine node, novelty inline.
The costs that remain attackable are then CPU-side only — the
full-frame rkyv re-serialize (memcpy of every column/value), the
full-frame blake3, the per-persist buffer clones, and the allocator
traffic under all three. Bytes written to storage stay O(frame) by
design; that is the price of single-block sync and is accepted.

### Why not splice raw rkyv bytes

The no-format-change variant — keep `rkyv::to_bytes` layout, cache
the previous frame, splice the changed range — fails structurally:
rkyv serializes out-of-line data in field/element order with
relative pointers, so a grown buffer shifts every later byte AND
rewrites the (late-positioned) struct tables' relative offsets;
the stable prefix ends at the first touched buffer, which is
positionally random, so expected savings cap near half the hash and
none of the serialize. Fragile and small. Rejected.

### The container layout

A spine node's block becomes an explicitly-assembled container of
independently-serialized regions (a deliberate format change;
`Manifest.version` bump; still exactly one block):

    [buffer region 0][pad][buffer region 1][pad]...
    [core region][pad][trailer]

- **Buffer region** = one link's `NoveltyBuffer`, serialized on its
  own via rkyv (own little archive), padded to 16-byte alignment so
  in-place archived access works per region.
- **Core region** = the node body minus novelty (header, prefix,
  suffixes, ends, hashes, scales) — the small part (~1-3 KiB at
  observed fanouts), also its own rkyv archive.
- **Trailer** = region table (child id + offset + length per
  buffer region, core offset, format tag). Fixed-size-ish, last.
- **Region order is mutation-coldness order, hottest last**:
  untouched buffer regions first IN THEIR PREVIOUS STORED ORDER,
  then the buffers this commit touched, then core (root cores
  change on most commits — child hashes move under cascades — so
  core sits behind the cold bulk), then the trailer. Child identity
  lives in the trailer, so decode is order-independent; the strict
  ascending-child-order validation moves from the novelty vec to
  the region table (each child at most once, ids in range).

### Incremental assembly

The live spine already holds per-link `Sealed`/`Cached` encodings;
they now hold the region BYTES (the little rkyv archive), not the
struct — killing the per-persist clone of columns/polarity/values
outright. Persist becomes:

1. Unchanged regions: one bulk memcpy each from the previous
   frame's bytes (~10 us for 100 KiB at memcpy speed; unavoidable,
   the block must be materialized — the previous Buffer is
   Arc-shared with cache/delta/storage and cannot be mutated).
2. Touched buffers: columnar re-encode as today (the remaining
   O(accumulated buffer) term — see phase 2), serialized into
   fresh region bytes, appended after the cold bulk.
3. Core: re-serialized only when links/scales/hashes changed;
   memcpy'd otherwise.
4. Trailer: rebuilt (tiny).

### Incremental hash

`blake3::Hasher` is `Clone` (blake3 1.x public API): cloning
snapshots the CV stack (~1.9 KB). While absorbing frame N, snapshot
the hasher at every region boundary (~20 clones, negligible). For
frame N+1, find the longest prefix of regions that is byte- and
order-identical to frame N, resume from that boundary's snapshot,
absorb only the tail (touched buffers + core + trailer). Snapshots
are valid at arbitrary byte offsets (the hasher buffers partial
blocks), so region padding is for rkyv alignment only, not for the
hash. No hazmat/CV-level API needed. Positional reuse of MIDDLE
regions (slot-padded, power-of-two capacities) would need CV-tree
surgery — explicitly deferred unless prefix reuse measures short.

Expected quiet-commit cost: touched-buffer re-encode + tail hash
(tens of KiB) + one full-frame memcpy + storage copy, against
today's full re-serialize + full hash + clones of ~100-160 KiB.
The callgrind 60% (memcpy + blake3 + allocator) shrinks toward the
memcpy floor.

### Phase 2 (only if phase 1 measures short): in-frame append runs

The remaining O(buffer) term is the touched buffer's columnar
re-encode — ops insert by key into a sorted run, so front-coding
cannot append. If it matters: a touched link's region becomes a
base sorted run plus small append runs (still INSIDE the single
block — no extra network reads), readers merge runs newest-last,
and the existing weight cap compacts runs back into one on
overflow. This is the oplog idea relocated inside the frame, where
it cannot cost sync anything.

### Semantics, determinism, discipline

- One block per node, novelty inline: sync exchange count is
  unchanged; block grows only by padding + trailer (~1%).
- Stored bytes become a pure function of (region order, contents),
  and region order is carried in the trailer, so fresh-open →
  re-persist reproduces bytes exactly (the existing byte-identity
  debug pins keep their meaning). Cross-HISTORY byte determinism
  of the buffered form is relaxed — two replicas reaching the same
  buffered state through different touch orders may differ in
  bytes. The buffered form was already batching-dependent and
  non-canonical; recorded here as deliberate.
- The canonical (empty-novelty) form also moves to the container
  (zero buffer regions), so canonical bytes change vs v0: a format
  version bump, hard break on the spike branch (no read-old
  migration until the design proves out).
- `converge_check` compares canonicalized roots within one build —
  unaffected as an oracle. Byte-identity A/B vs pre-change builds
  is void by definition; the oracle for the new format is
  fresh-open/re-persist identity plus cross-grouping convergence.
- Read paths that consume `index.novelty` (walker, differential,
  flush, `buffer_for`, `all_novelty`) move behind accessors that
  resolve regions through the trailer.

### Owner amendment: deterministic order, head/tail split (2026-07-31)

Owner prefers determinism over hottest-last ordering — try it and
measure. Amended layout, per the owner's sketch:

    [head: core (links table, sans novelty)]
    [tail: length-encoded buffer regions, ascending child order]
    [footer: region table + format tag]

- The head is byte-stable until a flush/cascade moves a child hash
  or scale; the tail's regions are reusable up to the first touched
  child.
- The determinism cost lands only on the HASH resume (prefix-bound:
  an early touched child re-hashes most of the tail), NOT on the
  serialize reuse: memcpy-reuse of an unchanged region's bytes
  works wherever the region sits, so the clone/re-serialize
  elimination — likely the larger term, since blake3 is
  instruction-heavy but SIMD-fast while rkyv serialize is
  allocator-bound — is position-independent and keeps its full
  value. Cross-history byte determinism of the stored form is
  preserved: bytes are a pure function of the buffered state.
- If prefix-bound hash reuse measures poor (SE touches all three
  key regions per commit, so the root's first touched child is
  often early), the escalation path is CV-level middle-region reuse
  (blake3 chunk-counter surgery), not a return to touch-ordering.

Before the refactor: instrument the seal path (settle vs persist
wall time) — the callgrind 60% is instruction counts; the wall-time
prize needs sizing so the A/B has a denominator.

## Wall-time phase attribution: the persist premise fails (2026-07-31)

Added phase timers (audit counters, this branch): `SETTLE_NS` /
`ENQUEUE_NS` / `REPLAY_NS` around the write halves, `PERSIST_NS` /
`SERIALIZE_NS` / `NODE_HASH_NS` around the spine persist, and
`OPENS` / `OPEN_NS` / `OPEN_BYTES` around `TransientNode::open`.
`measure_se_replay` prints them per window. Results at 3000/1000,
default config (window totals per 1000 commits; ratios stable
across three runs, absolutes noisy ±30%):

- settle 2.2-14.5 s, of which **replay_ops (canonical leaf edits)
  2.1-11.4 s — 73-92% of commit wall time**.
- enqueue (routing + cascade decision): 0.11-0.24 s (~1-2%).
- persist (assembly + hash + delta): 0.3-4.1 s (3-21%, the high
  end observed only in noisy runs; typically ~0.3-0.5 s ≈ 3-5%),
  of which serialize 0.07-1.8 s and **node blake3 30-56 ms — about
  40-50 us per commit, ~0.3-1.5% of wall time**.
- opens: 140-395 frame opens PER COMMIT, average block 2.4 KB,
  340-800 KB decoded per commit — but only 0.3-0.9 s per window
  (~10-15% of replay). These are forced-run piece frames opened as
  widening/regroup context, not big leaves.

Conclusions:

1. The callgrind "memcpy + blake3 + allocator = 60% of commit
   instructions" was instruction-weighted and mis-attributed:
   SIMD blake3 is instruction-dense but wall-cheap, and the
   memcpy/alloc mass lives in the EDIT path (piece opens, entry
   materialization, regroup context), not the root-frame persist.
2. The incremental-frame campaign (container + prefix-resume
   hashing) has a measured wall-time ceiling of ~5-15% at default
   config: serialize is 1-10% and the hash it would incrementalize
   is ~1%. Bytes written stay O(frame) under the single-block
   constraint by design. The design (v2, amended) is recorded and
   remains valid, but it is not the higher-price ticket.
3. The higher-price ticket at default config is the canonical edit
   machinery inside replay_ops: per-op elections/regroups and the
   widened/forced-run context work (140+ piece opens per commit and
   the O(run) streams — a share of which is this branch's own quiet
   check, which runs ~8 times per commit here; the 32K A/B already
   measured it at 6-9% and the notes call for it not to merge).
   The known levers aim exactly there: the pacing ramp (measured
   -30-35% per-commit time at scale), per-piece summaries (the V2
   licenced by 98% election stability), and keeping hot leaves/
   pieces decoded across commits instead of re-opening them
   (Cached-style retention for children, format-unchanged).

Decision needed from the owner: proceed with the containerized
frame anyway (modest, bounded win, format change), or pivot the
campaign to the edit path with this attribution as the new
baseline.
