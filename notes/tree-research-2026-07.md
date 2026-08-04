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

Owner's call (2026-07-31): the container's added complexity is not
worth a 1-7% wall win — pivot to what has impact. The containerized
frame design stays recorded for if the economics change.

## Replay interior attribution: widening is the ticket (2026-07-31)

Added edit-path timers: per-op `APPLY_NS`/`APPLY_OPS`, the widening
seams (`QUIET_NS`, `MERGE_RUN_NS` for `merge_forced_run` including
the neighbor-side call, `MERGE_INDEX_NS` for
`merge_forced_index_runs` likewise), the phase-two synchronous
`RESHAPE_NS`, and `ELECTION_NS` inside `choose_cuts` (nested in the
others). Accounting closes: quiet + merges + reshape ≈ 99% of
apply, and apply ≈ 99% of replay.

3000/1000 default config, window totals per 1000 commits (~8-9
leaf-bound ops per commit):

- Window 1 (2-level tree, root = one big frame): apply 2.13 s —
  **quiet check 1.30 s (61%)**, reshape 0.47 s, merge_run 0.22 s.
  In the early epoch the quiet check's own O(run) streams are the
  single largest cost in the whole commit path (~45% of total
  commit time), while saving nothing measurable.
- Window 2 (3 levels): apply 9.30 s — reshape 4.54 s (49%),
  merge_index 2.66 s (29%), quiet 1.43 s (15%), merge_run 0.56 s.
- Window 3: apply 11.85 s — **reshape 6.84 s (58%), merge_index
  3.05 s (26%), merge_run 1.33 s (11%)**, quiet 0.47 s, election
  0.29 s. ~750 us per op on average.

Mechanism: at scale the level-2 frames are force-split (over the
frame ceiling), so nearly every membership edit descending through
them fires `merge_forced_index_runs` (index_widened ≈ 8/commit ≈
one per op) — the merge opens and lifts the WHOLE frame run (the
140-395 small piece opens per commit), and the phase-two reshape
then regroups the whole merged window. Same at the leaf level for
forced runs (`merge_forced_run` + regroup). Every edit into a
forced run pays O(run), and the SE workload lives inside forced
runs at scale. The elections themselves are trivial (2.5%) — the
cost is materializing and regrouping run-sized windows per op.

The ticket, therefore: **skip the run-sized materialization when
the boundary plan provably stands still** — the V2 per-piece
summary design the quiet-check conclusions already licensed
(election moves anchors in ~2% of full-run recomputes):

1. Replace the leaf quiet check's O(run) stream with O(pieces)
   summary combination (piece weight, entry count, per-class
   interior candidate minima, edge keys) — this also kills the
   window-1 anomaly where the check itself dominates.
2. Add the MISSING index-level twin: a quiet check for
   `merge_forced_index_runs`, which today merges unconditionally
   on every membership edit through a force-split frame and is
   26-29% of apply at scale.
3. Only when a cut genuinely lands inside a piece (measured ~2%)
   pay today's full merge + regroup.

Ceiling if quiet edits skip both merges and their reshape share:
most of the 85-95% of apply that the widening paths consume,
bounded by the true plan-change rate. Next: design the summary
shapes and the skip conditions precisely (the existing `cut_plan`
extraction is the plug point), with converge_check as the oracle
at every step.

## Widen census and the optimized landing zone (2026-07-31)

Census counters added: merge windows and pieces materialized per
level, origin-reuse ratios inside widened regroups (a direct proxy
for "the plan stood still for this piece"), and the widened share
of reshape time. 3000/1000 default config, per 1000 commits:

| window | leaf windows | leaf pieces | leaf reuse | index windows | index reuse | reshape widened |
|--------|-------------|-------------|-----------|---------------|-------------|-----------------|
| 1 | 719 | 66,866 (93/win) | 66,314/73,296 = 90.5% | 743 | 698/2,558 = 27% | 432/462 ms |
| 2 | 793 | 193,494 (244/win) | 89.7% | 8,083 | 18% | 4,373/4,372 ms |
| 3 | 840 | 375,295 (**447/win**) | **93.6%** | 9,033 | 22.6% | 6,655/6,654 ms |

What the census says:

1. **Leaf forced runs are ~450 pieces long at 3K txns and growing
   linearly** (93 → 244 → 447 pieces/window). Every membership
   edit that fails the quiet check merges and regroups the whole
   run — this linear growth is the growth term in the 3.3 → 12.7
   ms/commit curve. 93.6% of regroup output groups come back
   byte-identical (reused verbatim); the plan-reject rate says 97%
   of windows have no plan movement at all.
2. **Index widening fires on essentially every op at scale**
   (9,033 windows per 1000 commits ≈ one per op). Reuse there is
   only ~22%, but NOT because plans move: origin provenance is
   sparse (0.44 eligible origins/window) because only QUIET pieces
   (empty novelty) are eligible, and level-2 pieces carry buffers.
   So today's merge rebuilds, re-encodes, re-hashes, and re-stores
   the buffered pieces nearly every op — this is also a large
   slice of the 72-162 KB/commit index write volume. A skip
   avoids the merge entirely, so the buffered pieces stay stored
   and untouched — the wall AND byte cost both vanish on the
   quiet path.
3. Reshape time is ~100% widened-window regrouping at scale.
4. Index anchors need no stored mark (pure function of separators
   + weights), so the index-level quiet check is STRUCTURALLY
   EASIER than the leaf one: per-piece summaries (child count,
   summed link weight, per-class candidate minima, edge
   separators) decide the plan without opening any piece.

### Landing-zone model (window 3, per 1000 commits, apply = 11.4 s)

Assume a summary-based quiet skip at both levels with the measured
plan-stability rates (97% leaf windows quiet; index assumed
similar, to be validated), local piece edits on the skipped path at
the non-widened reshape rate (~58 us/op, from window 1), and
summary combines at O(pieces) numbers rather than O(run) entries:

- quiet check: 444 ms → ~50 ms (O(pieces) combine, no streams)
- merge_run: 1,182 ms → ~70 ms (3% residual full merges)
- merge_index: 2,974 ms → ~150 ms (5% residual assumed)
- reshape: 6,654 ms → ~800 ms (residual full regroups on plan
  changes + local piece edits on the quiet path)
- election: 293 ms → ~15 ms (only on true changes)
- descent/other: ~160 ms unchanged

Projected apply ≈ 1.2-1.5 s vs 11.4 s (**~8x on apply**); total
commit ≈ 12.3 - 11.4 + 1.4 ≈ **2.3 ms/commit vs 12.3 (~5x)** at
window 3, ≈ 1.2 vs 2.8 ms (~2.4x) at window 1 — and the growth
term flattens: per-op cost stops scaling with run length except in
the O(pieces) summary combine (100x+ cheaper per piece than entry
streams; itself incrementalizable later if runs keep growing).
Secondary effect, unmodeled: index bytes/commit should drop
substantially since buffered level-2 pieces stop being rebuilt on
quiet edits.

Validation gates for the implementation: converge_check at
200/1000/3000/10000 per-txn vs by-five vs single (the skip must
prove the plan identical, forced marks included); byte-identical
stored form (skips produce no new bytes at all on quiet paths);
full suite; A/B measure_se_replay against a pre-change rebuild.

## Stage A shipped: fast path hoisted above the index merge (2026-07-31)

The one-structural-change version of the index-level quiet check:
`merge_forced_index_runs` moved from before the gate computation to
AFTER the fast-path attempt, and `index_widened` dropped from the
fast-path gate. Soundness: the fast-path gates together prove the
edit changes no separator and no membership anywhere (min-move
either absent or byte-identical, `moves_seam_under_ceiling` covers
byte changes under a ceiling), and every index-level plan — seam
ranks, link weights, frame elections — is a pure function of child
separators, so no index merge can be needed for a fast-path edit.
Before this, index widening fired once per op at scale and its
`index_widened` flag blocked the fast path for essentially every
edit — silently cancelling the leaf quiet check's skips (which is
why V1 measured as saving nothing).

Measured (3000/1000, default config, per commit):

- window 2: 9.5 → 4.1 ms; window 3: **12.3 → 5.8 ms (2.1x)**
- index merge windows: 9,033 → 1,224 per 1000 commits (-86%);
  merge_index 2,974 → 509 ms; reshape 6,654 → 2,235 ms
- bytes/commit unchanged (135.5 KB — the rebuilt pieces were
  already byte-identical dup stores)

Validation: full suite (293); converge_check CONVERGED at
200/1000/3000/10000; canonical roots at 200/1000/3000 byte-identical
to the pre-change build (87dd1526... at 3000 on both) — the change
skips redundant work without altering any canonical outcome.

## Min-insert quiet skip: tried, sound, unprofitable — reverted (2026-07-31)

The reject census said min-position inserts are 83% of the interior
guard rejects (1,752/2,120 at 3000 txns), so the quiet check was
extended to them: accept an `Err(0)` insert into a non-first run
piece, verify the stored forced separator is byte-stable under the
new minimum (`frame_separator(prev_last, new_key) == stored`), and
apply in place with the min-move machinery suppressed (the
separator rewrite provably a no-op; re-deriving it would strip the
forced mark).

Measured: only **17 of ~1,150** qualifying inserts passed the
stability check. SE keys are long, so the stored forced separators
take the RIGHT-PREFIX form, whose bytes genuinely change with a new
minimum — those edits must rewrite the boundary and the widening is
semantically required. The failures paid the full O(run) stream
before merging anyway: window 3 regressed 5.8 → 6.7 ms. The
converge oracle passed at 10K even with the skip active (the logic
was sound), but the economics are upside-down: REVERTED, keeping
the reject classification counters and the piece-attribution fix
(the simulation now credits the edit to the descent piece directly
instead of a boundary-index heuristic).

Lesson recorded: at leaf level the remaining widenings are mostly
SEMANTICALLY REQUIRED boundary rewrites (separator bytes change),
not skippable identities. The remaining cost at window 3
(merge_run 1.33 s + reshape 2.15 s + quiet 0.44 s per 1000) is the
O(run) MATERIALIZATION around a 1-2 piece rewrite: ~450 pieces
streamed/lifted per merge so the plan can be re-derived, then 93.6%
of the output reused verbatim. The follow-up ticket is therefore
not more skip conditions but a COMPRESSED widening: evaluate
cut_plan's elections over per-piece summaries (weights, edge keys,
best-interior anchor per piece — memoizable by piece hash), open
only the pieces the verified plan actually rewrites, and emit the
rest as origins without ever materializing their entries. Same
verification-not-decision safety shape: any doubt falls back to the
full merge.

## Compressed quiet check shipped (2026-07-31)

Phase one of the compressed widening: the quiet check's plan
verification now runs at piece granularity over memoized per-piece
summaries, streaming no untouched piece.

- `distribution/summary.rs`: `PieceSummary` — count, weight, edge
  keys, last-entry weight, all-vetoed flag, piece-local coin
  verdicts (`interior_coin_cut`), trailing bank, heaviest interior
  vetoed stretch, and the best interior candidate of each backstop
  kind — memoized per node hash (thread-local, bounded, keyed with
  `max_separator` + `anchor_selector`). Piece-local coin evaluation
  is exact because banks reset at accepted seams, so an accepted
  left boundary means bank-in = 0.
- `cap::election_matches_boundaries`: `choose_cuts`'s recursive
  bisection mirrored at piece granularity — exact while every
  elected cut is a piece boundary (ranges stay whole-piece unions),
  bailing with a definitive "plan changed" the moment an interior
  candidate wins, a range with no candidate spans a boundary, or an
  under-threshold range spans one. Ties resolve by entry position,
  matching `min_by`'s first-minimum over the ascending scan.
- `compressed_run_quiet`: two exact regimes selected by the
  boundary seams' veto status. All-vetoed boundaries = the stretch
  regime (election at `max_segment` over `is_forced_candidate`
  seams). All-accepted = the CEILING regime — which the first
  measurement proved is the dominant one on SE (max_separator=512
  means region seams are almost never vetoed; the first cut of this
  check targeted only the stretch regime and delivered 0 verdicts
  in 5,449 attempts): summaries must show no interior coin cut and
  interior stretches under `max_segment`, each boundary's own coin
  verdict (`leaf_cut(last key, trailing bank + last weight)`) must
  be no-cut, and the election runs at `frame_ceiling` over
  `is_frame_candidate` seams. Mixed boundaries, over-weight interior
  stretches, transient index siblings, or an armed pacing ramp fall
  back to the full stream.
- Safety: in debug builds EVERY compressed verdict is pinned
  against the full streamed check (`debug_assert_eq`). The pin held
  across the whole suite and debug converge_check replays at 300
  and 1000 txns (~4,000 live verdicts).

Measured (3000/1000 release, per 1000 commits): verdicts 3,968
quiet + 44 widen with 1,437 fallbacks; QUIET_NS 440 → 122 ms at
window 3, 914-1,433 → 339 ms at window 2. Per commit: window 2
**4.1 → 2.5 ms**, window 3 **5.8 → 4.4 ms**. Cumulative campaign at
window 3: **12.3 → 4.4 ms (2.8x)**; window 2: 9.5 → 2.5 ms (3.8x).
Suites green (293 + 159), release converge_check CONVERGED at
200/1000/3000/10000 with canonical roots byte-identical to the
pre-change build at every scale (2bb2ee93 / 97d0d796 / 87dd1526 /
f5e466b7).

Remaining at window 3 (per 1000): merge_run 1.12 s + reshape 1.73 s
— the 840 semantically-required merges still materialize the whole
run to rewrite 1-2 pieces. Phase two (compressed merge/regroup:
derive the post-edit plan from summaries, open only the rewritten
pieces, emit the rest as origins) is designed but unbuilt; it is
also where the remaining growth term lives, since runs lengthen
linearly with the dataset.

## Phase two v0: the surgical min-insert (2026-07-31)

The structural key, from `index_frame_cut_positions`: forced-long
separators are NEVER index anchors (over-bound seams are excluded
from candidacy) and a link's election weight reads only its
separator's LENGTH. So a boundary rewrite that keeps the separator
length changes no index-level election input at any ancestor — the
`moves_seam_under_ceiling` concern that sank the earlier B1 attempt
evaporates when the length is preserved (`max(bound+1, lcp+1)` is
constant 513 for SE keys; B1 had demanded byte-stability and found
17/1150, where length-stability holds essentially always).

Shipped as `RunVerdict::SurgicalMin`: for an `Err(0)` insert into a
non-first run piece, the run's post-edit plan is verified boundary-
and mark-exact (compressed election where the regimes apply, the
full stream otherwise — the min insert usually flips its boundary
seam to vetoed, putting the run outside the two compressed regimes,
so the stream licenses most attempts), the forced separator is
re-derived and must keep its stored length; then the edit applies
in place, the separator is swapped, and `reroute_moved_seam`
re-homes the parent's buffered ops. No merge at any level; the
stored state is byte-for-byte what the full merge-and-regroup
would have produced.

Measured (3000/1000): only **107 of ~1,150 min-inserts qualify** —
the failure census says 1,144 fall outside the compressed regimes
(boundary flips to vetoed) and, decisive, **1,037 of those fail
the PLAN comparison**: the flipped seam joins the two pieces'
stretches, the stretch election re-runs over the joined stretch,
and the boundary genuinely moves or dissolves. These edits MUST
restructure 1-2 pieces; no skip condition can save them. Sep-length
failures: zero (the length argument holds universally).

Verdict: v0 is exact (suite green; converge_check CONVERGED at
200/1000/3000/10000 on the final build; canonical roots
byte-identical to the pre-campaign baseline at all scales; debug
pins held) but its hit rate caps at ~10%. The real phase-two ticket is now precisely
characterized: **the narrow merge** — the streamed check already
computes the exact post-edit plan; extend it to return the plan
DIFF, and when the changed boundaries confine to the edited piece's
neighborhood (the measured common case: the flip is local), merge
and regroup ONLY that span with the verified plan as an override,
emitting every other piece as its stored link without
materializing it. That converts the 840 O(run)-materializing merges
per 1000 commits into O(changed-span) rebuilds, and is where the
remaining merge_run (1.0-1.1 s) + reshape (1.7 s) per 1000 and the
linear growth term live.

## Full stats: SQLite vs main vs this branch (2026-07-31)

Closing scoreboard for the campaign, same 4-core x86_64 container,
same day, criterion medians (`--warm-up-time 1 --measurement-time
3`). "main" is `origin/main` @ 534bf48 (pre-campaign) with the
`dialog-baseline` harness grafted in (repo arms cut — main predates
the provider architecture); "branch" is
`claude/root-frame-write-amp-6wb1z8` @ b467707. SQLite arms are the
usual faithful EAV model (three orderings, WAL+NORMAL for
`sqlite_disk`, `synchronous=OFF` for `_nosync`), measured fresh on
the same machine.

### Synthetic `stuff` workloads

| workload | sqlite_mem | sqlite_disk | dialog main | dialog branch | branch vs main | branch vs sqlite_mem |
|---|---|---|---|---|---|---|
| write_small_txns (100 x 1-entity txns), mem | 0.635 ms | 2.30 ms | 10.97 ms | **7.94 ms** | 1.4x | 12.5x behind |
| write_small_txns, disk | — | — | 85.9 ms | **41.9 ms** | 2.0x | — |
| write_batch (1000 entities, 1 txn), mem | 4.38 ms | 4.57 ms | 2.010 s | **289 ms** | **7.0x** | 66x behind |
| write_batch, disk | — | — | 1.945 s | **313 ms** | 6.2x | — |
| point_get | 0.72 us | 1.63 us | 20.8 us | **13.4 us** | 1.55x | 18.6x behind |
| attr_scan (1000 rows) | 144 us | 150 us | 1.159 ms | **1.084 ms** | 1.07x | 7.5x behind |
| join (1000 rows) | 478 us | 477 us | 2.657 ms | **2.551 ms** | 1.04x | 5.3x behind |

Against the first captured SQLite baseline (2026-07-28, notes in
sqlite-baseline-results.md): the small-commit gap has closed from
19x to 12.5x in memory, the batch gap from ~430x to 66x, and
point reads from 29x to 18.6x. Scans and joins were never this
campaign's target and moved only marginally.

### SE replay (the campaign metric): 3000 txns, windows of 1000

| build | window 1 | window 2 | window 3 | bytes/commit | sets/commit (w3) | reads/commit (w3) |
|---|---|---|---|---|---|---|
| main | 5.03 ms | 7.41 ms | 11.27 ms | **592 KB** | 13.1 | 745 KB |
| branch | 1.81 ms | 2.31 ms | 4.10 ms | **135.5 KB** | 6.0 | 88 KB |
| ratio | 2.8x | 3.2x | **2.7x** | **4.4x** | 2.2x | 8.5x |

Main's numbers are WORSE than the spike baseline this campaign
started from (12.3 ms / 136 KB at window 3): the spike lineage
already carried the earlier campaigns' work (piece-origin reuse,
the boundary machinery, distribution rebalance), so against
shipping main the cumulative effect of the perf line is ~2.7x on
commit latency, 4.4x on bytes written per commit, and 8.5x on
bytes read per commit — with convergence verified at every step
and canonical roots byte-identical throughout this session's
changes.

### Durability annotations (correction, 2026-07-31)

Owner correction: main DOES have the capability-provider
architecture (`dialog_storage::capability::Provider`); what it
lacks is specifically the `provider::Dcaa` single-file archive
module, added later on the perf lineage. That is why the repo/DCAA
bench arms could not build on the main graft — the earlier caveat
overstated it, and the dialog_disk arms above DO use the same
`FileSystemStorageBackend` on both builds (like-for-like after
all).

What each dialog arm's durability actually is:

- SE replay and `dialog_mem`/`repo_mem`: `MemoryStorageBackend` —
  VOLATILE. These are the CPU-cost numbers.
- `dialog_disk`/`repo_disk`: file-per-block
  `FileSystemStorageBackend`, which does NOT fsync — durability
  equivalent to `sqlite_disk_nosync` (`synchronous=OFF`), weaker
  than SQLite's WAL+NORMAL bar.
- `repo_dcaa` is the only DURABLE dialog configuration (DCAA
  archive, one fdatasync per commit); `repo_dcaa_nosync` is the
  same archive without the sync.

Durable-tier numbers on this branch (repo surface =
`Branch::commit`, which adds version tags, history claims, and a
signed revision record on top of the same index writes — a heavier
surface than the raw `Artifacts::commit` the dialog_* rows use):

| workload | repo_mem | repo_disk | repo_dcaa (durable) | repo_dcaa_nosync | sqlite_disk (durable) |
|---|---|---|---|---|---|
| write_small_txns (100 x 1-entity txns) | 31.4 ms | 132 ms | **306 ms** | 116 ms | 2.30 ms |
| write_batch (1000 entities, 1 txn) | 698 ms | 759 ms | **675 ms** | 718 ms | 4.57 ms |

Readings: the durable small-commit configuration is fsync-bound —
repo_dcaa minus repo_dcaa_nosync is ~1.9 ms per commit of
fdatasync, ~62% of its time — while SQLite's WAL+NORMAL amortizes
syncs across checkpoints and pays ~23 us per small transaction.
The repo surface itself costs ~3-4x over raw `Artifacts::commit`
(repo_mem 31.4 ms vs dialog_mem 7.9 ms for the same rows: signing,
history, head publication). So the durable-vs-durable small-commit
gap (repo_dcaa vs sqlite_disk, ~130x) bundles three distinct
terms: index CPU (this campaign's target, now 12.5x at the raw
memory tier), the repository surface (~3-4x), and per-commit
fdatasync vs WAL group sync (~2 ms/commit) — the last two being
their own future tickets, not tree work.

Remaining caveat: SQLite arms were measured on the main-graft
build, but the SQLite code is identical in both.

## Bulk-dominance plant: the write_batch tweak (2026-07-31)

Owner question: write_batch suffers far more than anything else
(66x behind sqlite_mem where small txns are 12.5x) — can it be
tweaked? Attribution: a 1000-entity batch sends ~6,000 index ops
through `replay_ops` as ONE canonical edit each (descent + gates +
reshape, ~45 us/op) — for a batch into a fresh store that is pure
overhead, since the canonical tree of a known fact set can be built
bottom-up in one pass.

Two pieces shipped:

1. `TransientTree::plant`: sort ops (stable, so a batch's temporal
   order survives within a key), fold last-wins, drop retracts that
   survive (no-ops on empty), then build canonically —
   `regroup_entries` for the leaf level and `seal_root`'s per-level
   grouping loop upward. History independence makes this exact: the
   canonical form is a pure function of the surviving fact set.
2. The bulk-dominance rebuild in `write_with`: the empty-tree hook
   alone never fired in the real flow (a batch's first instruction
   seeds a tiny root before the rest buffer behind it), so after the
   settle cascade, when the leaf-bound batch dwarfs the live tree
   (>= 64 ops and > 8x the tree's entries, checked by a BOUNDED
   novelty-aware scan that a genuinely large tree aborts after
   deferred/8 entries), the tree's resolved state streams out, the
   deferred ops append (strictly newer — a key's ops cascade as a
   whole link buffer, so no key straddles), and one plant replaces
   the per-op replay. Per-txn commits (~7 ops against thousands of
   entries) never trigger it.

Measured, same machine window (the earlier 289 ms reading was a
faster thermal window; pre-change re-measured at 380-403 ms):
write_batch/dialog_mem **385 -> 270 ms (-29%)**, byte counts and
canonical roots unchanged, converge_check CONVERGED at
200/1000/3000/10000 with the same roots as every prior build, suites green, SE replay byte-identical.

What remains in write_batch's 270 ms is NOT tree work: the
per-instruction apply phase (cardinality-one supersession reads
against the buffered state, per-instruction value encodes — the
very first audit's finding) now dominates. That is an artifacts-
layer ticket: batch the instruction processing, not the tree.

## Read-path waste attribution + warm-batch bench (2026-08-01)

Owner directions: DCAA was decided against, so the durable-tier
rows above are historical context only — the focus is MEMORY
performance until the overall shape is right. The empty-store batch
bench is renamed `write_batch_empty` and a warm twin added.

### write_batch_warm (1000 fresh entities into a 1000-entity store)

| arm | time |
|---|---|
| sqlite_mem | 5.57 ms |
| sqlite_disk | 7.05 ms |
| dialog_mem | **124 ms** (22x behind sqlite_mem) |
| dialog_disk | 131 ms |

Notably CHEAPER than the empty-store batch (270 ms): the warm tree
absorbs the 6,000 ops through the optimized edit path at ~20 us/op
(the campaign's fast-path/quiet-check work), while the empty case
still pays the apply-phase instruction processing over a growing
buffered state. Both are now dominated by the artifacts layer, not
the tree.

### Where a point get's 146,193 instructions go

Callgrind, 4,000 point gets over 1,000 entities, seed profile
subtracted (SQLite spends ~3K instructions on the same lookup):

- **~44% allocator + memcpy + memcmp** — allocation churn: the
  "copying by select" suspicion confirmed, dozens of transient
  allocations per lookup.
- **17% TreeWalker::stream machinery** — `point_get` is implemented
  as an ArtifactSelector + `collect`: a full range-scan pipeline
  (selector build, async stream plan, walker state, Vec collect,
  pop) for a single-key lookup. The tree HAS a direct `get`
  descent; the artifacts layer never exposes it.
- **~20% key construction and parsing** — `Entity::from_str` /
  `Attribute::from_str` run the `url` crate parser per call
  (`Parser::parse_cannot_be_a_base_path` alone is 4.1%), then
  `varkey::build_key` (9.3%) and `parse_key_ref` (3.7%) rebuild
  and re-split key bytes.
- **1.9% StreamingLeaf::next_key** — the actual tree decode. The
  TREE is not the problem on reads; the API shape above it is.

### Ticket list for the read gap (artifacts layer, not tree)

1. (Corrected 2026-08-01: a true point get is impossible under the
   value-in-key format — the value is part of the key, so an
   entity+attribute lookup is inherently a PREFIX SCAN over
   `(of, the, *)`. The scan is there for a reason; the overhead
   around it is not.) A lightweight prefix-scan path: descend once
   to the range start and cursor through entries in place —
   typically one leaf, one to a few entries — instead of the full
   selector -> stream-plan -> TreeWalker -> collect-Vec pipeline.
   The 17% stream-machinery share and most of the allocation churn
   are per-scan setup, not per-entry work, and vanish for short
   prefixes.
2. Key construction without the url parser on the hot path:
   pre-validated `Entity`/`Attribute` types (parse once at the
   edge, reuse bytes), and a `build_key` that writes into a reused
   buffer.
3. Scan materialization: `collect` yields owned Artifacts (rkyv
   deserialize + String/Vec allocs + URI re-parse per row).
   A borrowed/streaming row view — or at minimum reusing buffers
   across rows — attacks the attr_scan (7.5x) and join (5.3x)
   gaps, which are per-row materialization, not tree traversal
   (the leaf streaming itself is ~2%).
4. The remaining write_batch gap is the same layer: apply-phase
   per-instruction supersession reads + value encodes.

## Prefix-scan collect path shipped (2026-08-01)

The first bite of the read tickets, on the branch:

1. `TreeWalker::collect_range` — the stream's exact walk-and-merge
   semantics (pinned by test `it_collects_exactly_what_it_streams`
   across buffered trees and range shapes) as one direct traversal
   into a `Vec`, no async-generator layers.
2. `ArtifactTreeExt::scan_collect` + `Artifacts::select_all` — the
   same per-entry pipeline as `scan` (parse once, match, spill,
   reconstruct, re-check) over the collected entries; `select`
   remains for genuinely large streaming reads.
3. The structural win: the memoized-decode arm now enters the leaf
   at the range's PARTITION POINT (`DecodedKeys::lower_bound`)
   instead of key-materializing every entry from the leaf's start —
   a point-shaped read did ~200 `Key::try_from_bytes` calls before
   reaching its range; now O(log leaf) + hits.

Measured same-window A/B (the stream baseline re-benched under
current thermal conditions at 18.9 us):

| read | stream path | collect + lower_bound | change |
|---|---|---|---|
| point_get/dialog_mem | 18.9 us | **10.0 us** | -47% |
| attr_scan/dialog_mem | 1.386 ms | 1.258 ms | -9% |
| join/dialog_mem | ~2.88 ms | 2.75 ms | -4% |

Both suites green (294 + 159 incl. the new equivalence pin),
converge_check smoke CONVERGED (reads only; writes untouched).

Remaining per point read (~10 us vs sqlite ~1 us same-window):
the per-call `Entity::from_str` URI parse (url crate), selector and
range-bound key builds, the RwLock + tree clone per select_all, the
3-level descent, and per-row Artifact reconstruction. Next bites:
a whitespace-free fast-path validator for `entity:`-scheme URIs
(skip `url::Parser` on the hot path), reusable selector/range
buffers, and a borrowed row view for scans (the attr_scan/join
residue is per-row materialization).

## Pivot: fold the collect wins back into the stream (2026-08-01)

Owner direction: "stick to async streams but avoid materializing
unless needed. Materialization could also probably borrow instead
of copying which would probably offer a big win." So the collect
fork is gone and its structural wins live in `TreeWalker::stream`
itself — one read path again, no equivalence pin to maintain.

Reverted: `TreeWalker::collect_range`, `PersistentTree::
collect_range`, `ArtifactTreeExt::scan_collect`, `Artifacts::
select_all`, the baseline collect switch, and the stream/collect
equivalence test. Kept: `DecodedKeys::lower_bound`.

Ported into `stream`:

1. The memoized-decode arm enters the leaf at the range's
   partition point (`keys.lower_bound(start)`) instead of visiting
   every entry from position 0 — the O(leaf) -> O(log leaf + hits)
   win, now for every streamed read.
2. Byte-level range checks in both arms (`below_start` /
   `past_end_bytes` against the already-computed `start_bytes` /
   `end_bytes`), sound because `Key`'s order agrees with its byte
   order (the same invariant `pending_for_leaf`'s range restriction
   already leans on). `Key::try_from_bytes` now runs only for
   entries that actually YIELD; before, every visited entry paid a
   typed-key materialization just to be range-checked.
3. Buffered-op yields drop their redundant re-check entirely:
   `pending_for_leaf` already restricts ops to the walk's byte
   bounds, so every surviving assert is in range by construction.

Measured same-window (same benches, stream path throughout):

| read | stream before | stream after | collect had |
|---|---|---|---|
| point_get/dialog_mem | 18.9 us | **9.7 us** | 10.0 us |
| attr_scan/dialog_mem | 1.386 ms | 1.474 ms | 1.258 ms |
| join/dialog_mem | ~2.88 ms | 3.34 ms | 2.75 ms |

The point-read win survives the port fully (the stream now beats
the collect fork's own number). attr_scan/join read a few percent
worse than the collect fork in this window, but those spreads are
within the session's observed thermal drift; the collect fork's
own -9%/-4% there were marginal to begin with. The stream
apparatus cost that motivated the fork was a point-read tax, and
the lower_bound + byte-check port removes the dominant share of
it without forking the API.

Suites green (293 + 159 — one fewer: the equivalence pin went with
the fork), converge_check smoke CONVERGED at 1000
(root 97d0d796..., byte-identical; reads only, writes untouched).

Recorded next step (owner-endorsed direction, needs a design pass
before implementing since it changes the yielded type across
consumers): BORROWED MATERIALIZATION. Today every yielded entry is
an owned `Entry<Key, Value>` (rkyv deserialize + Vec/String
allocs), and the artifacts layer builds an owned `Artifact` per
row (more allocs + a URI re-parse). Sketch: yield an `EntryView`
borrowing the leaf's node buffer (Arc + offsets — the buffer is
already Arc-shared and immutable), and an artifacts-level borrowed
row view over it; consumers that need ownership call `.to_owned()`
explicitly. Attribution says per-row materialization is the bulk
of the remaining attr_scan (44% alloc churn) and join gaps, so
this is where the next big read win lives.

## Borrowed materialization shipped: select yields ArtifactView (2026-08-01)

Owner-directed ("materialization could also probably borrow instead
of copying"), option (b): the select API itself changed, consumers
opt into ownership.

What changed:

1. `ArtifactView` (dialog-artifacts): a scanned row as the scan
   holds it — the index key (owned `Key` bytes), its `Datum`
   payload (moved, not cloned), and any fetched spill block.
   Accessors borrow from the key bytes on demand: `parts()` (one
   key walk for multi-field access), `value()` (decode just the
   value), `cause()` (no key walk at all), `to_owned()` (the full
   old per-row materialization, now opt-in).
2. `ArtifactTreeExt::scan`, `Artifacts::select`, and the
   `ArtifactStore` trait yield `ArtifactView`. The scan's
   NeedsValue re-check decodes only the value, not the entity or
   attribute.
3. `ArtifactViewStream::owned()` — chainable stream adapter
   (`select(..).owned()`), the explicit spelling of "materialize
   every row".
4. Branch-level `Select` statement (dialog-repository) yields view
   streams from `perform`/`execute`, and gained `.to_owned()`
   (returning `SelectOwned` with the same perform/execute surface),
   so pre-view consumers migrate with one token:
   `select(..).to_owned().perform(..)`. The query pipeline's
   ingestion points (session `select_from_branch`, the JS binding,
   query-engine test envs) sit on that form for now.
5. dialog-baseline's read arms consume views with per-row work
   equal to the SQLite arms' column reads (entity String + owned
   Value per row): no URI parse, no triple struct — the same work
   both engines are asked to do.

What the old default cost per row (now opt-in): an entity URI
parse (`Entity::from_str` through the url crate), an attribute
alloc, a full value decode + allocs, and a cause clone.

Same-window results (previous window's stream numbers as
baseline; SQLite arms drifted -2 to -19% in this window, so the
dialog deltas below overstate slightly — the vs-sqlite gap
columns are the honest cross-engine read):

| read (dialog_mem) | before | after | sqlite_mem | gap |
|---|---|---|---|---|
| point_get | 9.7 us | 8.5 us (-12%) | 0.99 us | 8.6x |
| attr_scan | 1.474 ms | 588 us (-60%) | 200 us | 2.9x |
| join | 3.34 ms | 1.319 ms (-61%) | 658 us | 2.0x |

The campaign's read-gap story, start to finish: point_get 18.9 ->
8.5 us (2.2x), attr_scan ~7.4x-vs-sqlite -> 2.9x, join ~4.4x ->
2.0x. Scans stopped paying for materialization they don't need;
what remains of the scan gap is stream/tree traversal plus the
per-row entity String + value decode both engines now share.

Validation: full workspace suite green (0 failures across every
crate incl. repository 737), clippy -D warnings clean, fmt clean,
converge_check CONVERGED at 1000 (root 97d0d796..., byte-identical
— reads only). One pre-existing conventions failure surfaced by
the first-ever full-workspace run this session (bare `Send` in the
DCAA provider's spawn_blocking helper from the durability work)
fixed with the sanctioned `bare-send-ok` exemption marker.

Remaining read tickets, updated:
1. Query pipeline on views: `ArtifactStream`/merge/overlay still
   traffic in owned Artifacts; branch select ingests via
   `.to_owned()`. Threading views through (sort keys can come
   straight from the tree key bytes — the merge comparator
   currently re-derives them from materialized rows) moves the
   -60% scan win into engine-level queries.
2. point_get residue: selector-side `Entity::from_str` URI parse,
   selector/range key builds, RwLock + tree clone per select —
   the per-call fixed costs that dominate a one-row read.
3. write_batch residue: apply-phase per-instruction supersession
   reads + value encodes (artifacts layer).

## Query pipeline on views: shipped, engine-level result NEUTRAL (2026-08-01)

The follow-up ticket from the borrowed-materialization work, landed
as 8f1cc5b: `ArtifactStream` now carries `ArtifactView`s end to
end. Branch scans enter the query layer un-materialized; the
`Changes` overlay wraps its rows as owned-backed views (boxed); the
k-way merge orders by a sort key derived ONCE per row from its
stored key bytes; dedup fingerprints on (value tail, cause) — the
tail identifies the value exactly (lossless inline encoding /
content hash when spilled) — replacing a per-row blake3 over the
materialized fact; tombstones compare stored-byte keys against
default-manifest retract keys (byte-equal under the default
manifest; documented). The cardinality-one sliding window compares
group membership on raw key bytes, materializes only group winners,
and the challenge path decodes only the surviving winner's value.
The pre-view merge also re-encoded every stream head's value on
every peek round — that pathology is gone outright.

Measured same-window (A/B against the parent commit under current
conditions):

| bench | before | after | verdict |
|---|---|---|---|
| query_join/1000 | 11.94 ms | 12.32 ms | +3% (noise-adjacent) |
| query_memory/100 | 176 us | 171 us | -3% |
| query_memory/1000 | 1.209 ms | 1.253 ms | +4% |
| query_memory/10000 | 15.33 ms | 15.38 ms | ~0 |

HONEST VERDICT: neutral at the engine level. The scan-side wins
(-60% at the Artifacts::select layer) do not register through the
query engine because the engine's own per-row machinery dominates:
at 1000 entities, query_join runs 12.3 ms against a 1.32 ms
storage-layer join ceiling (~9x engine overhead), and query_memory
1.25 ms against a 0.59 ms attr_scan. What the engine pays per row —
Match/extension clones, term binding plumbing, per-premise
re-selects — dwarfs what materialization cost. Two residual view
overheads partly offset the merge savings at this scale: each
borrowed accessor call re-parses the key (the window does several
per row), and in cardinality-one data with no history EVERY row is
a group winner, so winner-only materialization skips nothing.

Why it still deserved to land: one row currency across the whole
read stack (no double materialization anywhere), the merge's
per-peek value re-encode and per-row blake3 are structurally gone
(matters as sources multiply: multi-branch unions, overlays,
subscriptions), and the pipeline is now shaped for an engine that
binds from bytes.

Updated read-ticket priorities, in expected-impact order:
1. ENGINE BINDING MACHINERY — the dominant term by ~9x on joins.
   Attribute where the 11 ms of query_join/1000 goes (Match clone
   per row? selector resolve per premise? per-binding re-select
   setup?) before touching anything else engine-side.
2. point_get fixed costs (selector URI parse, range key builds,
   RwLock + tree clone per select) — the 8.5us-vs-1us residue.
3. write_batch apply-phase supersession reads + value encodes.
4. Minor: view accessors could carry a parsed-offset cache to
   avoid per-call key re-parsing if a profile ever shows it.

## Engine attribution: the ~9x is select fixed costs, not binding (2026-08-02)

Callgrind on `profile_join` (1000 entities), seed-profile subtracted
to isolate the query side: 125M instructions per query_join run.

Per-query breakdown:

| category | share |
|---|---|
| allocator churn (malloc/free/realloc family) | 30% |
| key construction (build_key, KeyParts::max) | 13% |
| raw memcpy (key builds, clones) | 11% |
| entity URI parse (url crate; winner materialization) | 8% |
| key re-parsing (parse_key_ref; view accessors) | 7% |
| engine binding proper (SipHash, HashMap<String,Binding>, bind) | 8% |
| stream/scan plumbing + blake3 verify | 7% |

HYPOTHESIS REVISED: the engine's binding machinery is only ~8%.
The dominant structure is the join's inner premise issuing ONE FULL
SELECT PER OUTER BINDING (1000 per query), each paying the whole
select fixed cost — selector build, two range-bound build_keys,
scan setup (manifest read, root probe), Changes-overlay scan + sort
+ box, tombstone set, merge setup, boxed stream per level — and the
30% allocator churn is largely this per-select scaffolding being
built and torn down 1000 times.

BYCATCH, and it is big: the seed profile locates the write-batch
residue exactly. Committing 2000 facts costs ~2.3B instructions,
~83% of it inside `Novelty::route` — every enqueued fact lifts the
root link buffer, appends, and re-sorts the WHOLE accumulated
buffer (adaptive sort, but the merge still walks the sorted prefix:
O(n) memmove/memcmp per op, O(n^2) per batch). This is the
"apply-phase" write_batch residue ticket, now with a mechanism.

Ranked tickets with landing estimates (discuss before building):

A. WRITE: batch novelty enqueue — sort each incoming batch once and
   merge, or mark links dirty and sort once at seal/read. Route is
   ~83% of the bulk-commit write path; landing zone is a multiple
   (3-5x?) on warm batch commits.
B. READ: premise-scoped scan context — reuse the pinned tree,
   manifest, catalog/NetworkedIndex, overlay scan, and tombstone
   set across a premise's inner selects (they differ only in the
   probed entity). Attacks the per-select fixed cost AND its alloc
   churn; est. -30-50% on query_join.
C. Reusable key buffers: selector/range build_keys into reused
   allocations (13% + memcpy/alloc share).
D. Winner bind without URI re-parse: the sliding window's winner
   to_owned re-parses the entity URI per yielded row (8%); bind
   entity from validated bytes instead.
E. ArtifactView parsed-offset cache: accessors currently re-walk
   the key per call (7%).
F. Engine binding (HashMap<String, Binding> etc.): real but last
   (~8%); interned symbols / small-map could halve it at most.

## Ticket A landed: two-run novelty insertion (2026-08-02)

`LinkNovelty::Open` is now a sorted main run plus a bounded sorted
tail (128 ops). Enqueue inserts each op into the tail — O(tail)
instead of the stable re-sort that walked the whole accumulated
buffer per op — and the tail merges into the run when full or when
a consumer needs one flat list (take, persist encode, boundary
reroute). resolve / collect_winners_in_range binary-search both
runs; weight and remove-key work on both without consolidating.
Per-key op order is exact (tail ops are newer than run ops for the
same key; the merge keeps run first on ties), so sealed bytes,
cascade timing, and canonical roots are unchanged — converge_check
CONVERGED at all four scales with the pinned roots (2bb2ee93 /
97d0d796 / 87dd1526 / f5e466b7), and the artifacts dev suite runs
the cached-vs-fresh byte-identity debug asserts green.

Same-window A/B (dialog_mem):

| bench | before | after | change |
|---|---|---|---|
| write_batch_empty/1000 | 307 ms | 31.8 ms | -90% (9.7x) |
| write_batch_warm/1000 | 153 ms | 122 ms | -20% |
| write_small_txns/100 | 8.77 ms | 8.17 ms | -7% |
| se_replay per-commit | — | 2.6/3.3/5.5 ms | unchanged |

write_batch_empty vs sqlite_mem (5.3 ms) is now 6x — from 66x at
the campaign's start. The "apply-phase residue" was never mostly
supersession value encodes; it was this insertion sort.

## Self-drive round: engine per-select costs (2026-08-02)

Wall clock in this window is unusable (the same committed code
measured 2x apart across runs), so every claim below is
instruction-counted under callgrind (profile_join, 1000 entities,
25 query iterations, seed subtracted). Baseline at the start of
the round: 121M instructions per query_join run.

Landed, in order:

1. e4bdbec — three per-select reductions:
   - selector_range now builds each range bound with ONE KeyParts
     mutation + one build_key. The old chain
     (min()/max().apply_selector) re-parsed and re-built the whole
     key inside every set_* call, ~10 round-trips per range, paid
     once per outer binding: measured 1.05B instructions inclusive
     = a THIRD of engine query time. -766M total, -26%/query.
   - Uri::from_str memoizes successful parses per thread (bounded
     4096, clear-at-capacity). Entity URIs repeat across scans and
     join probes; a hit is a hash lookup + Url clone vs ~7-10k
     instructions of url::Parser. About -280M gross.
   - QueryEnv pushes the overlay stream into the merge only when it
     matched rows (the common inner probe matches none and now
     takes merge_grouped's single-stream passthrough); tombstones
     share via Arc. Small on its own.
2. Match bindings/claims: HashMap<String, _> -> Vec<(Arc<str>, _)>
   with linear probes and order-insensitive PartialEq. A Match
   clones once per yielded row; the vec clone is one alloc plus
   Arc bumps where the map re-allocated every String key, and
   probes stop paying SipHash. -126M, -6%/query.

Net: 121M -> 79M instructions per query_join run (-35%). Remaining
profile: allocator family ~28%, memcpy ~16% (boxed per-select
stream scaffolding, per-row Key/Value clones), parse_key_ref ~3.4%
(ArtifactView accessors re-walk the key per call), blake3 verify
~3%, KeyParts::max filler allocs ~1%.

Still-open engine tickets, updated:
- B-full: premise-scoped scan context (reuse pinned tree +
  manifest + store handle across a premise's inner selects) —
  attacks the boxed-stream + setup alloc churn.
- The deeper fix for joins is strategic, not micro: the inner
  premise issues one EAV probe per outer binding (1000 selects);
  a merge-join strategy (sort outer bindings, one AEV scan
  interleaved) would collapse them into one scan. Planner
  territory; needs a design pass.
- ArtifactView parsed-offset cache (~3%).

## Round wrap: wall numbers and standing (2026-08-02)

The window quieted enough for the baseline read benches to be
trustworthy again (the sqlite arms — the noise control — returned
to their usual values):

| bench (1000) | dialog_mem | sqlite_mem | gap | campaign start |
|---|---|---|---|---|
| point_get | 4.97 us | 0.99 us | 5.0x | 18.6x |
| attr_scan | 609 us | 210 us | 2.9x | ~7.4x |
| join (storage) | 1.36 ms | 655 us | 2.1x | ~4.4x |
| write_batch_empty | 31.8 ms | 5.3 ms | 6.0x | 66x |
| write_batch_warm | 122 ms | ~5.5 ms | ~22x | — |
| write_small_txns/100 | 8.2 ms | ~0.7 ms | ~12x | ~12.5x |

point_get HALVED this round (8.5 -> 5.0 us): the one select a
point read issues was paying the same ~40k-instruction
selector_range construction the join paid per binding.

Engine-level (still wall-noisy; instruction counts are the source
of truth): query_join/1000 14.6 ms, query_memory/1000 1.61 ms,
/10000 35.4 ms in this window; per-query instructions 121M -> 79M
(-35%) across the round.

Both suites green throughout, clippy clean, converge oracle
byte-identical at every landed step.

## Hardening: the tests this campaign should have had (2026-08-02)

Testing infrastructure derived from the failure modes we actually
hit (or dodged by manually running an example at the right moment),
now part of the workspace suite:

1. CONVERGENCE IN-TREE (`dialog-baseline/tests/convergence.rs`):
   the history-independence oracle promoted from the manually-run
   `converge_check` example to a real test — per-txn vs by-five vs
   single-commit replays of the synthetic SE log must canonicalize
   to the same root, with the key+value digest separating "fact-set
   diverged" (a data bug, e.g. batched supersession dropping a
   write) from "same facts, different shape" (a history-independence
   break). Plus a canonicalize-is-idempotent fixpoint test. Default
   120 txns for CI budget; `DIALOG_CONVERGE_TXNS` scales it; the
   example remains for 10k-scale sweeps.
2. TWO-RUN NOVELTY MODEL TESTS (node/transient.rs): random single-op
   routing (the enqueue hot path) against a BTreeMap op-log
   reference model, checking resolve mid-run at every phase of the
   run/tail split, range winners with the tail non-empty, the flat
   drain order (which IS sealed-bytes identity, since the encoder is
   a pure function of it), multi-link partitioning, the exact
   tail-limit boundary with a single-key chain (the hardest shape
   for newest-last across a consolidation), remove-key across both
   runs, and consolidation idempotence.
3. SELECTOR RANGE EQUIVALENCE PIN (tree.rs tests): the direct
   KeyParts construction against the legacy min()/max()
   .apply_selector view chain it replaced, across a selector matrix
   (entity/attr/value combos, prefixes, value bounds) and a value
   population straddling every encoding decision — inline strings,
   strings AT and around the spill threshold, numerics, bytes,
   entity refs — under the default manifest AND one with a shifted
   inline_n that flips the spill decision.
4. EQUIVALENCE PINS THROUGH THE PUBLIC API
   (`dialog-artifacts/tests/hardening.rs`):
   - scanned-view sort keys (derived from stored key bytes) equal
     field-derived sort keys for every value shape incl. spilled —
     the invariant the query merge's ordering and dedup rest on;
   - sort-key identity tracks VALUE identity, including type
     differences over identical raw bytes ("5" as string vs bytes
     vs integer must not collide) — the dedup fingerprint's
     foundation;
   - the URI memo is transparent: hits equal misses including url's
     normalizations, failures are not cached, and entries survive
     the capacity-clear cycle.
5. MATCH MODEL TESTS (selection/match.rs): random
   bind/bind_absent/lookup sequences against a HashMap reference
   model (verdicts and lookups must agree exactly), and
   order-insensitive row equality pinned in both directions with
   subset/differing-value counterexamples.

Workspace suite now 2158 tests, all green; clippy clean.

What remains unpinned, honestly: the buffered (non-canonical)
published root's byte-stability across enqueue-structure changes is
still guarded only by the artifacts spine-reuse pin and the
converge oracle's canonical roots — a dedicated
"buffered-root replay equality" test (same commits, same published
roots, across two independent stores) exists
(it_commits_identically_when_the_spine_is_reused) but only for one
batch shape; the pacing-ramp prototype's known convergence residue
is documented in converge_check and deliberately not asserted; and
the wasm arms of the new tests run only where the wasm test rig
runs them.
