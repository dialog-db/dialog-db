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
