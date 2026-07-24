# Per-link novelty, segment-encoded

Design consolidation from the post-review perf discussion. Not implemented; rides the one planned format-version bump together with the config-into-manifest work.

## Layout

An index node's children become logically `{separator, hash, novelty}` per link: the ops pending against a child's range live WITH that child's link, routed once at enqueue (one binary search over the separators), instead of one flat node-wide buffer that flush partitions and readers span-filter.

One node, one stored block, the node hash covers exactly the stored block. Explicitly rejected alternatives: splitting a node into a links blob and a novelty blob (either costs a second IO per node, or the hash stops corresponding to a stored block); bao/incremental hashing of a prefix-stable serialization (depends on serializer byte layout as an accident rather than a contract).

Physically the columnar style survives: keep the front-coded separator arrays (prefix, suffixes, ends, hashes) and group the novelty with a per-child ends table. Logically the struct above; byte layout still columnar.

## Novelty uses the SEGMENT codec

The buffer region is encoded exactly as segment nodes encode entries: keys split into schema components, per-node content-derived dictionaries plus front-coded arenas per column, values in an index-aligned table, op polarity (assert/retract) as one more small column.

Consequences:
- **This attacks the persist band** (22% TransientNode::persist + 11% blake3 in the post-fix profile). Buffered ops repeat entities and attributes heavily (the SE workload shape), so dictionary and front-coding compression shrinks the serialized bytes the way M3 shrank leaves, and hash cost is proportional to bytes. The flat rkyv `Vec<NoveltyEntry { key: Vec<u8>, op }>` stores every key whole.
- **Per-link grouping is what makes the segment codec applicable.** A whole-node buffer near the root spans every key region (EAV, AEV, VAE, history, coverage: mixed tags), forcing the opaque MIXED_LAYOUT fallback. Per-link buffers are range-scoped, so below the top of the tree they become tag-homogeneous fast and the full columnar schema applies; MIXED_LAYOUT only where a buffer genuinely straddles, the same rule segments already use.
- Flush becomes column-shaped: columnar buffer into columnar segment can merge column-wise rather than reconstructing rows.
- resolve_pending / streaming reads reuse the existing columnar search machinery (StreamingLeaf-style component search) on smaller per-link buffers.

## What this deletes (the correctness payoff)

- The flush partition step (child i takes link i's buffer verbatim).
- The reader span-derivation machinery (pending_for_leaf's lower/upper inheritance): the descent takes exactly the descended link's ops. This is where the adversarial review found its bound-inheritance suspect; the structure makes the bug class inexpressible rather than carefully avoided.
- per_child_peak's scan (PerChild trigger reads a length; a byte-budget trigger reads column sizes, falling out naturally).

## What stays the same

- Hash coverage semantics: each node still holds (now grouped) the ops in transit through it. No canonical-children/convergence change; ownership does not move between levels.
- Cross-level precedence: a parent link's op is newer than the same key's op deeper down; shallowest wins, as pinned by the review fixes. The 15 pins are the migration safety net.
- "All buffers empty" remains THE canonical form of the format, deterministic and reproduced by canonicalize.

## Open questions for implementation time

- Capacity accounting: per-link op counts vs per-link byte budgets vs node total; the manifest work decides where the knobs live.
- Encode cost: columnar re-encode per commit replaces rkyv serialize of the flat vec; measured on leaves encode_columns is modest, but verify the buffer-sized case (append-mostly workload re-encodes the touched link's columns each persist).
- Root-adjacent MIXED_LAYOUT buffers: acceptable fallback or worth a per-region sub-grouping at the root only.
