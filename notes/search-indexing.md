# Search indexing: full-text, ranking, and vectors

Design assessment for adding full-text search (with BM25-style ranking) and
approximate nearest-neighbor vector search to dialog. The governing
requirement is incremental maintenance: indexes must update per-commit
without global rebuilds, replicate via the existing tree diff machinery,
and converge under merge.

## Prior art considered

- Dolt's vector index work: the announcement posts
  ([getting started](https://www.dolthub.com/blog/2025-02-06-getting-started-dolt-vectors/),
  [plan](https://www.dolthub.com/blog/2024-09-26-plan-for-vectors/)) and the
  technical
  [deep dive](https://www.dolthub.com/blog/2025-06-23-vector-index-deep-dive/)
  on their *ProximityMap* structure — a prolly-tree sibling where a key's
  level is derived from leading zeros of its hash (probabilistic chunking,
  the same trick as our geometric boundary coin) but keys are arranged by
  proximity to a parent representative vector instead of sort order. This
  yields history independence: identical data produces a bitwise-identical
  index, enabling structural sharing across branches, diffs, and merges.
  Incremental inserts are amortized O(log N), with rare O(N) worst cases
  when root-level representatives change (batch-friendly, not
  synchronous-write-friendly).
- [Fika's local-first search journey](https://paoramen.fika.bar/local-first-search-01K1B0WM1X4P5SV5QAES0Z5N75):
  every sidecar approach (server engine, in-memory browser engine with
  rebuild-on-load, second sync pipeline feeding the index) collapsed under
  rebuild cost, memory pressure, or sync complexity. Their endpoint — a
  persistent on-device index updated incrementally by diffing changed
  documents — is an argument for making the index a first-class replicated
  region of the database itself. Also notable: hybrid semantic search was
  *noisier* than keyword+BM25 in practice, and 50k × 768-dim embeddings
  weighed ~400–500 MB. Keyword search first is the user-validated ordering.

## Why an inverted index fits dialog unusually well

Three existing properties do most of the work.

### The tag-region pattern is the extension point

All indexes live in one prolly tree, discriminated by a leading tag byte:
EAV(0), AEV(1), VAE(2), history(3), blob(4), coverage(5). The blob index is
the precedent for adding a region. A posting region is:

```
POSTING_KEY_TAG ‖ attribute ‖ term ‖ entity  →  Datum { blob: {tf, doc_len, positions?} }
```

Touch points, following the blob-index precedent:

- new tag const + `Schema` in `TreeKey::schema()` (`dialog-artifacts/src/key.rs`)
- derivation in `write_instructions()` (`dialog-artifacts/src/tree.rs`),
  emitting posting keys alongside the EAV/AEV/VAE keys in the same
  `BufferedBatch`
- region classification in `merge.rs` (`tag_span`)
- shipment in `spill.rs` if entries ever reference out-of-tree blocks
  (they should not need to; postings are small)

Ordering `attribute ‖ term ‖ entity` (an AEV analogue with the value
tokenized) scopes search per attribute, which matches the data model and
keeps ranges tight. Global search unions across the attributes of
interest.

### Posting entries are a pure, fact-local function of each fact

Tokenize the string value at write time — the raw value is in hand in
`write_instructions` even for values that will spill — and emit one key per
term. No global state is consulted. This single property buys the whole
requirements list:

- **Incremental**: only changed facts' terms are touched; the prolly tree
  rewrites only affected leaves; hitchhiker novelty buffers absorb the
  per-fact key fan-out.
- **History-independent**: identical facts ⇒ identical postings ⇒ identical
  tree shape ⇒ structural sharing across branches. What Dolt engineered
  ProximityMap to achieve, an inverted index gets for free.
- **Mergeable**: a posting's fate follows its source fact's fate under the
  existing observed-remove screening; alternatively postings can be
  re-derived for facts that survive merge — deterministic either way.
  (Open decision: screen-shipped-postings vs. re-derive-on-integrate.)
- **Replicated**: postings ride the same node-diff shipment as every other
  region. No sidecar index, no per-device rebuild.

### `Scale` supplies the global statistics without global state

The mutable global counters that ranking needs (document frequency, corpus
size) are what usually make tf-idf/BM25 hostile to merging — counters do
not converge. The per-link `Scale` estimate
(`dialog-search-tree/src/scale.rs`) answers range-size questions without
descending:

- `df(term)` ≈ scale of the `(attribute, term, *)` range
- `N` ≈ scale of the `(attribute, *, *)` range in AEV
- `avgdl` — per-document length stored in the posting payload
  (fact-local), or estimated

Every BM25 input is either fact-local or a scale-based range estimate;
nothing global is stored. Approximate df is fine for ranking. Nothing in
`dialog-query` consumes `Scale` yet — this would be its first consumer,
and the same estimates order posting-range intersections in the planner.

## Query surface

A new `Proposition` variant (e.g. `Match { attribute, query, entity,
score }`) in `dialog-query`, evaluated by streaming per-term key ranges
and merge-union/intersecting with BM25 top-k. Two invariants:

- Every read flows through `Provider<Select>` (see
  `notes/tree-relations.md`) — this is also what makes standing search
  subscriptions incremental: the term ranges read become the demand cover,
  so a subscription re-evaluates only when postings in those ranges
  change.
- `estimate()` comes from `Scale` over the term ranges, so the greedy
  planner can order a `Match` premise against ordinary triple premises.

Prefix search already falls out of key ranges (the `StartsWith`
refinement pipeline); typo tolerance can come later as a second
fact-local posting region under a trigram analyzer.

## Costs and open questions

- **Write amplification is the real price.** A 5 KB document is ~800
  tokens ⇒ ~800 posting keys vs. 3 keys per fact today. Mitigations: term
  keys are short (long text already spills, so postings are new small
  keys); columnar leaves dictionary-compress repeated attributes/terms;
  hitchhiker buffers absorb bursty small writes. Benchmark with the
  StackExchange corpus (`notes/benchmark-dataset.md`) whose
  `se.post/body` values are exactly this shape. Keep term keys well under
  `max_separator` (512) to avoid the rank-0 demotion pathology from
  `notes/boundary-policy-experiment.md` — truncate terms at a fixed byte
  budget.
- **Retraction needs the old text.** Removing postings re-derives the old
  fact's tokens; for spilled values that means loading the archive block.
  `Replace` already scans the superseded range, so the hook exists; the
  cost is one block load per retraction of a large value.
- **Tokenizer determinism is a format concern.** Replicas must derive
  byte-identical postings, so the analyzer (Unicode segmentation +
  lowercase + unaccent to start) is versioned as a `Manifest` constant per
  `notes/config-audit.md`. Stemming and language-specific analysis arrive
  only behind version bumps.
- **Which attributes to index** is the main open design question.
  `AttributeDescriptor` is descriptive, not storage-directing. Opt-in via
  reserved `dialog.index/*` facts is attractive (config-as-data,
  replicates naturally) but makes derivation non-fact-local: config
  changes require region backfill and merge treatment of config becomes
  load-bearing. Start with index-all-String-values and measure; revisit
  if bloat demands opt-in.

## Vectors: phase 2, with a fork in the road

Vectors themselves arrive as ordinary facts (`Value::Bytes` now,
`Value::Record` per `notes/record-value.md` later). Embedding generation
stays outside the database. Two index paths:

1. **Deterministic LSH region (recommended first).** Fixed
   random-projection hyperplanes with seeds pinned in `Manifest` make
   bucket keys fact-local and deterministic — literally the same
   integration as the posting region (`tag ‖ bucket ‖ entity`), with
   multi-probe at query time. Worse recall than a real ANN structure, but
   incremental, mergeable, and nearly free once the FTS plumbing exists.
2. **A ProximityMap-style tree (if recall demands it).** The node codecs,
   storage, delta, cache, and diff machinery in `dialog-search-tree` are
   reusable; the ordered-key content-defined-chunking placement logic
   needs a proximity-based sibling where levels come from hash leading
   zeros and children cluster under representative vectors. Real work,
   but dialog is unusually well positioned: the hash-derived-level idea
   is already native here.

## Suggested phasing

1. Posting region + deterministic analyzer + `Match` proposition with
   scale-based BM25. Benchmark write amplification on the StackExchange
   corpus.
2. Positions in payloads for highlighting; trigram region for fuzziness;
   planner cost integration via `Scale`.
3. LSH vector region; evaluate a ProximityMap-style tree only if recall
   is insufficient.

Explicitly rejected: any per-device sidecar index (Tantivy/FlexSearch
style). That is the failure mode the Fika article documents — a shadow
database with its own rebuild cost and sync pipeline, discarding exactly
the structural-sharing and incremental-diff properties the prolly tree
already provides.
