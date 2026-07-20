# Config audit: what belongs in the manifest

Inventory of compile-time constants that shape stored data, for moving into the self-describing `Manifest` in ONE format bump.

Inclusion test, from `manifest.rs`'s own argument: **if two peers ran different values, would they produce different bytes/hashes for the same logical data, or diverge in tree shape?** Yes -> it is secretly part of the format. Constants that only affect in-memory performance with byte-identical output do not belong.

## The key distinction: breaks-old-reads vs merely-forks

This should drive the design, because the two groups want *different treatment*.

**Misparses existing data** (read back at fixed offsets or as unlength-prefixed fields, so a mismatched peer reads plausible garbage rather than failing):

- The per-layout column `Schema` table (`dialog-artifacts/src/key.rs:293`) - the leaf stores `layout: u8` but NOT the schema it selects
- `VERSION_LENGTH = 40` (`dialog-capability/src/history/version.rs:12`) - `parse_key` does `split_at_checked(VERSION_LENGTH)` with no delimiter
- `ValueDataType` discriminants (`artifacts/value.rs:832`)
- `SPILL_FLAG = 0x80` (`key/varkey.rs:39`)
- ordkey `TERMINATOR = 0x00` / `ESCAPE = 0xFF` (`artifacts/ordkey.rs:31,41`)
- `VALUE_REFERENCE_LENGTH = 32` (`key/varkey.rs:29`)

These must be **pinned and hard-rejected on mismatch**, not made settable. The manifest's existing "unknown version is rejected rather than silently misread" discipline is the right model; extend it to cover these.

**Merely forks** (new bytes differ, old data still decodes):

- `RESTART_INTERVAL = 16` (`search-tree/node/codec.rs:33`) - entries per restart block in the front-coded key stream. Its own doc says "Part of the storage format; changing it changes node bytes." Restart offsets are *stored*, not recomputed, so a decoder does not need the value to read. Needs a `>= 1` guard (`index % 0` panics).
- The hitchhiker triple: `DEFAULT_OP_BUF_SIZE = 256`, `FlushPolicy`, `FlushTrigger` (`search-tree/hitchhiker.rs:60,87,106`)
- `BOTTOM_RANK = 1` (`search-tree/node/transient.rs:20`)

Only this second group needs to be genuinely settable. Treating both groups uniformly would present dangerous knobs as safe ones.

## The hitchhiker triple is a design decision, not a mechanical move

`PersistentIndex.novelty` is a *persisted* field, so a node's hash covers its pending buffer, so these three constants decide node bytes by deciding where ops currently sit. `buffered.rs` already states the consequence: the same fact set hashes differently depending on where its ops sit.

But this is *already accepted as non-convergent by design*, with `canonicalize()` as the stated remedy. So the real question is the contract:

- If **only canonicalized roots are comparable**, these do not need to be in the manifest at all (canonicalize drives novelty to empty, and an empty-novelty node is byte-identical to canonical).
- If **non-canonicalized roots should compare equal across peers**, they must be.

Note they are *already* per-tree settable via `with_op_buf_size`/`with_flush_policy`/`with_flush_trigger`, with no record in the bytes of which values produced a given root. That makes them more dangerous than plain hardcoded constants. `FlushTrigger::PerChild { floor }` carries a payload, so the field is not a plain scalar.

## Active bug to fix in the same change

`Manifest::default()` is assumed in **three** places, not one. All three are correct only while every tree uses the default manifest, so all three activate the moment the manifest becomes genuinely configurable, i.e. the moment this project succeeds.

1. **`inline_threshold()`** (`dialog-artifacts/src/key.rs:120`) returns `Manifest::default().inline_n`. Roughly 8 call sites (`key.rs`, `key/entity.rs`, `key/attribute.rs`, `key/value.rs`, `key/history.rs`, `artifacts/match.rs`).

   **Correction to an earlier framing of mine:** I described this as `inline_n` being "honored on one path and ignored on the other", i.e. a live two-path disagreement with the search tree's boundary coin. That is wrong. Grepping `inline_n` inside `dialog-search-tree` finds it only in `manifest.rs` and tests: the coin reads `max_separator` and `branch_factor` only. `inline_n` is inert inside the search tree, and its sole consumer is `dialog-artifacts`. So this was a single global that would have disagreed with the tree's own *recorded* value once manifests became configurable, not a disagreement between two live paths today. Still worth fixing, for the same reason, but the severity was overstated.

2. **`TransientTree::new`** (`tree/transient.rs:123`) stamps `Manifest::default()`, discarding the loaded root's actual manifest. Its own TODO (line 88) states the blocker precisely: adopting it means reading the root node, which is *async*, and the synchronous `edit()`/`new` entry cannot do that.

3. **`TransientTree::from_loaded`** (`tree/transient.rs:138`) same.

So editing a non-default tree currently **rewrites it in the default format** rather than preserving its own. That is worse than the spill disagreement: it silently reformats data on write.

The stitch path (`tree/transient.rs:518-533`) already demonstrates the right pattern: load the first source root, read `node.manifest()?`, fall back to default only when there is no source to inherit from. It is async there, which is exactly why it works.

**Fix shape:** make manifest acquisition async at the edit entry point (or carry the manifest on `PersistentTree` so it is known without a load), then thread the tree's manifest into the key builders instead of `inline_threshold()`'s global default. The three sites want one solution, not three.

## Coupling hazards

- **`max_separator` (512) x `inline_n` (4096)**: both already in the manifest, independently settable, with nothing validating the relation. A value in the 512..4096 band is inline (so it rides in the key, making the key >512B) yet permanently rank 0, so it can never be a boundary. Raising `inline_n` without raising `max_separator` widens this dead band. Add a constructor check.
- **`VERSION_LENGTH` eats separator headroom**: history keys are `tag(1) + version(40) + entity + attribute + value_tail`, so 41 bytes are gone before any content, ~8% of the 512 budget. The constants live in *different crates* (`dialog-capability` vs `dialog-search-tree`) with no shared invariant.
- **ordkey escapes are a locked triple**: `ESCAPE = 0xFF`, `MAX_FILLER_BYTE = 0xFE`, and the no-field-may-begin-with-`0xFF` invariant. This already caused a real two-commit `Replace` data-loss bug (see `design_varkey_escape_invariant`). Pin under `version`; do not make independently configurable.
- **`SPILL_FLAG` x `ValueDataType`**: the flag bit must not collide with any discriminant, which is why discriminants are capped at 7 bits. These two move together.

## Duplicates and stale definitions to collapse

Each is a place where a future edit changes one copy and not the other, which is exactly how silent divergence gets introduced:

- `HISTORY_KEY_TAG = 3` defined twice (`key.rs:54`, `key/history.rs:58`)
- `VALUE_REFERENCE_LENGTH = 32` defined twice (`key.rs:88`, `key/varkey.rs:29`)
- `BRANCH_FACTOR = 254` defined twice (`distribution.rs:161`, `artifacts/constants.rs:11`), both apparently dead - production reads `manifest.branch_factor()`
- `HISTORY_KEY_LENGTH` + offsets (`artifacts/history/key.rs:9`): apparently-dead pre-M3 fixed-width history key layout; a stale parallel key layout is a real misread hazard

## Explicitly NOT the manifest (runtime-only, byte-identical output)

`CACHE_CAPACITY`, compression `BUFFER_SIZE`/`WINDOW_SIZE`/`COMPRESSION_LEVEL` (verified: compression is applied *below* the hash, in the storage backend, and brotli round-trips losslessly), `UPLOAD_CONCURRENCY`, `STREAM_CHUNK_SIZE`, `MAXIMUM_TREE_DEPTH`, `MAX_DEPTH`, `SMALL_DIVERGENCE`, IndexedDB schema version, `SPEC_KEY_LENGTH`.

`MIXED_LAYOUT = u8::MAX` IS written into node bytes but is a sentinel with no meaningful alternative; pin under `version`.

## Unresolved, worth checking

1. **Is `rkyv` output canonical for `State<Datum>`?** If any `HashMap`/`HashSet` appears in its transitive fields, node bytes could vary run-to-run on the *same* build - a divergence source no manifest can fix. Highest-value open item.
2. **rkyv version is itself an implicit format parameter.** Node bytes are `rkyv::to_bytes` output, so archive layout is part of the format. `FORMAT_VERSION` does not capture it; arguably pin rkyv with an exact `=` requirement in `Cargo.toml`.
3. `MAX_SKIP_LEVEL = 32` changes the signed `RevisionRecord` bytes (it is a serialized, signature-covered field), so two peers with different caps cannot produce byte-identical records for the same commit. Whether that matters depends on whether any path re-derives and byte-compares a record, which was not established.
4. `MAX_FILLER`/`KEY_SPAN_FILLER` affect query bounds and merge scope rather than stored bytes: two peers answer the same query differently against byte-identical trees. The TODO proposing exclusive prefix-successor bounds would delete the constant instead, which is the better fix.
