//! Shared tree-ops on the artifact search tree.
//!
//! Both [`Artifacts`](crate::Artifacts) and the higher-level branch
//! abstractions in `dialog-repository` and `dialog-query` operate on the
//! same EAV/AEV/VAE search tree. The per-instruction mutation loop and the
//! selector → key-range scan dispatch are identical across all of them, so
//! they live here as an extension trait on [`ArtifactTree`], parameterized
//! over any store that exposes the raw hash-addressed
//! [`StorageBackend<Key = Blake3Hash, Value = Vec<u8>>`].
//!
//! Callers responsible for revisions, upstreams, remote fallback, or any
//! other branch specifics keep that logic on their side and call
//! [`ArtifactTreeExt::apply`] / [`ArtifactTreeExt::scan`] for the actual
//! key writes and range scans. Mutations accumulate in the tree's delta;
//! callers must flush and persist the buffers when they mint a revision.
//!
//! The tree stores raw fixed-size key bytes and rkyv-native values:
//! [`Key`] is a transparent newtype over [`KeyBytes`] and passes through
//! unchanged, while [`State<Datum>`] is the tree's value type directly,
//! serialized into node buffers by the tree itself.
//!
//! `ArtifactTree` is a type alias for a `dialog_search_tree::PersistentTree`, so the
//! orphan rule rules out inherent methods — the operations are exposed as
//! an extension trait instead.

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::{Blake3Hash as NodeHash, ConditionalSend, ConditionalSync};
use dialog_search_tree::{
    Buffer, Cache, ContentAddressedStorage, Delta, PersistentTree, Value as TreeValue,
};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::{Stream, StreamExt};
use std::iter::repeat_n;
use std::ops::RangeInclusive;

use crate::{
    Artifact, ArtifactSelector, AttributeKey, AttributeKeyPart, Datum, DialogArtifactsError,
    EntityKey, EntityKeyPart, FromKey, Instruction, Key, KeyView, KeyViewConstruct, KeyViewMut,
    State, ValueDataType, ValueKey, encode_value_owned,
    key::value_spills,
    key::varkey::{self, ValuePayload, ValueRef, parse_key_ref},
    match_selector_and_key_ref,
    selector::Constrained,
};

/// The concrete search-tree type the artifact indexes use.
///
/// Keys are the raw fixed-size bytes of [`Key`]; values are [`State`]
/// payloads stored in the tree's native (rkyv) encoding.
pub type ArtifactTree = PersistentTree<Key, State<Datum>>;

impl TreeValue for State<Datum> {}

/// Adapts a [`StorageBackend`] keyed by raw `[u8; 32]` hashes (the
/// [`dialog_storage::Blake3Hash`] alias used throughout the artifact
/// stores) to the [`dialog_common::Blake3Hash`] newtype keys the search
/// tree addresses nodes by. The conversion is a transparent byte copy.
#[derive(Clone, Debug)]
pub struct TreeStorageBridge<S>(pub S);

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> StorageBackend for TreeStorageBridge<S>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    type Key = NodeHash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.0.set(*key.as_bytes(), value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.0.get(key.as_bytes()).await
    }
}

/// Writes a spilling value's raw bytes as a content-addressed block into the
/// raw archive block `store`, keyed by the value's 32-byte reference. A no-op
/// for a value that stays inline (its bytes live in the key). Idempotent:
/// content-addressed, so the same value writes the same block.
///
/// This uses the raw backend directly, NOT the tree's `ContentAddressedStorage`
/// bridge: a spilled value is a plain block addressed by its value reference,
/// living in the same store the tree nodes do.
async fn store_spilled_value<S>(
    store: &mut S,
    artifact: &Artifact,
) -> Result<(), DialogArtifactsError>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    if value_spills(&artifact.is) {
        let reference = artifact.is.to_reference();
        store.set(reference, artifact.is.to_bytes()).await?;
    }
    Ok(())
}

/// A cache of spilled value blocks, keyed by their 32-byte content reference.
///
/// Spilled blocks are content-addressed, so a reference always maps to the
/// same bytes: cached entries never go stale and need no invalidation. The
/// cache is small by entry count because the values are large (each above the
/// tree's inline threshold); see [`Cache::with_capacity`] and
/// [`SPILL_CACHE_CAPACITY`].
pub type SpillCache = Cache<Blake3Hash, Vec<u8>>;

/// Entry-count cap for a [`SpillCache`]. Small because spilled values are
/// large (kilobytes and up), so a large count would pin a lot of memory.
pub const SPILL_CACHE_CAPACITY: usize = 128;

/// Creates a [`SpillCache`] with the default [`SPILL_CACHE_CAPACITY`].
pub fn spill_cache() -> SpillCache {
    Cache::with_capacity(SPILL_CACHE_CAPACITY)
}

/// The spilled value reference a key carries, or `None` for an inline key.
///
/// A single `parse_key` walk yields the value payload as an already-classified
/// [`ValuePayload`] (inline vs reference), so this reads the spill flag and the
/// reference bytes from one parse rather than re-splitting the key per accessor.
fn spilled_reference(key: &Key) -> Result<Option<Blake3Hash>, DialogArtifactsError> {
    let Some(parts) = varkey::parse_key(key.as_ref()) else {
        return Ok(None);
    };
    let ValuePayload::Reference(payload) = parts.value else {
        return Ok(None);
    };
    let reference: Blake3Hash = payload.as_slice().try_into().map_err(|_| {
        DialogArtifactsError::InvalidKey("spilled value reference is not 32 bytes".to_string())
    })?;
    Ok(Some(reference))
}

/// Fetches the raw bytes of a spilled value for `key` from the raw archive block
/// `store`. Returns `None` for an inline key (its value lives in the key, no
/// block to fetch), `Some(bytes)` for a spilled key. Errors if a spilled key's
/// block is missing from the store.
///
/// Uses the raw backend directly (the value block is addressed by the key's
/// 32-byte reference), not the tree node bridge.
pub async fn fetch_spilled<S>(store: &S, key: &Key) -> Result<Option<Vec<u8>>, DialogArtifactsError>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    let Some(reference) = spilled_reference(key)? else {
        return Ok(None);
    };
    let bytes = store.get(&reference).await?.ok_or_else(|| {
        DialogArtifactsError::InvalidValue("spilled value block missing from store".to_string())
    })?;
    Ok(Some(bytes))
}

/// Like [`fetch_spilled`], but serves and populates a [`SpillCache`]: a hit
/// returns the cached bytes without touching `store`; a miss fetches from
/// `store` and inserts. Because spilled blocks are content-addressed the cache
/// never serves stale bytes.
pub async fn fetch_spilled_cached<S>(
    store: &S,
    cache: &SpillCache,
    key: &Key,
) -> Result<Option<Vec<u8>>, DialogArtifactsError>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    let Some(reference) = spilled_reference(key)? else {
        return Ok(None);
    };
    fetch_spilled_reference(store, cache, reference.as_ref())
        .await
        .map(Some)
}

/// Fetches (and caches) the bytes of a spilled value block by its raw 32-byte
/// content-addressed reference. The scan path holds the reference already
/// (parsed from the key), so it fetches directly rather than re-deriving the
/// reference from the key. Errors if the block is missing.
pub async fn fetch_spilled_reference<S>(
    store: &S,
    cache: &SpillCache,
    reference: &[u8],
) -> Result<Vec<u8>, DialogArtifactsError>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    let reference: Blake3Hash = reference.try_into().map_err(|_| {
        DialogArtifactsError::InvalidKey("spilled value reference is not 32 bytes".to_string())
    })?;
    cache
        .get_or_fetch(&reference, async |reference: &Blake3Hash| {
            store.get(reference).await
        })
        .await
        .map_err(DialogArtifactsError::from)?
        .ok_or_else(|| {
            DialogArtifactsError::InvalidValue("spilled value block missing from store".to_string())
        })
}

/// Filler length appended to a prefix to form its inclusive upper bound. Keys
/// are lossless and order-preserving, so `prefix ‖ 0xFE…` dominates every
/// UTF-8 continuation of `prefix` up to this many trailing bytes.
// TODO(m3): like `KeyParts::max`, this bounds an unbounded field with a
// generous but finite filler; exact for the 64-byte attribute cap,
// best-effort for arbitrarily long entity URIs. Revisit with exclusive
// (prefix-successor) range bounds.
const PREFIX_FILLER: usize = 256;

/// The lower key-segment bound for a string prefix: the prefix's raw bytes.
/// Every value beginning with the prefix is >= this.
fn prefix_lower(prefix: &str) -> Vec<u8> {
    prefix.as_bytes().to_vec()
}

/// The upper key-segment bound for a string prefix: the prefix followed by a
/// `0xFE` filler, >= every UTF-8 value beginning with the prefix (UTF-8 bytes
/// are `<= 0xF4`). `0xFE` rather than `0xFF`: a field must never begin with
/// the `ordkey` escape byte, or the preceding field's terminator misreads as
/// an escaped zero (see `varkey::MAX_FILLER_BYTE`); with an empty prefix the
/// filler's first byte IS the field's first byte.
fn prefix_upper(prefix: &str) -> Vec<u8> {
    let mut bytes = prefix.as_bytes().to_vec();
    bytes.extend(repeat_n(0xFEu8, PREFIX_FILLER));
    bytes
}

/// Tighten a scan's `(start, end)` key pair with the selector's
/// prefix bounds. A prefix on a field that also has an exact
/// constraint is skipped — the exact value is already in the keys
/// and is strictly tighter. Applying a prefix to a non-leading key
/// dimension is sound (the range stays a superset of the matches;
/// [`MatchCandidate::matches_selector`] filters the rest) and
/// tightens the range whenever every more-significant dimension is
/// exact.
fn apply_prefix_bounds<K: KeyViewMut>(
    start: K,
    end: K,
    selector: &ArtifactSelector<Constrained>,
) -> (K, K) {
    let mut start = start;
    let mut end = end;
    if selector.attribute().is_none()
        && let Some(prefix) = selector.attribute_prefix()
    {
        let lo = prefix_lower(prefix);
        let hi = prefix_upper(prefix);
        start = start.set_attribute(AttributeKeyPart(&lo));
        end = end.set_attribute(AttributeKeyPart(&hi));
    }
    if selector.entity().is_none()
        && let Some(prefix) = selector.entity_prefix()
    {
        let lo = prefix_lower(prefix);
        let hi = prefix_upper(prefix);
        start = start.set_entity(EntityKeyPart(&lo));
        end = end.set_entity(EntityKeyPart(&hi));
    }
    // A value prefix bounds the value tail directly: the payload's inline
    // order-preserving bytes for a string are the raw UTF-8, so the prefix's
    // raw bytes are the lower bound and `prefix ‖ 0xFE…` the upper (mirroring
    // the entity/attribute prefixes, but on the value slot). An exact value
    // takes precedence and skips this. Only sound on the VAE ordering, where
    // the value tail leads the key; on EAV/AEV the value is trailing, so
    // `selector_range` routes a value-prefix scan to `ValueKey`.
    if selector.value().is_none()
        && let Some(prefix) = selector.value_prefix()
    {
        let lo = prefix_lower(prefix);
        let hi = prefix_upper(prefix);
        start = start.set_value(ValueDataType::String, ValuePayload::Inline(lo));
        end = end.set_value(ValueDataType::String, ValuePayload::Inline(hi));
    }
    // A numeric value range bounds the value tail to a sub-band. A bound value
    // encodes order-preservingly, so its bytes are a key range edge; the open
    // side is the band edge of the bound's type (its lowest/highest inline
    // value). Exclusive bounds (`>`/`<`) still set the key edge at the bound
    // value — the range stays a superset and the per-entry re-check drops the
    // boundary value. An exact value takes precedence and skips this.
    if selector.value().is_none()
        && (selector.value_lower().is_some() || selector.value_upper().is_some())
    {
        // Both edges must sit in the same type band, so derive the band type
        // from whichever bound is present (they share a type when both are).
        let band = selector
            .value_lower()
            .or(selector.value_upper())
            .map(|bound| bound.value.data_type())
            .unwrap_or_else(ValueDataType::min);
        let lo = match selector.value_lower() {
            Some(bound) => encode_value_owned(&bound.value),
            None => value_band_min(band),
        };
        let hi = match selector.value_upper() {
            Some(bound) => encode_value_owned(&bound.value),
            None => value_band_max(band),
        };
        start = start.set_value(band, ValuePayload::Inline(lo));
        end = end.set_value(band, ValuePayload::Inline(hi));
    }
    (start, end)
}

/// The lowest inline value byte-encoding of a numeric type's band: all-zero
/// bytes of the type's fixed width. Order-preserving encodings put the type's
/// minimum at the bottom of its band, so this is the lower edge when only an
/// upper value bound is set.
fn value_band_min(value_type: ValueDataType) -> Vec<u8> {
    vec![0x00; numeric_width(value_type)]
}

/// The highest inline value byte-encoding of a numeric type's band: all-`0xFF`
/// bytes of the type's fixed width, the upper edge when only a lower bound is
/// set.
fn value_band_max(value_type: ValueDataType) -> Vec<u8> {
    vec![0xFF; numeric_width(value_type)]
}

/// The fixed inline width of a numeric value type's order-preserving encoding.
/// Non-numeric types have no fixed width; they return 0 (a value range over a
/// non-numeric type is not expressible and the caller never constructs one).
fn numeric_width(value_type: ValueDataType) -> usize {
    match value_type {
        ValueDataType::UnsignedInt | ValueDataType::SignedInt | ValueDataType::Float => 16,
        ValueDataType::Boolean => 1,
        _ => 0,
    }
}

/// Shared mutation + scan operations on an [`ArtifactTree`].
///
/// An extension trait rather than inherent methods because
/// `ArtifactTree` aliases a foreign `dialog_search_tree::PersistentTree` — the
/// orphan rule forbids `impl ArtifactTree { .. }`. Uses
/// `#[async_trait]` (matching [`ArtifactStore`](crate::ArtifactStore))
/// so the async `apply` desugars to a boxed future rather than a
/// bound-less native `async fn`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ArtifactTreeExt {
    /// Drain a stream of [`Instruction`]s into the tree, applying the
    /// same key writes that a branch commit or `Artifacts::commit`
    /// would.
    ///
    /// Each instruction touches all three EAV/AEV/VAE indexes;
    /// `Replace` additionally scans the `(entity, attribute)` range to
    /// supersede any different-valued priors (and skips inserting when
    /// a same-valued prior is already in place — that's the
    /// cardinality-one no-op).
    ///
    /// The batch's new nodes are written into `delta`, the caller-owned
    /// accumulator. Callers own everything else: building the change stream,
    /// choosing a base tree root, persisting a `Revision`, and flushing
    /// `delta`.
    async fn apply<S, I>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        instructions: I,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
        I: Stream<Item = Instruction> + ConditionalSend;

    /// Scan the tree for [`Artifact`]s matching the given constrained
    /// selector.
    ///
    /// Picks the EAV/AEV/VAE index based on which field of the
    /// selector is constrained (entity / value / attribute, in that
    /// priority order), then streams the matching key range. Items in
    /// the range that don't fully satisfy the selector and items in
    /// the `Removed` state are filtered out.
    ///
    /// Consumes `self` (the tree is moved into the returned stream to
    /// pin its root); `store` is the storage backing it, and `cache` serves
    /// spilled value blocks across scans so a repeated read of the same large
    /// value skips the store fetch.
    fn scan<'s, S>(
        self,
        store: S,
        cache: SpillCache,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's + ConditionalSend
    where
        Self: Sized,
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ArtifactTreeExt for ArtifactTree {
    async fn apply<S, I>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        instructions: I,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));

        // Snapshot the committed tree before editing. A retract consults it to
        // tell a fact that existed *before this batch* (tombstone it, so the
        // removal propagates on merge) from one that only appears within the
        // batch or not at all (drop it, leaving no tombstone). `edit()` borrows
        // `self`, so this cheap Arc-backed clone stays readable alongside the
        // in-flight `transient`.
        let base = self.clone();

        // Open one transient edit batch over this tree's spine and apply every
        // instruction's writes to it in flight, so the whole instruction stream
        // costs a single persist instead of one full tree rebuild per key.
        let mut transient = self.edit();

        tokio::pin!(instructions);
        while let Some(instruction) = instructions.next().await {
            match instruction {
                Instruction::Assert(artifact) => {
                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    // Persist a spilling value's bytes as a content-addressed
                    // block before recording the fact; the key holds only the
                    // 32-byte reference to it.
                    store_spilled_value(store, &artifact).await?;

                    let datum = Datum::for_artifact(&artifact);
                    let added = State::Added(datum);
                    transient = transient
                        .insert(entity_key.into_key(), added.clone(), &storage)
                        .await?;
                    transient = transient
                        .insert(attribute_key.into_key(), added.clone(), &storage)
                        .await?;
                    transient = transient
                        .insert(value_key.into_key(), added, &storage)
                        .await?;
                }
                Instruction::Replace(artifact) => {
                    let entity_key = EntityKey::from(&artifact);

                    // Scan priors at this (entity, attribute) against the
                    // in-flight transient tree, so writes from earlier
                    // instructions in this batch are visible. Same-valued priors
                    // already represent the desired state; only different-valued
                    // ones need superseding. The scan borrows `transient`
                    // immutably, so collect into owned vectors in a scope that
                    // ends before the subsequent mutating reassignments.
                    let mut superseded_keys: Vec<Key> = Vec::new();
                    let mut found_same_value = false;
                    {
                        let search_start = <EntityKey<Key> as KeyViewConstruct>::min()
                            .set_entity(entity_key.entity())
                            .set_attribute(entity_key.attribute())
                            .into_key();
                        let search_end = <EntityKey<Key> as KeyViewConstruct>::max()
                            .set_entity(entity_key.entity())
                            .set_attribute(entity_key.attribute())
                            .into_key();
                        let search_stream =
                            transient.stream_range(search_start..=search_end, &storage);
                        tokio::pin!(search_stream);
                        while let Some(candidate) = search_stream.next().await {
                            let candidate = candidate?;
                            if let State::Added(current_element) = &candidate.value {
                                // A prior with a spilled value carries only a
                                // reference in its key; fetch the block so the
                                // value comparison below sees the real value.
                                let spilled = fetch_spilled(store, &candidate.key).await?;
                                let current = Artifact::from_key_datum_with_value(
                                    &candidate.key,
                                    current_element,
                                    spilled,
                                )?;
                                // Supersession is scoped to this exact
                                // (entity, attribute). The range should already
                                // guarantee that, but deleting is destructive
                                // and unconditional across all three indexes,
                                // so verify rather than trust the bounds: a
                                // range-construction bug once widened this
                                // scan to unrelated entities and erased their
                                // facts.
                                if current.of != artifact.of || current.the != artifact.the {
                                    continue;
                                }
                                if current.is == artifact.is {
                                    found_same_value = true;
                                } else {
                                    superseded_keys.push(candidate.key);
                                }
                            }
                        }
                    }

                    for key in superseded_keys {
                        let entity_key = EntityKey(key);
                        let value_key = ValueKey::from_key(&entity_key);
                        let attribute_key = AttributeKey::from_key(&entity_key);

                        transient = transient.delete(&entity_key.into_key(), &storage).await?;
                        transient = transient.delete(&value_key.into_key(), &storage).await?;
                        transient = transient
                            .delete(&attribute_key.into_key(), &storage)
                            .await?;
                    }

                    if found_same_value {
                        continue;
                    }

                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    // Persist a spilling value's bytes as a content-addressed
                    // block before recording the fact.
                    store_spilled_value(store, &artifact).await?;

                    let datum = Datum::for_artifact(&artifact);
                    let added = State::Added(datum);
                    transient = transient
                        .insert(entity_key.into_key(), added.clone(), &storage)
                        .await?;
                    transient = transient
                        .insert(attribute_key.into_key(), added.clone(), &storage)
                        .await?;
                    transient = transient
                        .insert(value_key.into_key(), added, &storage)
                        .await?;
                }
                Instruction::Retract(artifact) => {
                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    // Was this exact fact committed *before* this batch? Read the
                    // value key (the fact identity) from the base snapshot, not
                    // the transient tree — so an assert earlier in this same
                    // batch doesn't count as a prior.
                    let committed = matches!(
                        base.get(&value_key.clone().into_key(), &storage).await?,
                        Some(State::Added(_))
                    );

                    if committed {
                        // Retracting a durable fact: replace it with a `Removed`
                        // tombstone across all three orderings so the removal
                        // survives a merge and beats a stale remote assert.
                        let removed: State<Datum> = State::Removed;
                        transient = transient
                            .insert(entity_key.into_key(), removed.clone(), &storage)
                            .await?;
                        transient = transient
                            .insert(attribute_key.into_key(), removed.clone(), &storage)
                            .await?;
                        transient = transient
                            .insert(value_key.into_key(), removed, &storage)
                            .await?;
                    } else {
                        // No committed prior: the fact only exists (if at all) as
                        // an assert earlier in this batch. Delete the keys so the
                        // assert and retract cancel to nothing — no tombstone,
                        // no tree churn. Deleting an absent key is a no-op, so a
                        // retract of a fact that never existed changes nothing.
                        transient = transient.delete(&entity_key.into_key(), &storage).await?;
                        transient = transient
                            .delete(&attribute_key.into_key(), &storage)
                            .await?;
                        transient = transient.delete(&value_key.into_key(), &storage).await?;
                    }
                }
            }
        }

        // Seal the whole batch with a single bottom-up persist into the
        // caller's delta.
        *self = transient.persist(delta)?;
        Ok(())
    }

    fn scan<'s, S>(
        self,
        store: S,
        cache: SpillCache,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's + ConditionalSend
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's,
    {
        let tree = self;
        // Keep the raw backend to fetch spilled value blocks by reference; the
        // bridge below is only for reading tree nodes.
        let raw_store = store.clone();
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store));
        try_stream! {
            let range = selector_range(&selector);

            let stream = tree.stream_range(range, &storage);
            tokio::pin!(stream);
            for await item in stream {
                let raw = item?;
                // Parse each entry's key ONCE into borrowed components, and reuse
                // that single parse for matching, spill resolution, and
                // reconstruction. The previous flow re-split the key many times
                // per entry (once per `KeyView` accessor in `matches_selector`,
                // again in the spill lookup, again in reconstruction); on the
                // variable-length M3 key that per-entry re-splitting dominated
                // scan cost.
                let Some(parts) = parse_key_ref(raw.key.as_ref()) else {
                    continue;
                };
                if !match_selector_and_key_ref(&selector, &parts) {
                    continue;
                }
                let State::Added(datum) = &raw.value else {
                    continue;
                };
                let spilled = match &parts.value {
                    ValueRef::Reference(reference) => {
                        Some(fetch_spilled_reference(&raw_store, &cache, reference).await?)
                    }
                    ValueRef::Inline(_) => None,
                };
                yield Artifact::from_key_ref_datum_value(&parts, datum, spilled)?;
            }
        }
    }
}

/// The inclusive key range a selector's scan reads.
///
/// Index choice: exact fields take priority (entity / value /
/// attribute); prefix bounds pick the index whose leading dimension
/// they constrain when no exact field does. The range is additionally
/// tightened with whatever prefix bounds the selector carries (sound
/// on any dimension, tight on leading ones) via
/// [`apply_prefix_bounds`]; per-entry re-checking against the full
/// selector happens during the scan, not here. The lower/upper bounds
/// collapse to the same exact key when every component is
/// constrained, and the inclusive range still selects it.
///
/// This is the selector's *demanded range*: everything a scan for it
/// would touch, whether or not entries exist there. Subscriptions use
/// it as the unit of a demand cover — a range that came back empty is
/// still demanded (the emptiness was read), so a later write into it
/// must invalidate the reader.
pub fn selector_range(selector: &ArtifactSelector<Constrained>) -> RangeInclusive<Key> {
    if selector.entity().is_some()
        || (selector.entity_prefix().is_some()
            && selector.value().is_none()
            && selector.attribute().is_none()
            && selector.attribute_prefix().is_none())
    {
        let (start, end) = apply_prefix_bounds(
            <EntityKey<Key> as KeyViewConstruct>::min().apply_selector(selector),
            <EntityKey<Key> as KeyViewConstruct>::max().apply_selector(selector),
            selector,
        );
        start.into_key()..=end.into_key()
    } else if selector.value().is_some()
        || selector.value_prefix().is_some()
        || selector.value_lower().is_some()
        || selector.value_upper().is_some()
    {
        let (start, end) = apply_prefix_bounds(
            <ValueKey<Key> as KeyViewConstruct>::min().apply_selector(selector),
            <ValueKey<Key> as KeyViewConstruct>::max().apply_selector(selector),
            selector,
        );
        start.into_key()..=end.into_key()
    } else if selector.attribute().is_some() || selector.attribute_prefix().is_some() {
        let (start, end) = apply_prefix_bounds(
            <AttributeKey<Key> as KeyViewConstruct>::min().apply_selector(selector),
            <AttributeKey<Key> as KeyViewConstruct>::max().apply_selector(selector),
            selector,
        );
        start.into_key()..=end.into_key()
    } else {
        // `Constrained` guarantees at least one field is set.
        unreachable!("ArtifactSelector will always have at least one field specified")
    }
}

#[cfg(test)]
mod spill_cache_tests {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{ArtifactTree, ArtifactTreeExt, fetch_spilled, fetch_spilled_cached, spill_cache};
    use crate::{Artifact, EntityKey, Instruction, KeyView, Value};
    use dialog_search_tree::Delta;
    use dialog_storage::{Blake3Hash, MeasuredStorage, MemoryStorageBackend, StorageBackend};
    use futures_util::stream;

    /// Commits one spilling fact and returns the store (with the spilled block
    /// written) plus the EAV key that references it.
    async fn spilled_setup() -> (
        MeasuredStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
        crate::Key,
        Value,
    ) {
        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        let value = Value::String("q".repeat(inline_n + 1));
        let mut store = MeasuredStorage::new(MemoryStorageBackend::default());
        let mut delta = Delta::zero();
        let mut tree = ArtifactTree::empty();
        let artifact = Artifact {
            the: "doc/body".parse().unwrap(),
            of: "doc:1".parse().unwrap(),
            is: value.clone(),
            cause: None,
        };
        tree.apply(
            &mut store,
            &mut delta,
            stream::iter(vec![Instruction::Assert(artifact.clone())]),
        )
        .await
        .unwrap();
        for (_, buffer) in delta.flush() {
            store
                .set(*buffer.blake3_hash().as_bytes(), buffer.as_ref().to_vec())
                .await
                .unwrap();
        }
        let key = EntityKey::from(&artifact).into_key();
        assert!(EntityKey(&key).value_is_spilled(), "value must spill");
        (store, key, value)
    }

    /// A cached fetch of the same spilled block reads the store once: the
    /// second fetch is a cache hit that touches no storage.
    #[dialog_common::test]
    async fn it_serves_a_cached_spilled_block_without_a_store_read() -> anyhow::Result<()> {
        let (store, key, value) = spilled_setup().await;
        let cache = spill_cache();

        let before = store.reads();
        let first = fetch_spilled_cached(&store, &cache, &key).await?;
        let after_miss = store.reads();
        let second = fetch_spilled_cached(&store, &cache, &key).await?;
        let after_hit = store.reads();

        assert_eq!(first, Some(value.to_bytes()), "miss returns the block");
        assert_eq!(second, first, "hit returns the same bytes");
        assert!(after_miss > before, "the miss reads the store");
        assert_eq!(
            after_hit, after_miss,
            "the hit reads nothing from the store"
        );
        Ok(())
    }

    /// The cached fetch and the uncached fetch return identical bytes.
    #[dialog_common::test]
    async fn it_matches_the_uncached_fetch() -> anyhow::Result<()> {
        let (store, key, _value) = spilled_setup().await;
        let cache = spill_cache();
        let cached = fetch_spilled_cached(&store, &cache, &key).await?;
        let uncached = fetch_spilled(&store, &key).await?;
        assert_eq!(cached, uncached);
        assert!(cached.is_some());
        Ok(())
    }

    /// An inline key spills nothing: both fetches return `None` and read no
    /// block regardless of the cache.
    #[dialog_common::test]
    async fn it_returns_none_for_an_inline_key() -> anyhow::Result<()> {
        let mut store = MeasuredStorage::new(MemoryStorageBackend::default());
        let mut delta = Delta::zero();
        let mut tree = ArtifactTree::empty();
        let artifact = Artifact {
            the: "user/name".parse().unwrap(),
            of: "user:1".parse().unwrap(),
            is: Value::String("Alice".to_string()),
            cause: None,
        };
        tree.apply(
            &mut store,
            &mut delta,
            stream::iter(vec![Instruction::Assert(artifact.clone())]),
        )
        .await?;
        let key = EntityKey::from(&artifact).into_key();
        let cache = spill_cache();
        assert_eq!(fetch_spilled_cached(&store, &cache, &key).await?, None);
        assert_eq!(fetch_spilled(&store, &key).await?, None);
        Ok(())
    }
}
