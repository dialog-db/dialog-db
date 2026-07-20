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
//! The tree stores raw key bytes and rkyv-native values: [`Key`] is a
//! newtype over the lossless, variable-length order-preserving key encoding
//! (see [`key::varkey`](crate::key::varkey)) and passes through unchanged,
//! while [`State<Datum>`] is the tree's value type directly, serialized into
//! node buffers by the tree itself. Because the fact's value is encoded into
//! the key, a scan reconstructs each [`Artifact`] from its key rather than
//! from the payload.
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

use crate::history::{Cause as HistoryCause, Claim, Record, Version};
use crate::{
    Artifact, ArtifactSelector, ArtifactWriter, AttributeKey, AttributeKeyPart, Datum,
    DialogArtifactsError, EntityKey, EntityKeyPart, FromKey, Instruction, Key, KeyView,
    KeyViewConstruct, KeyViewMut, State, ValueDataType, ValueKey, encode_value_owned,
    key::value_spills,
    key::varkey::{self, ValuePayload, ValueRef, parse_key_ref},
    match_selector_and_key_ref,
    selector::Constrained,
};

/// The concrete search-tree type the artifact indexes use.
///
/// Keys are the raw variable-length bytes of [`Key`]; values are [`State`]
/// payloads stored in the tree's native (rkyv) encoding.
pub type ArtifactTree = PersistentTree<Key, State<Datum>>;

// Deletion is no longer resolved at the slot: it travels as a history
// record and is applied to the active indexes by the observed-remove
// merge screen (see `crate::merge` and `notes/version-control.md`),
// so no `Removed` tombstone ever reaches a data-region `integrate`
// contest. The only remaining contest is `Added` vs `Added` — two
// byte-variants of the *same* value — which the default deterministic
// hash race resolves. No `prevails_over` override is needed.
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

/// Layers a [`Delta`]'s buffered nodes over a backing store for reads, so
/// that a tree persisted into the delta but not yet flushed remains
/// traversable. This lets a caller keep editing a tree across multiple
/// persist points (e.g. [`ArtifactTreeExt::apply_versioned`] followed by
/// [`ArtifactTreeExt::record`]) while the whole batch still travels to
/// storage as a single flush. Writes pass through to the backing store.
struct DeltaReadThrough<'a, S> {
    delta: &'a Delta<NodeHash, Buffer>,
    store: S,
}

impl<S: Clone> Clone for DeltaReadThrough<'_, S> {
    fn clone(&self) -> Self {
        Self {
            delta: self.delta,
            store: self.store.clone(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> StorageBackend for DeltaReadThrough<'_, S>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    type Key = NodeHash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.store.set(*key.as_bytes(), value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        if let Some(buffer) = self.delta.get(key) {
            return Ok(Some(buffer.as_ref().to_vec()));
        }
        self.store.get(key.as_bytes()).await
    }
}

/// Tighten a scan's `(start, end)` key pair with the selector's
/// prefix bounds. A prefix on a field that also has an exact
/// constraint is skipped — the exact value is already in the keys
/// and is strictly tighter. Applying a prefix to a non-leading key
/// dimension is sound (the range stays a superset of the matches;
/// [`match_selector_and_key_ref`] filters the rest) and
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
        ValueDataType::UnsignedInt | ValueDataType::SignedInt => 16,
        // `f64` encodes to 8 bytes (see `encode_f64` / `value_payload_len`).
        ValueDataType::Float => 8,
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

    /// Like [`ArtifactTreeExt::apply`], but tags every [`Datum`] written by
    /// the batch with the [`Version`](crate::history::Version) of the
    /// revision that produced it, and records each instruction's history
    /// claim into the tree's history region. This is the write path used
    /// by version-controlled branch commits; [`ArtifactTreeExt::apply`]
    /// leaves the version unset.
    ///
    /// Returns whether the batch changed the indexes at all. A batch made
    /// entirely of cardinality-one no-ops (re-asserting values already in
    /// place) leaves the tree untouched and records no history — there is
    /// nothing a revision could attribute, and callers should not mint one.
    async fn apply_versioned<S, I>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        version: Option<Version>,
        instructions: I,
    ) -> Result<bool, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
        I: Stream<Item = Instruction> + ConditionalSend;

    /// The currently asserted [`Datum`]s recorded for the given entity and
    /// attribute, scanned from the EAV index. Multiple data are possible for
    /// attributes with more than one asserted value.
    async fn select_data<S>(
        &self,
        store: S,
        of: &crate::Entity,
        the: &crate::Attribute,
    ) -> Result<Vec<Datum>, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync;

    /// Look up data at `(the, of)` through the attribute-ordered index,
    /// the ordering revision records are stored in.
    async fn select_record<S>(
        &self,
        store: S,
        of: &crate::Entity,
        the: &crate::Attribute,
    ) -> Result<Vec<Artifact>, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync;

    /// Write pre-built entries (e.g. revision lineage records — see
    /// [`Record::into_entry`](crate::history::Record::into_entry)) into the
    /// tree as one edit batch, accumulating new nodes in `delta`
    async fn record<S>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        entries: Vec<(Key, State<Datum>)>,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync;

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
        self.apply_versioned(store, delta, None, instructions)
            .await
            .map(|_| ())
    }

    async fn apply_versioned<S, I>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        version: Option<Version>,
        instructions: I,
    ) -> Result<bool, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));

        // Open one transient edit batch over this tree's spine and apply every
        // instruction's writes to it in flight, so the whole instruction stream
        // costs a single persist instead of one full tree rebuild per key.
        let (transient, changed) =
            write_instructions(self.edit(), store, &storage, version, instructions).await?;

        // Seal the whole batch with a single bottom-up persist into the
        // caller's delta.
        *self = transient.persist(delta)?;
        Ok(changed)
    }

    async fn select_data<S>(
        &self,
        store: S,
        of: &crate::Entity,
        the: &crate::Attribute,
    ) -> Result<Vec<Datum>, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store));

        let search_start = <EntityKey<Key> as KeyViewConstruct>::min()
            .set_entity(EntityKeyPart::from(of))
            .set_attribute(AttributeKeyPart::from(the))
            .into_key();
        let search_end = <EntityKey<Key> as KeyViewConstruct>::max()
            .set_entity(EntityKeyPart::from(of))
            .set_attribute(AttributeKeyPart::from(the))
            .into_key();

        let stream = self.stream_range(search_start..=search_end, &storage);
        tokio::pin!(stream);

        let mut data = Vec::new();
        while let Some(entry) = stream.next().await {
            if let State::Added(datum) = entry?.value {
                data.push(datum);
            }
        }

        Ok(data)
    }

    async fn select_record<S>(
        &self,
        store: S,
        of: &crate::Entity,
        the: &crate::Attribute,
    ) -> Result<Vec<Artifact>, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let raw_store = store.clone();
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store));

        let search_start = <AttributeKey<Key> as KeyViewConstruct>::min()
            .set_attribute(AttributeKeyPart::from(the))
            .set_entity(EntityKeyPart::from(of))
            .into_key();
        let search_end = <AttributeKey<Key> as KeyViewConstruct>::max()
            .set_attribute(AttributeKeyPart::from(the))
            .set_entity(EntityKeyPart::from(of))
            .into_key();

        let stream = self.stream_range(search_start..=search_end, &storage);
        tokio::pin!(stream);

        // A revision record is a large CBOR value, so it normally spills: the
        // key carries only its reference and the bytes live as an archive
        // block. Reconstruct through the same path a fact scan uses so both
        // the inline and spilled cases resolve.
        let mut records = Vec::new();
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            if let State::Added(datum) = &entry.value {
                let spilled = fetch_spilled(&raw_store, &entry.key).await?;
                records.push(Artifact::from_key_datum_with_value(
                    &entry.key, datum, spilled,
                )?);
            }
        }
        Ok(records)
    }

    async fn record<S>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        entries: Vec<(Key, State<Datum>)>,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let mut transient = self.edit();
        {
            // Read through the delta: this tree's latest nodes may only
            // exist there (persisted by an earlier batch, not yet flushed).
            let storage = ContentAddressedStorage::new(DeltaReadThrough {
                delta: &*delta,
                store: store.clone(),
            });
            for (key, entry) in entries {
                transient = transient.insert(key, entry, &storage).await?;
            }
        }
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

/// Applies an instruction stream to any [`ArtifactWriter`], returning the
/// written target and whether the batch changed the indexes.
///
/// This is the whole of the artifact write semantics: reserved-namespace
/// enforcement, cardinality-one supersession, value spilling, coverage records,
/// and the history entries each instruction contributes. It is generic over the
/// write target so the canonical edit path and the buffered (hitchhiker) path
/// run *identical* semantics; only where the writes land differs.
///
/// The supersession scans go through [`ArtifactWriter::scan`] and
/// [`ArtifactWriter::read`], which see the batch's own pending writes on both
/// targets. On the buffered target that means the node buffers are merged into
/// the scan: a `Replace` blind to a buffered prior would leave it live at a
/// cardinality-one slot, and a `Retract` blind to one would cite nothing and so
/// cover nothing at merge time.
///
/// `store` is the raw archive backend, used directly (not through the tree node
/// bridge) for the value blocks of spilling values: a value above the manifest's
/// inline threshold lives as a content-addressed block, and its key carries only
/// the 32-byte reference to it.
#[tracing::instrument(skip_all, name = "write_instructions")]
#[allow(clippy::too_many_lines)]
pub async fn write_instructions<W, S, I>(
    mut transient: W,
    store: &mut S,
    storage: &ContentAddressedStorage<TreeStorageBridge<S>>,
    version: Option<Version>,
    instructions: I,
) -> Result<(W, bool), DialogArtifactsError>
where
    W: ArtifactWriter,
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + ConditionalSync,
    I: Stream<Item = Instruction> + ConditionalSend,
{
    // History records are buffered and only written if the batch changed
    // the indexes: a batch of pure no-ops must leave the tree untouched,
    // history region included.
    let mut history_entries: Vec<(Key, State<Datum>)> = Vec::new();
    let mut changed = false;

    tokio::pin!(instructions);
    while let Some(instruction) = instructions.next().await {
        // The `dialog.` namespace is reserved for version-control
        // machinery (revision records — see
        // `history::RevisionRecord`), which writes through
        // [`ArtifactTreeExt::record`] rather than instructions. At the
        // library level lineage therefore cannot be corrupted through
        // the ordinary write path.
        {
            let (Instruction::Assert(artifact)
            | Instruction::Replace(artifact)
            | Instruction::Retract(artifact)) = &instruction;
            if artifact.the.as_str().starts_with("dialog.") {
                return Err(DialogArtifactsError::ReservedAttribute(
                    artifact.the.to_string(),
                ));
            }
        }
        match instruction {
            Instruction::Assert(artifact) => {
                changed = true;
                let entity_key = EntityKey::from(&artifact);
                let value_key = ValueKey::from_key(&entity_key);
                let attribute_key = AttributeKey::from_key(&entity_key);

                // Persist a spilling value's bytes as a content-addressed
                // block before recording the fact; the key holds only the
                // 32-byte reference to it.
                store_spilled_value(store, &artifact).await?;

                // A version-tagged assertion records its history: an
                // assertion is purely additive, so it supersedes nothing.
                if let Some(version) = &version {
                    let record = Record::Assert(Claim {
                        the: artifact.the.clone(),
                        of: artifact.of.clone(),
                        is: artifact.is.clone(),
                        cause: HistoryCause::genesis(),
                    });
                    if let Some(coverage) = record.coverage_entry(version) {
                        history_entries.push(coverage);
                    }
                    history_entries.push(record.into_entry(version));
                }

                let mut datum = Datum::for_artifact(&artifact);
                datum.version = version;
                let added = State::Added(datum);
                transient = transient
                    .write(entity_key.into_key(), added.clone(), storage)
                    .await?;
                transient = transient
                    .write(attribute_key.into_key(), added.clone(), storage)
                    .await?;
                transient = transient
                    .write(value_key.into_key(), added, storage)
                    .await?;
            }
            Instruction::Replace(artifact) => {
                let entity_key = EntityKey::from(&artifact);

                // Scan priors at this (entity, attribute) against the
                // in-flight write target, so writes from earlier instructions
                // in this batch are visible (on the buffered target that means
                // the node buffers are merged into the scan). Same-valued
                // priors already represent the desired state; only
                // different-valued ones need superseding. The value lives in
                // the key now, so each candidate's claim is reconstructed from
                // its key rather than read out of the payload. The scan borrows
                // `transient` immutably, so collect into owned vectors in a
                // scope that ends before the subsequent mutating reassignments.
                let mut superseded_keys: Vec<Key> = Vec::new();
                let mut superseded_versions: Vec<Version> = Vec::new();
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
                    let search_stream = transient.scan(search_start..=search_end, storage);
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
                                // The superseded claim's version feeds the
                                // replacement record's cause, so a reader
                                // can order the two without reading values.
                                superseded_versions.extend(current_element.version);
                                superseded_keys.push(candidate.key);
                            }
                        }
                    }
                }

                // Cardinality-one no-op: the identical claim already
                // stands, at its original version, and there is nothing
                // to supersede. Nothing changes in the indexes and no
                // history is recorded — a fresh record would fork the
                // claim's lineage away from the version the standing
                // datum carries.
                if found_same_value && superseded_keys.is_empty() {
                    continue;
                }
                changed = true;

                for key in superseded_keys {
                    let entity_key = EntityKey(key);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    transient = transient.erase(&entity_key.into_key(), storage).await?;
                    transient = transient.erase(&value_key.into_key(), storage).await?;
                    transient = transient.erase(&attribute_key.into_key(), storage).await?;
                }

                // A version-tagged replacement records its history: its
                // cause lists the versions of the claims it superseded —
                // exactly the data removed from the indexes above. The
                // record is written even when the insert below is skipped
                // because a same-valued prior survives; the supersession
                // of the different-valued claims still happened and must
                // be attributable.
                if let Some(version) = &version {
                    let record = Record::Assert(Claim {
                        the: artifact.the.clone(),
                        of: artifact.of.clone(),
                        is: artifact.is.clone(),
                        cause: HistoryCause::new(superseded_versions),
                    });
                    if let Some(coverage) = record.coverage_entry(version) {
                        history_entries.push(coverage);
                    }
                    history_entries.push(record.into_entry(version));
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

                let mut datum = Datum::for_artifact(&artifact);
                datum.version = version;
                let added = State::Added(datum);
                transient = transient
                    .write(entity_key.into_key(), added.clone(), storage)
                    .await?;
                transient = transient
                    .write(attribute_key.into_key(), added.clone(), storage)
                    .await?;
                transient = transient
                    .write(value_key.into_key(), added, storage)
                    .await?;
            }
            Instruction::Retract(artifact) => {
                changed = true;
                let entity_key = EntityKey::from(&artifact);
                let value_key = ValueKey::from_key(&entity_key);
                let attribute_key = AttributeKey::from_key(&entity_key);

                // A version-tagged retraction records its history: its
                // cause is the version of the assertion it withdraws.
                // An assertion made earlier in this same batch carries
                // this batch's own version; a record must not claim
                // itself as its cause, so that degenerates to a genesis
                // retraction.
                if let Some(version) = &version {
                    let withdrawn = match transient
                        .read(&entity_key.clone().into_key(), storage)
                        .await?
                    {
                        Some(State::Added(datum)) => {
                            datum.version.filter(|withdrawn| withdrawn != version)
                        }
                        _ => None,
                    };
                    let record = Record::Retract(Claim {
                        the: artifact.the.clone(),
                        of: artifact.of.clone(),
                        is: artifact.is.clone(),
                        cause: withdrawn.into_iter().collect(),
                    });
                    if let Some(coverage) = record.coverage_entry(version) {
                        history_entries.push(coverage);
                    }
                    history_entries.push(record.into_entry(version));
                }

                // Observed-remove semantics: retraction deletes the
                // fact's keys outright — no tombstone. The retract
                // record written above is the durable carrier of the
                // deletion (it replicates as history), and a replica's
                // causal context is what stops a stale peer's copy from
                // resurrecting the fact at merge time (see
                // `notes/version-control.md`). Deleting an absent
                // key is a no-op, so a same-batch assert+retract cancels
                // to nothing and a retract of a fact that never existed
                // changes nothing in the indexes.
                transient = transient.erase(&entity_key.into_key(), storage).await?;
                transient = transient.erase(&attribute_key.into_key(), storage).await?;
                transient = transient.erase(&value_key.into_key(), storage).await?;
            }
        }
    }

    for (key, entry) in history_entries {
        transient = transient.write(key, entry, storage).await?;
    }

    Ok((transient, changed))
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
