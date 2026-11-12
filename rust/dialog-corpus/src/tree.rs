//! Shared tree-ops on the corpus.
//!
//! The branch abstractions in `dialog-repository` and `dialog-query` all
//! operate on the same EAV/AEV/VAE search tree. The per-instruction mutation loop and the
//! selector → key-range scan dispatch are identical across all of them, so
//! they live here as an extension trait on [`Corpus`], parameterized
//! over any environment that answers the corpus's node reads
//! ([`nodes::Read<CorpusNode>`](dialog_search_tree::nodes::Read),
//! [`nodes::Preload<CorpusNode>`](dialog_search_tree::nodes::Preload))
//! and, where an operation reaches a value spilled out of its key, loads
//! it ([`blob::Load`](dialog_effects::blob::Load)). Where a node is kept,
//! and whether it is cached, is the environment's business.
//!
//! Callers responsible for revisions, upstreams, remote fallback, or any
//! other branch specifics keep that logic on their side and call
//! [`CorpusExt::apply`] / [`CorpusExt::scan`] for the actual
//! key writes and range scans. Mutations accumulate in the tree's delta;
//! callers must flush and persist the buffers when they mint a revision.
//!
//! The tree stores raw key bytes and rkyv-native values: [`Key`] is a
//! newtype over the lossless, variable-length order-preserving key encoding
//! (see [`key::varkey`](crate::key::varkey)) and passes through unchanged,
//! while [`State<Datum>`] is the tree's value type directly, serialized into
//! node buffers by the tree itself. Because the fact's value is encoded into
//! the key, a scan reconstructs each [`Fact`] from its key rather than
//! from the payload.
//!
//! `Corpus` is a type alias for a `dialog_search_tree::PersistentTree`, so the
//! orphan rule rules out inherent methods — the operations are exposed as
//! an extension trait instead.

use crate::Blake3Hash;
use crate::reads::*;
use async_stream::try_stream;
use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash as NodeHash, ConditionalSend, ConditionalSync};
use dialog_effects::archive::prelude::ArchiveScope;
use dialog_search_tree::{
    Buffer, Manifest, NodeCache, PersistentNode, PersistentTree, Value as TreeValue,
};
use futures_util::{Stream, StreamExt};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::iter::repeat_n;
use std::ops::{Deref, RangeInclusive};
use std::sync::{Arc, Mutex};

use crate::history::{Cause as HistoryCause, Claim, Record, RecordEntries, Version};
use crate::key::value_payload as build_value_payload;
use crate::{
    ATTRIBUTE_KEY_TAG, ArchiveDelta, AttributeKey, AttributeKeyPart, Cause, CorpusWriter, Datum,
    DeltaOverlay, DialogCorpusError, ENTITY_KEY_TAG, EntityKey, EntityKeyPart, Fact, FactView,
    Instruction, Key, KeyView, KeyViewConstruct, KeyViewMut, Pick, Selector, SelectorMatch,
    Standing, State, VALUE_KEY_TAG, Value, ValueDataType, ValueKey, decode_value_parts,
    encode_bytes, encode_value_owned,
    key::varkey::{self, KeyLayout, KeyRef, ValuePayload, ValueRef, parse_key_layout},
    key::{EncodedValue, fact_index_keys, fact_index_keys_with, reproject_index_keys},
    make_reference, match_selector_and_key_ref,
    selector::Constrained,
    value_predicates_admit,
};
use crate::{NUMBER_BAND, encode_key_value_owned};

pub mod distribution;
pub mod rekey;

/// The corpus: the body of claims a branch holds, in the search tree, keyed
/// for the entity-, attribute- and value-ordered scans.
///
/// Keys are the raw variable-length bytes of [`Key`]; values are [`State`]
/// payloads stored in the tree's native (rkyv) encoding.
pub type Corpus = PersistentTree<Key, State<Datum>>;

/// The node cache a [`Corpus`] reads through: nodes by content hash,
/// already checked.
pub type CorpusCache = NodeCache<Key, State<Datum>>;

/// A node of the corpus: the search tree's node at the corpus's keys and
/// values. What an environment a corpus is read or written through
/// answers the node layer's requests for: `Provider<nodes::Read<CorpusNode>>`,
/// `Provider<nodes::Preload<CorpusNode>>`, `Provider<nodes::Write<CorpusNode>>`
/// ([`dialog_search_tree::nodes`]).
pub type CorpusNode = PersistentNode<Key, State<Datum>>;

// Deletion is no longer resolved at the slot: it travels as a history
// record and is applied to the active indexes by the observed-remove
// merge screen (see `crate::merge` and `notes/version-control.md`),
// so no `Removed` tombstone ever reaches a data-region `integrate`
// contest. The only remaining contest is `Added` vs `Added` — two
// byte-variants of the *same* value (the key carries entity, attribute
// AND value) — which the default deterministic hash race resolves.
// The contest FUSES rather than drops: both sides' claim versions
// collapse into the winner (`Datum::absorb_versions`), so a later
// retraction can cover every claim its author observed — a dropped
// loser version could otherwise resurrect the fact through a peer that
// still holds it (spec D3 on identical values).
impl TreeValue for State<Datum> {
    /// Calibrated to measured rkyv footprints (the weight-proxy audit put
    /// the mean serialized payload at ~129 bytes for a typical
    /// cause-plus-version datum): base costs cover the enum and option
    /// tags, alignment, and rkyv's relative-pointer overhead, and each
    /// version-sized component adds its 40 bytes plus bookkeeping. An
    /// estimate, not an exact encoding size — pacing needs a pure,
    /// deterministic proxy that tracks reality, not byte equality.
    fn payload_weight(&self) -> usize {
        match self {
            State::Removed => 16,
            State::Added(datum) => {
                40 + datum.cause.as_ref().map_or(0, |_| 40)
                    + datum.version.as_ref().map_or(0, |_| 48)
                    + 48 * (datum.collapsed.len() + datum.supersedes.len())
                    + datum.blob.as_ref().map_or(0, |blob| 16 + blob.len())
            }
        }
    }

    fn fuse(winner: Self, loser: &Self) -> Self {
        match (winner, loser) {
            (State::Added(mut winner), State::Added(loser)) => {
                winner.absorb_versions(loser.versions());
                State::Added(winner)
            }
            (winner, _) => winner,
        }
    }
}

/// Stages a spilling value's raw bytes in the batch's blobs lane, under the
/// 32-byte reference its key carries (both from the instruction's single
/// [`EncodedValue`] pass); the commit writes them with the rest of the batch.
/// A no-op for a value that stays inline (its bytes live in the key).
/// Idempotent: content-addressed, so the same value stages the same blob.
fn stage_spilled_value(staged: &mut ArchiveDelta, spill: Option<(Blake3Hash, Vec<u8>)>) {
    if let Some((_, raw)) = spill {
        staged.stage_blob(Buffer::from(raw));
    }
}

/// A byte-bounded cache of spilled value blocks, keyed by their 32-byte
/// content reference.
///
/// Spilled blocks are content-addressed, so a reference always maps to the
/// same bytes: cached entries never go stale and need no invalidation. The
/// bound is a TOTAL BYTE budget rather than an entry count: every spilled
/// value is individually large and individually unbounded (a single 100 MB
/// document is one entry), so a count cap pins an unpredictable amount of
/// memory. Eviction is FIFO — with no staleness there is nothing smarter to
/// protect, and the scan/join access pattern re-touches recent references. A
/// block larger than the whole budget is served but never cached.
#[derive(Clone, Debug)]
pub struct SpillCache {
    state: Arc<Mutex<SpillCacheState>>,
    budget: usize,
}

#[derive(Debug, Default)]
struct SpillCacheState {
    map: HashMap<Blake3Hash, Vec<u8>>,
    order: VecDeque<Blake3Hash>,
    bytes: usize,
}

impl SpillCache {
    /// Creates a cache bounded to at most `budget` total cached bytes.
    pub fn with_budget(budget: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(SpillCacheState::default())),
            budget,
        }
    }

    /// The most bytes the cache holds.
    pub fn budget(&self) -> usize {
        self.budget
    }

    /// The bytes the cache holds now.
    pub fn bytes(&self) -> usize {
        self.state.lock().expect("spill cache lock").bytes
    }

    /// How many blocks the cache holds now.
    pub fn len(&self) -> usize {
        self.state.lock().expect("spill cache lock").map.len()
    }

    /// Whether the cache holds nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Lets every cached block go.
    pub fn clear(&self) {
        *self.state.lock().expect("spill cache lock") = SpillCacheState::default();
    }

    /// The cached bytes for `key`, if present.
    pub fn get(&self, key: &Blake3Hash) -> Option<Vec<u8>> {
        let state = self.state.lock().expect("spill cache lock");
        state.map.get(key).cloned()
    }

    /// Inserts a block, evicting oldest entries until the budget holds. A
    /// block exceeding the whole budget is not cached.
    pub fn insert(&self, key: Blake3Hash, value: Vec<u8>) {
        if value.len() > self.budget {
            return;
        }
        let mut state = self.state.lock().expect("spill cache lock");
        if state.map.contains_key(&key) {
            return;
        }
        while state.bytes + value.len() > self.budget {
            let Some(oldest) = state.order.pop_front() else {
                break;
            };
            if let Some(evicted) = state.map.remove(&oldest) {
                state.bytes -= evicted.len();
            }
        }
        state.bytes += value.len();
        state.order.push_back(key);
        state.map.insert(key, value);
    }

    /// Retrieves a block from the cache, or fetches (and caches) it using the
    /// provided function. The lock is never held across the fetch.
    pub async fn get_or_fetch<F, E>(
        &self,
        key: &Blake3Hash,
        fetcher: F,
    ) -> Result<Option<Vec<u8>>, E>
    where
        F: AsyncFnOnce(&Blake3Hash) -> Result<Option<Vec<u8>>, E>,
    {
        if let Some(hit) = self.get(key) {
            return Ok(Some(hit));
        }
        Ok(match fetcher(key).await? {
            Some(value) => {
                self.insert(*key, value.clone());
                Some(value)
            }
            None => None,
        })
    }
}

/// Byte budget for a [`SpillCache`]: enough to keep a working set of spilled
/// blocks warm across a join without letting a large-document workload pin
/// unbounded memory.
pub const SPILL_CACHE_BUDGET: usize = 8 * 1024 * 1024;

/// Creates a [`SpillCache`] with the default [`SPILL_CACHE_BUDGET`].
pub fn spill_cache() -> SpillCache {
    SpillCache::with_budget(SPILL_CACHE_BUDGET)
}

/// The spilled value reference a key carries, or `None` for an inline key.
///
/// A single `parse_key` walk yields the value payload as an already-classified
/// [`ValuePayload`] (inline vs reference), so this reads the spill flag and the
/// reference bytes from one parse rather than re-splitting the key per accessor.
fn spilled_reference(key: &Key) -> Result<Option<Blake3Hash>, DialogCorpusError> {
    let Some(parts) = varkey::parse_key(key.as_ref()) else {
        // An unparseable key is corruption, not an inline value: classifying
        // it as "no spill" would silently read a wrong value.
        return Err(DialogCorpusError::InvalidKey(
            "key does not parse while resolving its spill reference".to_string(),
        ));
    };
    let ValuePayload::Spilled { hash, .. } = parts.value else {
        return Ok(None);
    };
    let reference: Blake3Hash = hash.as_slice().try_into().map_err(|_| {
        DialogCorpusError::InvalidKey("spilled value reference is not 32 bytes".to_string())
    })?;
    Ok(Some(reference))
}

/// Fetches the raw bytes of a spilled value for `key` from `archive`'s blobs,
/// loaded through `store` ([`load_spill`](crate::load_spill)). Returns `None`
/// for an inline key (its value lives in the key, nothing to fetch),
/// `Some(bytes)` for a spilled key. Errors if a spilled key's value is
/// missing.
pub async fn fetch_spilled<S>(
    store: &S,
    archive: &ArchiveScope,
    key: &Key,
) -> Result<Option<Vec<u8>>, DialogCorpusError>
where
    S: Provider<blob::Load> + ConditionalSync,
{
    let Some(reference) = spilled_reference(key)? else {
        return Ok(None);
    };
    let blob = crate::load_spill(store, archive, &NodeHash::from(reference))
        .await?
        .ok_or_else(|| {
            DialogCorpusError::InvalidValue("spilled value missing from store".to_string())
        })?;
    Ok(Some(blob.into_vec()))
}

/// Like [`fetch_spilled`], but serves and populates a [`SpillCache`]: a hit
/// returns the cached bytes without touching `store`; a miss fetches from
/// `store` and inserts. Because spilled blocks are content-addressed the cache
/// never serves stale bytes.
pub async fn fetch_spilled_cached<S>(
    store: &S,
    archive: &ArchiveScope,
    cache: &SpillCache,
    key: &Key,
) -> Result<Option<Vec<u8>>, DialogCorpusError>
where
    S: Provider<blob::Load> + ConditionalSync,
{
    let Some(reference) = spilled_reference(key)? else {
        return Ok(None);
    };
    fetch_spilled_reference(store, archive, cache, reference.as_ref())
        .await
        .map(Some)
}

/// Fetches (and caches) the bytes of a spilled value block by its raw 32-byte
/// content-addressed reference. The scan path holds the reference already
/// (parsed from the key), so it fetches directly rather than re-deriving the
/// reference from the key. Errors if the block is missing.
pub async fn fetch_spilled_reference<S>(
    store: &S,
    archive: &ArchiveScope,
    cache: &SpillCache,
    reference: &[u8],
) -> Result<Vec<u8>, DialogCorpusError>
where
    S: Provider<blob::Load> + ConditionalSync,
{
    let reference: Blake3Hash = reference.try_into().map_err(|_| {
        DialogCorpusError::InvalidKey("spilled value reference is not 32 bytes".to_string())
    })?;
    cache
        .get_or_fetch(&reference, async |reference: &Blake3Hash| {
            crate::load_spill(store, archive, &NodeHash::from(*reference))
                .await
                .map(|blob| blob.map(Buffer::into_vec))
        })
        .await?
        .ok_or_else(|| {
            DialogCorpusError::InvalidValue("spilled value missing from store".to_string())
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

/// The lower key-segment bound for a byte prefix: the prefix's raw bytes.
/// Every value beginning with the prefix is >= this.
fn prefix_lower(prefix: &[u8]) -> Vec<u8> {
    prefix.to_vec()
}

/// The upper key-segment bound for a byte prefix: the prefix followed by a
/// `0xFE` filler, >= every UTF-8 value beginning with the prefix (UTF-8 bytes
/// are `<= 0xF4`). `0xFE` rather than `0xFF`: a field must never begin with
/// the `ordkey` escape byte, or the preceding field's terminator misreads as
/// an escaped zero (see `varkey::MAX_FILLER_BYTE`); with an empty prefix the
/// filler's first byte IS the field's first byte.
fn prefix_upper(prefix: &[u8]) -> Vec<u8> {
    let mut bytes = prefix.to_vec();
    bytes.extend(repeat_n(0xFEu8, PREFIX_FILLER));
    bytes
}

/// The lower range edge for a value bound: its full order-preserving
/// encoding, with two widenings that keep the scanned range a superset of the
/// true matches (the per-entry check keeps the result exact):
///
/// - A variable-length bound longer than the spilled key-prefix widens to its
///   prefix-cluster start: a spilled value sharing the bound's first
///   `spill_prefix` bytes carries only those bytes in its key and so sorts
///   BELOW the full bound bytes.
/// - A zero float widens across the `-0.0`/`+0.0` encoding cluster.
fn value_lower_edge(value: &Value, manifest: &Manifest) -> Vec<u8> {
    if value.data_type().band(manifest.value_encoding) == ValueDataType::Number {
        return encode_key_value_owned(value, manifest.value_encoding);
    }
    if let Value::Float(float) = value
        && *float == 0.0
    {
        return encode_value_owned(&Value::Float(-0.0));
    }
    if numeric_width(value.data_type()) == 0 {
        let raw = value.to_bytes();
        let prefix = manifest.spill_prefix as usize;
        if raw.len() > prefix {
            let mut out = Vec::new();
            encode_bytes(&raw[..prefix], &mut out);
            return out;
        }
    }
    encode_value_owned(value)
}

/// Tighten a scan's `(start, end)` key pair with the selector's
/// prefix bounds and its attribute and value ranges. A bound on a
/// field that also has an exact constraint is skipped — the exact
/// value is already in the keys and is strictly tighter. Applying a
/// bound to a non-leading key dimension is sound (the range stays a
/// superset of the matches; [`match_selector_and_key_ref`] filters
/// the rest) and tightens the range whenever every more-significant
/// dimension is exact.
fn apply_prefix_bounds<K: KeyViewMut + Deref<Target = [u8]>>(
    start: K,
    end: K,
    selector: &Selector<Constrained>,
    manifest: &Manifest,
) -> (K, K) {
    let mut start = start;
    let mut end = end;
    // The edges a prefix gives the attribute segment, for the attribute
    // bounds below to tighten.
    let mut attribute_edges: (Option<Vec<u8>>, Option<Vec<u8>>) = (None, None);
    if selector.attribute().is_none()
        && let Some(prefix) = selector.attribute_prefix()
    {
        // A name-shape constraint under a whole-domain prefix narrows
        // the range to the shape's contiguous first-byte class within
        // the domain: positions (`A`–`Z`) occupy one sub-range, symbols
        // (`a`–`z`) another, so the scan touches only the demanded half
        // of a mixed domain. The class byte extends the prefix on each
        // edge; a prefix not ending at the name boundary (`/`) leaves
        // the shape to the per-entry check.
        let (lo, hi) = match selector.name_shape() {
            Some(shape) if prefix.ends_with('/') => {
                let (first, last) = shape.first_byte_class();
                let mut lo = prefix.as_bytes().to_vec();
                lo.push(first);
                let mut hi = prefix.as_bytes().to_vec();
                hi.push(last);
                (prefix_lower(&lo), prefix_upper(&hi))
            }
            _ => (
                prefix_lower(prefix.as_bytes()),
                prefix_upper(prefix.as_bytes()),
            ),
        };
        start = start.set_attribute(AttributeKeyPart(&lo));
        end = end.set_attribute(AttributeKeyPart(&hi));
        attribute_edges = (Some(lo), Some(hi));
    }
    // An attribute range bounds the attribute segment from each side it
    // names. The edge sits at the bound itself: an inclusive upper bound
    // must reach every entity and value keyed under that attribute, which
    // the max filler of the components after it gives, and an exclusive
    // bound keeps the same edge, so the range stays a superset and the
    // per-entry re-check drops the attribute at the bound. Beside a prefix
    // the tighter edge of the two stands. An exact attribute is already in
    // the keys, so it skips this, and the per-entry re-check applies the
    // bounds to it.
    if selector.attribute().is_none() {
        let (prefix_lo, prefix_hi) = attribute_edges;
        if let Some(bound) = selector.attribute_lower() {
            let lo = bound.attribute.as_bytes();
            if prefix_lo.as_deref().is_none_or(|edge| edge < lo) {
                start = start.set_attribute(AttributeKeyPart(lo));
            }
        }
        if let Some(bound) = selector.attribute_upper() {
            let hi = bound.attribute.as_bytes();
            if prefix_hi.as_deref().is_none_or(|edge| edge > hi) {
                end = end.set_attribute(AttributeKeyPart(hi));
            }
        }
    }
    if selector.entity().is_none()
        && let Some(prefix) = selector.entity_prefix()
    {
        let lo = prefix_lower(prefix.as_bytes());
        let hi = prefix_upper(prefix.as_bytes());
        start = start.set_entity(EntityKeyPart(&lo));
        end = end.set_entity(EntityKeyPart(&hi));
    }
    let has_value_bounds = selector.value_lower().is_some() || selector.value_upper().is_some();
    // A value prefix bounds the value tail directly: the payload's inline
    // order-preserving bytes for a string are the raw UTF-8, so the prefix's
    // raw bytes are the lower bound and `prefix ‖ 0xFE…` the upper (mirroring
    // the entity/attribute prefixes, but on the value slot). An exact value
    // takes precedence and skips this. Only sound on the VAE ordering, where
    // the value tail leads the key; on EAV/AEV the value is trailing, so
    // `selector_range` routes a value-prefix scan to `ValueKey`.
    //
    // When explicit value bounds are ALSO present they take the value slot
    // instead (below) and the prefix stays a per-entry filter: the prefix's
    // range payload is an unterminated fragment, so a later `set_value` on
    // top of it cannot re-parse the key and would fall back to
    // `KeyParts::max`, discarding every previously set field. The bound
    // range is a superset of the intersection, so results are exact either
    // way.
    if selector.value().is_none()
        && !has_value_bounds
        && let Some(prefix) = selector.value_prefix()
    {
        // A probe longer than the spilled key-prefix clamps its LOWER edge to
        // the prefix-cluster start: a spilled string matching the probe
        // carries only its first `spill_prefix` bytes in the key and sorts
        // below the full probe bytes. The upper edge needs no clamp (cluster
        // keys terminate before the probe's next byte) and the per-entry
        // check re-establishes exactness.
        let bytes = prefix.as_bytes();
        let lo = prefix_lower(&bytes[..bytes.len().min(manifest.spill_prefix as usize)]);
        let hi = prefix_upper(bytes);
        // In the value index the attribute and the entity follow the value
        // slot. The lower edge holds the prefix as a fragment with no
        // terminator, so an attribute after it compares with the bytes that
        // follow the prefix in a stored value. The value `Alice` sorts below
        // the prefix `Al` with the attribute `x/b` after it, because `i`
        // sorts below `x`, and the range would start above it. So the lower
        // edge leaves the attribute and the entity empty. It then sits at or
        // below each key whose value starts with the prefix, and the check
        // of each entry applies the attribute and the entity. In the entity
        // index the value slot ends the key, and its lower edge keeps them.
        if start.first() == Some(&VALUE_KEY_TAG) {
            start = start
                .set_attribute(AttributeKeyPart(&[]))
                .set_entity(EntityKeyPart(&[]));
        }
        start = start.set_value(ValueDataType::String, ValuePayload::Inline(lo));
        end = end.set_value(ValueDataType::String, ValuePayload::Inline(hi));
    }
    // A numeric value range bounds the value tail to a sub-band. A bound value
    // encodes order-preservingly, so its bytes are a key range edge; the open
    // side is the band edge of the bound's type (its lowest/highest inline
    // value). Exclusive bounds (`>`/`<`) still set the key edge at the bound
    // value — the range stays a superset and the per-entry re-check drops the
    // boundary value. An exact value takes precedence and skips this.
    if selector.value().is_none() && has_value_bounds {
        // Both edges must sit in the same type band, so derive the band type
        // from whichever bound is present (they share a type when both are).
        let band = selector
            .value_lower()
            .or(selector.value_upper())
            .map(|bound| bound.value.data_type().band(manifest.value_encoding))
            .unwrap_or_else(ValueDataType::min);
        let lo = match selector.value_lower() {
            Some(bound) => value_lower_edge(&bound.value, manifest),
            None => value_band_min(band),
        };
        let hi = match selector.value_upper() {
            Some(bound) => match &bound.value {
                _ if band == ValueDataType::Number => {
                    encode_key_value_owned(&bound.upper_edge(), manifest.value_encoding)
                }
                Value::Float(float) if *float == 0.0 => encode_value_owned(&Value::Float(0.0)),
                value => encode_value_owned(value),
            },
            None => value_band_max(band, manifest),
        };
        start = start.set_value(band, ValuePayload::Inline(lo));
        end = end.set_value(band, ValuePayload::Inline(hi));
    }
    (start, end)
}

/// The lowest inline value byte-encoding of a type's band: all-zero bytes of
/// the type's fixed width for numerics, or the terminated empty encoding
/// (`[0x00]`) for variable-length types — the smallest VALID payload, so the
/// bound key still parses (an empty payload would let the following key
/// component slide into the value position and corrupt the range edge).
fn value_band_min(value_type: ValueDataType) -> Vec<u8> {
    // The number band's lowest number is NaN.
    if value_type == ValueDataType::Number {
        return encode_key_value_owned(&Value::Float(f64::NAN), NUMBER_BAND);
    }
    match numeric_width(value_type) {
        0 => vec![0x00],
        width => vec![0x00; width],
    }
}

/// The highest inline value byte-encoding of a type's band: all-`0xFF` bytes
/// of the type's fixed width for numerics. For variable-length types, a run
/// of `0xFF` one longer than the inline threshold: every inline payload is at
/// most `threshold + 1` encoded bytes (value bytes plus terminator) and its
/// terminator (`0x00`) sorts below `0xFF`, so this sits above the whole
/// band. Used only as a raw range edge; it deliberately does not parse (it
/// is the last field set on the bound).
fn value_band_max(value_type: ValueDataType, manifest: &Manifest) -> Vec<u8> {
    // The number band's highest number is positive infinity.
    if value_type == ValueDataType::Number {
        return encode_key_value_owned(&Value::Float(f64::INFINITY), NUMBER_BAND);
    }
    match numeric_width(value_type) {
        0 => vec![0xFF; manifest.inline_n as usize + 2],
        width => vec![0xFF; width],
    }
}

/// The fixed inline width of a numeric value type's order-preserving encoding.
/// Variable-length types (strings, bytes, symbols) have no fixed width and
/// return 0; their band edges come from the terminated-empty / over-long
/// `0xFF` forms above.
fn numeric_width(value_type: ValueDataType) -> usize {
    match value_type {
        ValueDataType::UnsignedInt | ValueDataType::SignedInt => 16,
        // `f64` encodes to 8 bytes (see `encode_f64` / `value_payload_len`).
        ValueDataType::Float => 8,
        ValueDataType::Boolean => 1,
        _ => 0,
    }
}

/// Shared mutation + scan operations on a [`Corpus`].
///
/// An extension trait rather than inherent methods because
/// `Corpus` aliases a foreign `dialog_search_tree::PersistentTree` — the
/// orphan rule forbids `impl Corpus { .. }`. Uses `#[async_trait]`
/// so the async `apply` desugars to a boxed future rather than a
/// bound-less native `async fn`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait CorpusExt {
    /// Drain a stream of [`Instruction`]s into the tree, applying the
    /// same key writes that a branch commit would.
    ///
    /// Each instruction touches all three EAV/AEV/VAE indexes; an
    /// assertion under a choosing pick additionally scans the
    /// `(entity, attribute)` range to elect the prior it succeeds (and
    /// skips inserting when a same-valued prior is already in place,
    /// the no-op of writing a standing value).
    ///
    /// The batch's new nodes are written into `delta`, the caller-owned
    /// accumulator. Callers own everything else: building the change stream,
    /// choosing a base tree root, persisting a `Revision`, and flushing
    /// `delta`.
    async fn apply<S, I>(
        &mut self,
        store: &S,
        delta: &mut ArchiveDelta,
        instructions: I,
    ) -> Result<(), DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSync
            + Clone,
        I: Stream<Item = Instruction> + ConditionalSend;

    /// Like [`CorpusExt::apply`], but tags every [`Datum`] written by
    /// the batch with the [`Version`](crate::history::Version) of the
    /// revision that produced it, and records each instruction's history
    /// claim into the tree's history region. This is the write path used
    /// by version-controlled branch commits; [`CorpusExt::apply`]
    /// leaves the version unset.
    ///
    /// Returns whether the batch changed the indexes at all. A batch made
    /// entirely of cardinality-one no-ops (re-asserting values already in
    /// place) leaves the tree untouched and records no history — there is
    /// nothing a revision could attribute, and callers should not mint one.
    async fn apply_versioned<S, I>(
        &mut self,
        store: &S,
        delta: &mut ArchiveDelta,
        version: Option<Version>,
        instructions: I,
    ) -> Result<bool, DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSync
            + Clone,
        I: Stream<Item = Instruction> + ConditionalSend;

    /// The currently asserted [`Datum`]s recorded for the given entity and
    /// attribute, scanned from the EAV index. Multiple data are possible for
    /// attributes with more than one asserted value.
    async fn select_data<S>(
        &self,
        store: S,
        of: &crate::Entity,
        the: &crate::Relation,
    ) -> Result<Vec<Datum>, DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSend
            + ConditionalSync
            + Clone;

    /// Look up data at `(the, of)` through the attribute-ordered index,
    /// the ordering revision records are stored in.
    async fn select_record<S>(
        &self,
        store: S,
        of: &crate::Entity,
        the: &crate::Relation,
    ) -> Result<Vec<Fact>, DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSend
            + ConditionalSync
            + Clone;

    /// This tree's format [`Manifest`], as carried by its root node.
    ///
    /// Every key built for this tree must go through this manifest, both on the
    /// write path and when a reader builds a selector range, so that a
    /// boundary-sized value lands at the same key on both. An empty tree has no
    /// root to read and reports the default (the format a first write would
    /// stamp into it).
    ///
    /// `delta` is read through, because the tree's root may live only in an
    /// unflushed batch.
    async fn format_manifest<S>(
        &self,
        store: S,
        delta: &ArchiveDelta,
    ) -> Result<Manifest, DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSend
            + ConditionalSync
            + Clone;

    /// Write pre-built entries (e.g. revision lineage records — see
    /// [`RevisionRecord::entries`](crate::history::RevisionRecord::entries))
    /// into the tree as one edit batch, accumulating new nodes in `delta`,
    /// and store a spilled value's block in `store`.
    async fn record<S>(
        &mut self,
        store: &S,
        delta: &mut ArchiveDelta,
        entries: RecordEntries,
    ) -> Result<(), DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Clone;

    /// Scan the tree for facts matching the given constrained selector,
    /// yielding each as a borrowed-access [`FactView`] rather than a
    /// materialized [`Fact`] — the caller decides per row whether to
    /// read a field off the view or [`to_owned`](FactView::to_owned)
    /// the whole fact.
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
        selector: Selector<Constrained>,
    ) -> impl Stream<Item = Result<FactView, DialogCorpusError>> + 's + ConditionalSend
    where
        Self: Sized,
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's;

    /// [`scan`](Self::scan) in reverse: the same rows, in the opposite
    /// order. It walks the same key range from its end, and matches and
    /// filters each entry as [`scan`](Self::scan) does. The walk is lazy, so
    /// a reader that stops after a few rows reads about those rows, and the
    /// few more that the fetch of spilled values reads ahead.
    fn scan_reverse<'s, S>(
        self,
        store: S,
        cache: SpillCache,
        selector: Selector<Constrained>,
    ) -> impl Stream<Item = Result<FactView, DialogCorpusError>> + 's + ConditionalSend
    where
        Self: Sized,
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's;

    /// Like [`scan`](Self::scan), but yields every row as an owned
    /// [`Fact`] materialized from the scan's OWN key parse.
    ///
    /// This is the fast path for consumers that materialize every row (a
    /// `select(..).to_owned()`, an export): the scan already parsed each
    /// key once for selector matching and spill resolution, and
    /// reconstruction reuses that parse. Routing the same rows through
    /// [`FactView`]s and per-row
    /// [`to_owned`](crate::FactView::to_owned) parses every key a
    /// second time — measured at double-digit percent on large
    /// scan-and-materialize workloads. Consumers that read only some
    /// fields, or only some rows, should keep using
    /// [`scan`](Self::scan)'s views and pay materialization selectively.
    fn scan_owned<'s, S>(
        self,
        store: S,
        cache: SpillCache,
        selector: Selector<Constrained>,
    ) -> impl Stream<Item = Result<Fact, DialogCorpusError>> + 's + ConditionalSend
    where
        Self: Sized,
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's;

    /// An advisory upper-bound estimate of how many facts the `selector`'s
    /// key range spans, read from the range's edge paths (see
    /// `PersistentTree::range_estimate`).
    ///
    /// Reads at most two blocks per level instead of scanning, so a planner
    /// can compare the range sizes of independent scans cheaply. Returns
    /// `None` for an empty tree.
    async fn estimate<S>(
        self,
        store: S,
        selector: Selector<Constrained>,
    ) -> Result<Option<u64>, DialogCorpusError>
    where
        Self: Sized,
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSend
            + ConditionalSync
            + Clone;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CorpusExt for Corpus {
    async fn apply<S, I>(
        &mut self,
        store: &S,
        delta: &mut ArchiveDelta,
        instructions: I,
    ) -> Result<(), DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSync
            + Clone,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        self.apply_versioned(store, delta, None, instructions)
            .await
            .map(|_| ())
    }

    async fn apply_versioned<S, I>(
        &mut self,
        store: &S,
        delta: &mut ArchiveDelta,
        version: Option<Version>,
        instructions: I,
    ) -> Result<bool, DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSync
            + Clone,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        // Every read goes THROUGH the delta: this tree's root may have been
        // persisted by an earlier batch that the caller has not flushed to
        // `store` yet, so it exists only in `delta`, and a value this batch
        // spills is staged there until the commit writes it.
        let staged = DeltaOverlay::new(delta, store);

        // Every key this batch builds must use THIS tree's value-spill
        // threshold, and the edit batch must keep this tree's format rather
        // than restamping it with the defaults; both come from the manifest
        // the tree's own root node carries.
        let manifest = self.manifest(&staged).await?;
        let transient = self.edit();
        // Open one transient edit batch over this tree's spine and apply every
        // instruction's writes to it in flight, so the whole instruction stream
        // costs a single persist instead of one full tree rebuild per key.
        let (transient, changed) = write_instructions(
            transient,
            &staged,
            delta,
            version.into(),
            &manifest,
            instructions,
            WriteScope::Application,
        )
        .await?;
        // Seal the whole batch with a single bottom-up persist into the
        // caller's delta.
        *self = transient.persist(delta.nodes())?;
        Ok(changed)
    }

    async fn select_data<S>(
        &self,
        store: S,
        of: &crate::Entity,
        the: &crate::Relation,
    ) -> Result<Vec<Datum>, DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSend
            + ConditionalSync
            + Clone,
    {
        let storage = store;

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
        the: &crate::Relation,
    ) -> Result<Vec<Fact>, DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSend
            + ConditionalSync
            + Clone,
    {
        let storage = store;

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
                let spilled =
                    fetch_spilled(&storage, &self.catalog().archive(), &entry.key).await?;
                records.push(Fact::from_key_datum_with_value(&entry.key, datum, spilled)?);
            }
        }
        Ok(records)
    }

    async fn format_manifest<S>(
        &self,
        store: S,
        delta: &ArchiveDelta,
    ) -> Result<Manifest, DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSend
            + ConditionalSync
            + Clone,
    {
        // Read through `delta`: this tree's root may have been persisted by an
        // earlier batch that the caller has not flushed to `store` yet, so the
        // node exists only there.
        let staged = DeltaOverlay::new(delta, &store);
        Ok(self.manifest(&staged).await?)
    }

    async fn record<S>(
        &mut self,
        store: &S,
        delta: &mut ArchiveDelta,
        entries: RecordEntries,
    ) -> Result<(), DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Clone,
    {
        let RecordEntries { entries, spill } = entries;
        stage_spilled_value(delta, spill);
        let transient = {
            // Read through the delta: this tree's latest nodes may only
            // exist there (persisted by an earlier batch, not yet flushed).
            let storage = DeltaOverlay::new(delta, store);
            // Open the edit under the tree's OWN manifest (as
            // `apply_versioned` does), not the default: an edit through the
            // default restamps the touched path with the default format,
            // silently rewriting a tree built under other constants.
            let mut transient = self.edit();
            for (key, entry) in entries {
                transient = transient.insert(key, entry, &storage).await?;
            }
            transient
        };
        *self = transient.persist(delta.nodes())?;
        Ok(())
    }

    async fn estimate<S>(
        self,
        store: S,
        selector: Selector<Constrained>,
    ) -> Result<Option<u64>, DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSend
            + ConditionalSync
            + Clone,
    {
        let tree = self;
        let storage = store;
        // The range must be built under the manifest the facts were written
        // with, the same requirement `scan` has.
        let manifest = tree.manifest(&storage).await?;
        let selector = selector.for_manifest(&manifest);
        let range = selector_range(&selector, &manifest);
        // `range_scale` intersects half-open `[lower, upper)`; the selector's
        // range is inclusive, so the upper bound is the successor of the last
        // key. A single extra trailing byte is below any real successor key
        // and keeps the estimate an upper bound.
        let lower = range.start().as_ref();
        let mut upper = range.end().as_ref().to_vec();
        upper.push(0);
        Ok(tree.range_estimate(lower, &upper, &storage).await?)
    }

    fn scan<'s, S>(
        self,
        store: S,
        cache: SpillCache,
        selector: Selector<Constrained>,
    ) -> impl Stream<Item = Result<FactView, DialogCorpusError>> + 's + ConditionalSend
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's,
    {
        let tree = self;
        // Spilled value blocks are fetched by reference from the blobs of
        // the archive the tree's catalog belongs to.
        let archive = tree.catalog().archive();
        let storage = store;
        try_stream! {
            // Both the scan range and the per-entry match must be built under
            // the manifest the stored facts were WRITTEN with, or a
            // boundary-sized value encodes differently here than in the tree
            // and the scan silently misses it.
            let manifest = tree.manifest(&storage).await?;
            let selector = selector.for_manifest(&manifest);
            let range = selector_range(&selector, &manifest);

            let stream = tree.stream_range_handles(range, &storage);
            let fetched =
                scanned_views(stream, &selector, &manifest, &storage, &archive, &cache);
            tokio::pin!(fetched);
            for await item in fetched {
                if let Some(view) = item? {
                    yield view;
                }
            }
        }
    }

    fn scan_reverse<'s, S>(
        self,
        store: S,
        cache: SpillCache,
        selector: Selector<Constrained>,
    ) -> impl Stream<Item = Result<FactView, DialogCorpusError>> + 's + ConditionalSend
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's,
    {
        let tree = self;
        let archive = tree.catalog().archive();
        let storage = store;
        try_stream! {
            // The range and the match of `scan`, under the manifest the
            // facts were written with.
            let manifest = tree.manifest(&storage).await?;
            let selector = selector.for_manifest(&manifest);
            let range = selector_range(&selector, &manifest);

            let stream = tree.stream_range_handles_reverse(range, &storage);
            let fetched =
                scanned_views(stream, &selector, &manifest, &storage, &archive, &cache);
            tokio::pin!(fetched);
            for await item in fetched {
                if let Some(view) = item? {
                    yield view;
                }
            }
        }
    }

    fn scan_owned<'s, S>(
        self,
        store: S,
        cache: SpillCache,
        selector: Selector<Constrained>,
    ) -> impl Stream<Item = Result<Fact, DialogCorpusError>> + 's + ConditionalSend
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's,
    {
        let tree = self;
        // Spilled value blocks are fetched by reference from the blobs of
        // the archive the tree's catalog belongs to.
        let archive = tree.catalog().archive();
        let storage = store;
        try_stream! {
            // Both the scan range and the per-entry match must be built under
            // the manifest the stored facts were WRITTEN with — see `scan`.
            let manifest = tree.manifest(&storage).await?;
            let selector = selector.for_manifest(&manifest);
            let range = selector_range(&selector, &manifest);

            let stream = tree.stream_range_handles(range, &storage);
            // The two stages of `scan`, reconstructing whole facts: every
            // row is parsed once, and a spilled row is materialized from
            // that parse once its block has landed.
            let staged = stream.map(|item| -> Result<Staged<_, Fact>, DialogCorpusError> {
                let raw = item?;
                let layout = parse_key_layout(raw.key.as_ref()).ok_or_else(|| {
                    DialogCorpusError::InvalidKey(
                        "scanned entry's key does not parse".to_string(),
                    )
                })?;
                let parts = layout.view(raw.key.as_ref());
                let verdict = match_selector_and_key_ref(&selector, &parts, &manifest);
                if verdict == SelectorMatch::Excluded {
                    return Ok(Staged::Skip);
                }
                let State::Added(datum) = &raw.value else {
                    return Ok(Staged::Skip);
                };
                if let ValueRef::Spilled { hash, .. } = &parts.value {
                    let hash = hash.to_vec();
                    return Ok(Staged::Spilled { raw, layout, verdict, hash });
                }
                Ok(match reconstruct(&selector, &parts, datum, None, verdict)? {
                    Some(fact) => Staged::Ready(fact),
                    None => Staged::Skip,
                })
            });
            let fetched = staged
                .map(|staged| {
                    let raw_store = &storage;
                    let archive = &archive;
                    let cache = &cache;
                    let selector = &selector;
                    async move {
                        let (raw, layout, verdict, hash) = match staged? {
                            Staged::Spilled { raw, layout, verdict, hash } => (raw, layout, verdict, hash),
                            finished => return Ok(staged_ready(finished)),
                        };
                        let spilled = fetch_spilled_reference(raw_store, archive, cache, &hash).await?;
                        let parts = layout.view(raw.key.as_ref());
                        let State::Added(datum) = &raw.value else {
                            unreachable!("Added state checked above")
                        };
                        reconstruct(selector, &parts, datum, Some(spilled), verdict)
                    }
                })
                .buffered(SPILL_LOOKAHEAD);
            tokio::pin!(fetched);
            for await item in fetched {
                if let Some(fact) = item? {
                    yield fact;
                }
            }
        }
    }
}

/// The rows of a scan of `raw`, in the order that `raw` yields its entries,
/// which [`scan`](CorpusExt::scan) and
/// [`scan_reverse`](CorpusExt::scan_reverse) share. An item is `None`
/// for an entry whose spilled value fails the value predicates of
/// `selector`.
fn scanned_views<'r, S, R>(
    raw: R,
    selector: &'r Selector<Constrained>,
    manifest: &'r Manifest,
    storage: &'r S,
    archive: &'r ArchiveScope,
    cache: &'r SpillCache,
) -> impl Stream<Item = Result<Option<FactView>, DialogCorpusError>> + 'r
where
    S: Provider<blob::Load> + ConditionalSync + 'r,
    R: Stream<
            Item = Result<
                dialog_search_tree::Entry<dialog_search_tree::KeyHandle, State<Datum>>,
                dialog_search_tree::DialogSearchTreeError,
            >,
        > + 'r,
{
    // Stage one, synchronous per entry: parse the key ONCE into
    // borrowed components for matching and spill resolution, and
    // finish every inline-valued row on the spot. Nothing else is
    // materialized: the entry's key and payload travel into the
    // yielded view as-is, and the consumer decides per row whether
    // to borrow a field or reconstruct the whole fact. A row whose
    // value spilled is handed on with its block reference, so the
    // fetch it needs runs alongside its neighbours' (stage two)
    // instead of holding the scan for one round trip per row.
    let staged = raw.map(
        move |item| -> Result<Staged<_, FactView>, DialogCorpusError> {
            let raw = item?;
            // A key that does not parse is corruption; dropping it
            // silently would make the corrupt entry vanish from results
            // with no signal.
            let layout = parse_key_layout(raw.key.as_ref()).ok_or_else(|| {
                DialogCorpusError::InvalidKey("scanned entry's key does not parse".to_string())
            })?;
            let parts = layout.view(raw.key.as_ref());
            let verdict = match_selector_and_key_ref(selector, &parts, manifest);
            if verdict == SelectorMatch::Excluded || !matches!(raw.value, State::Added(_)) {
                return Ok(Staged::Skip);
            }
            if let ValueRef::Spilled { hash, .. } = &parts.value {
                let hash = hash.to_vec();
                return Ok(Staged::Spilled {
                    raw,
                    layout,
                    verdict,
                    hash,
                });
            }
            // A NeedsValue verdict means some value predicate's answer
            // lies beyond what the key decides; re-check semantically
            // before yielding. Only the value is decoded for the check,
            // not the entity or attribute.
            if verdict == SelectorMatch::NeedsValue
                && !value_predicates_admit(selector, &decode_value_parts(&parts, None)?)
            {
                return Ok(Staged::Skip);
            }
            let State::Added(datum) = raw.value else {
                unreachable!("Added state checked above")
            };
            Ok(Staged::Ready(FactView::new(raw.key, layout, datum, None)))
        },
    );
    // Stage two: the spilled rows' block fetches, SPILL_LOOKAHEAD in
    // flight, in row order.
    staged
        .map(move |staged| {
            let raw_store = storage;
            async move {
                let (raw, layout, verdict, hash) = match staged? {
                    Staged::Spilled {
                        raw,
                        layout,
                        verdict,
                        hash,
                    } => (raw, layout, verdict, hash),
                    finished => return Ok(staged_ready(finished)),
                };
                let spilled = fetch_spilled_reference(raw_store, archive, cache, &hash).await?;
                // The block is in hand now (it was fetched for the
                // view anyway), so the value predicates can be
                // answered past the in-key prefix.
                if verdict == SelectorMatch::NeedsValue {
                    let parts = layout.view(raw.key.as_ref());
                    if !value_predicates_admit(
                        selector,
                        &decode_value_parts(&parts, Some(spilled.clone()))?,
                    ) {
                        return Ok(None);
                    }
                }
                let State::Added(datum) = raw.value else {
                    unreachable!("Added state checked above")
                };
                Ok::<_, DialogCorpusError>(Some(FactView::new(
                    raw.key,
                    layout,
                    datum,
                    Some(spilled),
                )))
            }
        })
        .buffered(SPILL_LOOKAHEAD)
}

/// How many spilled value blocks a scan keeps in flight ahead of the row
/// it is yielding. A spilled value is its own block, so over a hydrating
/// store every spilled row is a round trip; fetched one row at a time a
/// scan of large values would cost one round trip per row. The history
/// reader walks its records the same way.
pub(crate) const SPILL_LOOKAHEAD: usize = 16;

/// A scanned entry between the scan's two stages: finished from its key
/// alone, waiting on its spilled value block, or filtered out.
enum Staged<Raw, Ready> {
    Ready(Ready),
    Spilled {
        raw: Raw,
        layout: KeyLayout,
        verdict: SelectorMatch,
        hash: Vec<u8>,
    },
    Skip,
}

/// The finished row of a stage-one entry, if any. Only called for entries
/// that are not [`Staged::Spilled`].
fn staged_ready<Raw, Ready>(staged: Staged<Raw, Ready>) -> Option<Ready> {
    match staged {
        Staged::Ready(ready) => Some(ready),
        Staged::Skip | Staged::Spilled { .. } => None,
    }
}

/// Reconstruct a scanned row as a whole fact, applying the value
/// predicates a `NeedsValue` verdict deferred to the materialized value.
///
/// A row whose stored bytes fail read-side validation (a non-canonical
/// entity, a broken attribute — `CorruptEntry`) is a corrupt or
/// foreign-written entry: skipped with a warning rather than failing the
/// whole query. Structural key corruption (the parse before this) stays
/// loud.
fn reconstruct(
    selector: &Selector<Constrained>,
    parts: &KeyRef<'_>,
    datum: &Datum,
    spilled: Option<Vec<u8>>,
    verdict: SelectorMatch,
) -> Result<Option<Fact>, DialogCorpusError> {
    let fact = match Fact::from_key_ref_datum_value(parts, datum, spilled) {
        Ok(fact) => fact,
        Err(DialogCorpusError::CorruptEntry(reason)) => {
            tracing::warn!(%reason, "ignoring corrupt stored row in scan");
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    if verdict == SelectorMatch::NeedsValue && !value_predicates_admit(selector, &fact.is) {
        return Ok(None);
    }
    Ok(Some(fact))
}

/// The inclusive key range a selector's scan reads.
///
/// Index choice: exact fields take priority (entity / value /
/// attribute); prefix bounds, and the bounds of an attribute range,
/// pick the index whose leading dimension they constrain when no exact
/// field does. The range is additionally tightened with whatever
/// bounds the selector carries (sound on any dimension, tight on
/// leading ones and on one that follows exact fields, as an attribute
/// range under one entity is) via
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
///
/// `manifest` is the target tree's format. A value-constrained selector's
/// bounds carry the value's payload through the same inline-vs-spill decision
/// (`inline_n`) and spilled key-prefix width (`spill_prefix`) the facts were
/// written under, so passing a different manifest brackets the wrong keys and
/// an equality scan on a boundary-sized value silently returns nothing.
pub fn selector_range(
    selector: &Selector<Constrained>,
    manifest: &Manifest,
) -> RangeInclusive<Key> {
    // One bound = one `KeyParts` mutation + one `build_key`. The previous
    // construction chained `min()/max().apply_selector(..)`, and every
    // `set_*` in that chain re-parsed and re-built the whole key — around
    // ten parse/build round-trips per range, which a join pays once per
    // outer binding; it measured as a third of engine query time. Applying
    // the selector's exact fields onto the parts directly is equivalent by
    // construction: `set_*` parses the built bound back into exactly these
    // parts and mutates the same field.
    let exact_bound = |tag: u8, upper: bool| {
        let parts = varkey::KeyParts::bound(
            tag,
            upper,
            selector
                .entity()
                .map(|entity| EntityKeyPart::from(entity).raw().to_vec()),
            selector
                .attribute()
                .map(|attribute| AttributeKeyPart::from(attribute).raw().to_vec()),
            selector.value().map(|value| {
                (
                    value.data_type().band(manifest.value_encoding),
                    build_value_payload(value, manifest),
                )
            }),
        );
        Key::from(varkey::build_key(&parts))
    };
    if selector.entity().is_some()
        || (selector.entity_prefix().is_some()
            && selector.value().is_none()
            && selector.attribute().is_none()
            && selector.attribute_prefix().is_none()
            && selector.attribute_lower().is_none()
            && selector.attribute_upper().is_none())
    {
        let (start, end) = apply_prefix_bounds(
            EntityKey(exact_bound(ENTITY_KEY_TAG, false)),
            EntityKey(exact_bound(ENTITY_KEY_TAG, true)),
            selector,
            manifest,
        );
        start.into_key()..=end.into_key()
    } else if selector.value().is_some()
        || selector.value_prefix().is_some()
        || selector.value_lower().is_some()
        || selector.value_upper().is_some()
    {
        let (start, end) = apply_prefix_bounds(
            ValueKey(exact_bound(VALUE_KEY_TAG, false)),
            ValueKey(exact_bound(VALUE_KEY_TAG, true)),
            selector,
            manifest,
        );
        start.into_key()..=end.into_key()
    } else if selector.attribute().is_some()
        || selector.attribute_prefix().is_some()
        || selector.attribute_lower().is_some()
        || selector.attribute_upper().is_some()
    {
        let (start, end) = apply_prefix_bounds(
            AttributeKey(exact_bound(ATTRIBUTE_KEY_TAG, false)),
            AttributeKey(exact_bound(ATTRIBUTE_KEY_TAG, true)),
            selector,
            manifest,
        );
        start.into_key()..=end.into_key()
    } else {
        // `Constrained` guarantees at least one field is set.
        unreachable!("Selector will always have at least one field specified")
    }
}

/// Which attribute namespaces an instruction stream may write.
///
/// The `dialog.` namespace is reserved for machinery-written facts (revision
/// records, delegation records): [`Application`](WriteScope::Application)
/// writes reject it, so at the library level such facts cannot be corrupted
/// through the ordinary write path. Machinery write paths pass
/// [`Machinery`](WriteScope::Machinery) and take responsibility for what they
/// write — the same trust level as [`CorpusExt::record`], which appends
/// reserved entries without instructions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteScope {
    /// Ordinary application data: reserved attributes are rejected.
    Application,
    /// Machinery-written facts: reserved attributes are permitted.
    Machinery,
}

/// What a batch stamps its writes with.
///
/// A versioned batch tags every datum it writes with its [`Version`] and
/// records each instruction's history under it. A batch normally writes
/// under a version nothing in the tree carries yet; an amending batch
/// writes more under a version the tree already carries (an amended
/// commit), so what it records folds into what is recorded there, exactly
/// as two writes of one batch fold into each other.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Stamp {
    /// Unversioned writes: no history is recorded.
    Unversioned,
    /// Writes under a version the tree carries nothing under yet.
    Fresh(Version),
    /// More writes under a version the tree already carries writes under.
    Amend(Version),
}

impl Stamp {
    /// The version writes are tagged with, if any.
    pub fn version(&self) -> Option<Version> {
        match self {
            Stamp::Unversioned => None,
            Stamp::Fresh(version) | Stamp::Amend(version) => Some(*version),
        }
    }
}

impl From<Option<Version>> for Stamp {
    fn from(version: Option<Version>) -> Self {
        match version {
            None => Stamp::Unversioned,
            Some(version) => Stamp::Fresh(version),
        }
    }
}

/// Folds a later history record into the versions an earlier record at the
/// same history key superseded: the later record's polarity, citing both
/// records' superseded versions, earlier first.
fn fold_record(earlier: &[Version], later: Record) -> Record {
    if earlier.is_empty() {
        return later;
    }
    let mut versions = earlier.to_vec();
    versions.extend_from_slice(later.claim().cause.versions());
    let claim = Claim {
        cause: HistoryCause::new(versions),
        ..later.claim().clone()
    };
    if later.is_assertion() {
        Record::Assert(claim)
    } else {
        Record::Retract(claim)
    }
}

/// Applies an instruction stream to any [`CorpusWriter`], returning the
/// written target and whether the batch changed the indexes.
///
/// This is the whole of the fact write semantics: reserved-namespace
/// enforcement, cardinality-one supersession, value spilling, coverage records,
/// and the history entries each instruction contributes. It is generic over the
/// write target so the canonical edit path and the buffered (hitchhiker) path
/// run *identical* semantics; only where the writes land differs.
///
/// The supersession scans go through [`CorpusWriter::scan`] and
/// [`CorpusWriter::read`], which see the batch's own pending writes on both
/// targets. On the buffered target that means the node buffers are merged into
/// the scan: an election blind to a buffered prior would leave it live where
/// the pick would have retired it, and a `Retract` blind to one would cite
/// nothing and so cover nothing at merge time.
///
/// `storage` loads the tree's nodes and spilled values; a value above the
/// manifest's inline threshold lives as a content-addressed blob, and its key
/// carries only the 32-byte reference to it. The blobs of the values this batch
/// spills are staged in `staged`'s blobs lane for the commit to write, so pass
/// a `storage` that reads through `staged` (a [`DeltaOverlay`]) for the batch
/// to see its own spills.
///
/// `manifest` carries that inline threshold (`inline_n`) and the spilled
/// key-prefix width (`spill_prefix`), and it must be the TARGET TREE's own,
/// read via
/// [`PersistentTree::manifest`](dialog_search_tree::PersistentTree::manifest),
/// not a process-wide default. Every key this function builds and every
/// selector range a later read builds must agree on both: a boundary-sized
/// value that inlines on one side and spills on the other lands at a different
/// key, so the read misses the fact entirely.
#[tracing::instrument(skip_all, name = "write_instructions")]
#[allow(clippy::too_many_lines)]
pub async fn write_instructions<W, S, I>(
    mut transient: W,
    storage: &S,
    staged: &mut ArchiveDelta,
    stamp: Stamp,
    manifest: &Manifest,
    instructions: I,
    scope: WriteScope,
) -> Result<(W, bool), DialogCorpusError>
where
    W: CorpusWriter + ConditionalSend,
    S: Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + Provider<blob::Load>
        + ConditionalSync
        + Clone,
    I: Stream<Item = Instruction> + ConditionalSend,
{
    // History records are buffered and only written if the batch changed
    // the indexes: a batch of pure no-ops must leave the tree untouched,
    // history region included. Buffering is per history key, folding
    // collisions: two instructions on the same (entity, attribute, value)
    // in one batch land at ONE history key, and last-write-wins would
    // silently drop the earlier record's lineage — a retract-then-re-assert
    // of one value lost the retract's cause from the log (while its
    // coverage mirror survived), so the screened merge path never retired
    // a stale peer's copy while the graft path did. The fold keeps the
    // later record's polarity and unions the superseded versions: a
    // re-assert citing what it overrode.
    let version = stamp.version();
    let mut history_records: BTreeMap<Key, Record> = BTreeMap::new();
    let mut changed = false;
    let buffer_record = |records: &mut BTreeMap<Key, Record>, record: Record, version: &Version| {
        let key = record.key(version, manifest);
        let record = match records.remove(&key) {
            None => record,
            Some(earlier) => fold_record(earlier.claim().cause.versions(), record),
        };
        records.insert(key, record);
    };

    tokio::pin!(instructions);
    while let Some(instruction) = instructions.next().await {
        // The `dialog.` namespace is reserved for machinery (revision
        // records — see `history::RevisionRecord` — and delegation
        // records), written through [`CorpusExt::record`] or a
        // [`WriteScope::Machinery`] stream. At the library level such
        // facts therefore cannot be corrupted through the ordinary
        // write path. Four prefixes are carved out of the application
        // gate: `dialog.rule/*` (rule storage), `dialog.concept/*`
        // (concept markers), `dialog.attribute/*` (attribute placement
        // declarations), and `dialog.link/*` (stack links), whose
        // integrity is semantic rather than positional — rules are
        // content-addressed, so a forged rule fact fails the hydration
        // check upstream and is inert, a marker only ever selects a
        // behaviour for facts the writer could have written anyway,
        // and a link is a content-addressed record of a head the
        // writer observed.
        if scope == WriteScope::Application {
            let (Instruction::Assert(fact, _) | Instruction::Retract(fact)) = &instruction;
            let the = fact.the.as_str();
            if the.starts_with("dialog.")
                && !the.starts_with("dialog.rule/")
                && !the.starts_with("dialog.concept/")
                && !the.starts_with("dialog.attribute/")
                && !the.starts_with("dialog.link/")
            {
                return Err(DialogCorpusError::ReservedRelation(fact.the.to_string()));
            }
        }
        match instruction {
            Instruction::Assert(fact, Pick::All) => {
                changed = true;
                // ONE value encode per instruction: the payload feeds all
                // three index keys, and a spilling value's block bytes and
                // reference come from the same pass.
                let encoded = EncodedValue::new(&fact.is, manifest);
                let (entity_key, attribute_key, value_key) =
                    fact_index_keys_with(&fact, encoded.value_type, encoded.payload);

                // Persist a spilling value's bytes as a content-addressed
                // block before recording the fact; the key holds only the
                // 32-byte reference to it.
                stage_spilled_value(staged, encoded.spill);

                // A version-tagged assertion records its history: an
                // assertion is purely additive, so it supersedes nothing.
                if let Some(version) = &version {
                    let record = Record::Assert(Claim {
                        the: fact.the.clone(),
                        of: fact.of.clone(),
                        is: fact.is.clone(),
                        cause: HistoryCause::genesis(),
                    });
                    buffer_record(&mut history_records, record, version);
                }

                let mut datum = Datum::for_fact(&fact);
                datum.version = version;
                // The fact orderings address a claim by (entity, attribute,
                // value), so asserting a value that already stands re-asserts
                // the SAME key: the standing claims collapse into the new
                // datum rather than being overwritten. A later retraction
                // covers the whole set — an insert-overwrite here silently
                // orphaned the earlier claim, which could then resurrect the
                // fact through a merge. Versioned writes only.
                //
                // On the canonical target this probe is free (the insert
                // rebuilds the same leaf it reads); on the buffered target
                // the insert is a blind append and this probe is the ONLY
                // read the assert performs — one EAV spine, on a partial
                // replica hydrated through the store's remote fallback. The
                // AEV/VAE/history spines are never read by any write, so
                // nothing downstream may assume a commit hydrated the paths
                // it touched (see notes/version-control.md, "Push from a
                // partial replica").
                if version.is_some()
                    && let Some(State::Added(standing)) =
                        transient.read(&entity_key, storage).await?
                {
                    datum.absorb_versions(standing.versions());
                }
                let added = State::Added(datum);
                transient = transient
                    .write_all(
                        vec![
                            (entity_key, added.clone()),
                            (attribute_key, added.clone()),
                            (value_key, added),
                        ],
                        storage,
                    )
                    .await?;
            }
            Instruction::Assert(fact, policy) => {
                let entity_key = EntityKey::from_fact(&fact, manifest);

                // Scan the cell's claims against the in-flight write target,
                // so writes from earlier instructions in this batch are
                // candidates too. Each prior is kept with its
                // key, its versions and its standing: the deepest version it
                // carries and its cause, which is how a read orders it.
                let mut priors: Vec<(Key, Vec<Version>, Value, Standing)> = Vec::new();
                // The prior holding the written value, when the cell holds
                // it: one more candidate the election may return.
                let mut held: Option<usize> = None;
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
                            let spilled = fetch_spilled(
                                storage,
                                &transient.catalog().archive(),
                                &candidate.key,
                            )
                            .await?;
                            let current = Fact::from_key_datum_with_value(
                                &candidate.key,
                                current_element,
                                spilled,
                            )?;
                            if current.of != fact.of || current.the != fact.the {
                                continue;
                            }
                            // The cell holds the written value when a
                            // reader of the value's type observes it: an
                            // integer 4 asserted over a stored 4.3 is what
                            // the cell already reads as.
                            if current.is.reads_as(&fact.is) {
                                held = Some(priors.len());
                            }
                            let versions: Vec<Version> =
                                current_element.versions().copied().collect();
                            let standing = Standing::new(
                                versions
                                    .iter()
                                    .map(|version| {
                                        (version.edition, make_reference(version.key_bytes()))
                                    })
                                    .max(),
                                current.cause.clone().unwrap_or(Cause([0; 32])),
                            );
                            priors.push((candidate.key, versions, current.is, standing));
                        }
                    }
                }

                // The elected claim is the one a read under the pick
                // returns over these priors, the written value's own claim
                // among them when the cell holds it.
                let elected = policy.elect(
                    priors
                        .iter()
                        .map(|(_, _, value, standing)| (value, Some(standing))),
                );
                // A read returns the written value already: nothing to
                // succeed, and a fresh record would fork the claim's
                // lineage away from the version the standing datum carries.
                if held.is_some() && elected == held {
                    continue;
                }
                changed = true;
                // The written value's claim, when the cell holds one the
                // read does not return: the write folds the commit's
                // version into it, as an assertion of a value that stands
                // does, so it stands past the claim it succeeds.
                let held_versions: Vec<Version> = held
                    .map(|index| priors[index].1.clone())
                    .unwrap_or_default();

                // The elected claim is retracted outright, as a retraction
                // is, and its versions become the new claim's cause, as a
                // replacement's superseded versions do.
                let mut superseded_versions: Vec<Version> = Vec::new();
                if let Some(index) = elected {
                    let (key, versions, elected_value, _) = priors.swap_remove(index);
                    // A claim this batch asserted carries the batch's own
                    // version, and its assertion record is buffered under
                    // that version. Succeeding it within the batch
                    // withdraws it, as a retraction would: the record fold
                    // nets the two, so the history never shows a claim no
                    // index held.
                    if let Some(version) = &version
                        && versions.contains(version)
                    {
                        let withdrawn: Vec<Version> = versions
                            .iter()
                            .filter(|withdrawn| *withdrawn != version)
                            .copied()
                            .collect();
                        let record = Record::Retract(Claim {
                            the: fact.the.clone(),
                            of: fact.of.clone(),
                            is: elected_value,
                            cause: HistoryCause::new(withdrawn),
                        });
                        buffer_record(&mut history_records, record, version);
                    }
                    superseded_versions = versions;
                    let (entity_key, attribute_key, value_key) = reproject_index_keys(&key)?;
                    transient = transient.erase(&entity_key, storage).await?;
                    transient = transient.erase(&value_key, storage).await?;
                    transient = transient.erase(&attribute_key, storage).await?;
                }

                // A record must not claim itself as its cause: a claim this
                // batch asserted earlier carries this batch's own version.
                if let Some(version) = &version {
                    let superseded: Vec<Version> = superseded_versions
                        .into_iter()
                        .filter(|superseded| superseded != version)
                        .collect();
                    let record = Record::Assert(Claim {
                        the: fact.the.clone(),
                        of: fact.of.clone(),
                        is: fact.is.clone(),
                        cause: HistoryCause::new(superseded),
                    });
                    buffer_record(&mut history_records, record, version);
                }

                // The assertion itself, as `Assert` writes one, folding in
                // the versions of the written value's own claim when the
                // cell held it.
                let encoded = EncodedValue::new(&fact.is, manifest);
                let (entity_key, attribute_key, value_key) =
                    fact_index_keys_with(&fact, encoded.value_type, encoded.payload);
                stage_spilled_value(staged, encoded.spill);
                let mut datum = Datum::for_fact(&fact);
                datum.version = version;
                if version.is_some() {
                    datum.absorb_versions(held_versions.iter());
                }
                let added = State::Added(datum);
                transient = transient
                    .write_all(
                        vec![
                            (entity_key, added.clone()),
                            (attribute_key, added.clone()),
                            (value_key, added),
                        ],
                        storage,
                    )
                    .await?;
            }
            Instruction::Retract(fact) => {
                let (entity_key, attribute_key, value_key) = fact_index_keys(&fact, manifest);

                // The standing datum decides everything below: whether
                // the retract changes anything at all, and which versions
                // it withdraws. Retracting a fact that is not there is a
                // no-op, spec'd as such: no index change, no record, no
                // minted revision. (A same-batch assert of the same fact
                // IS visible here — the write target carries it — so
                // assert+retract still cancels through the erases below,
                // and the record fold nets their lineage.)
                let Some(State::Added(standing)) = transient.read(&entity_key, storage).await?
                else {
                    continue;
                };
                changed = true;

                // A version-tagged retraction records its history: its
                // cause is EVERY claim the standing entry collapses —
                // same-value asserts from different writers share one
                // key, and the retraction's author observed all of them
                // (spec D3: a retraction covers exactly what its author
                // had seen). An assertion made earlier in this same
                // batch carries this batch's own version; a record must
                // not claim itself as its cause, so that one is dropped
                // (alone, it degenerates to a genesis retraction).
                if let Some(version) = &version {
                    let withdrawn: Vec<Version> = standing
                        .versions()
                        .filter(|withdrawn| *withdrawn != version)
                        .copied()
                        .collect();
                    let record = Record::Retract(Claim {
                        the: fact.the.clone(),
                        of: fact.of.clone(),
                        is: fact.is.clone(),
                        cause: HistoryCause::new(withdrawn),
                    });
                    buffer_record(&mut history_records, record, version);
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
                transient = transient.erase(&entity_key, storage).await?;
                transient = transient.erase(&attribute_key, storage).await?;
                transient = transient.erase(&value_key, storage).await?;
            }
        }
    }

    // Write the folded records and their coverage mirrors. Emitting
    // coverage from the FOLDED record (rather than per instruction)
    // keeps the mirror consistent with the log when a batch touched
    // one (entity, attribute, value) twice: the coverage entry's key
    // collides exactly when the record key does, and both then carry
    // the same folded lineage.
    if let Some(version) = &version {
        let mut entries = Vec::with_capacity(history_records.len() * 2);
        for (key, record) in history_records {
            // An amended version already has history recorded under it:
            // fold into the record at this key the way two writes of one
            // batch fold, so amending a commit records exactly what one
            // commit making both sets of writes would have.
            let record = match stamp {
                Stamp::Amend(_) => match transient.read(&key, storage).await? {
                    Some(State::Added(stored)) => fold_record(&stored.supersedes, record),
                    _ => record,
                },
                Stamp::Fresh(_) | Stamp::Unversioned => record,
            };
            if let Some(coverage) = record.coverage_entry(version) {
                entries.push(coverage);
            }
            // The map key IS the record's history key (that is what the fold
            // deduplicated on), so the write reuses it instead of rebuilding
            // it from the claim.
            let entry = record.into_datum(version);
            entries.push((key, entry));
        }
        transient = transient.write_all(entries, storage).await?;
    }

    Ok((transient, changed))
}

#[cfg(test)]
mod spill_cache_tests {
    use dialog_search_tree::helpers::test_catalog;
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{Corpus, CorpusExt, SpillCache, fetch_spilled, fetch_spilled_cached, spill_cache};
    use crate::{EntityKey, Fact, Instruction, KeyView, Value};

    use crate::ArchiveDelta;

    use crate::Blake3Hash;
    use crate::helpers::CountingBlocks;
    use futures_util::stream;

    /// A scan over spilled values fetches their blocks concurrently: each
    /// spilled row is its own block, and over a hydrating store that block
    /// is a round trip, so a scan that fetched them one row at a time would
    /// cost one round trip per row. Both scan shapes, and the rows come out
    /// complete and in order.
    #[dialog_common::test]
    async fn it_fetches_spilled_values_concurrently_in_scans() -> anyhow::Result<()> {
        use crate::Selector;

        use futures_util::TryStreamExt as _;

        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        let store = CountingBlocks::new();
        let facts: Vec<Fact> = (0..24)
            .map(|index| Fact {
                the: "doc/body".parse().unwrap(),
                of: format!("doc:{index}").parse().unwrap(),
                is: Value::String(format!("{index}:").repeat(inline_n + 1)),
                cause: None,
            })
            .collect();
        let mut delta = ArchiveDelta::zero();
        let mut tree = Corpus::empty_in(test_catalog());
        tree.apply(
            &store,
            &mut delta,
            stream::iter(
                facts
                    .iter()
                    .cloned()
                    .map(|fact| Instruction::Assert(fact, crate::Pick::All)),
            ),
        )
        .await?;
        store.flush(&mut delta);
        let selector = Selector::new().the("doc/body".parse()?);

        for owned in [false, true] {
            // A fresh spill cache and node cache: every spilled block reads.
            let cold = Corpus::at(test_catalog(), tree.root().clone());
            store.reset();
            let values: Vec<Value> = if owned {
                cold.scan_owned(store.clone(), spill_cache(), selector.clone())
                    .map_ok(|fact| fact.is)
                    .try_collect()
                    .await?
            } else {
                cold.scan(store.clone(), spill_cache(), selector.clone())
                    .map_ok(|view| view.value())
                    .try_collect::<Vec<_>>()
                    .await?
                    .into_iter()
                    .collect::<Result<Vec<Value>, _>>()?
            };
            let mut values: Vec<String> = values
                .into_iter()
                .map(|value| format!("{value:?}"))
                .collect();
            values.sort();
            let mut expected: Vec<String> =
                facts.iter().map(|fact| format!("{:?}", fact.is)).collect();
            expected.sort();
            assert_eq!(values, expected, "every spilled value comes out whole");
            let peak = store.peak_reads_in_flight();
            assert!(
                peak > 1,
                "the scan (owned: {owned}) must fetch spilled blocks together, \
                 but only {peak} read was ever in flight"
            );
        }
        Ok(())
    }

    /// The spill cache is bounded by TOTAL BYTES: inserting past the budget
    /// evicts the oldest blocks, and a block larger than the whole budget is
    /// served uncached rather than pinning it.
    #[dialog_common::test]
    fn it_bounds_the_spill_cache_by_bytes() {
        let cache = SpillCache::with_budget(10_000);
        let key = |byte: u8| -> Blake3Hash { [byte; 32] };

        cache.insert(key(1), vec![0; 4_000]);
        cache.insert(key(2), vec![0; 4_000]);
        cache.insert(key(3), vec![0; 4_000]);
        assert!(cache.get(&key(1)).is_none(), "oldest block evicted");
        assert!(cache.get(&key(2)).is_some());
        assert!(cache.get(&key(3)).is_some());

        cache.insert(key(4), vec![0; 20_000]);
        assert!(
            cache.get(&key(4)).is_none(),
            "an over-budget block is never cached"
        );
        assert!(
            cache.get(&key(2)).is_some(),
            "an over-budget insert evicts nothing"
        );
    }

    /// Commits one spilling fact and returns the store (with the spilled block
    /// written) plus the EAV key that references it.
    async fn spilled_setup() -> (CountingBlocks, crate::Key, Value) {
        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        let value = Value::String("q".repeat(inline_n + 1));
        let store = CountingBlocks::new();
        let mut delta = ArchiveDelta::zero();
        let mut tree = Corpus::empty_in(test_catalog());
        let fact = Fact {
            the: "doc/body".parse().unwrap(),
            of: "doc:1".parse().unwrap(),
            is: value.clone(),
            cause: None,
        };
        tree.apply(
            &store,
            &mut delta,
            stream::iter(vec![Instruction::Assert(fact.clone(), crate::Pick::All)]),
        )
        .await
        .unwrap();
        store.flush(&mut delta);
        let key = EntityKey::from_fact(&fact, &dialog_search_tree::Manifest::default()).into_key();
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
        let first = fetch_spilled_cached(&store, &test_catalog().archive(), &cache, &key).await?;
        let after_miss = store.reads();
        let second = fetch_spilled_cached(&store, &test_catalog().archive(), &cache, &key).await?;
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
        let cached = fetch_spilled_cached(&store, &test_catalog().archive(), &cache, &key).await?;
        let uncached = fetch_spilled(&store, &test_catalog().archive(), &key).await?;
        assert_eq!(cached, uncached);
        assert!(cached.is_some());
        Ok(())
    }

    /// An inline key spills nothing: both fetches return `None` and read no
    /// block regardless of the cache.
    #[dialog_common::test]
    async fn it_returns_none_for_an_inline_key() -> anyhow::Result<()> {
        let store = CountingBlocks::new();
        let mut delta = ArchiveDelta::zero();
        let mut tree = Corpus::empty_in(test_catalog());
        let fact = Fact {
            the: "user/name".parse().unwrap(),
            of: "user:1".parse().unwrap(),
            is: Value::String("Alice".to_string()),
            cause: None,
        };
        tree.apply(
            &store,
            &mut delta,
            stream::iter(vec![Instruction::Assert(fact.clone(), crate::Pick::All)]),
        )
        .await?;
        let key = EntityKey::from_fact(&fact, &dialog_search_tree::Manifest::default()).into_key();
        let cache = spill_cache();
        assert_eq!(
            fetch_spilled_cached(&store, &test_catalog().archive(), &cache, &key).await?,
            None
        );
        assert_eq!(
            fetch_spilled(&store, &test_catalog().archive(), &key).await?,
            None
        );
        Ok(())
    }
}

#[cfg(test)]
mod range_tests {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::selector_range;
    use crate::{NameShape, Selector};

    /// A name shape under a whole-domain prefix narrows the scanned
    /// key range itself: each shape's first-byte class is contiguous,
    /// so the domain range splits into a position half strictly below
    /// a symbol half, and a shape-constrained scan touches only its
    /// half instead of sweeping the domain and filtering.
    #[dialog_common::test]
    fn it_narrows_domain_ranges_by_name_shape() {
        let manifest = dialog_search_tree::Manifest::default();
        let domain = || Selector::new().the_starting_with("todo.list/");

        let all = selector_range(&domain(), &manifest);
        let positions = selector_range(&domain().with_name_shape(NameShape::Position), &manifest);
        let symbols = selector_range(&domain().with_name_shape(NameShape::Symbol), &manifest);

        assert!(
            all.start() < positions.start() && positions.end() < all.end(),
            "the position half is strictly inside the domain range"
        );
        assert!(
            all.start() < symbols.start() && symbols.end() < all.end(),
            "the symbol half is strictly inside the domain range"
        );
        assert!(
            positions.end() < symbols.start(),
            "the halves are disjoint, positions below symbols"
        );

        // A partial prefix does not end at the name boundary, so the
        // shape cannot extend it: the range stays the prefix range and
        // the shape remains a per-entry filter.
        let partial = Selector::new().the_starting_with("todo.li");
        let partial_shaped = selector_range(
            &partial.clone().with_name_shape(NameShape::Position),
            &manifest,
        );
        assert_eq!(
            selector_range(&partial, &manifest),
            partial_shaped,
            "a mid-domain prefix range is unchanged by a shape"
        );
    }
}

#[cfg(test)]
mod number_band_tests;

#[cfg(test)]
mod number_semantics_tests;

#[cfg(test)]
mod selector_range_tests {
    #![allow(unexpected_cfgs)]
    // The dialog_common::test macro requires async test fns; this pure
    // construction test awaits nothing.
    #![allow(clippy::unused_async)]

    use std::ops::RangeInclusive;

    use std::str::FromStr as _;

    use super::{apply_prefix_bounds, selector_range};
    use crate::selector::Constrained;
    use crate::{
        AttributeKey, Entity, EntityKey, Key, KeyViewConstruct, KeyViewMut as _, Relation,
        Selector, Value, ValueKey,
    };
    use dialog_search_tree::Manifest;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// The range construction `selector_range` replaced: chained
    /// `min()/max().apply_selector(..)` view mutations (each a full key
    /// parse + rebuild), fed through the same `apply_prefix_bounds`. The
    /// rewrite claimed byte-equivalence by construction; this pin holds it
    /// to that claim across the selector shapes and value sizes that pick
    /// different encodings.
    fn legacy_selector_range(
        selector: &Selector<Constrained>,
        manifest: &Manifest,
    ) -> RangeInclusive<Key> {
        if selector.entity().is_some()
            || (selector.entity_prefix().is_some()
                && selector.value().is_none()
                && selector.attribute().is_none()
                && selector.attribute_prefix().is_none()
                && selector.attribute_lower().is_none()
                && selector.attribute_upper().is_none())
        {
            let (start, end) = apply_prefix_bounds(
                <EntityKey<Key> as KeyViewConstruct>::min().apply_selector(selector, manifest),
                <EntityKey<Key> as KeyViewConstruct>::max().apply_selector(selector, manifest),
                selector,
                manifest,
            );
            start.into_key()..=end.into_key()
        } else if selector.value().is_some()
            || selector.value_prefix().is_some()
            || selector.value_lower().is_some()
            || selector.value_upper().is_some()
        {
            let (start, end) = apply_prefix_bounds(
                <ValueKey<Key> as KeyViewConstruct>::min().apply_selector(selector, manifest),
                <ValueKey<Key> as KeyViewConstruct>::max().apply_selector(selector, manifest),
                selector,
                manifest,
            );
            start.into_key()..=end.into_key()
        } else if selector.attribute().is_some()
            || selector.attribute_prefix().is_some()
            || selector.attribute_lower().is_some()
            || selector.attribute_upper().is_some()
        {
            let (start, end) = apply_prefix_bounds(
                <AttributeKey<Key> as KeyViewConstruct>::min().apply_selector(selector, manifest),
                <AttributeKey<Key> as KeyViewConstruct>::max().apply_selector(selector, manifest),
                selector,
                manifest,
            );
            start.into_key()..=end.into_key()
        } else {
            unreachable!("Selector will always have at least one field specified")
        }
    }

    fn entity() -> Entity {
        Entity::from_str("did:key:z6Mk2WiNvjBbuWZ8jYNmFzh4uFyt8iqwpDND6ymg6KnKzchw")
            .expect("valid entity")
    }

    fn attribute() -> Relation {
        Relation::from_str("person/name").expect("valid attribute")
    }

    /// Values chosen to straddle every encoding decision the bound
    /// construction makes: short inline strings, strings AT and just past
    /// the spill threshold (which flip the bound's payload from a full
    /// inline encoding to a prefix + hash), numerics with fixed-width
    /// encodings, negative and fractional floats, booleans, raw bytes, and
    /// an entity-valued reference.
    fn probe_values(manifest: &Manifest) -> Vec<Value> {
        let spill_at = manifest.inline_n as usize;
        vec![
            Value::String("Alice".into()),
            Value::String("x".repeat(spill_at.saturating_sub(1))),
            Value::String("x".repeat(spill_at)),
            Value::String("x".repeat(spill_at + 1)),
            Value::String("x".repeat(spill_at * 2)),
            Value::UnsignedInt(0),
            Value::UnsignedInt(u128::from(u64::MAX)),
            Value::SignedInt(-42),
            Value::Float(-0.5),
            Value::Boolean(true),
            Value::Bytes(vec![0xFF; 9]),
            Value::Entity(entity()),
        ]
    }

    fn selector_matrix(manifest: &Manifest) -> Vec<Selector<Constrained>> {
        let mut matrix: Vec<Selector<Constrained>> = vec![
            Selector::new().of(entity()),
            Selector::new().the(attribute()),
            Selector::new().of(entity()).the(attribute()),
            Selector::new().the_starting_with("person/"),
            Selector::new().of_starting_with("did:key:"),
            Selector::new().is_starting_with("Al"),
            Selector::new().the(attribute()).is_starting_with("Al"),
            Selector::new().is_at_least(Value::UnsignedInt(10)),
            Selector::new().is_at_most(Value::Float(0.0)),
            Selector::new()
                .is_at_least(Value::UnsignedInt(5))
                .is_at_most(Value::UnsignedInt(50)),
            Selector::new().the_at_least("person/b"),
            Selector::new().the_less_than("person/n"),
            Selector::new()
                .of(entity())
                .the_at_least("person/b")
                .the_less_than("person/n"),
            Selector::new()
                .the_starting_with("person/")
                .the_greater_than("person/b")
                .the_at_most("person/n"),
            Selector::new()
                .the_at_least("person/b")
                .is_at_least(Value::UnsignedInt(10)),
        ];
        for value in probe_values(manifest) {
            matrix.push(Selector::new().is(value.clone()));
            matrix.push(Selector::new().the(attribute()).is(value.clone()));
            matrix.push(Selector::new().of(entity()).the(attribute()).is(value));
        }
        matrix
    }

    /// Every selector shape must produce byte-identical ranges through the
    /// direct-parts construction and the legacy view-mutation chain, under
    /// the default manifest and under one with a shifted spill threshold
    /// (which moves the inline-vs-spill decision for the probe values).
    #[dialog_common::test]
    async fn it_builds_ranges_identical_to_the_view_chain() {
        let shifted = dialog_search_tree::Manifest {
            inline_n: 24,
            ..dialog_search_tree::Manifest::default()
        };
        for manifest in [dialog_search_tree::Manifest::default(), shifted] {
            for (at, selector) in selector_matrix(&manifest).into_iter().enumerate() {
                let fast = selector_range(&selector, &manifest);
                let legacy = legacy_selector_range(&selector, &manifest);
                assert_eq!(
                    fast.start().as_ref(),
                    legacy.start().as_ref(),
                    "selector {at}: range START diverged (inline_n {})",
                    manifest.inline_n
                );
                assert_eq!(
                    fast.end().as_ref(),
                    legacy.end().as_ref(),
                    "selector {at}: range END diverged (inline_n {})",
                    manifest.inline_n
                );
            }
        }
    }
}

#[cfg(test)]
mod attribute_range_tests {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{Corpus, CorpusExt, selector_range, spill_cache};
    use crate::selector::Constrained;
    use crate::{ArchiveDelta, Entity, Fact, Instruction, Selector, Value};
    use dialog_search_tree::Manifest;
    use dialog_search_tree::helpers::{Blocks, test_catalog};
    use futures_util::{StreamExt as _, TryStreamExt as _, stream};
    use std::ops::Range;

    /// How many entries a log holds in these tests.
    const ENTRIES: usize = 10_000;

    fn channel() -> Entity {
        "channel:general".parse().expect("an entity")
    }

    /// The attribute of entry `number` of a log: its domain, a `/`, and a
    /// key that orders as the number does.
    fn entry(number: usize) -> String {
        format!("chat.channel.log/n{number:05}")
    }

    fn fact(the: &str, of: &str, is: Value) -> Fact {
        Fact {
            the: the.parse().expect("an attribute"),
            of: of.parse().expect("an entity"),
            is,
            cause: None,
        }
    }

    /// A tree that holds a log of [`ENTRIES`] entries under one channel,
    /// with facts on each side of it: one attribute of the same channel
    /// that sorts below the log's domain and two that sort above it, and
    /// the same log under two other channels, one keyed below this
    /// channel and one above.
    async fn chat() -> anyhow::Result<(Corpus, Blocks)> {
        let store = Blocks::new();
        let mut delta = ArchiveDelta::zero();
        let mut tree = Corpus::empty_in(test_catalog());
        let mut facts = vec![
            fact(
                "chat.box/name",
                "channel:general",
                "below".to_string().into(),
            ),
            fact(
                "chat.channel.zone/n00010",
                "channel:general",
                "above".to_string().into(),
            ),
            fact(
                "chat.channel/name",
                "channel:general",
                "general".to_string().into(),
            ),
        ];
        for number in 0..ENTRIES {
            facts.push(fact(
                &entry(number),
                "channel:general",
                Value::Entity(format!("message:{number}").parse()?),
            ));
        }
        for other in ["channel:a", "channel:random"] {
            for number in [0, 4950, 9999] {
                facts.push(fact(
                    &entry(number),
                    other,
                    Value::Entity(format!("message:{other}:{number}").parse()?),
                ));
            }
        }
        tree.apply(
            &store,
            &mut delta,
            stream::iter(
                facts
                    .into_iter()
                    .map(|fact| Instruction::Assert(fact, crate::Pick::All)),
            ),
        )
        .await?;
        delta.flush_into(&store);
        Ok((tree, store))
    }

    /// The attributes of the rows a selector's scan yields, in scan order.
    async fn scanned(
        tree: &Corpus,
        store: &Blocks,
        selector: Selector<Constrained>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let rows: Vec<Fact> = tree
            .clone()
            .scan_owned(store.clone(), spill_cache(), selector)
            .try_collect()
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.of.to_string(), row.the.to_string()))
            .collect())
    }

    /// How many entries of the tree a selector's scan visits: every entry
    /// its key range holds, matched or not.
    async fn visited(tree: &Corpus, store: &Blocks, selector: &Selector<Constrained>) -> usize {
        let range = selector_range(selector, &Manifest::default());
        tree.stream_range(range, store).count().await
    }

    /// An attribute range under one entity is one range of the entity
    /// index: the scan visits the entries between the bounds and no
    /// other, however many entries the entity holds on each side.
    #[dialog_common::test]
    async fn it_reads_only_the_entries_between_attribute_bounds() -> anyhow::Result<()> {
        let (tree, store) = chat().await?;
        let window = Selector::new()
            .of(channel())
            .the_at_least(entry(4901))
            .the_less_than(entry(5000));

        let rows = scanned(&tree, &store, window.clone()).await?;
        let expected: Vec<(String, String)> = (4901..5000)
            .map(|number| ("channel:general".to_string(), entry(number)))
            .collect();
        assert_eq!(rows, expected, "the 99 entries of the window, in key order");

        // The edge of an exclusive bound sits at the bound, so the scan
        // visits the one entry there and drops it.
        let visits = visited(&tree, &store, &window).await;
        assert_eq!(
            visits, 100,
            "a window of 99 entries visits them and the entry at its exclusive bound, \
             of {ENTRIES} under the entity"
        );

        // The whole log, by its domain, visits every entry of it.
        let log = Selector::new()
            .of(channel())
            .the_starting_with("chat.channel.log/");
        assert_eq!(visited(&tree, &store, &log).await, ENTRIES);
        Ok(())
    }

    /// Each form of a bound admits the entries on its side, and an
    /// exclusive one leaves out the entry at the bound.
    #[dialog_common::test]
    async fn it_admits_each_form_of_an_attribute_bound() -> anyhow::Result<()> {
        let (tree, store) = chat().await?;
        let log = || {
            Selector::new()
                .of(channel())
                .the_starting_with("chat.channel.log/")
        };
        let numbers = |rows: Vec<(String, String)>| -> Vec<String> {
            rows.into_iter().map(|(_, the)| the).collect()
        };
        let entries = |range: Range<usize>| -> Vec<String> { range.map(entry).collect() };

        let at_least = scanned(&tree, &store, log().the_at_least(entry(9997))).await?;
        assert_eq!(numbers(at_least), entries(9997..ENTRIES));
        let greater = scanned(&tree, &store, log().the_greater_than(entry(9997))).await?;
        assert_eq!(numbers(greater), entries(9998..ENTRIES));
        let at_most = scanned(&tree, &store, log().the_at_most(entry(2))).await?;
        assert_eq!(numbers(at_most), entries(0..3));
        let less = scanned(&tree, &store, log().the_less_than(entry(2))).await?;
        assert_eq!(numbers(less), entries(0..2));

        // A bound that names no stored attribute bounds just the same.
        let between = scanned(
            &tree,
            &store,
            Selector::new()
                .of(channel())
                .the_greater_than("chat.channel.log/n00004x")
                .the_less_than("chat.channel.log/n00007x"),
        )
        .await?;
        assert_eq!(numbers(between), entries(5..8));

        // Bounds that cross admit nothing.
        let crossed = scanned(
            &tree,
            &store,
            Selector::new()
                .of(channel())
                .the_at_least(entry(50))
                .the_less_than(entry(10)),
        )
        .await?;
        assert!(crossed.is_empty());
        Ok(())
    }

    /// With no entity, an attribute range narrows the attribute index: it
    /// reads the facts of every entity under the attributes in the range,
    /// and the rows equal a filter over the whole index.
    #[dialog_common::test]
    async fn it_narrows_the_attribute_index_with_no_entity() -> anyhow::Result<()> {
        let (tree, store) = chat().await?;
        let window = Selector::new()
            .the_at_least(entry(4950))
            .the_at_most(entry(4951));
        let mut rows = scanned(&tree, &store, window.clone()).await?;
        rows.sort();

        let everything = Selector::new().the_starting_with("chat.");
        let mut filtered: Vec<(String, String)> = scanned(&tree, &store, everything)
            .await?
            .into_iter()
            .filter(|(_, the)| *the >= entry(4950) && *the <= entry(4951))
            .collect();
        filtered.sort();
        assert_eq!(rows, filtered);
        assert_eq!(
            rows,
            vec![
                ("channel:a".to_string(), entry(4950)),
                ("channel:general".to_string(), entry(4950)),
                ("channel:general".to_string(), entry(4951)),
                ("channel:random".to_string(), entry(4950)),
            ]
        );
        assert_eq!(
            visited(&tree, &store, &window).await,
            4,
            "the range holds the facts under the two attributes alone"
        );
        Ok(())
    }

    /// A bound beside a prefix reads the part of the prefix that the bound
    /// admits. Beside an exact attribute the bound only filters.
    #[dialog_common::test]
    async fn it_composes_an_attribute_bound_with_a_prefix_and_an_exact_attribute()
    -> anyhow::Result<()> {
        let (tree, store) = chat().await?;

        // The bound alone reaches past the log's domain, into each domain
        // above it; the prefix keeps the read inside the log.
        let from = Selector::new().of(channel()).the_at_least(entry(9998));
        let open = scanned(&tree, &store, from.clone()).await?;
        assert_eq!(
            open.iter().map(|(_, the)| the.as_str()).collect::<Vec<_>>(),
            vec![
                entry(9998).as_str(),
                entry(9999).as_str(),
                "chat.channel.zone/n00010",
                "chat.channel/name",
            ]
        );
        let inside = from.the_starting_with("chat.channel.log/");
        let closed = scanned(&tree, &store, inside.clone()).await?;
        assert_eq!(
            closed
                .iter()
                .map(|(_, the)| the.clone())
                .collect::<Vec<_>>(),
            vec![entry(9998), entry(9999)]
        );
        assert_eq!(visited(&tree, &store, &inside).await, 2);

        // A bound that a prefix already implies leaves the prefix's range.
        let wide = Selector::new()
            .of(channel())
            .the_starting_with("chat.channel.log/")
            .the_at_least("chat.");
        assert_eq!(visited(&tree, &store, &wide).await, ENTRIES);

        // An exact attribute decides the key range, and a bound beside it
        // still filters: the attribute shows when the bounds admit it.
        let exact = Selector::new().of(channel()).the(entry(7).parse()?);
        let inside = exact.clone().the_at_least(entry(5));
        assert_eq!(visited(&tree, &store, &inside).await, 1);
        assert_eq!(
            scanned(&tree, &store, inside).await?,
            vec![("channel:general".to_string(), entry(7))]
        );
        let outside = exact.the_at_least(entry(5000));
        assert_eq!(visited(&tree, &store, &outside).await, 1);
        assert!(scanned(&tree, &store, outside).await?.is_empty());
        Ok(())
    }

    /// Selectors that differ in a bound are different selectors, and the
    /// ranges they demand differ, so a reader of one window is no reader
    /// of another.
    #[dialog_common::test]
    fn it_tells_selectors_apart_by_their_attribute_bounds() {
        let manifest = Manifest::default();
        let window = |lower: usize, upper: usize| {
            Selector::new()
                .of(channel())
                .the_at_least(entry(lower))
                .the_less_than(entry(upper))
        };
        assert_eq!(window(10, 20), window(10, 20));
        assert_ne!(window(10, 20), window(10, 21));
        assert_ne!(window(10, 20), window(11, 20));
        assert_ne!(
            Selector::new().of(channel()).the_at_most(entry(20)),
            Selector::new().of(channel()).the_less_than(entry(20)),
            "an inclusive bound is not its exclusive form"
        );

        let below = selector_range(&window(10, 20), &manifest);
        let above = selector_range(&window(30, 40), &manifest);
        assert!(
            below.end() < above.start(),
            "two windows apart demand disjoint ranges"
        );

        let cells = window(10, 20).cells().expect("bounded cells");
        assert_eq!(cells, window(10, 20), "the cells keep the attribute bounds");
    }

    /// The rows a selector's reverse scan yields, in scan order.
    async fn scanned_in_reverse(
        tree: &Corpus,
        store: &Blocks,
        selector: Selector<Constrained>,
    ) -> anyhow::Result<Vec<Fact>> {
        let views: Vec<_> = tree
            .clone()
            .scan_reverse(store.clone(), spill_cache(), selector)
            .try_collect()
            .await?;
        Ok(views
            .into_iter()
            .map(|view| view.to_owned())
            .collect::<Result<_, _>>()?)
    }

    /// A reverse scan of a selector yields the rows of its forward scan, in
    /// reverse order, for each index and each kind of bound on the key of a
    /// collection: inclusive, exclusive, and open, on each side, beside a
    /// prefix and beside the shape of a name.
    #[dialog_common::test]
    async fn it_scans_each_selector_in_reverse_as_a_reversed_forward_scan() -> anyhow::Result<()> {
        let (tree, store) = chat().await?;
        let log = || {
            Selector::new()
                .of(channel())
                .the_starting_with("chat.channel.log/")
        };
        let selectors = vec![
            log(),
            log().the_at_least(entry(9997)),
            log().the_greater_than(entry(9997)),
            log().the_at_most(entry(2)),
            log().the_less_than(entry(2)),
            log().the_at_least(entry(4901)).the_less_than(entry(5000)),
            log().the_greater_than(entry(4901)).the_at_most(entry(5000)),
            log()
                .the_greater_than("chat.channel.log/n00004x")
                .the_less_than("chat.channel.log/n00007x"),
            log().the_at_least(entry(50)).the_less_than(entry(10)),
            Selector::new()
                .of(channel())
                .with_domain(&"chat.channel.log".parse()?)
                .with_name_shape(crate::NameShape::Symbol)
                .the_less_than(entry(300)),
            Selector::new().of(channel()),
            Selector::new()
                .the_at_least(entry(4950))
                .the_at_most(entry(4951)),
            Selector::new().the_starting_with("chat.channel.zone/"),
            Selector::new().is(Value::Entity("message:4950".parse()?)),
            Selector::new().of_starting_with("channel:"),
        ];
        for selector in selectors {
            let mut forward: Vec<Fact> = tree
                .clone()
                .scan_owned(store.clone(), spill_cache(), selector.clone())
                .try_collect()
                .await?;
            forward.reverse();
            let reverse = scanned_in_reverse(&tree, &store, selector.clone()).await?;
            assert_eq!(
                reverse, forward,
                "the reverse scan of {selector:?} is the forward scan reversed"
            );
        }
        Ok(())
    }

    /// A reverse read of the greatest entries of a log of 100,000 entries
    /// reads about the blocks of one path of the tree, as a read of a log of
    /// 1,000 does: it reads no leaf beside the ones that hold its rows. A
    /// forward scan of the same log reads each of its leaves.
    #[dialog_common::test]
    async fn it_reads_the_greatest_entries_of_a_long_log_from_one_path() -> anyhow::Result<()> {
        use crate::helpers::CountingBlocks;

        async fn built(entries: usize) -> anyhow::Result<(Corpus, CountingBlocks)> {
            let store = CountingBlocks::new();
            let mut delta = ArchiveDelta::zero();
            let mut tree = Corpus::empty_in(test_catalog());
            tree.apply(
                &store,
                &mut delta,
                stream::iter((0..entries).map(|number| {
                    Instruction::Assert(
                        fact(
                            &format!("chat.channel.log/n{number:06}"),
                            "channel:general",
                            Value::Entity(format!("message:{number}").parse().expect("an entity")),
                        ),
                        crate::Pick::All,
                    )
                })),
            )
            .await?;
            store.flush(&mut delta);
            Ok((tree, store))
        }
        // A fresh handle, so that its node cache is cold and each node a
        // read touches reaches the store.
        let cold = |tree: &Corpus| Corpus::at(test_catalog(), tree.root().clone());

        let log = Selector::new()
            .of(channel())
            .the_starting_with("chat.channel.log/");
        let mut reads = Vec::new();
        for entries in [1_000, 100_000] {
            let (tree, store) = built(entries).await?;
            store.reset();
            let rows: Vec<_> = cold(&tree)
                .scan_reverse(store.clone(), spill_cache(), log.clone())
                .take(50)
                .try_collect()
                .await?;
            let keys: Vec<String> = rows
                .iter()
                .map(|row| row.to_owned().map(|row| row.the.to_string()))
                .collect::<Result<_, _>>()?;
            let expected: Vec<String> = ((entries - 50)..entries)
                .rev()
                .map(|number| format!("chat.channel.log/n{number:06}"))
                .collect();
            assert_eq!(keys, expected, "the 50 greatest entries of {entries}");
            reads.push(store.reads());
            println!(
                "the 50 greatest entries of {entries} read {} blocks",
                store.reads()
            );

            if entries == 100_000 {
                store.reset();
                let all = cold(&tree)
                    .scan(store.clone(), spill_cache(), log.clone())
                    .count()
                    .await;
                assert_eq!(all, entries);
                println!("a forward scan of {entries} reads {} blocks", store.reads());
                assert!(
                    store.reads() > 20 * reads[1],
                    "a forward scan of the log reads each of its leaves: {} blocks against {}",
                    store.reads(),
                    reads[1]
                );
            }
        }
        let [small, large] = reads[..] else {
            unreachable!("two reads")
        };
        assert!(
            large <= small + 3,
            "the read of 100,000 entries reads {large} blocks, and of 1,000 reads {small}"
        );
        assert!(
            large <= 8,
            "the read reads one path and a leaf or two: {large} blocks"
        );
        Ok(())
    }

    /// A reverse read of the 50 greatest entries of a log visits about 50
    /// entries of the tree, for a log of 100,000 entries as for one of 1,000.
    /// A counter on the walk counts each entry that it gives the scan: the
    /// rows that the read takes, and the few that the fetch of spilled values
    /// reads ahead. The walk is the one of `scan_reverse`, with the same
    /// range, match, and stages.
    #[dialog_common::test]
    async fn it_visits_about_the_count_of_entries_for_the_greatest_entries() -> anyhow::Result<()> {
        use super::{SPILL_LOOKAHEAD, scanned_views};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let log = Selector::new()
            .of(channel())
            .the_starting_with("chat.channel.log/");
        let mut visits = Vec::new();
        for entries in [1_000, 100_000] {
            let store = Blocks::new();
            let mut delta = ArchiveDelta::zero();
            let mut tree = Corpus::empty_in(test_catalog());
            tree.apply(
                &store,
                &mut delta,
                stream::iter((0..entries).map(|number| {
                    Instruction::Assert(
                        fact(
                            &format!("chat.channel.log/n{number:06}"),
                            "channel:general",
                            Value::Entity(format!("message:{number}").parse().expect("an entity")),
                        ),
                        crate::Pick::All,
                    )
                })),
            )
            .await?;
            delta.flush_into(&store);

            let manifest = tree.manifest(&store).await?;
            let selector = log.clone().for_manifest(&manifest);
            let range = selector_range(&selector, &manifest);
            let cache = spill_cache();
            let visited = AtomicUsize::new(0);
            let walk = tree
                .stream_range_handles_reverse(range, &store)
                .inspect(|_| {
                    visited.fetch_add(1, Ordering::SeqCst);
                });
            let archive = tree.catalog().archive();
            let rows: Vec<_> = scanned_views(walk, &selector, &manifest, &store, &archive, &cache)
                .take(50)
                .try_collect()
                .await?;
            assert_eq!(rows.iter().flatten().count(), 50);
            let visited = visited.load(Ordering::SeqCst);
            println!("the 50 greatest entries of {entries} visit {visited} entries");
            assert!(
                visited <= 50 + SPILL_LOOKAHEAD,
                "the read of 50 of {entries} entries visits {visited}"
            );
            visits.push(visited);
        }
        assert_eq!(
            visits[0], visits[1],
            "the visits follow the count, and not the length of the log"
        );
        Ok(())
    }
}

#[cfg(test)]
mod value_prefix_tests {
    //! A prefix of the value beside a constraint on the attribute. The value
    //! index sorts each key by its value, then its attribute, then its
    //! entity, so a value prefix with no entity reads a range of the value
    //! index. A value prefix under one entity reads a range of the entity
    //! index. These tests hold each such scan to the rows that a filter over
    //! the whole index gives.

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{Corpus, CorpusExt, spill_cache};
    use crate::selector::Constrained;
    use crate::{ArchiveDelta, Fact, Instruction, Selector, Value};
    use dialog_search_tree::helpers::{Blocks, test_catalog};
    use futures_util::{TryStreamExt as _, stream};
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn fact(the: &str, of: &str, is: Value) -> Fact {
        Fact {
            the: the.parse().expect("an attribute"),
            of: of.parse().expect("an entity"),
            is,
            cause: None,
        }
    }

    async fn tree_of(facts: Vec<Fact>) -> anyhow::Result<(Corpus, Blocks)> {
        let store = Blocks::new();
        let mut delta = ArchiveDelta::zero();
        let mut tree = Corpus::empty_in(test_catalog());
        tree.apply(
            &store,
            &mut delta,
            stream::iter(
                facts
                    .into_iter()
                    .map(|fact| Instruction::Assert(fact, crate::Pick::All)),
            ),
        )
        .await?;
        delta.flush_into(&store);
        Ok((tree, store))
    }

    /// A row as its entity, its attribute, and its value. A long string
    /// shows its first bytes and its length, so that a failure stays short.
    type Row = (String, String, String);

    fn row(artifact: &Fact) -> Row {
        let is = match &artifact.is {
            Value::String(text) if text.len() > 100 => {
                format!("String({:?}, {} bytes)", &text[..90], text.len())
            }
            other => format!("{other:?}"),
        };
        (artifact.of.to_string(), artifact.the.to_string(), is)
    }

    fn rows(artifacts: &[Fact]) -> Vec<Row> {
        let mut rows: Vec<Row> = artifacts.iter().map(row).collect();
        rows.sort();
        rows
    }

    /// The rows a scan yields.
    async fn scanned(
        tree: &Corpus,
        store: &Blocks,
        selector: Selector<Constrained>,
    ) -> anyhow::Result<Vec<Fact>> {
        let artifacts: Vec<Fact> = tree
            .clone()
            .scan_owned(store.clone(), spill_cache(), selector)
            .try_collect()
            .await?;
        Ok(artifacts)
    }

    /// The rows a reverse scan yields.
    async fn scanned_in_reverse(
        tree: &Corpus,
        store: &Blocks,
        selector: Selector<Constrained>,
    ) -> anyhow::Result<Vec<Fact>> {
        let views: Vec<_> = tree
            .clone()
            .scan_reverse(store.clone(), spill_cache(), selector)
            .try_collect()
            .await?;
        Ok(views
            .into_iter()
            .map(|view| view.to_owned())
            .collect::<Result<Vec<Fact>, _>>()?)
    }

    /// Each row of the tree: a scan of the whole attribute index.
    async fn everything(tree: &Corpus, store: &Blocks) -> anyhow::Result<Vec<Fact>> {
        scanned(tree, store, Selector::new().the_starting_with("")).await
    }

    fn starts_with(row: &Fact, prefix: &str) -> bool {
        matches!(&row.is, Value::String(text) if text.starts_with(prefix))
    }

    /// The facts of the review of the bounds on an attribute: values that
    /// share the prefix `Al` under several attributes. A value that goes on
    /// with a byte below the first byte of the attribute, such as `Alice`
    /// beside `x/b`, sorts below the prefix with the attribute after it.
    fn names() -> Vec<Fact> {
        vec![
            fact("x/a", "e:1", "Al".to_string().into()),
            fact("x/b", "e:1", "Alice".to_string().into()),
            fact("x/b", "e:2", "Albert".to_string().into()),
            fact("x/c", "e:3", "Alzzzz".to_string().into()),
            fact("x/c", "e:4", "Bob".to_string().into()),
            fact("y/a", "e:5", "Alice".to_string().into()),
        ]
    }

    /// A value prefix beside an exact attribute, a prefix of the attribute,
    /// or a bound on the attribute finds each row whose value starts with
    /// the prefix, and none other.
    #[dialog_common::test]
    async fn it_finds_each_row_for_a_value_prefix_beside_an_attribute_constraint()
    -> anyhow::Result<()> {
        let (tree, store) = tree_of(names()).await?;
        let all = everything(&tree, &store).await?;
        let expect = |admit: &dyn Fn(&str) -> bool| -> Vec<Row> {
            let kept: Vec<Fact> = all
                .iter()
                .filter(|row| starts_with(row, "Al") && admit(&row.the.to_string()))
                .cloned()
                .collect();
            rows(&kept)
        };

        let exact = Selector::new().the("x/b".parse()?).is_starting_with("Al");
        let found = rows(&scanned(&tree, &store, exact).await?);
        assert_eq!(found, expect(&|the| the == "x/b"));
        assert_eq!(found.len(), 2, "Alice and Albert under x/b");

        let prefix = Selector::new()
            .the_starting_with("x/")
            .is_starting_with("Al");
        let found = rows(&scanned(&tree, &store, prefix).await?);
        assert_eq!(found, expect(&|the| the.starts_with("x/")));
        assert_eq!(
            found.len(),
            4,
            "each value of the domain x that starts with Al"
        );

        let bound = Selector::new()
            .the_at_least("x/a")
            .the_less_than("y/")
            .is_starting_with("Al");
        let found = rows(&scanned(&tree, &store, bound).await?);
        assert_eq!(
            found,
            expect(&|the| ("x/a"..="y/").contains(&the) && the != "y/")
        );
        assert_eq!(found.len(), 4);

        let lower = Selector::new().the_at_least("x/b").is_starting_with("Al");
        let found = rows(&scanned(&tree, &store, lower).await?);
        assert_eq!(found, expect(&|the| the >= "x/b"));
        assert_eq!(
            found.len(),
            4,
            "Alice and Albert under x/b, Alzzzz, and Alice under y/a"
        );
        Ok(())
    }

    /// The letters of the random values and prefixes. A value may also
    /// hold a zero byte, which a key escapes.
    const LETTERS: &[char] = &['A', 'l', 'i', 'c', 'e', 'b', 'x', 'z'];

    const ATTRIBUTES: &[&str] = &["x/a", "x/b", "x/c", "x.y/b", "xa/b", "y/a", "a/z"];

    const ENTITIES: &[&str] = &["e:1", "e:2", "e:3", "f:1", "f:2"];

    fn text(rng: &mut ChaCha8Rng, longest: usize) -> String {
        let length = rng.gen_range(0..=longest);
        (0..length)
            .map(|_| LETTERS[rng.gen_range(0..LETTERS.len())])
            .collect()
    }

    /// A random value. Most are short strings. Some are strings that hold a
    /// zero byte, some are strings long enough to spill out of the key, so
    /// that the key holds only their first bytes, and some are not strings.
    fn value(rng: &mut ChaCha8Rng) -> Value {
        match rng.gen_range(0..20) {
            0 => Value::UnsignedInt(rng.gen_range(0..100)),
            1 => Value::Symbol("Al/ice".parse().expect("a symbol")),
            2 | 3 => {
                let head = text(rng, 3);
                let tail = LETTERS[rng.gen_range(0..LETTERS.len())];
                Value::String(format!("{head}{}", tail.to_string().repeat(4_200)))
            }
            4 => Value::String(format!("{}\0{}", text(rng, 2), text(rng, 2))),
            _ => Value::String(text(rng, 5)),
        }
    }

    /// A random prefix of a value. Some are longer than the part of a
    /// spilled value that its key holds.
    fn value_prefix(rng: &mut ChaCha8Rng, facts: &[Fact]) -> String {
        if rng.gen_range(0..8) == 0 {
            let long = facts.iter().find_map(|fact| match &fact.is {
                Value::String(text) if text.len() > 4_000 => Some(text.clone()),
                _ => None,
            });
            if let Some(long) = long {
                let length = rng.gen_range(60..90);
                return long[..length].to_string();
            }
        }
        text(rng, 3)
    }

    /// A random prefix of a random attribute, which may be empty or whole.
    fn attribute_prefix(rng: &mut ChaCha8Rng) -> String {
        let attribute = ATTRIBUTES[rng.gen_range(0..ATTRIBUTES.len())];
        attribute[..rng.gen_range(0..=attribute.len())].to_string()
    }

    /// One of the three constraints on the attribute that a selector with a
    /// value prefix can hold.
    #[derive(Debug, Clone)]
    enum Attribute {
        Exact(String),
        Prefix(String),
        Bounds {
            lower: Option<(String, bool)>,
            upper: Option<(String, bool)>,
        },
    }

    impl Attribute {
        fn random(rng: &mut ChaCha8Rng, form: usize) -> Attribute {
            match form {
                0 => Attribute::Exact(ATTRIBUTES[rng.gen_range(0..ATTRIBUTES.len())].to_string()),
                1 => Attribute::Prefix(attribute_prefix(rng)),
                _ => {
                    let bound = |rng: &mut ChaCha8Rng| {
                        rng.gen_bool(0.7)
                            .then(|| (attribute_prefix(rng), rng.gen_bool(0.5)))
                    };
                    let (lower, upper) = loop {
                        let lower = bound(rng);
                        let upper = bound(rng);
                        if lower.is_some() || upper.is_some() {
                            break (lower, upper);
                        }
                    };
                    Attribute::Bounds { lower, upper }
                }
            }
        }

        fn admits(&self, the: &str) -> bool {
            match self {
                Attribute::Exact(attribute) => the == attribute,
                Attribute::Prefix(prefix) => the.starts_with(prefix.as_str()),
                Attribute::Bounds { lower, upper } => {
                    lower.as_ref().is_none_or(|(bound, inclusive)| {
                        the > bound.as_str() || (*inclusive && the == bound)
                    }) && upper.as_ref().is_none_or(|(bound, inclusive)| {
                        the < bound.as_str() || (*inclusive && the == bound)
                    })
                }
            }
        }
    }

    /// The constraint on the entity: none, which reads the value index, one
    /// entity, which reads the entity index, or a prefix of the entity,
    /// which reads the value index.
    #[derive(Debug, Clone)]
    enum Of {
        Any,
        Exact(String),
        Prefix(String),
    }

    impl Of {
        fn admits(&self, of: &str) -> bool {
            match self {
                Of::Any => true,
                Of::Exact(entity) => of == entity,
                Of::Prefix(prefix) => of.starts_with(prefix.as_str()),
            }
        }
    }

    fn selector(
        of: &Of,
        attribute: &Attribute,
        prefix: &str,
    ) -> anyhow::Result<Selector<Constrained>> {
        let selector = Selector::new().is_starting_with(prefix);
        let selector = match of {
            Of::Any => selector,
            Of::Exact(entity) => selector.of(entity.parse()?),
            Of::Prefix(prefix) => selector.of_starting_with(prefix.clone()),
        };
        Ok(match attribute {
            Attribute::Exact(attribute) => selector.the(attribute.parse()?),
            Attribute::Prefix(prefix) => selector.the_starting_with(prefix.clone()),
            Attribute::Bounds { lower, upper } => {
                let selector = match lower {
                    Some((bound, true)) => selector.the_at_least(bound.clone()),
                    Some((bound, false)) => selector.the_greater_than(bound.clone()),
                    None => selector,
                };
                match upper {
                    Some((bound, true)) => selector.the_at_most(bound.clone()),
                    Some((bound, false)) => selector.the_less_than(bound.clone()),
                    None => selector,
                }
            }
        })
    }

    /// For random facts and random prefixes of the value, a scan with the
    /// prefix beside an exact attribute, a prefix of the attribute, or bounds
    /// on the attribute gives the rows of a filter over the whole index. Each
    /// shape runs with no entity and under an entity prefix, which read the
    /// value index, and under one entity, which reads the entity index. A
    /// reverse scan gives the same rows.
    #[dialog_common::test]
    async fn it_scans_a_value_prefix_beside_an_attribute_as_a_filter_does() -> anyhow::Result<()> {
        // The rows that the selectors of each shape admit, by the form of
        // the entity and then the form of the attribute, and the rows that
        // the prefixes longer than the key part of a spilled value admit.
        let mut admitted = [[0usize; 3]; 3];
        let mut admitted_by_long_prefixes = 0usize;
        for seed in 0..64u64 {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let facts: Vec<Fact> = (0..rng.gen_range(20..80))
                .map(|_| {
                    fact(
                        ATTRIBUTES[rng.gen_range(0..ATTRIBUTES.len())],
                        ENTITIES[rng.gen_range(0..ENTITIES.len())],
                        value(&mut rng),
                    )
                })
                .collect();
            let (tree, store) = tree_of(facts.clone()).await?;
            let all = everything(&tree, &store).await?;
            for round in 0..60usize {
                let attribute_form = round % 3;
                let entity_form = (round / 3) % 3;
                let attribute = Attribute::random(&mut rng, attribute_form);
                let of = match entity_form {
                    0 => Of::Any,
                    1 => Of::Exact(ENTITIES[rng.gen_range(0..ENTITIES.len())].to_string()),
                    _ => {
                        let entity = ENTITIES[rng.gen_range(0..ENTITIES.len())];
                        Of::Prefix(entity[..rng.gen_range(0..=entity.len())].to_string())
                    }
                };
                let prefix = value_prefix(&mut rng, &facts);
                let selector = selector(&of, &attribute, &prefix)?;
                let kept: Vec<Fact> = all
                    .iter()
                    .filter(|row| {
                        starts_with(row, &prefix)
                            && attribute.admits(&row.the.to_string())
                            && of.admits(&row.of.to_string())
                    })
                    .cloned()
                    .collect();
                let expected = rows(&kept);
                let forward = rows(&scanned(&tree, &store, selector.clone()).await?);
                assert_eq!(
                    forward, expected,
                    "seed {seed}, round {round}: {of:?} {attribute:?} {prefix:?}"
                );
                let reverse = rows(&scanned_in_reverse(&tree, &store, selector).await?);
                assert_eq!(
                    reverse, expected,
                    "reverse, seed {seed}, round {round}: {of:?} {attribute:?} {prefix:?}"
                );
                admitted[entity_form][attribute_form] += expected.len();
                if prefix.len() > 64 {
                    admitted_by_long_prefixes += expected.len();
                }
            }
        }
        assert!(
            admitted.iter().flatten().all(|rows| *rows > 0),
            "the selectors of each shape admit rows: {admitted:?}"
        );
        assert!(
            admitted_by_long_prefixes > 0,
            "a prefix longer than the key part of a spilled value admits rows"
        );
        Ok(())
    }
}

#[cfg(test)]
mod corrupt_row_tests {
    use dialog_search_tree::helpers::test_catalog;
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{Corpus, CorpusExt, spill_cache};
    use crate::key::varkey::{KeyParts, ValuePayload, build_key};
    use crate::{
        ATTRIBUTE_KEY_TAG, Datum, ENTITY_KEY_TAG, Fact, FactViewStream as _, Instruction, Key,
        Selector, State, VALUE_KEY_TAG, Value, ValueDataType, encode_value_owned,
    };

    use crate::ArchiveDelta;
    use dialog_search_tree::helpers::Blocks;

    use futures_util::{TryStreamExt, stream};

    /// Manufactures a tree whose persisted nodes carry both valid facts and
    /// crafted entries with invalid entities, written through the TREE layer —
    /// below the facts layer's validating constructors — exactly as a
    /// buggy or hostile writer would produce them. The entities cover both
    /// failure classes: a string that is no URI at all, and one that parses
    /// but is not its own canonical rendering.
    async fn poisoned_tree() -> anyhow::Result<(Corpus, Blocks)> {
        let store = Blocks::new();
        let mut delta = ArchiveDelta::zero();
        let mut tree = Corpus::empty_in(test_catalog());

        let valid: Vec<Instruction> = ["user:alice", "user:bob", "user:carol"]
            .into_iter()
            .map(|of| {
                Instruction::Assert(
                    Fact {
                        the: "user/name".parse().expect("attribute"),
                        of: of.parse().expect("entity"),
                        is: Value::String(of.to_string()),
                        cause: None,
                    },
                    crate::Pick::All,
                )
            })
            .collect();
        tree.apply(&store, &mut delta, stream::iter(valid)).await?;

        let corrupt = [
            b"not a uri at all".to_vec(),
            b"HTTPS://Example.com/".to_vec(),
        ];
        let mut entries = Vec::new();
        for entity in corrupt {
            // All three orderings, as a real writer would index a fact, so
            // the poison is visible whichever index a selector dispatches to.
            for tag in [ENTITY_KEY_TAG, ATTRIBUTE_KEY_TAG, VALUE_KEY_TAG] {
                let parts = KeyParts {
                    tag,
                    entity: entity.clone(),
                    attribute: b"user/name".to_vec(),
                    value_type: ValueDataType::String,
                    value: ValuePayload::Inline(encode_value_owned(&Value::String("evil".into()))),
                    version: None,
                };
                entries.push((
                    Key::from(build_key(&parts)),
                    State::Added(Datum {
                        cause: None,
                        blob: None,
                        version: None,
                        collapsed: vec![],
                        supersedes: vec![],
                        retraction: false,
                    }),
                ));
            }
        }
        tree.record(&store, &mut delta, entries.into()).await?;

        delta.flush_into(&store);
        Ok((tree, store))
    }

    /// The owned scan silently skips manufactured corrupt rows: every valid
    /// fact comes back, no row errors, and the corrupt entries are absent.
    #[dialog_common::test]
    async fn it_skips_corrupt_rows_in_owned_scans() -> anyhow::Result<()> {
        let (tree, store) = poisoned_tree().await?;
        let selector = Selector::new().the("user/name".parse()?);
        let rows: Vec<Fact> = tree
            .scan_owned(store, spill_cache(), selector)
            .try_collect()
            .await?;
        let mut entities: Vec<String> = rows.iter().map(|row| row.of.to_string()).collect();
        entities.sort();
        assert_eq!(entities, ["user:alice", "user:bob", "user:carol"]);
        Ok(())
    }

    /// The view scan yields every row without paying validation (corrupt
    /// included), and `.owned()` — the query engine's materialization — is
    /// where the corrupt ones drop out.
    #[dialog_common::test]
    async fn it_skips_corrupt_rows_when_materializing_views() -> anyhow::Result<()> {
        let (tree, store) = poisoned_tree().await?;
        let selector = Selector::new().the("user/name".parse()?);

        let views: Vec<_> = tree
            .clone()
            .scan(store.clone(), spill_cache(), selector.clone())
            .try_collect()
            .await?;
        assert_eq!(views.len(), 5, "corrupt rows still scan as views");

        let rows: Vec<Fact> = tree
            .scan(store, spill_cache(), selector)
            .owned()
            .try_collect()
            .await?;
        assert_eq!(rows.len(), 3, "materialization drops the corrupt rows");
        Ok(())
    }
}
