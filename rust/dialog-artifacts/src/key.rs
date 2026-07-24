//! Key structures for indexing artifacts in prolly trees.
//!
//! This module provides the key layout and manipulation utilities for creating
//! efficient indexes over semantic triples. Keys are structured to enable fast
//! range queries over different access patterns (by entity, attribute, or value).

use std::ops::{Deref, DerefMut};

use dialog_common::ConditionalSync;
use dialog_search_tree::{
    Component as TreeComponent, DialogSearchTreeError, Key as TreeKey, Manifest, Schema,
};
use serde::de::DeserializeOwned;
use std::fmt::Debug;

/// A key used to reference values in a search tree index.
///
/// Hosted here since the search tree itself keys on raw byte arrays; this
/// trait is the artifact-level abstraction over the typed key views.
pub trait KeyType:
    Debug + TryFrom<Vec<u8>> + ConditionalSync + Clone + PartialEq + Ord + Serialize + DeserializeOwned
{
    /// Get the raw bytes of this [`KeyType`]
    fn bytes(&self) -> &[u8];
}

/// A value that may be stored against a key in an artifact index.
pub trait ValueType: Debug + ConditionalSync + Clone + Serialize + DeserializeOwned {}
use serde::{Deserialize, Serialize};

mod attribute;
pub use attribute::*;

mod entity;
pub use entity::*;

mod value;
pub use value::*;

mod blob;
pub use blob::*;

mod history;
pub use history::*;

mod part;
pub use part::*;

pub(crate) mod varkey;

/// Tag byte of the history index (the fourth index ordering): the region of
/// the artifact tree holding per-instruction claim-lineage records (see
/// [`history`]). Allocated below the blob index's tag `4`.
pub const HISTORY_KEY_TAG: u8 = 3;

use crate::{
    ArtifactSelector, Value, ValueDataType, encode_value_owned, key::varkey::ValuePayload,
    selector::Constrained,
};

/// Helper macro for creating mutable slices from byte arrays at compile time.
///
/// Still used by the padded `[u8; N]` byte representations that [`Entity`] and
/// [`Uri`](crate::Uri) carry alongside their string form.
macro_rules! mutable_slice {
    ( $array:expr, $index:expr, $run:expr ) => {{
        const START: usize = $index;
        const END: usize = $index + $run;
        &mut $array[START..END]
    }};
}

pub(crate) use mutable_slice;

/// Length of the key tag field in bytes
pub(crate) const TAG_LENGTH: usize = 1;
/// Length of the padded entity byte representation carried by [`Entity`].
///
/// Keys no longer pad entities (they are lossless and variable-length); this
/// width only sizes the legacy `[u8; ENTITY_LENGTH]` companion buffer.
pub(crate) const ENTITY_LENGTH: usize = 64;
/// Maximum attribute length in bytes (still capped for the dictionary column
/// and for filler-based range bounds).
pub(crate) const ATTRIBUTE_LENGTH: usize = 64;
/// Length of the value data type field in key bytes
pub(crate) const VALUE_DATA_TYPE_LENGTH: usize = 1;

/// Decides how a [`Value`] is carried in a key: a small value is encoded inline
/// in its order-preserving form; a value whose encoded form exceeds
/// `manifest.inline_n` spills. Its value slot carries the order-preserving
/// encoding of the first `manifest.spill_prefix` RAW bytes (a byte-prefix of
/// the full inline encoding, since escaping is per-byte, so spilled values sort
/// INTO their type band next to inline ones) and the key gains a trailing
/// 32-byte whole-value hash, which keeps distinct large values distinct
/// (cardinality-many) and addresses the archive block. This is the single place
/// the inline-vs-spill decision is made, so the fact-building path and the
/// selector path agree.
///
/// Both constants come from the `manifest` of the tree the key belongs to,
/// never from a process-wide default: they are properties of the tree being
/// written, and a reader that used different ones would encode its probe
/// differently from the stored fact and miss it.
pub(crate) fn value_payload(value: &Value, manifest: &Manifest) -> ValuePayload {
    let encoded = encode_value_owned(value);
    if encoded.len() <= manifest.inline_n as usize {
        ValuePayload::Inline(encoded)
    } else {
        let raw = value.to_bytes();
        let take = (manifest.spill_prefix as usize).min(raw.len());
        let mut prefix = Vec::new();
        crate::encode_bytes(&raw[..take], &mut prefix);
        ValuePayload::Spilled {
            prefix,
            hash: value.to_reference().to_vec(),
        }
    }
}

/// The default key format: the [`Manifest`] a tree with no manifest of its own
/// (an empty tree) would be created under.
///
/// This is a *fallback*, not the answer. The format that governs a given tree
/// is the manifest of that tree, recovered with
/// [`PersistentTree::manifest`](dialog_search_tree::PersistentTree::manifest)
/// and threaded to the key builders. Reach for this default only where no tree
/// can be in scope, and say why at the call site.
pub(crate) fn default_manifest() -> Manifest {
    Manifest::default()
}

/// Whether `value` spills under `manifest` (its encoded form exceeds
/// `inline_n`, so the key carries a prefix plus the whole-value hash and the
/// value's raw bytes must be archived as a block). The single source of truth
/// the payload builder and the key builder share.
pub(crate) fn value_spills(value: &Value, manifest: &Manifest) -> bool {
    value_payload(value, manifest).is_reference()
}

/// Builds all three index keys — `(EAV, AEV, VAE)` — for an artifact from a
/// single field-encoding pass: the entity/attribute bytes and the value
/// payload are computed once and serialized per ordering.
///
/// This is the commit hot path. Building the EAV key through the chained
/// `set_*` rebuilds (each a full parse + re-encode) and then projecting the
/// other two orderings through `FromKey` (each accessor re-splitting the key)
/// costs ~ten key walks per instruction; this costs three plain
/// serializations of the same parts.
pub(crate) fn artifact_index_keys(
    artifact: &crate::Artifact,
    manifest: &Manifest,
) -> (Key, Key, Key) {
    let mut parts = varkey::KeyParts {
        tag: ENTITY_KEY_TAG,
        entity: artifact.of.as_str().as_bytes().to_vec(),
        attribute: artifact.the.as_str().as_bytes().to_vec(),
        value_type: artifact.is.data_type(),
        value: value_payload(&artifact.is, manifest),
        // The fact orderings carry no version; only history/coverage keys do.
        version: None,
    };
    let eav = Key::from(varkey::build_key(&parts));
    parts.tag = ATTRIBUTE_KEY_TAG;
    let aev = Key::from(varkey::build_key(&parts));
    parts.tag = VALUE_KEY_TAG;
    let vae = Key::from(varkey::build_key(&parts));
    (eav, aev, vae)
}

/// Re-projects an existing index key of any ordering into all three
/// orderings — `(EAV, AEV, VAE)` — with a single parse. Errors on a key that
/// does not parse (corruption), rather than silently projecting garbage.
pub(crate) fn reproject_index_keys(
    key: &Key,
) -> Result<(Key, Key, Key), crate::DialogArtifactsError> {
    let mut parts = varkey::parse_key(key.as_ref()).ok_or_else(|| {
        crate::DialogArtifactsError::InvalidKey(
            "key does not parse while re-projecting orderings".to_string(),
        )
    })?;
    parts.tag = ENTITY_KEY_TAG;
    let eav = Key::from(varkey::build_key(&parts));
    parts.tag = ATTRIBUTE_KEY_TAG;
    let aev = Key::from(varkey::build_key(&parts));
    parts.tag = VALUE_KEY_TAG;
    let vae = Key::from(varkey::build_key(&parts));
    Ok((eav, aev, vae))
}

/// The exact value-tail bytes a key carries for `value` under `manifest`: the
/// value-type byte followed by the value slot (inline order-preserving
/// encoding, or a spilled value's encoded prefix) and, for a spilled value, the
/// whole-value hash.
///
/// This is what makes a [`SortKey`](crate::SortKey) reproduce the tree's byte
/// order: same-`(the, of, type)` facts sort by this tail exactly as the
/// EAV/AEV/VAE keys do.
pub(crate) fn value_tail_bytes(value: &Value, manifest: &Manifest) -> Vec<u8> {
    let payload = value_payload(value, manifest);
    let mut tail = Vec::with_capacity(1 + payload.slot_bytes().len());
    tail.push(value.data_type().into());
    tail.extend_from_slice(payload.slot_bytes());
    // A spilled key's whole-value hash trails the key; within a same-
    // `(the, of)` group the components between the value slot and the hash
    // are equal, so appending it here reproduces the tree's tie-break order.
    if let ValuePayload::Spilled { hash, .. } = &payload {
        tail.extend_from_slice(hash);
    }
    tail
}

/// An opaque, generic [`KeyType`] backing the subtrees of an [`Artifacts`]
/// index.
///
/// A key is a variable-length, lossless, order-preserving byte string built by
/// [`varkey::build_key`]: a tag byte followed by the ordering's components,
/// each encoded so byte order equals semantic order. The entity and attribute
/// are stored at their true length (no 64-byte padding, no truncate-and-hash).
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Key(Vec<u8>);

impl Key {
    /// Construct the lowest possible [`Key`] (a single zero byte: the smallest
    /// possible tag with an empty tail).
    pub fn min() -> Self {
        Self(vec![u8::MIN])
    }

    /// Construct the highest possible [`Key`] (a single `0xFF` byte: larger
    /// than any real key, whose first byte is a tag `<= 0xFF`).
    pub fn max() -> Self {
        Self(vec![u8::MAX])
    }

    /// Returns the tag byte that identifies the key type (entity, attribute, or value)
    pub fn tag(&self) -> u8 {
        self.0.first().copied().unwrap_or(u8::MIN)
    }

    /// Sets the tag byte and returns the modified key
    pub fn set_tag(mut self, tag: u8) -> Self {
        if self.0.is_empty() {
            self.0.push(tag);
        } else {
            self.0[0] = tag;
        }
        self
    }
}

impl KeyType for Key {
    fn bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<Vec<u8>> for Key {
    fn from(value: Vec<u8>) -> Self {
        Key(value)
    }
}

impl From<Key> for Vec<u8> {
    fn from(value: Key) -> Self {
        value.0
    }
}

impl Deref for Key {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Key {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Column schema for the EAV ordering (`tag ‖ entity ‖ attribute ‖
/// value_type ‖ value_payload`). The entity and value payload are large
/// and mostly distinct (arena); the tag, attribute, and value type are small
/// and highly repeated, recurring non-adjacently across the leaf
/// (dictionary). Entity, attribute, and the value payload are variable-length
/// (the payload is an inline order-preserving value or a spilled 32-byte
/// reference).
const EAV_SCHEMA: &[TreeComponent] = &[
    TreeComponent::dictionary(TAG_LENGTH),
    TreeComponent::arena_var(),
    TreeComponent::dictionary_var(),
    TreeComponent::dictionary(VALUE_DATA_TYPE_LENGTH),
    TreeComponent::arena_var(),
];

/// Column schema for the AEV ordering (`tag ‖ attribute ‖ entity ‖
/// value_type ‖ value_payload`).
const AEV_SCHEMA: &[TreeComponent] = &[
    TreeComponent::dictionary(TAG_LENGTH),
    TreeComponent::dictionary_var(),
    TreeComponent::arena_var(),
    TreeComponent::dictionary(VALUE_DATA_TYPE_LENGTH),
    TreeComponent::arena_var(),
];

/// Column schema for the VAE ordering (`tag ‖ value_type ‖ value_payload ‖
/// attribute ‖ entity`).
const VAE_SCHEMA: &[TreeComponent] = &[
    TreeComponent::dictionary(TAG_LENGTH),
    TreeComponent::dictionary(VALUE_DATA_TYPE_LENGTH),
    TreeComponent::arena_var(),
    TreeComponent::dictionary_var(),
    TreeComponent::arena_var(),
];

/// The blob index ordering (`BLOB_KEY_TAG ‖ blob_hash ‖ 0…`) has a single
/// large distinct component after the tag; store it as one variable arena.
const BLOB_SCHEMA: &[TreeComponent] = &[
    TreeComponent::dictionary(TAG_LENGTH),
    TreeComponent::arena_var(),
];

impl TreeKey for Key {
    fn try_from_bytes(bytes: &[u8]) -> Result<Self, DialogSearchTreeError> {
        Ok(Key(bytes.to_vec()))
    }

    fn min() -> Self {
        Key::min()
    }

    fn max() -> Self {
        Key::max()
    }

    /// The layout id is the key's tag byte, which selects the ordering's
    /// column schema. Every key in one leaf shares a tag (the tag sorts
    /// first), so a leaf is single-layout except at the rare tag boundaries,
    /// which the codec handles by falling back to the opaque schema.
    fn layout(&self) -> u8 {
        self.0.first().copied().unwrap_or(u8::MIN)
    }

    /// The layout is the leading tag byte, readable from the stored bytes
    /// without reconstructing the key; the novelty encoder classifies whole
    /// buffers this way before deciding whether a typed parse is needed.
    fn layout_of(bytes: &[u8]) -> Result<u8, DialogSearchTreeError> {
        Ok(bytes.first().copied().unwrap_or(u8::MIN))
    }

    fn schema(layout: u8) -> Schema {
        match layout {
            ENTITY_KEY_TAG => Schema::new(EAV_SCHEMA),
            ATTRIBUTE_KEY_TAG => Schema::new(AEV_SCHEMA),
            VALUE_KEY_TAG => Schema::new(VAE_SCHEMA),
            BLOB_KEY_TAG => Schema::new(BLOB_SCHEMA),
            // History tag and any future ordering: opaque whole key.
            _ => Schema::opaque(),
        }
    }

    fn components<'a>(&'a self, out: &mut Vec<&'a [u8]>) {
        // Split the key into the *encoded* component slices for its ordering.
        // Every slice borrows from `self`, and their concatenation is the key
        // bytes exactly, matching the tag's schema. A key that does not split
        // cleanly (an unknown tag, a `min`/`max` sentinel, or malformed bytes)
        // falls back to the opaque whole-key component.
        match varkey::split_components(&self.0) {
            Some(slices) => out.extend(slices),
            None => out.push(&self.0),
        }
    }

    fn components_of<'a>(
        bytes: &'a [u8],
        _layout: u8,
        out: &mut Vec<&'a [u8]>,
    ) -> Result<(), DialogSearchTreeError> {
        // The same split as `components`, straight from the stored bytes: a
        // typed key wraps exactly these bytes, so the two agree by
        // construction, and the encoders can split without reconstructing
        // (copying) the key first.
        match varkey::split_components(bytes) {
            Some(slices) => out.extend(slices),
            None => out.push(bytes),
        }
        Ok(())
    }
}

/// Trait for constructing key views with minimum and maximum values.
///
/// This trait enables the creation of key views for range-based queries, providing
/// the ability to construct keys with boundary values for efficient prolly tree navigation.
pub trait KeyViewConstruct: KeyViewMut + Default {
    /// Construct the lowest possible [`KeyView`] (all non-tag bits are zero)
    fn min() -> Self;

    /// Construct the highest possible [`KeyView`] (all non-tag bits are one)
    fn max() -> Self;
}

/// Trait for mutably modifying key view components.
///
/// This trait provides methods to modify individual parts of a key view,
/// enabling the construction of keys with specific entity, attribute, and value constraints.
pub trait KeyViewMut: KeyView {
    /// Set the [`EntityKeyPart`], altering the [`Entity`] part of this
    /// [`KeyView`].
    fn set_entity(self, entity: EntityKeyPart) -> Self;

    /// Set the [`AttributeKeyPart`], altering the [`Attribute`] part of this
    /// [`KeyView`].
    fn set_attribute(self, attribute: AttributeKeyPart) -> Self;

    /// Set the value slot: the [`ValueDataType`] and the payload (an inline
    /// order-preserving value or a spilled reference), together.
    ///
    /// The type and payload are set atomically because a payload's byte length
    /// depends on the type (fixed-width numerics, a terminated string, or a
    /// 32-byte spilled reference): setting one without the other would leave a
    /// value tail that no longer self-delimits, so the next parse-and-rebuild
    /// would fail and silently drop back to the bound's fallback parts.
    fn set_value(self, value_type: ValueDataType, value: ValuePayload) -> Self;

    /// Sets the constrained parts of the given [`ArtifactSelector`] to the associated
    /// components of this [`KeyView`].
    ///
    /// `manifest` is the target tree's format. It must be the manifest the
    /// stored facts were WRITTEN under, or a value-constrained bound is built
    /// with the wrong payload shape (a different `inline_n` changes whether the
    /// bound spills, and a different `spill_prefix` changes how much of it the
    /// key carries) and the range brackets the wrong keys.
    fn apply_selector(self, selector: &ArtifactSelector<Constrained>, manifest: &Manifest) -> Self {
        let mut key = self;

        if let Some(entity) = selector.entity() {
            key = key.set_entity(entity.into());
        };

        if let Some(attribute) = selector.attribute() {
            key = key.set_attribute(attribute.into());
        }

        // A value-constrained (equality) selector collapses the value slot to
        // the value's payload, encoded with the same inline-vs-spill decision
        // the fact-building path uses, so a value range narrows to the exact
        // value. The type and payload (with its spill flag) are set together.
        if let Some(value) = selector.value() {
            key = key.set_value(value.data_type(), value_payload(value, manifest));
        }

        key
    }
}

/// Trait for reading components from key views.
///
/// This trait provides read-only access to the individual parts of a key,
/// enabling pattern matching and component extraction during query operations.
pub trait KeyView: Sized + Clone {
    /// Get an [`EntityKeyPart`] that refers to the [`Entity`] part of this
    /// [`KeyView`].
    fn entity(&self) -> EntityKeyPart<'_>;

    /// Get an [`AttributeKeyPart`] that refers to the [`Attribute`] part of
    /// this [`KeyView`].
    fn attribute(&self) -> AttributeKeyPart<'_>;

    /// Get the [`ValueDataType`] that is represented by this [`KeyView`].
    fn value_type(&self) -> ValueDataType;

    /// Borrow the bytes occupying this [`KeyView`]'s value SLOT: the full
    /// inline order-preserving encoding, or a spilled value's encoded prefix.
    /// [`value_is_spilled`](KeyView::value_is_spilled) says which.
    fn value_payload(&self) -> &[u8];

    /// Whether this [`KeyView`]'s value spilled (the key carries the value's
    /// encoded prefix plus a trailing whole-value hash).
    fn value_is_spilled(&self) -> bool;

    /// Borrow the 32-byte whole-value hash trailing a spilled key (the
    /// archive block address), or `None` for an inline value.
    fn value_spill_hash(&self) -> Option<&[u8]>;
}

/// Trait for constructing key views from artifact selectors.
///
/// This trait enables the creation of key views that match the constraints
/// specified in an artifact selector, used during query range construction.
pub trait FromSelector: KeyViewConstruct {
    /// Creates a key view from an artifact selector's constraints, under the
    /// target tree's format `manifest`.
    fn from_selector(selector: &ArtifactSelector<Constrained>, manifest: &Manifest) -> Self {
        Self::default().apply_selector(selector, manifest)
    }
}

impl<K> FromSelector for K where K: KeyViewConstruct {}

/// Trait for constructing key views from other key views.
///
/// This trait enables the conversion between different key view types,
/// allowing transformation from one index type to another during query operations.
pub trait FromKey<K>
where
    K: KeyView,
{
    /// Creates a key view from another key view.
    fn from_key(key: &K) -> Self;
}

impl<Ka, Kb> FromKey<Ka> for Kb
where
    Ka: KeyView,
    Kb: KeyViewConstruct,
{
    fn from_key(key: &Ka) -> Self {
        // Copy the value payload faithfully so all three orderings (EAV/AEV/VAE)
        // for one fact carry the identical value slot: an inline payload stays
        // inline, a spilled value keeps its encoded prefix and trailing hash.
        let payload = match key.value_spill_hash() {
            Some(hash) => ValuePayload::Spilled {
                prefix: key.value_payload().to_vec(),
                hash: hash.to_vec(),
            },
            None => ValuePayload::Inline(key.value_payload().to_vec()),
        };
        Kb::default()
            .set_entity(key.entity())
            .set_attribute(key.attribute())
            .set_value(key.value_type(), payload)
    }
}

// impl<T> Deref for T
// where
//     T: KeyView,
// {
//     type Target = <Key as Deref>::Target;

//     fn deref(&self) -> &Self::Target {
//         *self.0
//     }
// }

// impl<T> Default for T
// where
//     T: KeyView,
// {
//     fn default() -> Self {
//         Self::min()
//     }
// }

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]
    // The dialog_common::test macro requires async test fns; these pure tests
    // await nothing.
    #![allow(clippy::unused_async)]

    use std::str::FromStr;

    use super::{
        AttributeKey, EntityKey, FromKey, KeyView, Manifest, ValueKey, default_manifest,
        value_payload,
    };
    use crate::{Artifact, Attribute, Entity, Value, decode_value, key::varkey::ValuePayload};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn fact(value: Value) -> Artifact {
        Artifact {
            the: Attribute::from_str("person/name").unwrap(),
            of: Entity::from_str("did:key:z6MkExample").unwrap(),
            is: value,
            cause: None,
        }
    }

    /// Every value type's key splits into exactly its schema's component
    /// count and parses back to the fields it was built from, across all
    /// three orderings. Pins the whole width table at once: the class of bug
    /// where an encoder's byte width disagrees with the parser's claimed
    /// width (the f64 8-vs-16 bug) breaks the split for whichever type
    /// regressed. (NaN is covered separately in `ordkey`; `Value`'s equality
    /// cannot compare it.)
    #[dialog_common::test]
    async fn it_splits_and_parses_keys_for_every_value_type() -> anyhow::Result<()> {
        use crate::key::varkey::{ValueRef, parse_key_ref, split_components};

        let values = vec![
            Value::Bytes(vec![]),
            Value::Bytes(vec![0x00, 0xFF, 0x7F, 0x00]),
            Value::Entity(Entity::from_str("did:key:z6MkOther")?),
            Value::Boolean(false),
            Value::Boolean(true),
            Value::String(String::new()),
            Value::String("hello \u{0} world".into()),
            Value::UnsignedInt(0),
            Value::UnsignedInt(u128::MAX),
            Value::SignedInt(i128::MIN),
            Value::SignedInt(-1),
            Value::Float(-0.0),
            Value::Float(1783112056217.0),
            Value::Record(vec![1, 2, 3]),
            Value::Symbol(Attribute::from_str("open/status")?),
        ];
        for value in values {
            let fact = fact(value.clone());
            let keys = [
                EntityKey::from_artifact(&fact, &default_manifest()).into_key(),
                AttributeKey::from_artifact(&fact, &default_manifest()).into_key(),
                ValueKey::from_artifact(&fact, &default_manifest()).into_key(),
            ];
            for key in keys {
                assert!(
                    split_components(key.as_ref()).is_some(),
                    "key must split with full byte coverage: {value:?}"
                );
                let parts = parse_key_ref(key.as_ref())
                    .unwrap_or_else(|| panic!("key must parse: {value:?}"));
                assert_eq!(parts.entity.as_ref(), fact.of.as_str().as_bytes());
                assert_eq!(parts.attribute.as_ref(), fact.the.as_str().as_bytes());
                assert_eq!(parts.value_type, value.data_type());
                let ValueRef::Inline(payload) = parts.value else {
                    panic!("small values stay inline: {value:?}");
                };
                let (decoded, rest) =
                    decode_value(parts.value_type, payload).expect("payload decodes");
                assert!(rest.is_empty(), "payload is exactly one value: {value:?}");
                assert_eq!(decoded, value, "value round-trips through the key");
            }
        }
        Ok(())
    }

    /// A spilled value sorts INTO its type band by its in-key prefix, right
    /// next to inline values sharing those leading bytes — there is no
    /// separate spilled band — and two large values sharing their whole
    /// key-prefix stay DISTINCT keys via the trailing whole-value hash
    /// (cardinality-many must not collapse).
    #[dialog_common::test]
    async fn it_sorts_spilled_values_into_their_type_band() -> anyhow::Result<()> {
        let big = "z".repeat(default_manifest().inline_n as usize + 1);
        let spilled_key =
            EntityKey::from_artifact(&fact(Value::String(big.clone())), &default_manifest())
                .into_key();

        // In-band neighbors: ordered by leading value bytes, not banished
        // above the type band.
        let below =
            EntityKey::from_artifact(&fact(Value::String("y-below".into())), &default_manifest())
                .into_key();
        let shorter =
            EntityKey::from_artifact(&fact(Value::String("zz".into())), &default_manifest())
                .into_key();
        let next_band =
            EntityKey::from_artifact(&fact(Value::UnsignedInt(1)), &default_manifest()).into_key();
        assert!(
            below < spilled_key,
            "sorts by leading bytes within the band"
        );
        assert!(
            shorter < spilled_key,
            "an inline string that is a prefix of the spilled one sorts below it"
        );
        assert!(
            spilled_key < next_band,
            "stays inside the String band, below the next type band"
        );

        // Cardinality-many: two large values sharing their entire key-prefix
        // differ only in the trailing hash — distinct keys, both spilled.
        let sibling = format!("{}A", "z".repeat(default_manifest().inline_n as usize + 1));
        let sibling_key =
            EntityKey::from_artifact(&fact(Value::String(sibling)), &default_manifest()).into_key();
        assert_ne!(
            spilled_key, sibling_key,
            "same-prefix large values stay distinct via the trailing hash"
        );
        assert_eq!(
            EntityKey(&spilled_key).value_payload(),
            EntityKey(&sibling_key).value_payload(),
            "their value slots (the in-key prefixes) are identical"
        );

        let key = EntityKey(&spilled_key);
        assert!(key.value_is_spilled());
        assert_eq!(
            key.value_spill_hash(),
            Some(Value::String(big).to_reference().as_slice()),
            "the trailing hash addresses the archive block"
        );
        Ok(())
    }

    /// A small value round-trips through a key inline: the key carries the
    /// value's order-preserving bytes (not spilled), and decoding the payload
    /// by its type reproduces the original [`Value`].
    #[dialog_common::test]
    async fn it_round_trips_a_small_value_inline() -> anyhow::Result<()> {
        let value = Value::String("Alice".into());
        let key = EntityKey::from_artifact(&fact(value.clone()), &default_manifest());

        assert!(!key.value_is_spilled(), "a small value stays inline");
        assert_eq!(key.value_type(), value.data_type());

        let (decoded, rest) =
            decode_value(key.value_type(), key.value_payload()).expect("decodes inline payload");
        assert!(rest.is_empty(), "payload is exactly one value encoding");
        assert_eq!(decoded, value, "inline value round-trips");
        Ok(())
    }

    /// All three orderings (EAV/AEV/VAE) for one fact carry the identical value
    /// payload, and `from_key` preserves it across a re-projection.
    #[dialog_common::test]
    async fn it_carries_one_payload_across_orderings() -> anyhow::Result<()> {
        let value = Value::UnsignedInt(1234);
        let fact = fact(value.clone());

        let eav = EntityKey::from_artifact(&fact, &default_manifest());
        let aev = AttributeKey::from_artifact(&fact, &default_manifest());
        let vae = ValueKey::from_artifact(&fact, &default_manifest());

        assert_eq!(eav.value_payload(), aev.value_payload());
        assert_eq!(eav.value_payload(), vae.value_payload());
        assert_eq!(eav.value_is_spilled(), vae.value_is_spilled());

        // Re-projecting EAV onto VAE preserves the exact payload and spill flag.
        let projected: ValueKey<crate::Key> = FromKey::from_key(&eav);
        assert_eq!(projected.value_payload(), vae.value_payload());
        assert_eq!(projected.value_is_spilled(), vae.value_is_spilled());
        assert_eq!(projected.value_type(), value.data_type());
        Ok(())
    }

    /// The inline-vs-spill decision spills a value whose encoded form exceeds
    /// the threshold: the payload keeps the encoded leading raw bytes as the
    /// key-prefix and carries the whole-value hash; a value within the
    /// threshold stays inline.
    #[dialog_common::test]
    async fn it_spills_above_the_inline_threshold() -> anyhow::Result<()> {
        // A tiny threshold forces even a short string to spill.
        let value = Value::String("this-string-exceeds-a-tiny-threshold".into());
        // A tiny inline_n forces even a short string to spill.
        let tiny = Manifest {
            inline_n: 4,
            ..Manifest::default()
        };
        let spilled = value_payload(&value, &tiny);
        assert!(spilled.is_reference(), "oversized value spills");
        let ValuePayload::Spilled { prefix, hash } = &spilled else {
            panic!("expected a spilled payload");
        };
        assert_eq!(hash.as_slice(), value.to_reference().as_slice());
        let raw = value.to_bytes();
        let mut expected = Vec::new();
        crate::encode_bytes(
            &raw[..raw.len().min(default_manifest().spill_prefix as usize)],
            &mut expected,
        );
        assert_eq!(
            prefix, &expected,
            "the slot keeps the encoded leading raw bytes"
        );

        // Under a generous threshold the same value stays inline.
        let generous = Manifest {
            inline_n: 4096,
            ..Manifest::default()
        };
        let inline = value_payload(&value, &generous);
        assert!(matches!(inline, ValuePayload::Inline(_)), "fits inline");
        Ok(())
    }

    /// A value that spills builds a key whose value slot holds the encoded
    /// prefix (sorting into the type band next to inline values) and whose
    /// trailing 32 bytes are the whole-value hash.
    #[dialog_common::test]
    async fn it_builds_a_spilled_key_with_prefix_and_trailing_hash() -> anyhow::Result<()> {
        let manifest = default_manifest();
        let value = Value::String("x".repeat(manifest.inline_n as usize + 1));
        let key = EntityKey::from_artifact(&fact(value.clone()), &manifest);

        assert!(key.value_is_spilled(), "an oversized value spills the key");
        assert_eq!(
            key.value_type(),
            value.data_type(),
            "type survives spilling"
        );
        let raw = value.to_bytes();
        let mut expected = Vec::new();
        crate::encode_bytes(&raw[..manifest.spill_prefix as usize], &mut expected);
        assert_eq!(
            key.value_payload(),
            expected.as_slice(),
            "the slot holds the encoded prefix"
        );
        assert_eq!(
            key.value_spill_hash(),
            Some(value.to_reference().as_slice()),
            "the whole-value hash trails the key"
        );
        Ok(())
    }
}
