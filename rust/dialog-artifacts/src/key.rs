//! Key structures for indexing artifacts in prolly trees.
//!
//! This module provides the key layout and manipulation utilities for creating
//! efficient indexes over semantic triples. Keys are structured to enable fast
//! range queries over different access patterns (by entity, attribute, or value).

use std::ops::{Deref, DerefMut};

use dialog_common::ConditionalSync;
use dialog_search_tree::{
    Component as TreeComponent, DialogSearchTreeError, Key as TreeKey, Schema,
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

mod part;
pub use part::*;

pub(crate) mod varkey;

/// Tag byte reserved for the history index (the fourth index ordering).
///
/// Allocated so the blob index lands at tag `4`; the history index itself is
/// not yet implemented, so no key view is built on this tag.
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
/// Length of the value reference field in key bytes
pub(crate) const VALUE_REFERENCE_LENGTH: usize = 32;

pub(crate) const MINIMUM_VALUE_REFERENCE: [u8; VALUE_REFERENCE_LENGTH] =
    [u8::MIN; VALUE_REFERENCE_LENGTH];
pub(crate) const MAXIMUM_VALUE_REFERENCE: [u8; VALUE_REFERENCE_LENGTH] =
    [u8::MAX; VALUE_REFERENCE_LENGTH];

/// Decides how a [`Value`] is carried in a key: a small value is encoded inline
/// in its order-preserving form (and stays range-queryable); a value whose
/// encoded form exceeds `inline_n` spills to its 32-byte content-addressed
/// reference (equality-only). This is the single place the inline-vs-spill
/// decision is made, so the fact-building path and the selector path agree.
pub(crate) fn value_payload(value: &Value, inline_n: usize) -> ValuePayload {
    let encoded = encode_value_owned(value);
    if encoded.len() <= inline_n {
        ValuePayload::Inline(encoded)
    } else {
        ValuePayload::Reference(value.to_reference().to_vec())
    }
}

/// The inline-vs-spill threshold for value payloads in keys.
///
// TODO(m3.2c): the manifest is read from `Manifest::default()` here. A later
// stage persists the manifest into node bytes and threads it down to the key
// builders; until then this mirrors the default note in the search tree's
// distribution/transient reshape path.
pub(crate) fn inline_threshold() -> usize {
    dialog_search_tree::Manifest::default().inline_n as usize
}

/// Whether `value` spills (its encoded form exceeds the inline threshold, so
/// the key carries a reference and the payload must carry the raw bytes).
/// The single source of truth the payload builder and the key builder share.
pub(crate) fn value_spills(value: &Value) -> bool {
    value_payload(value, inline_threshold()).is_reference()
}

/// The exact value-tail bytes a key carries for `value`: the value-type byte
/// (with the spill flag set when the value spilled) followed by the payload
/// (inline order-preserving encoding, or the 32-byte spilled reference).
///
/// This is what makes a [`SortKey`](crate::SortKey) reproduce the tree's byte
/// order: same-`(the, of, type)` facts sort by this tail exactly as the
/// EAV/AEV/VAE keys do.
pub(crate) fn value_tail_bytes(value: &Value) -> Vec<u8> {
    let payload = value_payload(value, inline_threshold());
    let mut type_byte: u8 = value.data_type().into();
    if payload.is_reference() {
        type_byte |= varkey::SPILL_FLAG;
    }
    let mut tail = Vec::with_capacity(1 + payload.as_bytes().len());
    tail.push(type_byte);
    tail.extend_from_slice(payload.as_bytes());
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

    /// Construct a [`KeyView`] from the provided component key parts.
    fn from_parts(
        entity: EntityKeyPart,
        attribute: AttributeKeyPart,
        value_type: ValueDataType,
        value_reference: ValueReferenceKeyPart,
    ) -> Self;
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
    /// components of this [`KeyView`]
    fn apply_selector(self, selector: &ArtifactSelector<Constrained>) -> Self {
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
            key = key.set_value(value.data_type(), value_payload(value, inline_threshold()));
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

    /// Borrow the raw value payload bytes of this [`KeyView`]: the inline
    /// order-preserving value encoding, or a spilled 32-byte reference.
    /// [`value_is_spilled`](KeyView::value_is_spilled) says which.
    fn value_payload(&self) -> &[u8];

    /// Whether this [`KeyView`]'s value spilled (its payload is a reference
    /// rather than the inline order-preserving value).
    fn value_is_spilled(&self) -> bool;
}

/// Trait for constructing key views from artifact selectors.
///
/// This trait enables the creation of key views that match the constraints
/// specified in an artifact selector, used during query range construction.
pub trait FromSelector: KeyViewConstruct {
    /// Creates a key view from an artifact selector's constraints.
    fn from_selector(selector: &ArtifactSelector<Constrained>) -> Self {
        Self::default().apply_selector(selector)
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
        // inline, a spilled reference stays spilled.
        let payload = if key.value_is_spilled() {
            ValuePayload::Reference(key.value_payload().to_vec())
        } else {
            ValuePayload::Inline(key.value_payload().to_vec())
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
        AttributeKey, EntityKey, FromKey, KeyView, ValueKey, inline_threshold, value_payload,
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

    /// A small value round-trips through a key inline: the key carries the
    /// value's order-preserving bytes (not spilled), and decoding the payload
    /// by its type reproduces the original [`Value`].
    #[dialog_common::test]
    async fn it_round_trips_a_small_value_inline() -> anyhow::Result<()> {
        let value = Value::String("Alice".into());
        let key = EntityKey::from(&fact(value.clone()));

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

        let eav = EntityKey::from(&fact);
        let aev = AttributeKey::from(&fact);
        let vae = ValueKey::from(&fact);

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
    /// the threshold to its 32-byte reference; a value within the threshold
    /// stays inline.
    #[dialog_common::test]
    async fn it_spills_above_the_inline_threshold() -> anyhow::Result<()> {
        // A tiny threshold forces even a short string to spill to its reference.
        let value = Value::String("this-string-exceeds-a-tiny-threshold".into());
        let spilled = value_payload(&value, 4);
        assert!(spilled.is_reference(), "oversized value spills");
        assert_eq!(spilled.as_bytes(), value.to_reference().to_vec());

        // Under a generous threshold the same value stays inline.
        let inline = value_payload(&value, 4096);
        assert!(matches!(inline, ValuePayload::Inline(_)), "fits inline");
        Ok(())
    }

    /// A value that spills builds a key whose value-type byte has the spill bit
    /// set and whose payload is the 32-byte reference.
    #[dialog_common::test]
    async fn it_builds_a_spilled_key_with_the_spill_flag() -> anyhow::Result<()> {
        let value = Value::String("x".repeat(inline_threshold() + 1));
        let key = EntityKey::from(&fact(value.clone()));

        assert!(key.value_is_spilled(), "an oversized value spills the key");
        assert_eq!(
            key.value_type(),
            value.data_type(),
            "type survives spilling"
        );
        assert_eq!(key.value_payload(), value.to_reference().to_vec());
        Ok(())
    }
}
