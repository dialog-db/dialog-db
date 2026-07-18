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

use crate::{ArtifactSelector, ValueDataType, selector::Constrained};

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
/// value_type ‖ value_reference`). The entity and value reference are large
/// and mostly distinct (arena); the tag, attribute, and value type are small
/// and highly repeated, recurring non-adjacently across the leaf
/// (dictionary). Entity and attribute are variable-length.
const EAV_SCHEMA: &[TreeComponent] = &[
    TreeComponent::dictionary(TAG_LENGTH),
    TreeComponent::arena_var(),
    TreeComponent::dictionary_var(),
    TreeComponent::dictionary(VALUE_DATA_TYPE_LENGTH),
    TreeComponent::arena(VALUE_REFERENCE_LENGTH),
];

/// Column schema for the AEV ordering (`tag ‖ attribute ‖ entity ‖
/// value_type ‖ value_reference`).
const AEV_SCHEMA: &[TreeComponent] = &[
    TreeComponent::dictionary(TAG_LENGTH),
    TreeComponent::dictionary_var(),
    TreeComponent::arena_var(),
    TreeComponent::dictionary(VALUE_DATA_TYPE_LENGTH),
    TreeComponent::arena(VALUE_REFERENCE_LENGTH),
];

/// Column schema for the VAE ordering (`tag ‖ value_type ‖ value_reference ‖
/// attribute ‖ entity`).
const VAE_SCHEMA: &[TreeComponent] = &[
    TreeComponent::dictionary(TAG_LENGTH),
    TreeComponent::dictionary(VALUE_DATA_TYPE_LENGTH),
    TreeComponent::arena(VALUE_REFERENCE_LENGTH),
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

    /// Set the [`ValueDataType`] that is represented by this [`KeyView`].
    fn set_value_type(self, value_type: ValueDataType) -> Self;

    /// Set the [`ValueReferenceKeyPart`], altering the [`Value`] part of this
    /// [`KeyView`].
    fn set_value_reference(self, value_reference: ValueReferenceKeyPart) -> Self;

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

        if let Some(value_type) = selector.value().map(|value| value.data_type()) {
            key = key.set_value_type(value_type);
        }

        if let Some(value_reference) = selector.value_reference() {
            key = key.set_value_reference(ValueReferenceKeyPart(value_reference));
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

    /// Get a [`ValueReferenceKeyPart`] that refers to the [`Value`] part of
    /// this [`KeyView`].
    fn value_reference(&self) -> ValueReferenceKeyPart<'_>;
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
        Kb::default()
            .set_entity(key.entity())
            .set_attribute(key.attribute())
            .set_value_type(key.value_type())
            .set_value_reference(key.value_reference())
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
