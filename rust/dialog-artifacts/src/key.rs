//! Key structures for indexing artifacts in prolly trees.
//!
//! This module provides the key layout and manipulation utilities for creating
//! efficient indexes over semantic triples. Keys are structured to enable fast
//! range queries over different access patterns (by entity, attribute, or value).

use std::ops::{Deref, DerefMut};

use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

mod attribute;
pub use attribute::*;

mod entity;
pub use entity::*;

mod value;
pub use value::*;

mod part;
pub use part::*;

/// Helper macro for creating mutable slices from byte arrays at compile time.
///
/// This macro is used internally for efficient manipulation of key byte layouts
/// without runtime bounds checking.
macro_rules! mutable_slice {
    ( $array:expr, $index:expr, $run:expr ) => {{
        const START: usize = $index;
        const END: usize = $index + $run;
        &mut $array[START..END]
    }};
}

pub(crate) use mutable_slice;

use crate::{ArtifactSelector, DialogArtifactsError, ValueDataType, selector::Constrained};

/// Length of the key tag field in bytes
pub(crate) const TAG_LENGTH: usize = 1;
/// Length of the entity field in key bytes
pub(crate) const ENTITY_LENGTH: usize = 64;
/// Length of the attribute field in key bytes
pub(crate) const ATTRIBUTE_LENGTH: usize = 64;
/// Length of the value data type field in key bytes
pub(crate) const VALUE_DATA_TYPE_LENGTH: usize = 1;
/// Length of the value reference field in key bytes
pub(crate) const VALUE_REFERENCE_LENGTH: usize = 32;

/// Total length of a complete key in bytes
pub(crate) const KEY_LENGTH: usize =
    TAG_LENGTH + ENTITY_LENGTH + ATTRIBUTE_LENGTH + VALUE_DATA_TYPE_LENGTH + VALUE_REFERENCE_LENGTH;

pub(crate) const MINIMUM_KEY: [u8; KEY_LENGTH] = [u8::MIN; KEY_LENGTH];
pub(crate) const MAXIMUM_KEY: [u8; KEY_LENGTH] = [u8::MAX; KEY_LENGTH];

pub(crate) const MINIMUM_ENTITY: [u8; ENTITY_LENGTH] = [u8::MIN; ENTITY_LENGTH];
pub(crate) const MAXIMUM_ENTITY: [u8; ENTITY_LENGTH] = [u8::MAX; ENTITY_LENGTH];
pub(crate) const MINIMUM_ATTRIBUTE: [u8; ATTRIBUTE_LENGTH] = [u8::MIN; ATTRIBUTE_LENGTH];
pub(crate) const MAXIMUM_ATTRIBUTE: [u8; ATTRIBUTE_LENGTH] = [u8::MAX; ATTRIBUTE_LENGTH];
pub(crate) const MINIMUM_VALUE_REFERENCE: [u8; VALUE_REFERENCE_LENGTH] =
    [u8::MIN; VALUE_REFERENCE_LENGTH];
pub(crate) const MAXIMUM_VALUE_REFERENCE: [u8; VALUE_REFERENCE_LENGTH] =
    [u8::MAX; VALUE_REFERENCE_LENGTH];

/// Type alias for the raw byte representation of a key
pub type KeyBytes = [u8; KEY_LENGTH];

/// An opaque, generic [`KeyType`] that is used when constructing the subtrees
/// of an [`Artifacts`] index.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Key(#[serde(with = "BigArray")] KeyBytes);

impl Key {
    /// Construct the lowest possible [`EntityKey`] (all bits are zero)
    pub fn min() -> Self {
        Self(MINIMUM_KEY)
    }

    /// Construct the highest possible [`EntityKey`] (all bits are one)
    pub fn max() -> Self {
        Self(MAXIMUM_KEY)
    }

    /// Returns the tag byte that identifies the key type (entity, attribute, or value)
    pub fn tag(&self) -> u8 {
        self.0[0]
    }

    /// Sets the tag byte and returns the modified key
    pub fn set_tag(mut self, tag: u8) -> Self {
        self.0[0] = tag;
        self
    }
}

impl KeyType for Key {
    fn bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl TryFrom<Vec<u8>> for Key {
    type Error = DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into().map_err(|value: Vec<u8>| {
            DialogArtifactsError::InvalidKey(format!(
                "Wrong byte length for entity key: {}",
                value.len()
            ))
        })?))
    }
}

impl From<KeyBytes> for Key {
    fn from(value: KeyBytes) -> Self {
        Key(value)
    }
}

impl From<Key> for KeyBytes {
    fn from(value: Key) -> Self {
        value.0
    }
}

impl Deref for Key {
    type Target = KeyBytes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Key {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<KeyBytes> for Key {
    fn as_ref(&self) -> &KeyBytes {
        &self.0
    }
}

impl AsMut<KeyBytes> for Key {
    fn as_mut(&mut self) -> &mut KeyBytes {
        &mut self.0
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
    fn entity(&self) -> EntityKeyPart;

    /// Get an [`AttributeKeyPart`] that refers to the [`Attribute`] part of
    /// this [`KeyView`].
    fn attribute(&self) -> AttributeKeyPart;

    /// Get the [`ValueDataType`] that is represented by this [`KeyView`].
    fn value_type(&self) -> ValueDataType;

    /// Get a [`ValueReferenceKeyPart`] that refers to the [`Value`] part of
    /// this [`KeyView`].
    fn value_reference(&self) -> ValueReferenceKeyPart;
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
