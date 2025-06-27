use std::ops::{Deref, DerefMut};

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

macro_rules! mutable_slice {
    ( $array:expr, $index:expr, $run:expr ) => {{
        const START: usize = $index;
        const END: usize = $index + $run;
        &mut $array[START..END]
    }};
}

pub(crate) use mutable_slice;

use crate::{ArtifactSelector, ValueDataType, selector::Constrained};

pub(crate) const TAG_LENGTH: usize = 1;
pub(crate) const ENTITY_LENGTH: usize = 64;
pub(crate) const ATTRIBUTE_LENGTH: usize = 64;
pub(crate) const VALUE_DATA_TYPE_LENGTH: usize = 1;
pub(crate) const VALUE_REFERENCE_LENGTH: usize = 32;

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

/// An opaque, generic [`KeyType`] that is used when constructing the subtrees
/// of an [`Artifacts`] index.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Key(#[serde(with = "BigArray")] [u8; KEY_LENGTH]);

impl Key {
    /// Construct the lowest possible [`EntityKey`] (all bits are zero)
    pub fn min() -> Self {
        Self(MINIMUM_KEY)
    }

    /// Construct the highest possible [`EntityKey`] (all bits are one)
    pub fn max() -> Self {
        Self(MAXIMUM_KEY)
    }

    pub fn tag(&self) -> u8 {
        self.0[0]
    }

    pub fn set_tag(mut self, tag: u8) -> Self {
        self.0[0] = tag;
        self
    }
}

impl From<[u8; KEY_LENGTH]> for Key {
    fn from(value: [u8; KEY_LENGTH]) -> Self {
        Key(value)
    }
}

impl From<Key> for [u8; KEY_LENGTH] {
    fn from(value: Key) -> Self {
        value.0
    }
}

impl Deref for Key {
    type Target = [u8; KEY_LENGTH];

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
        self.0.as_ref()
    }
}

pub trait KeyView: Sized + Clone + Default {
    /// Construct a [`KeyView`] from the provided component key parts.
    fn from_parts(
        entity: EntityKeyPart,
        attribute: AttributeKeyPart,
        value_type: ValueDataType,
        value_reference: ValueReferenceKeyPart,
    ) -> Self;

    /// Construct the lowest possible [`KeyView`] (all non-tag bits are zero)
    fn min() -> Self;

    /// Construct the highest possible [`KeyView`] (all non-tag bits are one)
    fn max() -> Self;

    /// Get an [`EntityKeyPart`] that refers to the [`Entity`] part of this
    /// [`KeyView`].
    fn entity(&self) -> EntityKeyPart;

    /// Set the [`EntityKeyPart`], altering the [`Entity`] part of this
    /// [`KeyView`].
    fn set_entity(self, entity: EntityKeyPart) -> Self;

    /// Get an [`AttributeKeyPart`] that refers to the [`Attribute`] part of
    /// this [`KeyView`].
    fn attribute(&self) -> AttributeKeyPart;

    /// Set the [`AttributeKeyPart`], altering the [`Attribute`] part of this
    /// [`KeyView`].
    fn set_attribute(self, attribute: AttributeKeyPart) -> Self;

    /// Get the [`ValueDataType`] that is represented by this [`KeyView`].
    fn value_type(&self) -> ValueDataType;

    /// Set the [`ValueDataType`] that is represented by this [`KeyView`].
    fn set_value_type(self, value_type: ValueDataType) -> Self;

    /// Get a [`ValueReferenceKeyPart`] that refers to the [`Value`] part of
    /// this [`KeyView`].
    fn value_reference(&self) -> ValueReferenceKeyPart;

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

pub trait FromSelector: KeyView {
    fn from_selector(selector: &ArtifactSelector<Constrained>) -> Self {
        Self::default().apply_selector(selector)
    }
}

impl<K> FromSelector for K where K: KeyView {}

pub trait FromKey<K>
where
    K: KeyView,
{
    fn from_key(key: &K) -> Self;
}

impl<Ka, Kb> FromKey<Ka> for Kb
where
    Ka: KeyView,
    Kb: KeyView,
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
