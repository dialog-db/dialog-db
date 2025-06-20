use std::ops::Deref;

use arrayref::array_ref;
use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

use crate::{
    ATTRIBUTE_LENGTH, Artifact, ArtifactSelector, AttributeKeyPart, DialogArtifactsError,
    ENTITY_KEY_LENGTH, ENTITY_LENGTH, EntityKeyPart, VALUE_REFERENCE_LENGTH, ValueDataType,
    mutable_slice, selector::Constrained,
};

use super::{AttributeKey, VALUE_DATA_TYPE_LENGTH, ValueKey, ValueReferenceKeyPart};

const ENTITY_OFFSET: usize = 0;
const ATTRIBUTE_OFFSET: usize = ENTITY_LENGTH;
const VALUE_DATA_TYPE_OFFSET: usize = ENTITY_LENGTH + ATTRIBUTE_LENGTH;
const VALUE_REFERENCE_OFFSET: usize = ENTITY_LENGTH + ATTRIBUTE_LENGTH + VALUE_DATA_TYPE_LENGTH;

const MINIMUM_ENTITY_KEY: [u8; ENTITY_KEY_LENGTH] = [u8::MIN; ENTITY_KEY_LENGTH];
const MAXIMUM_ENTITY_KEY: [u8; ENTITY_KEY_LENGTH] = [u8::MAX; ENTITY_KEY_LENGTH];

/// A [`KeyType`] that is used when constructing an index of the [`Entity`]s
/// of [`Artifact`]s.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntityKey(#[serde(with = "BigArray")] [u8; ENTITY_KEY_LENGTH]);

impl EntityKey {
    /// Construct an [`EntityKey`] from the provided component key parts.
    pub fn from_parts(
        entity: EntityKeyPart,
        attribute: AttributeKeyPart,
        value_type: ValueDataType,
        value_reference: ValueReferenceKeyPart,
    ) -> Self {
        Self::default()
            .set_entity(entity)
            .set_attribute(attribute)
            .set_value_type(value_type)
            .set_value_reference(value_reference)
    }

    /// Construct the lowest possible [`EntityKey`] (all bits are zero)
    pub fn min() -> Self {
        Self(MINIMUM_ENTITY_KEY)
    }

    /// Construct the highest possible [`EntityKey`] (all bits are one)
    pub fn max() -> Self {
        Self(MAXIMUM_ENTITY_KEY)
    }

    /// Get an [`EntityKeyPart`] that refers to the [`Entity`] part of this
    /// [`EntityKey`].
    pub fn entity(&self) -> EntityKeyPart {
        EntityKeyPart(array_ref![self.0, ENTITY_OFFSET, ENTITY_LENGTH])
    }

    /// Set the [`EntityKeyPart`], altering the [`Entity`] part of this
    /// [`EntityKey`].
    pub fn set_entity(self, entity: EntityKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice![inner, ENTITY_OFFSET, ENTITY_LENGTH].copy_from_slice(entity.0);
        Self(inner)
    }

    /// Get an [`AttributeKeyPart`] that refers to the [`Attribute`] part of
    /// this [`EntityKey`].
    pub fn attribute(&self) -> AttributeKeyPart {
        AttributeKeyPart(array_ref![self.0, ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH])
    }

    /// Set the [`AttributeKeyPart`], altering the [`Attribute`] part of this
    /// [`EntityKey`].
    pub fn set_attribute(self, attribute: AttributeKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice![inner, ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH].copy_from_slice(attribute.0);
        Self(inner)
    }

    /// Get the [`ValueDataType`] that is represented by this [`EntityKey`].
    pub fn value_type(&self) -> ValueDataType {
        self.0[VALUE_DATA_TYPE_OFFSET].into()
    }

    /// Set the [`ValueDataType`] that is represented by this [`EntityKey`].
    pub fn set_value_type(self, value_type: ValueDataType) -> Self {
        let mut inner = self.0;
        inner[VALUE_DATA_TYPE_OFFSET] = value_type.into();
        Self(inner)
    }

    /// Get a [`ValueReferenceKeyPart`] that refers to the [`Value`] part of
    /// this [`EntityKey`].
    pub fn value_reference(&self) -> ValueReferenceKeyPart {
        ValueReferenceKeyPart(array_ref![
            self.0,
            VALUE_REFERENCE_OFFSET,
            VALUE_REFERENCE_LENGTH
        ])
    }

    /// Set the [`ValueReferenceKeyPart`], altering the [`Value`] part of this
    /// [`EntityKey`].
    pub fn set_value_reference(self, value_reference: ValueReferenceKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice!(inner, VALUE_REFERENCE_OFFSET, VALUE_REFERENCE_LENGTH)
            .copy_from_slice(value_reference.0);
        Self(inner)
    }

    /// Sets the constrained parts of the given [`ArtifactSelector`] to the associated
    /// components of this [`EntityKey`]
    pub fn apply_selector(self, selector: &ArtifactSelector<Constrained>) -> Self {
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

impl From<&AttributeKey> for EntityKey {
    fn from(value: &AttributeKey) -> Self {
        EntityKey::default()
            .set_entity(value.entity())
            .set_attribute(value.attribute())
            .set_value_type(value.value_type())
            .set_value_reference(value.value_reference())
    }
}

impl From<&ValueKey> for EntityKey {
    fn from(value: &ValueKey) -> Self {
        EntityKey::default()
            .set_entity(value.entity())
            .set_attribute(value.attribute())
            .set_value_type(value.value_type())
            .set_value_reference(value.value_reference())
    }
}

impl From<&Artifact> for EntityKey {
    fn from(fact: &Artifact) -> Self {
        EntityKey::default()
            .set_entity(EntityKeyPart::from(&fact.of))
            .set_attribute(AttributeKeyPart::from(&fact.the))
            .set_value_type(fact.is.data_type())
            .set_value_reference(ValueReferenceKeyPart(&fact.is.to_reference()))
    }
}

impl From<&ArtifactSelector<Constrained>> for EntityKey {
    fn from(selector: &ArtifactSelector<Constrained>) -> Self {
        EntityKey::default().apply_selector(selector)
    }
}

impl Default for EntityKey {
    fn default() -> Self {
        Self(MINIMUM_ENTITY_KEY)
    }
}

impl AsRef<[u8]> for EntityKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for EntityKey {
    type Target = [u8; ENTITY_KEY_LENGTH];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Vec<u8>> for EntityKey {
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

impl KeyType for EntityKey {}
