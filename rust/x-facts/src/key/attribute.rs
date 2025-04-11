use std::ops::Deref;

use arrayref::array_ref;
use x_prolly_tree::KeyType;

use crate::{
    ATTRIBUTE_KEY_LENGTH, ATTRIBUTE_LENGTH, AttributeKeyPart, ENTITY_LENGTH, EntityKeyPart, Fact,
    ValueDataType, XFactsError, mutable_slice,
};

const ATTRIBUTE_KEY_ATTRIBUTE_OFFSET: usize = 0;
const ATTRIBUTE_KEY_ENTITY_OFFSET: usize = ATTRIBUTE_LENGTH;
const ATTRIBUTE_KEY_VALUE_DATA_TYPE_OFFSET: usize = ENTITY_LENGTH + ATTRIBUTE_LENGTH;

const MINIMUM_ATTRIBUTE_KEY: [u8; ATTRIBUTE_KEY_LENGTH] = [u8::MIN; ATTRIBUTE_KEY_LENGTH];
const MAXIMUM_ATTRIBUTE_KEY: [u8; ATTRIBUTE_KEY_LENGTH] = [u8::MAX; ATTRIBUTE_KEY_LENGTH];

/// A [`KeyType`] that is used when constructing an index of the [`Attribute`]s
/// of [`Fact`]s.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AttributeKey([u8; ATTRIBUTE_KEY_LENGTH]);

impl AttributeKey {
    /// Construct an [`AttributeKey`] from the provided component key parts.
    pub fn from_parts(
        attribute: AttributeKeyPart,
        entity: EntityKeyPart,
        value_type: ValueDataType,
    ) -> Self {
        let mut inner = MINIMUM_ATTRIBUTE_KEY;
        mutable_slice![inner, ATTRIBUTE_KEY_ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH]
            .copy_from_slice(attribute.0);
        mutable_slice![inner, ATTRIBUTE_KEY_ENTITY_OFFSET, ENTITY_LENGTH].copy_from_slice(entity.0);
        inner[ATTRIBUTE_KEY_VALUE_DATA_TYPE_OFFSET] = value_type.into();
        Self(inner)
    }

    /// Construct the lowest possible [`AttributeKey`] (all bits are zero)
    pub fn min() -> Self {
        Self(MINIMUM_ATTRIBUTE_KEY)
    }

    /// Construct the highest possible [`AttributeKey`] (all bits are one)
    pub fn max() -> Self {
        Self(MAXIMUM_ATTRIBUTE_KEY)
    }

    /// Get an [`AttributeKeyPart`] that refers to the [`Attribute`] part of
    /// this [`AttributeKey`].
    pub fn attribute(&self) -> AttributeKeyPart {
        AttributeKeyPart(array_ref![
            self.0,
            ATTRIBUTE_KEY_ATTRIBUTE_OFFSET,
            ATTRIBUTE_LENGTH
        ])
    }

    /// Set the [`AttributeKeyPart`], altering the [`Attribute`] part of this
    /// [`AttributeKey`].
    pub fn set_attribute(&self, attribute: AttributeKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice![inner, ATTRIBUTE_KEY_ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH]
            .copy_from_slice(attribute.0);
        Self(inner)
    }

    /// Get an [`EntityKeyPart`] that refers to the [`Entity`] part of this
    /// [`AttributeKey`].
    pub fn entity(&self) -> EntityKeyPart {
        EntityKeyPart(array_ref![
            self.0,
            ATTRIBUTE_KEY_ENTITY_OFFSET,
            ENTITY_LENGTH
        ])
    }

    /// Set the [`EntityKeyPart`], altering the [`Entity`] part of this
    /// [`AttributeKey`].
    pub fn set_entity(&self, entity: EntityKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice![inner, ATTRIBUTE_KEY_ENTITY_OFFSET, ENTITY_LENGTH].copy_from_slice(entity.0);
        Self(inner)
    }

    /// Get the [`ValueDataType`] that is represented by this [`AttributeKey`].
    pub fn value_type(&self) -> ValueDataType {
        self.0[ATTRIBUTE_KEY_VALUE_DATA_TYPE_OFFSET].into()
    }

    /// Set the [`ValueDataType`] that is represented by this [`AttributeKey`].
    pub fn set_value_type(&self, value_type: ValueDataType) -> Self {
        let mut inner = self.0;
        inner[ATTRIBUTE_KEY_VALUE_DATA_TYPE_OFFSET] = value_type.into();
        Self(inner)
    }
}

impl Default for AttributeKey {
    fn default() -> Self {
        Self::min()
    }
}

impl AsRef<[u8]> for AttributeKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for AttributeKey {
    type Target = [u8; ATTRIBUTE_KEY_LENGTH];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&Fact> for AttributeKey {
    fn from(fact: &Fact) -> Self {
        AttributeKey::default()
            .set_attribute(AttributeKeyPart::from(&fact.the))
            .set_entity(EntityKeyPart(&fact.of))
            .set_value_type(fact.is.data_type())
    }
}

impl TryFrom<Vec<u8>> for AttributeKey {
    type Error = XFactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into().map_err(|value: Vec<u8>| {
            XFactsError::InvalidKey(format!(
                "Wrong byte length for attribute key: {}",
                value.len()
            ))
        })?))
    }
}

impl KeyType for AttributeKey {}
