use std::ops::Deref;

use arrayref::array_ref;
use x_prolly_tree::KeyType;

use crate::{
    ATTRIBUTE_LENGTH, Artifact, AttributeKeyPart, VALUE_DATA_TYPE_LENGTH, VALUE_KEY_LENGTH,
    VALUE_REFERENCE_LENGTH, ValueDataType, ValueReferenceKeyPart, XArtifactsError, mutable_slice,
};

const VALUE_KEY_VALUE_DATA_TYPE_OFFSET: usize = 0;
const VALUE_KEY_VALUE_REFERENCE_OFFSET: usize = VALUE_DATA_TYPE_LENGTH;
const VALUE_KEY_ATTRIBUTE_OFFSET: usize = VALUE_DATA_TYPE_LENGTH + VALUE_REFERENCE_LENGTH;
const MINIMUM_VALUE_KEY: [u8; VALUE_KEY_LENGTH] = [u8::MIN; VALUE_KEY_LENGTH];
const MAXIMUM_VALUE_KEY: [u8; VALUE_KEY_LENGTH] = [u8::MAX; VALUE_KEY_LENGTH];

/// A [`KeyType`] that is used when constructing an index of the [`Value`]s
/// of [`Artifact`]s.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValueKey([u8; VALUE_KEY_LENGTH]);

impl ValueKey {
    /// Construct a [`ValueKey`] from the provided component key parts.
    pub fn from_parts(
        value_type: ValueDataType,
        value_reference: ValueReferenceKeyPart,
        attribute: AttributeKeyPart,
    ) -> Self {
        let mut inner = MINIMUM_VALUE_KEY;
        inner[VALUE_KEY_VALUE_DATA_TYPE_OFFSET] = value_type.into();
        mutable_slice![
            inner,
            VALUE_KEY_VALUE_REFERENCE_OFFSET,
            VALUE_REFERENCE_LENGTH
        ]
        .copy_from_slice(value_reference.0);
        mutable_slice![inner, VALUE_KEY_ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH]
            .copy_from_slice(attribute.0);
        Self(inner)
    }

    /// Construct the lowest possible [`ValueKey`] (all bits are zero)
    pub fn min() -> Self {
        Self(MINIMUM_VALUE_KEY)
    }

    /// Construct the highest possible [`ValueKey`] (all bits are one)
    pub fn max() -> Self {
        Self(MAXIMUM_VALUE_KEY)
    }

    /// Get an [`AttributeKeyPart`] that refers to the [`Attribute`] part of
    /// this [`ValueKey`].
    pub fn attribute(&self) -> AttributeKeyPart {
        AttributeKeyPart(array_ref![
            self.0,
            VALUE_KEY_ATTRIBUTE_OFFSET,
            ATTRIBUTE_LENGTH
        ])
    }

    /// Set the [`AttributeKeyPart`], altering the [`Attribute`] part of this
    /// [`ValueKey`].
    pub fn set_attribute(&self, attribute: AttributeKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice![inner, VALUE_KEY_ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH]
            .copy_from_slice(attribute.0);
        Self(inner)
    }

    /// Get a [`ValueReferenceKeyPart`] that refers to the [`Value`] part of
    /// this [`ValueKey`].
    pub fn value_reference(&self) -> ValueReferenceKeyPart {
        ValueReferenceKeyPart(array_ref![
            self.0,
            VALUE_KEY_VALUE_REFERENCE_OFFSET,
            VALUE_REFERENCE_LENGTH
        ])
    }

    /// Set the [`ValueReferenceKeyPart`], altering the [`Value`] part of this
    /// [`ValueKey`].
    pub fn set_value_reference(&self, value: ValueReferenceKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice!(
            inner,
            VALUE_KEY_VALUE_REFERENCE_OFFSET,
            VALUE_REFERENCE_LENGTH
        )
        .copy_from_slice(value.0);
        Self(inner)
    }

    /// Get the [`ValueDataType`] that is represented by this [`ValueKey`].
    pub fn value_type(&self) -> ValueDataType {
        self.0[VALUE_KEY_VALUE_DATA_TYPE_OFFSET].into()
    }

    /// Set the [`ValueDataType`] that is represented by this [`ValueKey`].
    pub fn set_value_type(&self, value_type: ValueDataType) -> Self {
        let mut inner = self.0;
        inner[VALUE_KEY_VALUE_DATA_TYPE_OFFSET] = value_type.into();
        Self(inner)
    }
}

impl Default for ValueKey {
    fn default() -> Self {
        Self::min()
    }
}

impl AsRef<[u8]> for ValueKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for ValueKey {
    type Target = [u8; VALUE_KEY_LENGTH];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&Artifact> for ValueKey {
    fn from(fact: &Artifact) -> Self {
        let value_reference = fact.is.to_reference();
        ValueKey::default()
            .set_value_type(fact.is.data_type())
            .set_value_reference(ValueReferenceKeyPart(&value_reference))
            .set_attribute(AttributeKeyPart::from(&fact.the))
    }
}

impl TryFrom<Vec<u8>> for ValueKey {
    type Error = XArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into().map_err(|value: Vec<u8>| {
            XArtifactsError::InvalidKey(format!("Wrong byte length for value key: {}", value.len()))
        })?))
    }
}

impl KeyType for ValueKey {}
