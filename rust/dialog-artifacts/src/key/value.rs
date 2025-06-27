use std::ops::Deref;

use arrayref::array_ref;
use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};

use crate::{
    ATTRIBUTE_LENGTH, Artifact, AttributeKeyPart, DialogArtifactsError, ENTITY_LENGTH, TAG_LENGTH,
    VALUE_DATA_TYPE_LENGTH, VALUE_REFERENCE_LENGTH, ValueDataType, ValueReferenceKeyPart,
    mutable_slice,
};

use super::{EntityKeyPart, Key, KeyView};

const TAG_OFFSET: usize = 0;
const VALUE_DATA_TYPE_OFFSET: usize = TAG_LENGTH;
const VALUE_REFERENCE_OFFSET: usize = TAG_LENGTH + VALUE_DATA_TYPE_LENGTH;
const ATTRIBUTE_OFFSET: usize = TAG_LENGTH + VALUE_DATA_TYPE_LENGTH + VALUE_REFERENCE_LENGTH;
const ENTITY_OFFSET: usize =
    TAG_LENGTH + VALUE_DATA_TYPE_LENGTH + VALUE_REFERENCE_LENGTH + ATTRIBUTE_LENGTH;

pub const VALUE_KEY_TAG: u8 = 2;

/// A [`KeyType`] that is used when constructing an index of the [`Value`]s
/// of [`Artifact`]s.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ValueKey(Key);

impl KeyView for ValueKey {
    fn from_parts(
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

    fn min() -> Self {
        Self(Key::min().set_tag(VALUE_KEY_TAG))
    }

    fn max() -> Self {
        Self(Key::max().set_tag(VALUE_KEY_TAG))
    }

    fn attribute(&self) -> AttributeKeyPart {
        AttributeKeyPart(array_ref![self.0, ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH])
    }

    fn entity(&self) -> EntityKeyPart {
        EntityKeyPart(array_ref![self.0, ENTITY_OFFSET, ENTITY_LENGTH])
    }

    fn set_attribute(self, attribute: AttributeKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice![inner, ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH].copy_from_slice(attribute.0);
        Self(inner)
    }

    fn set_entity(self, entity: EntityKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice![inner, ENTITY_OFFSET, ENTITY_LENGTH].copy_from_slice(entity.0);
        Self(inner)
    }

    fn value_reference(&self) -> ValueReferenceKeyPart {
        ValueReferenceKeyPart(array_ref![
            self.0,
            VALUE_REFERENCE_OFFSET,
            VALUE_REFERENCE_LENGTH
        ])
    }

    fn set_value_reference(self, value: ValueReferenceKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice!(inner, VALUE_REFERENCE_OFFSET, VALUE_REFERENCE_LENGTH)
            .copy_from_slice(value.0);
        Self(inner)
    }

    fn value_type(&self) -> ValueDataType {
        self.0[VALUE_DATA_TYPE_OFFSET].into()
    }

    fn set_value_type(self, value_type: ValueDataType) -> Self {
        let mut inner = self.0;
        inner[VALUE_DATA_TYPE_OFFSET] = value_type.into();
        Self(inner)
    }
}

impl Default for ValueKey {
    fn default() -> Self {
        <Self as KeyView>::min()
    }
}

impl AsRef<[u8]> for ValueKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for ValueKey {
    type Target = <Key as Deref>::Target;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl From<&Artifact> for ValueKey {
    fn from(fact: &Artifact) -> Self {
        let value_reference = fact.is.to_reference();
        ValueKey::default()
            .set_value_type(fact.is.data_type())
            .set_value_reference(ValueReferenceKeyPart(&value_reference))
            .set_attribute(AttributeKeyPart::from(&fact.the))
            .set_entity(EntityKeyPart::from(&fact.of))
    }
}

impl KeyType for ValueKey {}

impl TryFrom<Vec<u8>> for ValueKey {
    type Error = DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(Key(value.try_into().map_err(|value: Vec<u8>| {
            DialogArtifactsError::InvalidKey(format!(
                "Wrong byte length for entity key: {}",
                value.len()
            ))
        })?)))
    }
}
