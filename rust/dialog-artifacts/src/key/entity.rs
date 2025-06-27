use std::ops::Deref;

use arrayref::array_ref;
use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};

use crate::{
    ATTRIBUTE_LENGTH, Artifact, AttributeKeyPart, DialogArtifactsError, ENTITY_LENGTH,
    EntityKeyPart, TAG_LENGTH, VALUE_REFERENCE_LENGTH, ValueDataType, mutable_slice,
};

use super::{Key, KeyView, VALUE_DATA_TYPE_LENGTH, ValueReferenceKeyPart};

const TAG_OFFSET: usize = 0;
const ENTITY_OFFSET: usize = TAG_LENGTH;
const ATTRIBUTE_OFFSET: usize = TAG_LENGTH + ENTITY_LENGTH;
const VALUE_DATA_TYPE_OFFSET: usize = TAG_LENGTH + ENTITY_LENGTH + ATTRIBUTE_LENGTH;
const VALUE_REFERENCE_OFFSET: usize =
    TAG_LENGTH + ENTITY_LENGTH + ATTRIBUTE_LENGTH + VALUE_DATA_TYPE_LENGTH;

pub const ENTITY_KEY_TAG: u8 = 0;

/// A [`KeyType`] that is used when constructing an index of the [`Entity`]s
/// of [`Artifact`]s.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntityKey(Key);

impl KeyView for EntityKey {
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
        Self(Key::min().set_tag(ENTITY_KEY_TAG))
    }

    fn max() -> Self {
        Self(Key::max().set_tag(ENTITY_KEY_TAG))
    }

    fn entity(&self) -> EntityKeyPart {
        EntityKeyPart(array_ref![*self.0, ENTITY_OFFSET, ENTITY_LENGTH])
    }

    fn set_entity(self, entity: EntityKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice![*inner, ENTITY_OFFSET, ENTITY_LENGTH].copy_from_slice(entity.0);
        Self(inner)
    }

    fn attribute(&self) -> AttributeKeyPart {
        AttributeKeyPart(array_ref![*self.0, ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH])
    }

    fn set_attribute(self, attribute: AttributeKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice![*inner, ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH].copy_from_slice(attribute.0);
        Self(inner)
    }

    fn value_type(&self) -> ValueDataType {
        (*self.0)[VALUE_DATA_TYPE_OFFSET].into()
    }

    fn set_value_type(self, value_type: ValueDataType) -> Self {
        let mut inner = self.0;
        (*inner)[VALUE_DATA_TYPE_OFFSET] = value_type.into();
        Self(inner)
    }

    fn value_reference(&self) -> ValueReferenceKeyPart {
        ValueReferenceKeyPart(array_ref![
            *self.0,
            VALUE_REFERENCE_OFFSET,
            VALUE_REFERENCE_LENGTH
        ])
    }

    fn set_value_reference(self, value_reference: ValueReferenceKeyPart) -> Self {
        let mut inner = self.0;
        mutable_slice!(*inner, VALUE_REFERENCE_OFFSET, VALUE_REFERENCE_LENGTH)
            .copy_from_slice(value_reference.0);
        Self(inner)
    }
}

impl Default for EntityKey {
    fn default() -> Self {
        <Self as KeyView>::min()
    }
}

impl AsRef<[u8]> for EntityKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for EntityKey {
    type Target = <Key as Deref>::Target;

    fn deref(&self) -> &Self::Target {
        &*self.0
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

impl KeyType for EntityKey {}

impl TryFrom<Vec<u8>> for EntityKey {
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
