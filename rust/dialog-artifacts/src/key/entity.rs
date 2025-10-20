use std::ops::Deref;

use arrayref::array_ref;
use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};

use crate::{
    ATTRIBUTE_LENGTH, Artifact, AttributeKeyPart, ENTITY_LENGTH, EntityKeyPart, TAG_LENGTH,
    VALUE_REFERENCE_LENGTH, ValueDataType, mutable_slice,
};

use super::{
    Key, KeyBytes, KeyView, KeyViewConstruct, KeyViewMut, VALUE_DATA_TYPE_LENGTH,
    ValueReferenceKeyPart,
};

const ENTITY_OFFSET: usize = TAG_LENGTH;
const ATTRIBUTE_OFFSET: usize = TAG_LENGTH + ENTITY_LENGTH;
const VALUE_DATA_TYPE_OFFSET: usize = TAG_LENGTH + ENTITY_LENGTH + ATTRIBUTE_LENGTH;
const VALUE_REFERENCE_OFFSET: usize =
    TAG_LENGTH + ENTITY_LENGTH + ATTRIBUTE_LENGTH + VALUE_DATA_TYPE_LENGTH;

/// Tag byte that identifies entity-based index keys
pub const ENTITY_KEY_TAG: u8 = 0;

/// A [`KeyType`] that is used when constructing an index of the [`Entity`]s
/// of [`Artifact`]s.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntityKey<K>(pub K);

impl EntityKey<Key> {
    /// Converts this entity key into a generic key for storage in the prolly tree
    pub fn into_key(self) -> Key {
        self.0
    }
}

impl KeyViewConstruct for EntityKey<Key> {
    fn min() -> Self {
        Self(Key::min().set_tag(ENTITY_KEY_TAG))
    }

    fn max() -> Self {
        Self(Key::max().set_tag(ENTITY_KEY_TAG))
    }

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
}

impl<K> KeyView for EntityKey<K>
where
    K: AsRef<KeyBytes> + Clone,
{
    fn entity(&self) -> EntityKeyPart {
        EntityKeyPart(array_ref![self.0.as_ref(), ENTITY_OFFSET, ENTITY_LENGTH])
    }

    fn attribute(&self) -> AttributeKeyPart {
        AttributeKeyPart(array_ref![
            self.0.as_ref(),
            ATTRIBUTE_OFFSET,
            ATTRIBUTE_LENGTH
        ])
    }

    fn value_type(&self) -> ValueDataType {
        self.0.as_ref()[VALUE_DATA_TYPE_OFFSET].into()
    }

    fn value_reference(&self) -> ValueReferenceKeyPart {
        ValueReferenceKeyPart(array_ref![
            self.0.as_ref(),
            VALUE_REFERENCE_OFFSET,
            VALUE_REFERENCE_LENGTH
        ])
    }
}

impl<K> KeyViewMut for EntityKey<K>
where
    K: AsRef<KeyBytes> + AsMut<KeyBytes> + Clone,
{
    fn set_entity(mut self, entity: EntityKeyPart) -> Self {
        mutable_slice![self.0.as_mut(), ENTITY_OFFSET, ENTITY_LENGTH].copy_from_slice(entity.0);
        self
    }

    fn set_attribute(mut self, attribute: AttributeKeyPart) -> Self {
        mutable_slice![self.0.as_mut(), ATTRIBUTE_OFFSET, ATTRIBUTE_LENGTH]
            .copy_from_slice(attribute.0);
        self
    }

    fn set_value_type(mut self, value_type: ValueDataType) -> Self {
        self.0.as_mut()[VALUE_DATA_TYPE_OFFSET] = value_type.into();
        self
    }

    fn set_value_reference(mut self, value_reference: ValueReferenceKeyPart) -> Self {
        mutable_slice!(
            self.0.as_mut(),
            VALUE_REFERENCE_OFFSET,
            VALUE_REFERENCE_LENGTH
        )
        .copy_from_slice(value_reference.0);
        self
    }
}

impl Default for EntityKey<Key> {
    fn default() -> Self {
        <Self as KeyViewConstruct>::min()
    }
}

impl<K> AsRef<KeyBytes> for EntityKey<K>
where
    K: AsRef<KeyBytes>,
{
    fn as_ref(&self) -> &KeyBytes {
        self.0.as_ref()
    }
}

impl<K> Deref for EntityKey<K>
where
    K: Deref<Target = KeyBytes>,
{
    type Target = K::Target;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&Artifact> for EntityKey<Key> {
    fn from(fact: &Artifact) -> Self {
        EntityKey::<Key>::default()
            .set_entity(EntityKeyPart::from(&fact.of))
            .set_attribute(AttributeKeyPart::from(&fact.the))
            .set_value_type(fact.is.data_type())
            .set_value_reference(ValueReferenceKeyPart(&fact.is.to_reference()))
    }
}

impl<K> KeyType for EntityKey<K>
where
    K: AsRef<KeyBytes> + AsMut<KeyBytes> + Clone + KeyType,
{
    fn bytes(&self) -> &[u8] {
        self.as_ref().as_ref()
    }
}

impl<K> TryFrom<Vec<u8>> for EntityKey<K>
where
    K: KeyType,
{
    type Error = <K as TryFrom<Vec<u8>>>::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(EntityKey(K::try_from(value)?))
    }
}
