use std::ops::Deref;

use arrayref::array_ref;
use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};

use crate::{
    ATTRIBUTE_LENGTH, Artifact, AttributeKeyPart, ENTITY_LENGTH, TAG_LENGTH,
    VALUE_DATA_TYPE_LENGTH, VALUE_REFERENCE_LENGTH, ValueDataType, ValueReferenceKeyPart,
    mutable_slice,
};

use super::{EntityKeyPart, Key, KeyBytes, KeyView, KeyViewConstruct, KeyViewMut};

const VALUE_DATA_TYPE_OFFSET: usize = TAG_LENGTH;
const VALUE_REFERENCE_OFFSET: usize = TAG_LENGTH + VALUE_DATA_TYPE_LENGTH;
const ATTRIBUTE_OFFSET: usize = TAG_LENGTH + VALUE_DATA_TYPE_LENGTH + VALUE_REFERENCE_LENGTH;
const ENTITY_OFFSET: usize =
    TAG_LENGTH + VALUE_DATA_TYPE_LENGTH + VALUE_REFERENCE_LENGTH + ATTRIBUTE_LENGTH;

/// Tag byte that identifies value-based index keys
pub const VALUE_KEY_TAG: u8 = 2;

/// A [`KeyType`] that is used when constructing an index of the [`Value`]s
/// of [`Artifact`]s.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ValueKey<K>(pub K);

impl ValueKey<Key> {
    /// Converts this value key into a generic key for storage in the prolly tree
    pub fn into_key(self) -> Key {
        self.0
    }
}

impl KeyViewConstruct for ValueKey<Key> {
    fn min() -> Self {
        Self(Key::min().set_tag(VALUE_KEY_TAG))
    }

    fn max() -> Self {
        Self(Key::max().set_tag(VALUE_KEY_TAG))
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

impl<K> KeyView for ValueKey<K>
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

impl<K> KeyViewMut for ValueKey<K>
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

impl Default for ValueKey<Key> {
    fn default() -> Self {
        <Self as KeyViewConstruct>::min()
    }
}

impl<K> AsRef<KeyBytes> for ValueKey<K>
where
    K: AsRef<KeyBytes>,
{
    fn as_ref(&self) -> &KeyBytes {
        self.0.as_ref()
    }
}

impl<K> Deref for ValueKey<K>
where
    K: Deref<Target = KeyBytes>,
{
    type Target = K::Target;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&Artifact> for ValueKey<Key> {
    fn from(fact: &Artifact) -> Self {
        ValueKey::<Key>::default()
            .set_entity(EntityKeyPart::from(&fact.of))
            .set_attribute(AttributeKeyPart::from(&fact.the))
            .set_value_type(fact.is.data_type())
            .set_value_reference(ValueReferenceKeyPart(&fact.is.to_reference()))
    }
}

impl<K> KeyType for ValueKey<K>
where
    K: AsRef<KeyBytes> + AsMut<KeyBytes> + Clone + KeyType,
{
    fn bytes(&self) -> &[u8] {
        self.as_ref().as_ref()
    }
}

impl<K> TryFrom<Vec<u8>> for ValueKey<K>
where
    K: KeyType,
{
    type Error = <K as TryFrom<Vec<u8>>>::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(ValueKey(K::try_from(value)?))
    }
}
