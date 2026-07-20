use std::ops::Deref;

use crate::KeyType;
use serde::{Deserialize, Serialize};

use crate::{
    Artifact, AttributeKeyPart, EntityKeyPart, ValueDataType, key::inline_threshold,
    key::value_payload, key::varkey, key::varkey::KeyParts, key::varkey::ValuePayload,
};

use super::{Key, KeyView, KeyViewConstruct, KeyViewMut};

/// Tag byte that identifies value-based index keys
pub const VALUE_KEY_TAG: u8 = 2;

/// A [`KeyType`] that is used when constructing an index of the [`Value`]s
/// of [`Artifact`]s.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ValueKey<K>(pub K);

impl ValueKey<Key> {
    /// Converts this value key into a generic key for storage in the search tree
    pub fn into_key(self) -> Key {
        self.0
    }
}

impl KeyViewConstruct for ValueKey<Key> {
    fn min() -> Self {
        Self(Key::from(varkey::build_key(&KeyParts::min(VALUE_KEY_TAG))))
    }

    fn max() -> Self {
        Self(Key::from(varkey::build_key(&KeyParts::max(VALUE_KEY_TAG))))
    }
}

impl<K> KeyView for ValueKey<K>
where
    K: AsRef<[u8]> + Clone,
{
    fn entity(&self) -> EntityKeyPart<'_> {
        EntityKeyPart(varkey::field(
            self.0.as_ref(),
            VALUE_KEY_TAG,
            varkey::Field::Entity,
        ))
    }

    fn attribute(&self) -> AttributeKeyPart<'_> {
        AttributeKeyPart(varkey::field(
            self.0.as_ref(),
            VALUE_KEY_TAG,
            varkey::Field::Attribute,
        ))
    }

    fn value_type(&self) -> ValueDataType {
        varkey::value_type(self.0.as_ref(), VALUE_KEY_TAG)
    }

    fn value_payload(&self) -> &[u8] {
        varkey::value_payload(self.0.as_ref(), VALUE_KEY_TAG)
    }

    fn value_is_spilled(&self) -> bool {
        varkey::value_is_spilled(self.0.as_ref(), VALUE_KEY_TAG)
    }

    fn value_spill_hash(&self) -> Option<&[u8]> {
        varkey::value_spill_hash(self.0.as_ref(), VALUE_KEY_TAG)
    }
}

impl KeyViewMut for ValueKey<Key> {
    fn set_entity(self, entity: EntityKeyPart) -> Self {
        Self(rebuild(self.0, |parts| parts.entity = entity.0.to_vec()))
    }

    fn set_attribute(self, attribute: AttributeKeyPart) -> Self {
        Self(rebuild(self.0, |parts| {
            parts.attribute = attribute.0.to_vec()
        }))
    }

    fn set_value(self, value_type: ValueDataType, value: ValuePayload) -> Self {
        Self(rebuild(self.0, |parts| {
            parts.value_type = value_type;
            parts.value = value;
        }))
    }
}

/// Parse `key`'s components, mutate them, and rebuild the key bytes for the
/// [`VALUE_KEY_TAG`] ordering.
///
/// See the note on `key::entity::rebuild`: real keys and both bound sentinels
/// parse, so chained `set_*` calls preserve previously-set fields; the
/// max-parts fallback is a malformed-input safety net.
fn rebuild(key: Key, mutate: impl FnOnce(&mut KeyParts)) -> Key {
    let mut parts = varkey::parse_key(key.as_ref()).unwrap_or_else(|| KeyParts::max(VALUE_KEY_TAG));
    parts.tag = VALUE_KEY_TAG;
    mutate(&mut parts);
    Key::from(varkey::build_key(&parts))
}

impl Default for ValueKey<Key> {
    fn default() -> Self {
        <Self as KeyViewConstruct>::min()
    }
}

impl<K> AsRef<[u8]> for ValueKey<K>
where
    K: AsRef<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<K> Deref for ValueKey<K>
where
    K: Deref<Target = [u8]>,
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
            .set_value(
                fact.is.data_type(),
                value_payload(&fact.is, inline_threshold()),
            )
    }
}

impl<K> KeyType for ValueKey<K>
where
    K: AsRef<[u8]> + Clone + KeyType,
{
    fn bytes(&self) -> &[u8] {
        self.0.as_ref()
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
