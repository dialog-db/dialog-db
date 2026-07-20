use std::ops::Deref;

use crate::KeyType;
use serde::{Deserialize, Serialize};

use crate::{
    Artifact, AttributeKeyPart, EntityKeyPart, ValueDataType, key::value_payload, key::varkey,
    key::varkey::KeyParts, key::varkey::ValuePayload,
};
use dialog_search_tree::Manifest;

use super::{Key, KeyView, KeyViewConstruct, KeyViewMut};

/// Tag byte that identifies entity-based index keys
pub const ENTITY_KEY_TAG: u8 = 0;

/// A [`KeyType`] that is used when constructing an index of the [`Entity`]s
/// of [`Artifact`]s.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntityKey<K>(pub K);

impl EntityKey<Key> {
    /// Converts this entity key into a generic key for storage in the search tree
    pub fn into_key(self) -> Key {
        self.0
    }
}

impl KeyViewConstruct for EntityKey<Key> {
    fn min() -> Self {
        Self(Key::from(varkey::build_key(&KeyParts::min(ENTITY_KEY_TAG))))
    }

    fn max() -> Self {
        Self(Key::from(varkey::build_key(&KeyParts::max(ENTITY_KEY_TAG))))
    }
}

impl<K> KeyView for EntityKey<K>
where
    K: AsRef<[u8]> + Clone,
{
    fn entity(&self) -> EntityKeyPart<'_> {
        EntityKeyPart(varkey::field(
            self.0.as_ref(),
            ENTITY_KEY_TAG,
            varkey::Field::Entity,
        ))
    }

    fn attribute(&self) -> AttributeKeyPart<'_> {
        AttributeKeyPart(varkey::field(
            self.0.as_ref(),
            ENTITY_KEY_TAG,
            varkey::Field::Attribute,
        ))
    }

    fn value_type(&self) -> ValueDataType {
        varkey::value_type(self.0.as_ref(), ENTITY_KEY_TAG)
    }

    fn value_payload(&self) -> &[u8] {
        varkey::value_payload(self.0.as_ref(), ENTITY_KEY_TAG)
    }

    fn value_is_spilled(&self) -> bool {
        varkey::value_is_spilled(self.0.as_ref(), ENTITY_KEY_TAG)
    }

    fn value_spill_hash(&self) -> Option<&[u8]> {
        varkey::value_spill_hash(self.0.as_ref(), ENTITY_KEY_TAG)
    }
}

impl KeyViewMut for EntityKey<Key> {
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
/// [`ENTITY_KEY_TAG`] ordering.
///
/// Every real key AND both bound sentinels parse cleanly (the max filler byte
/// is `0xFE`, chosen so the built sentinel never contains a `0x00 0xFF`
/// escape-collision at a field boundary), so chained `set_*` calls preserve
/// previously-set fields. The max-parts fallback is a safety net for
/// malformed input only; falling back to MIN here would silently collapse an
/// upper bound onto its lower bound.
fn rebuild(key: Key, mutate: impl FnOnce(&mut KeyParts)) -> Key {
    let mut parts =
        varkey::parse_key(key.as_ref()).unwrap_or_else(|| KeyParts::max(ENTITY_KEY_TAG));
    parts.tag = ENTITY_KEY_TAG;
    mutate(&mut parts);
    Key::from(varkey::build_key(&parts))
}

impl Default for EntityKey<Key> {
    fn default() -> Self {
        <Self as KeyViewConstruct>::min()
    }
}

impl<K> AsRef<[u8]> for EntityKey<K>
where
    K: AsRef<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<K> Deref for EntityKey<K>
where
    K: Deref<Target = [u8]>,
{
    type Target = K::Target;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl EntityKey<Key> {
    /// Builds the key for `fact` under the target tree's format `manifest`.
    ///
    /// The format is a parameter rather than a global because it is a property
    /// of the tree being written, not of the process: a fact written into a
    /// tree must be looked up under that same manifest, or a boundary-sized
    /// value inlines on one path and spills on the other and the lookup
    /// misses.
    pub fn from_artifact(fact: &Artifact, manifest: &Manifest) -> Self {
        EntityKey::<Key>::default()
            .set_entity(EntityKeyPart::from(&fact.of))
            .set_attribute(AttributeKeyPart::from(&fact.the))
            .set_value(fact.is.data_type(), value_payload(&fact.is, manifest))
    }
}

impl<K> KeyType for EntityKey<K>
where
    K: AsRef<[u8]> + Clone + KeyType,
{
    fn bytes(&self) -> &[u8] {
        self.0.as_ref()
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
