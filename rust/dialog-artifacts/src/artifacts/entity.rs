//! Entity types for semantic triple subjects.
//!
//! This module defines the [`Entity`] type which represents the subject part of
//! semantic triples. Entities are based on URIs and provide unique identification
//! for objects in the triple store.

use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    ops::Deref,
    str::FromStr,
};

use base58::{FromBase58, ToBase58};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::{DialogArtifactsError, ENTITY_LENGTH, Uri};

/// An [`Entity`] is the subject part of a semantic triple. An [`Entity`] can
/// be embodied by any valid [`Uri`].
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(into = "String", try_from = "String")]
pub struct Entity(Uri, [u8; ENTITY_LENGTH]);

/// Serializes an entity to UTF-8 format for CSV export.
pub(crate) fn to_utf8<S>(entity: &Entity, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    entity.0.serialize(serializer)
}

/// Deserializes an entity from UTF-8 format for CSV import.
pub(crate) fn from_utf8<'de, D>(deserializer: D) -> Result<Entity, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .parse::<Entity>()
        .map_err(|error| de::Error::custom(format!("{error:?}")))
}

impl AsRef<Entity> for Entity {
    fn as_ref(&self) -> &Entity {
        self
    }
}

impl Deref for Entity {
    type Target = Uri;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Uri> for Entity {
    type Error = DialogArtifactsError;

    fn try_from(value: Uri) -> Result<Self, Self::Error> {
        let bytes = value.key_bytes()?;
        Ok(Self(value, bytes))
    }
}

impl FromStr for Entity {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(Uri::from_str(s)?)
    }
}

impl TryFrom<String> for Entity {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<Vec<u8>> for Entity {
    type Error = DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Entity::try_from(
            String::from_utf8(value)
                .map_err(|error| DialogArtifactsError::InvalidEntity(format!("{error}")))?,
        )
    }
}

impl From<Entity> for String {
    fn from(value: Entity) -> Self {
        value.to_string()
    }
}

impl Display for Entity {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", **self)
    }
}

/// Scheme prefix for blob-reference entities.
const BLOB_SCHEME: &str = "blob:";

impl Entity {
    /// Initialize a new [`Entity`] with a randomly generated, globally unique
    /// URI. The URI is formatted as an ed25519 DID Key.
    pub fn new() -> Result<Entity, DialogArtifactsError> {
        Self::try_from(Uri::unique()?)
    }

    /// Get the [`Entity`] as a string reference
    pub fn as_str(&self) -> &str {
        (**self).as_str()
    }

    /// Get the raw byte representation of the [`Entity`] as it should be
    /// formatted for use in an index key.
    pub fn key_bytes(&self) -> &[u8; ENTITY_LENGTH] {
        &self.1
    }

    /// The canonical entity reference for a stored blob:
    /// `blob:<base58(hash)>`.
    pub fn from_blob(hash: &dialog_storage::Blake3Hash) -> Result<Entity, DialogArtifactsError> {
        format!("{}{}", BLOB_SCHEME, hash.to_base58()).parse()
    }

    /// The blob hash carried by a `blob:` entity, if this entity
    /// is one and its payload decodes to 32 base58 bytes.
    pub fn blob_hash(&self) -> Option<dialog_storage::Blake3Hash> {
        let payload = self.as_str().strip_prefix(BLOB_SCHEME)?;
        let bytes = payload.from_base58().ok()?;
        <[u8; 32]>::try_from(bytes).ok()
    }
}

impl Debug for Entity {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(&self.0.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_round_trips_a_blob_entity() {
        let hash: dialog_storage::Blake3Hash = [7u8; 32];
        let entity = Entity::from_blob(&hash).expect("constructs");
        assert!(entity.as_str().starts_with("blob:"));
        assert_eq!(entity.blob_hash(), Some(hash));
        // String round-trip: parse the display form back.
        let reparsed: Entity = entity.as_str().parse().expect("parses");
        assert_eq!(reparsed.blob_hash(), Some(hash));
    }

    #[test]
    fn it_returns_none_for_non_blob_entities() {
        let entity: Entity = "user:alice".parse().expect("parses");
        assert_eq!(entity.blob_hash(), None);
        // Garbage after the scheme is not a hash.
        let bogus: Entity = "blob:notbase58!!!".parse().expect("still a valid uri");
        assert_eq!(bogus.blob_hash(), None);
    }
}
