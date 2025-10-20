//! Entity types for semantic triple subjects.
//!
//! This module defines the [`Entity`] type which represents the subject part of
//! semantic triples. Entities are based on URIs and provide unique identification
//! for objects in the triple store.

use std::{fmt::Display, ops::Deref, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{DialogArtifactsError, ENTITY_LENGTH, Uri};

/// An [`Entity`] is the subject part of a semantic triple. An [`Entity`] can
/// be embodied by any valid [`Uri`].
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
        .map_err(|error| serde::de::Error::custom(format!("{:?}", error)))
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", **self)
    }
}

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
}
