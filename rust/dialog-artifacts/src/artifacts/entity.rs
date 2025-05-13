use std::{fmt::Display, str::FromStr};

use crate::{DialogArtifactsError, make_reference, make_seed, reference_type};
use base58::{FromBase58, ToBase58};
use dialog_storage::Blake3Hash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// An [`Entity`] is the subject part of a semantic triple. Internally, an
/// [`Entity`] is represented as a unique 32-byte hash.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Entity(Blake3Hash);

impl Default for Entity {
    fn default() -> Self {
        Self::new()
    }
}

reference_type!(Entity);

impl Entity {
    /// Generate a new, unique [`Entity`].
    pub fn new() -> Self {
        Self(make_reference(make_seed()))
    }
}

pub(crate) fn to_utf8<S>(entity: &Entity, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    entity.as_ref().to_base58().serialize(serializer)
}

pub(crate) fn from_utf8<'de, D>(deserializer: D) -> Result<Entity, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .from_base58()
        .map_err(|error| serde::de::Error::custom(format!("{:?}", error)))
        .and_then(|value| {
            Entity::try_from(value).map_err(|error| serde::de::Error::custom(format!("{}", error)))
        })
}

impl Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#{}...",
            self.0
                .iter()
                .take(6)
                .map(|byte| format!("{:X}", byte))
                .collect::<Vec<String>>()
                .concat()
        )
    }
}

impl FromStr for Entity {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Entity::try_from(s.from_base58().map_err(|error| {
            DialogArtifactsError::InvalidEntity(format!(
                "Could not convert from base58: {:?}",
                error
            ))
        })?)
    }
}

impl TryFrom<String> for Entity {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Entity> for String {
    fn from(value: Entity) -> Self {
        value.to_base58()
    }
}
