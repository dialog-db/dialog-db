//! Artifact data structure representing semantic triples.
//!
//! This module defines the core [`Artifact`] type which represents a semantic triple
//! (subject-predicate-object) in the Dialog database. Artifacts are the fundamental
//! units of data storage and retrieval.

use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    str::{FromStr, from_utf8},
};

use serde::{Deserialize, Serialize};

use crate::{
    ATTRIBUTE_KEY_TAG, AttributeKey, Datum, DialogArtifactsError, ENTITY_KEY_TAG, EntityKey, Key,
    KeyView, VALUE_KEY_TAG, ValueKey, decode_value,
};

use super::{Attribute, Cause, Entity, Value};

/// A [`Artifact`] embodies a datum - a semantic triple - that may be stored in or
/// retrieved from a [`ArtifactStore`].
#[derive(Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Artifact {
    /// The [`Attribute`] of the [`Artifact`]; the predicate of the triple
    pub the: Attribute,
    /// The [`Entity`] of the [`Artifact`]; the subject of the triple
    #[serde(
        serialize_with = "crate::artifacts::entity::to_utf8",
        deserialize_with = "crate::artifacts::entity::from_utf8"
    )]
    pub of: Entity,
    /// The [`Value`] of the [`Artifact`]; the object of the triple
    // TODO: This is in support of Artifacts<->CSV but we probably want
    // different (de)serialization for Artifacts<->JSON (assuming we ever
    // want that.
    #[serde(
        serialize_with = "crate::artifacts::value::to_utf8",
        deserialize_with = "crate::artifacts::value::from_utf8"
    )]
    pub is: Value,
    /// The [`Cause`] of the [`Artifact`], which is a reference to an ancester
    /// version with a different [`Value`].
    pub cause: Option<Cause>,
}

impl Debug for Artifact {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Artifact")
            .field("the", &self.the.to_string())
            .field("of", &self.of.to_string())
            .field("is", &self.is)
            .field("cause", &self.cause.as_ref().map(|cause| cause.to_string()))
            .finish()
    }
}

impl Display for Artifact {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let attribute = self.the.to_string();
        let entity = format!("{}", &self.of);
        let value = self.is.to_utf8();

        write!(f, "Artifact: the '{attribute}' of '{entity}' is '{value}'")
    }
}

impl Artifact {
    /// Reconstructs a fact from an index `key` and its stored [`Datum`] payload.
    ///
    /// The entity, attribute, and value type come from the key (stored
    /// losslessly and order-preservingly by [`EntityKey::from`] and friends).
    /// The value is decoded from the key's inline order-preserving payload when
    /// it fits inline, or taken from `datum.value` (the raw bytes carried
    /// because the key holds only a 32-byte reference) when it spilled.
    pub fn from_key_datum(key: &Key, datum: &Datum) -> Result<Self, DialogArtifactsError> {
        // View the key under its own ordering so the accessors return the
        // right components regardless of which index this entry came from, and
        // reconstruct through the shared helper.
        match key.tag() {
            ENTITY_KEY_TAG => reconstruct(EntityKey(key), datum),
            ATTRIBUTE_KEY_TAG => reconstruct(AttributeKey(key), datum),
            VALUE_KEY_TAG => reconstruct(ValueKey(key), datum),
            tag => Err(DialogArtifactsError::InvalidKey(format!(
                "unknown index key tag {tag}"
            ))),
        }
    }
}

/// Reconstructs an [`Artifact`] from a key view and its payload. The entity,
/// attribute, and value type come from the key; the value is decoded inline
/// from the key or taken from `datum.value` when it spilled.
fn reconstruct<K: KeyView>(key: K, datum: &Datum) -> Result<Artifact, DialogArtifactsError> {
    let of = Entity::from_str(from_utf8(key.entity().raw()).map_err(|error| {
        DialogArtifactsError::InvalidEntity(format!("entity key is not UTF-8: {error}"))
    })?)?;
    let the = Attribute::from_str(from_utf8(key.attribute().raw()).map_err(|error| {
        DialogArtifactsError::InvalidAttribute(format!("attribute key is not UTF-8: {error}"))
    })?)?;
    let value_type = key.value_type();

    let is = if key.value_is_spilled() {
        // The key carries only a reference; the raw value bytes travel in the
        // payload.
        let bytes = datum.value.clone().ok_or_else(|| {
            DialogArtifactsError::InvalidValue("spilled value key has no payload bytes".to_string())
        })?;
        Value::try_from((value_type, bytes))?
    } else {
        // Decode the inline order-preserving value from the key.
        let (value, rest) = decode_value(value_type, key.value_payload()).ok_or_else(|| {
            DialogArtifactsError::InvalidValue("inline value payload did not decode".to_string())
        })?;
        if !rest.is_empty() {
            return Err(DialogArtifactsError::InvalidValue(
                "inline value payload had trailing bytes".to_string(),
            ));
        }
        value
    };

    Ok(Artifact {
        the,
        of,
        is,
        cause: datum.cause.clone(),
    })
}
