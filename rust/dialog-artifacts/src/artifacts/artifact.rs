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
    key::varkey::{self, KeyRef, ValueRef},
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
    /// Reconstructs a fact from an index `key` and its stored [`Datum`] payload,
    /// for a key whose value is stored INLINE.
    ///
    /// The entity, attribute, value type, and value all come from the key
    /// (stored losslessly and order-preservingly by [`EntityKey::from`] and
    /// friends). This is a convenience over
    /// [`Artifact::from_key_datum_with_value`] with no spilled bytes; if the key
    /// is spilled (`key.value_is_spilled()`) it errors, because the raw value
    /// bytes live in a separate archive block the caller must fetch. Use
    /// [`Artifact::from_key_datum_with_value`] for the general case.
    pub fn from_key_datum(key: &Key, datum: &Datum) -> Result<Self, DialogArtifactsError> {
        Self::from_key_datum_with_value(key, datum, None)
    }

    /// Reconstructs a fact from an index `key`, its stored [`Datum`] payload,
    /// and, for a *spilled* key, the raw value bytes fetched from the archive
    /// block store.
    ///
    /// The entity, attribute, and value type come from the key. The value is
    /// decoded from the key's inline order-preserving payload when it fits
    /// inline (in which case `spilled` is ignored), or reconstructed from
    /// `spilled` (the block fetched by the key's 32-byte reference) when the key
    /// is spilled. Pass `spilled = None` for inline keys; pass `Some(bytes)` for
    /// spilled keys (an inline key with `Some` bytes just ignores them, and a
    /// spilled key with `None` errors).
    pub fn from_key_datum_with_value(
        key: &Key,
        datum: &Datum,
        spilled: Option<Vec<u8>>,
    ) -> Result<Self, DialogArtifactsError> {
        // Parse the key ONCE into borrowed components. Every ordering
        // (EAV/AEV/VAE) decodes to the same logical
        // entity/attribute/value_type/payload, so a single `parse_key_ref` walk
        // yields all fields the reconstruction needs, borrowing them from the
        // key bytes (no per-field allocation) except an escaped entity/
        // attribute. This replaces the previous per-field `KeyView` accessors,
        // each of which re-ran `split_components` (a fresh alloc + full key
        // walk) — a scan reconstructing N facts paid ~6 such walks per fact,
        // which dominated the scan cost on the variable-length M3 key format.
        let parts = varkey::parse_key_ref(key.as_ref()).ok_or_else(|| {
            DialogArtifactsError::InvalidKey("key did not parse into components".to_string())
        })?;
        reconstruct(&parts, datum, spilled)
    }

    /// Reconstructs an [`Artifact`] from an already-parsed [`KeyRef`], a datum,
    /// and (for a spilled value) the fetched block bytes. The scan path parses
    /// each key once into a [`KeyRef`] for matching and spill resolution, then
    /// hands that same parse here so reconstruction adds no further key walk.
    pub fn from_key_ref_datum_value(
        parts: &KeyRef<'_>,
        datum: &Datum,
        spilled: Option<Vec<u8>>,
    ) -> Result<Self, DialogArtifactsError> {
        reconstruct(parts, datum, spilled)
    }

    /// Reconstructs a fact for display when the raw value bytes are not
    /// available: the entity, attribute, and cause come from the key and
    /// payload, and a spilled value is stood in for by a `<spilled value>`
    /// placeholder string. For a sync render path (the diagnose TUI) that has no
    /// store to fetch the spilled block. An inline key reconstructs its real
    /// value as usual.
    pub fn from_key_datum_placeholder(
        key: &Key,
        datum: &Datum,
    ) -> Result<Self, DialogArtifactsError> {
        // Reconstruct entity/attribute from the key under its ordering; whether
        // the value spilled is read from that same view. If it did not spill,
        // fall through to the normal inline reconstruction.
        let (of, the, spilled) = match key.tag() {
            ENTITY_KEY_TAG => {
                let view = EntityKey(key);
                let (of, the) = entity_attribute(view.clone())?;
                (of, the, view.value_is_spilled())
            }
            ATTRIBUTE_KEY_TAG => {
                let view = AttributeKey(key);
                let (of, the) = entity_attribute(view.clone())?;
                (of, the, view.value_is_spilled())
            }
            VALUE_KEY_TAG => {
                let view = ValueKey(key);
                let (of, the) = entity_attribute(view.clone())?;
                (of, the, view.value_is_spilled())
            }
            tag => {
                return Err(DialogArtifactsError::InvalidKey(format!(
                    "unknown index key tag {tag}"
                )));
            }
        };
        if !spilled {
            return Self::from_key_datum(key, datum);
        }
        Ok(Artifact {
            the,
            of,
            is: Value::String("<spilled value>".to_string()),
            cause: datum.cause.clone(),
        })
    }
}

/// Extracts the entity and attribute from a key view, decoding the raw UTF-8
/// key columns.
fn entity_attribute<K: KeyView>(key: K) -> Result<(Entity, Attribute), DialogArtifactsError> {
    let of = Entity::from_str(from_utf8(key.entity().raw()).map_err(|error| {
        DialogArtifactsError::InvalidEntity(format!("entity key is not UTF-8: {error}"))
    })?)?;
    let the = Attribute::from_str(from_utf8(key.attribute().raw()).map_err(|error| {
        DialogArtifactsError::InvalidAttribute(format!("attribute key is not UTF-8: {error}"))
    })?)?;
    Ok((of, the))
}

/// Reconstructs an [`Artifact`] from a single borrowed parse of the key's
/// components and its payload. The entity, attribute, and value type come from
/// the parsed key; the value is decoded inline from the key's payload or taken
/// from `spilled` (the archive block bytes) when it spilled.
///
/// Takes the already-parsed [`KeyRef`] so the whole reconstruction is a single
/// key walk that borrows the key bytes; see
/// [`Artifact::from_key_datum_with_value`] for why.
fn reconstruct(
    parts: &KeyRef<'_>,
    datum: &Datum,
    spilled: Option<Vec<u8>>,
) -> Result<Artifact, DialogArtifactsError> {
    let of = Entity::from_str(from_utf8(&parts.entity).map_err(|error| {
        DialogArtifactsError::InvalidEntity(format!("entity key is not UTF-8: {error}"))
    })?)?;
    let the = Attribute::from_str(from_utf8(&parts.attribute).map_err(|error| {
        DialogArtifactsError::InvalidAttribute(format!("attribute key is not UTF-8: {error}"))
    })?)?;

    let is = match parts.value {
        // The key carries the value's prefix and hash; the raw value bytes
        // live in a content-addressed archive block the caller fetched and
        // passed in.
        ValueRef::Spilled { .. } => {
            let bytes = spilled.ok_or_else(|| {
                DialogArtifactsError::InvalidValue(
                    "spilled value key has no fetched block bytes".to_string(),
                )
            })?;
            Value::try_from((parts.value_type, bytes))?
        }
        // Decode the inline order-preserving value from the key.
        ValueRef::Inline(inline_payload) => {
            let (value, rest) =
                decode_value(parts.value_type, inline_payload).ok_or_else(|| {
                    DialogArtifactsError::InvalidValue(
                        "inline value payload did not decode".to_string(),
                    )
                })?;
            if !rest.is_empty() {
                return Err(DialogArtifactsError::InvalidValue(
                    "inline value payload had trailing bytes".to_string(),
                ));
            }
            value
        }
    };

    Ok(Artifact {
        the,
        of,
        is,
        cause: datum.cause.clone(),
    })
}
