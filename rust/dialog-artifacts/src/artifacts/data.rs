//! Internal data representation for storing artifacts in indexes.
//!
//! This module defines the [`Datum`] type: the per-entry payload stored
//! alongside a key in the prolly tree indexes.
//!
//! The key already carries the entity, attribute, value type, and (for a value
//! within the inline threshold) the value itself in order-preserving form, so
//! the payload no longer duplicates them. It holds only what the key cannot
//! reconstruct: the raw bytes of a *spilled* value (whose key carries just a
//! 32-byte reference), and the [`Cause`]. A [`Artifact`] is reconstructed from
//! the key plus this payload by [`Artifact::from_key_datum`].

use crate::ValueType;
use crate::key::value_spills;
use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::{Artifact, Cause};

#[cfg(doc)]
use crate::{Artifacts, Attribute, Entity, Value};

/// A [`Datum`] is the per-entry payload stored against a key in the
/// [`Artifacts`] indexes: the parts of a fact the key does not already carry.
#[derive(
    Clone, Debug, PartialEq, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct Datum {
    /// The raw bytes of a *spilled* [`Value`], present only when the key stores
    /// a 32-byte reference instead of the inline value. An inline value is
    /// recovered from the key, so this is `None` for it.
    pub value: Option<Vec<u8>>,
    /// The [`Cause`] of this fact, if any: a reference to an ancestor version
    /// with a different [`Value`].
    pub cause: Option<Cause>,
}

impl Datum {
    /// The payload for `artifact`, carrying the raw value bytes only when the
    /// value spills (so the key stores just a 32-byte reference); an inline
    /// value is recovered from the key, so only the cause is carried.
    ///
    /// The spill decision must match the one the key builder makes
    /// ([`EntityKey::from`] via `key::value_payload`), so the payload carries
    /// value bytes exactly when [`Artifact::from_key_datum`] needs them.
    pub fn for_artifact(artifact: &Artifact) -> Self {
        if value_spills(&artifact.is) {
            Self {
                value: Some(artifact.is.to_bytes()),
                cause: artifact.cause.clone(),
            }
        } else {
            Self {
                value: None,
                cause: artifact.cause.clone(),
            }
        }
    }
}

impl ValueType for Datum {}
