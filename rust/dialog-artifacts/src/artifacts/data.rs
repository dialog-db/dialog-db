//! Internal data representation for storing artifacts in indexes.
//!
//! This module defines the [`Datum`] type which is the internal serializable
//! representation of artifacts stored within the prolly tree indexes.

use crate::ValueType;
use dialog_storage::Blake3Hash;
use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::{Artifact, Cause, history::Version, make_reference};

#[cfg(doc)]
use crate::{Artifacts, Attribute, Entity};

/// A [`Datum`] is the layout of data stored in one of the indexes of [`Artifacts`]
#[derive(
    Clone, Debug, PartialEq, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct Datum {
    /// The stringified [`Entity`] associated with this [`Datum`]
    pub entity: String,
    /// The stringified [`Attribute`] associated with this [`Datum`]
    pub attribute: String,
    /// The type of the [`Value`] associated with this [`Datum`]
    pub value_type: u8,
    /// The raw byte representation of the [`Value`] associated with this [`Datum`]
    pub value: Vec<u8>,
    /// Get the [`Cause`] of this [`ValueDatum`], if any
    pub cause: Option<Cause>,
    /// The [`Version`] of the revision that produced this [`Datum`], when it
    /// was committed through a version-tagged write (see
    /// [`ArtifactTreeExt::apply_versioned`](crate::tree::ArtifactTreeExt::apply_versioned)).
    /// Data committed directly through [`Artifacts`] carries no version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<Version>,
    /// For history records (entries under
    /// [`HISTORY_KEY_TAG`](crate::HISTORY_KEY_TAG)): the versions of the
    /// prior claims on the same `(entity, attribute)` that this record
    /// supersedes. Always empty on index data.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supersedes: Vec<Version>,
    /// For history records: whether this record withdraws (retracts) its
    /// value rather than asserting it. Always `false` on index data.
    #[serde(default)]
    pub retraction: bool,
}

impl Datum {
    /// Returns the hash reference that corresponds to this [`Datum`]'s [`Value`].
    ///
    /// This hash is used for indexing by value and enables efficient value-based
    /// queries in the triple store.
    pub fn value_reference(&self) -> Blake3Hash {
        // TODO: Cache this
        make_reference(&self.value)
    }
}

impl ValueType for Datum {}

impl From<Artifact> for Datum {
    fn from(artifact: Artifact) -> Self {
        Self {
            entity: artifact.of.to_string(),
            attribute: artifact.the.to_string(),
            value_type: artifact.is.data_type().into(),
            value: artifact.is.to_bytes(),
            cause: artifact.cause,
            version: None,
            supersedes: Vec::new(),
            retraction: false,
        }
    }
}
