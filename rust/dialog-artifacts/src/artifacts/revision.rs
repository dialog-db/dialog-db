use dialog_storage::{CborEncoder, Encoder};
use serde::{Deserialize, Serialize};

use crate::DialogArtifactsError;

use super::Blake3Hash;

/// A hash representing a null [`Revision`] that represents an empty (perhaps
/// newly created) [`Artifacts`].
pub static NULL_REVISION_HASH: Blake3Hash = [0; 32];

/// A [`Revision`] represents the root of [`Artifacts`] for a given set of data.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Revision {
    entity_index: Blake3Hash,
    attribute_index: Blake3Hash,
    value_index: Blake3Hash,
}

impl Revision {
    /// The component of the [`Revision`] that corresponds to the [`Entity`] index
    pub fn entity_index(&self) -> &Blake3Hash {
        &self.entity_index
    }

    /// The component of the [`Revision`] that corresponds to the [`Attribute`] index
    pub fn attribute_index(&self) -> &Blake3Hash {
        &self.attribute_index
    }

    /// The component of the [`Revision`] that corresponds to the [`Value`] index
    pub fn value_index(&self) -> &Blake3Hash {
        &self.value_index
    }

    /// Encodes the [`Revision`] as IPLD-compatible CBOR and returns the raw
    /// bytes
    pub async fn as_cbor(&self) -> Result<Vec<u8>, DialogArtifactsError> {
        Ok(CborEncoder.encode(self).await?.1)
    }

    /// Encodes the [`Revision`] as IPLD-compatible CBOR and returns the hash
    /// reference to the bytes
    pub async fn as_reference(&self) -> Result<Blake3Hash, DialogArtifactsError> {
        Ok(CborEncoder.encode(self).await?.0)
    }
}

impl From<(Blake3Hash, Blake3Hash, Blake3Hash)> for Revision {
    fn from(
        (entity_index, attribute_index, value_index): (Blake3Hash, Blake3Hash, Blake3Hash),
    ) -> Self {
        Self {
            entity_index,
            attribute_index,
            value_index,
        }
    }
}
