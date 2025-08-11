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
    /// The hash of the prolly tree index root for this revision
    index: Blake3Hash,
}

impl Revision {
    /// Creates a new revision with the given index hash.
    pub fn new(index: &Blake3Hash) -> Self {
        Self {
            index: index.to_owned(),
        }
    }
    /// The component of the [`Revision`] that corresponds to the [`Entity`] index
    pub fn index(&self) -> &Blake3Hash {
        &self.index
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
