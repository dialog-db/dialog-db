use serde::{Deserialize, Serialize};

use crate::{Artifact, Attribute, DialogArtifactsError, Entity, Value};

use super::{REVISION_ATTRIBUTE, Version};

/// Everything a revision states about itself, as one atomic record.
///
/// Stored as a single fact in the ordinary EAV/AEV/VAE indexes — entity =
/// [`Version::entity`], attribute = [`REVISION_ATTRIBUTE`], value =
/// [`Value::Record`] of this struct's dag-cbor encoding. One record per
/// revision keeps the metadata atomic (a revision is never partially
/// described: the record is present or it is not), makes each step of
/// ancestor traversal a single exact lookup, and is the natural unit for a
/// future signature. Individual fields are exposed to queries through
/// formulas over the record rather than stored as separate facts.
///
/// The attribute lives in the reserved `dialog.` namespace: user
/// instructions cannot write it (see
/// [`ReservedAttribute`](DialogArtifactsError::ReservedAttribute)), so at
/// the library level lineage cannot be corrupted through the ordinary
/// write path. A hostile peer can still craft arbitrary records on the
/// wire; detecting that is the job of signatures over this record.
///
/// The revision's tree root is deliberately absent: the record lives
/// inside that tree, so the root cannot appear inside itself. The head
/// `Revision` published to the branch cell carries the root.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RevisionRecord {
    /// Encoding version of this record, for forward evolution
    pub format: u8,
    /// The branch lineage entity this revision was minted on
    pub lineage: Entity,
    /// DID of the operator (session key) that minted the revision
    pub issuer: String,
    /// DID of the profile (long-lived key) that authorized it
    pub authority: String,
    /// Parent revision versions — the revision DAG edge. Empty for
    /// genesis; two entries for a merge.
    pub parents: Vec<Version>,
    /// Skip links: entry `i` leaps 2^(i+1) first-parent steps back (see
    /// [`extend_skips`](super::extend_skips)). Empty for genesis and merge
    /// revisions.
    pub skips: Vec<Version>,
}

/// The current [`RevisionRecord::format`]
pub const REVISION_RECORD_FORMAT: u8 = 0;

impl RevisionRecord {
    /// Encode this record into the bytes carried by its [`Value::Record`]
    pub fn to_bytes(&self) -> Result<Vec<u8>, DialogArtifactsError> {
        serde_ipld_dagcbor::to_vec(self)
            .map_err(|error| DialogArtifactsError::InvalidValue(format!("{error}")))
    }

    /// Decode a record from the bytes of its [`Value::Record`]
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, DialogArtifactsError> {
        serde_ipld_dagcbor::from_slice(bytes)
            .map_err(|error| DialogArtifactsError::InvalidValue(format!("{error}")))
    }

    /// The fact carrying this record: an [`Artifact`] on the revision
    /// entity under [`REVISION_ATTRIBUTE`], valued with the encoded record
    pub fn to_artifact(&self, version: &Version) -> Result<Artifact, DialogArtifactsError> {
        Ok(Artifact {
            the: Attribute::try_from(REVISION_ATTRIBUTE.to_string())?,
            of: version.entity(),
            is: Value::Record(self.to_bytes()?),
            cause: None,
        })
    }
}
