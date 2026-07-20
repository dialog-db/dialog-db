//! Internal data representation for storing artifacts in indexes.
//!
//! This module defines the [`Datum`] type: the per-entry payload stored
//! alongside a key in the prolly tree indexes.
//!
//! The key already carries the entity, attribute, value type, and (for a value
//! within the inline threshold) the value itself in order-preserving form, so
//! the payload no longer duplicates them. For a *spilled* value (whose key
//! carries just a 32-byte reference), the raw value bytes are not in the payload
//! either: they live as a content-addressed block in the archive block store,
//! keyed by that same 32-byte reference (written on commit by
//! [`ArtifactTreeExt::apply`](crate::tree::ArtifactTreeExt::apply) and fetched
//! on read). The payload holds only the [`Cause`], which the key cannot
//! reconstruct. A [`Artifact`] is reconstructed from the key plus this payload
//! (plus the fetched spilled block, if any) by
//! [`Artifact::from_key_datum`]/[`Artifact::from_key_datum_with_value`].

use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::ValueType;

use crate::{Artifact, Cause, history::Version};

#[cfg(doc)]
use crate::{Artifacts, Attribute, Entity, Value};

/// A [`Datum`] is the per-entry payload stored against a key in the
/// [`Artifacts`] indexes: the parts of a fact the key does not already carry.
#[derive(
    Clone, Debug, PartialEq, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct Datum {
    /// The [`Cause`] of this fact, if any: a reference to an ancestor version
    /// with a different [`Value`].
    pub cause: Option<Cause>,
    /// An opaque record carried ONLY by the blob index
    /// ([`BlobRecord`](crate::BlobRecord)), never by an EAV/AEV/VAE fact. Blob
    /// keys occupy a tag range disjoint from the fact indexes, so a blob
    /// entry's `Datum` is never seen by the fact scan; this field is its
    /// storage. It is NOT the raw bytes of a spilled value — those live as a
    /// content-addressed block in the archive, keyed by the key's 32-byte
    /// reference.
    pub blob: Option<Vec<u8>>,
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
    /// The payload for `artifact`: only the cause. A spilled value's bytes are
    /// written separately as a content-addressed block in the archive (keyed by
    /// the key's 32-byte reference), so the payload never carries value bytes.
    pub fn for_artifact(artifact: &Artifact) -> Self {
        Self {
            cause: artifact.cause.clone(),
            blob: None,
            version: None,
            supersedes: Vec::new(),
            retraction: false,
        }
    }
}

impl ValueType for Datum {}
