use serde::{Deserialize, Serialize};

use crate::DialogArtifactsError;

use super::{Edition, VERSION_LENGTH, Version};

/// A set of [`Version`]s identifying prior claims (or parent revisions)
/// superseded by the claim (or revision) that carries it.
///
/// A [`Cause`] is empty on first write (or for a genesis revision). It
/// contains one entry in the normal sequential case, and multiple entries
/// when explicitly resolving concurrent claims (or when a revision merges
/// multiple lineages), recording that the author saw and deliberately
/// superseded all of them.
///
/// Entries are kept sorted and deduplicated so that the encoding of a cause
/// is deterministic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Cause(Vec<Version>);

impl Cause {
    /// An empty [`Cause`], as carried by a first write or a genesis revision
    pub fn genesis() -> Self {
        Self(Vec::new())
    }

    /// Create a [`Cause`] from the given versions, sorting and deduplicating
    /// them for deterministic encoding
    pub fn new(mut versions: Vec<Version>) -> Self {
        versions.sort();
        versions.dedup();
        Self(versions)
    }

    /// Whether this [`Cause`] is empty
    pub fn is_genesis(&self) -> bool {
        self.0.is_empty()
    }

    /// The versions superseded by the carrier of this [`Cause`]
    pub fn versions(&self) -> &[Version] {
        &self.0
    }

    /// Whether this [`Cause`] directly contains the given version
    pub fn contains(&self, version: &Version) -> bool {
        self.0.binary_search(version).is_ok()
    }

    /// The [`Edition`] of a revision that carries this [`Cause`]:
    /// `max(edition of every version in cause) + 1`, or
    /// [`Edition::GENESIS`] when the cause is empty.
    ///
    /// This is the authoritative form of the edition rule; per-replica
    /// counters that advance when remote revisions are observed are an
    /// optimization that caches this value.
    pub fn edition(&self) -> Edition {
        self.0
            .iter()
            .map(|version| version.edition)
            .max()
            .map(|edition| edition.successor())
            .unwrap_or(Edition::GENESIS)
    }

    /// The byte representation of this [`Cause`]: the concatenation of the
    /// key bytes of its (sorted) versions
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.0.len() * VERSION_LENGTH);
        for version in &self.0 {
            bytes.extend_from_slice(&version.key_bytes());
        }
        bytes
    }
}

impl From<Version> for Cause {
    fn from(version: Version) -> Self {
        Self(vec![version])
    }
}

impl FromIterator<Version> for Cause {
    fn from_iter<T: IntoIterator<Item = Version>>(iter: T) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

impl TryFrom<&[u8]> for Cause {
    type Error = DialogArtifactsError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if !bytes.len().is_multiple_of(VERSION_LENGTH) {
            return Err(DialogArtifactsError::InvalidReference(format!(
                "Cause bytes must be a multiple of {} (got {})",
                VERSION_LENGTH,
                bytes.len()
            )));
        }
        Ok(Self::new(
            bytes
                .chunks_exact(VERSION_LENGTH)
                .map(Version::from_key_bytes)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }
}
