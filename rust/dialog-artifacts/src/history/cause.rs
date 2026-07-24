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
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize)]
#[repr(transparent)]
pub struct Cause(Vec<Version>);

/// Deserialization routes through [`Cause::new`], NOT the transparent
/// derive: [`contains`](Cause::contains) is a binary search over the
/// sorted-and-deduplicated invariant, so a wire-supplied unsorted cause
/// would silently miss tier-1 citations in conflict detection. Every
/// constructor enforces the invariant; the wire must too.
impl<'de> Deserialize<'de> for Cause {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::new(Vec::<Version>::deserialize(deserializer)?))
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::Origin;

    /// A wire-supplied cause re-establishes the sorted-and-deduplicated
    /// invariant on deserialization: `contains` is a binary search, so a
    /// derive-deserialized unsorted cause would silently miss tier-1
    /// citations in conflict detection.
    #[test]
    fn it_restores_the_invariant_on_deserialization() -> anyhow::Result<()> {
        let early = Version::new(Origin::from([1u8; 32]), Edition::new(1));
        let late = Version::new(Origin::from([2u8; 32]), Edition::new(5));

        // Encode a RAW vec, unsorted with a duplicate — what a hostile or
        // buggy peer could put on the wire where a cause is expected.
        let wire = serde_ipld_dagcbor::to_vec(&vec![late, early, late])?;
        let cause: Cause = serde_ipld_dagcbor::from_slice(&wire)?;

        assert_eq!(cause.versions(), &[early, late], "sorted and deduplicated");
        assert!(cause.contains(&early), "binary search finds every entry");
        assert!(cause.contains(&late));
        Ok(())
    }
}
