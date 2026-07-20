use dialog_common::Blake3Hash;

/// Blake3 of `bytes`, the content reference every derivation here uses.
fn make_reference<B: AsRef<[u8]>>(bytes: B) -> [u8; 32] {
    *Blake3Hash::hash(bytes.as_ref()).as_bytes()
}

use std::fmt::{self, Display};

use base58::ToBase58;
use serde::{Deserialize, Serialize};

use super::Issuer;

/// Repository membership identifier derived as `Blake3(issuer + subject)`.
///
/// Deriving from both the signing key and the repository DID ensures that the
/// same principal acting on two different repositories produces two distinct
/// origins, preventing collisions when independent repositories later merge.
/// Because the issuer is a fixed-width (32 byte) key, the concatenation is
/// unambiguous.
///
/// An [`Origin`] MUST identify a single sequential actor: the same issuer key
/// used concurrently from multiple replicas of the same repository can mint
/// colliding [`Version`](super::Version)s. Each replica should act under its
/// own issuer key.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[repr(transparent)]
pub struct Origin(pub [u8; 32]);

/// The byte width of an [`Origin`] in key encodings
pub const ORIGIN_LENGTH: usize = 32;

impl Origin {
    /// Derive the [`Origin`] for the given issuer acting on the given
    /// repository, named by its DID string.
    pub fn derive(issuer: &Issuer, subject: &str) -> Self {
        Self(make_reference(
            [issuer.0.as_slice(), subject.as_bytes()].concat(),
        ))
    }

    /// Derive the [`Origin`] for a lineage identified by a sequence of
    /// string identifiers — typically the issuer DID, the repository DID,
    /// and the branch name.
    ///
    /// Unlike [`Origin::derive`], where the fixed-width issuer key makes
    /// concatenation unambiguous, these identifiers are variable-width, so
    /// every component is length-prefixed to keep the derivation injective.
    ///
    /// The components must together identify a single sequential actor: a
    /// branch head advances independently of every other branch, so within
    /// a multi-branch repository the branch belongs in the scope — deriving
    /// from the issuer and repository alone would let two branches advanced
    /// by the same issuer mint colliding versions.
    pub fn derive_from_identifiers<'a>(identifiers: impl IntoIterator<Item = &'a str>) -> Self {
        let mut bytes = Vec::new();
        for identifier in identifiers {
            let identifier = identifier.as_bytes();
            bytes.extend_from_slice(&(identifier.len() as u64).to_be_bytes());
            bytes.extend_from_slice(identifier);
        }
        Self(make_reference(bytes))
    }

    /// The byte representation of this [`Origin`], suitable for use as a key
    /// component
    pub fn key_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for Origin {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.to_base58())
    }
}

impl fmt::Debug for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Origin({self})")
    }
}
