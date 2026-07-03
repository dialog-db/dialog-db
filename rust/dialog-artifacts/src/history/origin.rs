use std::fmt::{self, Display};

use base58::ToBase58;
use serde::{Deserialize, Serialize};

use crate::{Entity, HASH_SIZE, make_reference};
use dialog_storage::Blake3Hash;

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
pub struct Origin(pub Blake3Hash);

/// The byte width of an [`Origin`] in key encodings
pub const ORIGIN_LENGTH: usize = HASH_SIZE;

impl Origin {
    /// Derive the [`Origin`] for the given issuer acting on the given
    /// repository (identified by its DID, represented as an [`Entity`])
    pub fn derive(issuer: &Issuer, subject: &Entity) -> Self {
        Self(make_reference(
            [issuer.0.as_slice(), subject.as_str().as_bytes()].concat(),
        ))
    }

    /// The byte representation of this [`Origin`], suitable for use as a key
    /// component
    pub fn key_bytes(&self) -> &Blake3Hash {
        &self.0
    }
}

impl From<Blake3Hash> for Origin {
    fn from(value: Blake3Hash) -> Self {
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
