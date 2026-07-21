use std::cmp::Ordering;
use std::fmt::{self, Display};

use base58::ToBase58 as _;
use serde::{Deserialize, Serialize};

use super::HistoryError;

use super::{EDITION_LENGTH, Edition, ORIGIN_LENGTH, Origin};

/// The byte width of a [`Version`] in key encodings
pub const VERSION_LENGTH: usize = EDITION_LENGTH + ORIGIN_LENGTH;

/// Uniquely identifies a specific revision by a specific origin.
///
/// Sorts naturally by causal depth via edition (ties broken by origin so that
/// ordering is total and deterministic). Two versions with the same edition
/// but different origins are concurrent: neither can have seen the other,
/// since seeing it would have forced a higher edition.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct Version {
    /// The repository-scoped identity of the actor that produced the revision
    pub origin: Origin,
    /// The causal depth of the revision
    pub edition: Edition,
}

impl Version {
    /// Create a [`Version`] from its parts
    pub fn new(origin: Origin, edition: Edition) -> Self {
        Self { origin, edition }
    }

    /// The byte representation of this [`Version`], suitable for use as a key
    /// component. Edition leads so that lexicographic order matches causal
    /// depth order.
    pub fn key_bytes(&self) -> [u8; VERSION_LENGTH] {
        let mut bytes = [0u8; VERSION_LENGTH];
        bytes[..EDITION_LENGTH].copy_from_slice(&self.edition.key_bytes());
        bytes[EDITION_LENGTH..].copy_from_slice(self.origin.key_bytes());
        bytes
    }

    /// The content-derived DID naming the revision this version identifies.
    /// Any replica that knows a revision's version derives the same DID, so
    /// metadata can be attached to (or queried from) a revision without
    /// holding it — this is the entity its revision record is recorded under.
    ///
    /// The `did:key:z6Mk<base58(hash)>` shape follows the repository-wide
    /// convention for content-derived entities (see `EntityExt::of` in
    /// `dialog-repository`'s schema): the `6Mk` prefix nominally means
    /// ed25519 key material, but the bytes here are a blake3 hash and
    /// nothing validates the multicodec — the string is an opaque,
    /// deterministic name, not a resolvable key DID. If the convention
    /// ever changes, it changes everywhere at once.
    ///
    /// Returned as a string: `Entity` lives in `dialog-artifacts`, which
    /// depends on this crate, so the parse happens there (see
    /// `Version::entity`).
    pub fn entity_did(&self) -> String {
        /// Canonical dag-cbor input for the derivation. The shape (variant
        /// and field names) is part of the derivation; changing it changes
        /// every revision entity.
        #[derive(Serialize)]
        enum RevisionHash<'a> {
            Revision { origin: &'a [u8], edition: u64 },
        }

        let bytes = serde_ipld_dagcbor::to_vec(&RevisionHash::Revision {
            origin: self.origin.key_bytes().as_slice(),
            edition: self.edition.value(),
        })
        .expect("dag-cbor encoding of a version cannot fail");
        let hash = blake3::hash(&bytes);
        format!("did:key:z6Mk{}", hash.as_bytes().to_base58())
    }

    /// Reconstruct a [`Version`] from its key byte representation
    pub fn from_key_bytes(bytes: &[u8]) -> Result<Self, HistoryError> {
        if bytes.len() != VERSION_LENGTH {
            return Err(HistoryError::InvalidReference(format!(
                "Incorrect version length (expected {}, got {})",
                VERSION_LENGTH,
                bytes.len()
            )));
        }
        let mut edition = [0u8; EDITION_LENGTH];
        edition.copy_from_slice(&bytes[..EDITION_LENGTH]);
        let mut origin = [0u8; ORIGIN_LENGTH];
        origin.copy_from_slice(&bytes[EDITION_LENGTH..]);
        Ok(Self {
            origin: Origin(origin),
            edition: Edition::from_key_bytes(edition),
        })
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.edition
            .cmp(&other.edition)
            .then_with(|| self.origin.cmp(&other.origin))
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.edition, self.origin)
    }
}
