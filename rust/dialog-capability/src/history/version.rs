use crate::identity::{Entity, hashed_uri};
use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::io::Cursor;

use serde::{Deserialize, Serialize};

use super::HistoryError;

use super::{EDITION_LENGTH, Edition, ORIGIN_LENGTH, Origin};

/// The scheme of a revision's entity: `revision:<base58(hash)>`.
pub const REVISION_SCHEME: &str = "revision";

/// What a revision's entity begins with: its scheme and the colon.
const REVISION_PREFIX: &str = "revision:";

/// What a revision's entity began with before `revision:`.
const LEGACY_REVISION_PREFIX: &str = "did:key:z6Mk";

/// `prefix` then `reference`, in a string allocated once at its length:
/// every commit names its revision, so this runs on the commit path.
fn prefixed(prefix: &str, reference: &str) -> String {
    let mut uri = String::with_capacity(prefix.len() + reference.len());
    uri.push_str(prefix);
    uri.push_str(reference);
    uri
}

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
#[rkyv(derive(PartialEq), compare(PartialEq))]
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

    /// The hash a revision's entity is formed from: blake3 of the
    /// canonical dag-cbor of the version.
    fn entity_hash(&self) -> [u8; 32] {
        /// Canonical dag-cbor input for the derivation. The shape (variant
        /// and field names) is part of the derivation; changing it changes
        /// every revision entity.
        #[derive(Serialize)]
        enum RevisionHash<'a> {
            Revision { origin: &'a [u8], edition: u64 },
        }

        // Encoded on the stack: the encoding is a few dozen bytes, and a
        // revision is named on every commit and every read of a branch.
        let mut buffer = [0u8; 128];
        let mut cursor = Cursor::new(&mut buffer[..]);
        serde_ipld_dagcbor::to_writer(
            &mut cursor,
            &RevisionHash::Revision {
                origin: self.origin.key_bytes().as_slice(),
                edition: self.edition.value(),
            },
        )
        .expect("dag-cbor encoding of a version fits in 128 bytes");
        let length = cursor.position() as usize;
        *blake3::hash(&buffer[..length]).as_bytes()
    }

    /// The URI of the entity naming the revision this version identifies:
    /// `revision:<base58(hash)>`, the hash being blake3 of the version's
    /// canonical dag-cbor. Any replica that knows a revision's version
    /// derives the same URI, so metadata can be attached to (or queried
    /// from) a revision without holding it. It is the entity the
    /// revision's record is recorded under.
    pub fn entity_uri(&self) -> String {
        hashed_uri(REVISION_PREFIX, &self.entity_hash())
    }

    /// The URI revisions were named by before they had a scheme of their
    /// own: `did:key:z6Mk<base58(hash)>`, over the same hash as
    /// [`entity_uri`](Self::entity_uri). It reads as a key and is none.
    /// A replica written by an older release holds its revision records
    /// under this name, so a reader looks under both.
    pub fn legacy_entity_uri(&self) -> String {
        hashed_uri(LEGACY_REVISION_PREFIX, &self.entity_hash())
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

impl Version {
    /// The content-derived entity naming the revision this version
    /// identifies ([`entity_uri`](Self::entity_uri)).
    pub fn entity(&self) -> Entity {
        Entity::try_from(self.entity_uri())
            .expect("a revision URI formed from a 32-byte hash is always a valid entity")
    }

    /// The entity an older release named this revision by
    /// ([`legacy_entity_uri`](Self::legacy_entity_uri)).
    pub fn legacy_entity(&self) -> Entity {
        Entity::try_from(self.legacy_entity_uri())
            .expect("a did:key URI formed from a 32-byte hash is always a valid entity")
    }

    /// The entity an older release named a revision by, given the one it
    /// is named by now; `None` for an entity that names no revision.
    pub fn legacy_of(entity: &Entity) -> Option<Entity> {
        let reference = entity.as_str().strip_prefix(REVISION_PREFIX)?;
        Entity::try_from(prefixed(LEGACY_REVISION_PREFIX, reference)).ok()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::history::Origin;
    use base58::ToBase58 as _;

    /// A revision is named by blake3 of its version's dag-cbor, however
    /// that encoding is buffered: the name is what every replica derives
    /// and stores records under, so it must not move.
    #[dialog_common::test]
    fn it_names_a_revision_by_its_versions_encoding() {
        #[derive(Serialize)]
        enum RevisionHash<'a> {
            Revision { origin: &'a [u8], edition: u64 },
        }
        for (seed, edition) in [(0u8, 0u64), (1, 1), (2, u64::MAX), (3, 1 << 40)] {
            let origin = Origin::from(*blake3::hash(&[seed]).as_bytes());
            let version = Version::new(origin, Edition::new(edition));
            let encoded = serde_ipld_dagcbor::to_vec(&RevisionHash::Revision {
                origin: origin.key_bytes().as_slice(),
                edition,
            })
            .expect("encodes");
            let reference = blake3::hash(&encoded).as_bytes().to_base58();
            assert_eq!(version.entity_uri(), format!("revision:{reference}"));
            assert_eq!(
                version.legacy_entity_uri(),
                format!("did:key:z6Mk{reference}")
            );
        }
    }
}
