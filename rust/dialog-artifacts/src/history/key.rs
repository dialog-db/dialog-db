use core::fmt;

use crate::{ATTRIBUTE_LENGTH, Attribute, ENTITY_LENGTH, Entity, HASH_SIZE};
use dialog_storage::Blake3Hash;

use super::{Claim, EDITION_LENGTH, ORIGIN_LENGTH, Origin, VERSION_LENGTH, Version};

/// The byte width of a [`HistoryKey`]
pub const HISTORY_KEY_LENGTH: usize =
    EDITION_LENGTH + ORIGIN_LENGTH + ENTITY_LENGTH + ATTRIBUTE_LENGTH + HASH_SIZE;

const ORIGIN_OFFSET: usize = EDITION_LENGTH;
const ENTITY_OFFSET: usize = ORIGIN_OFFSET + ORIGIN_LENGTH;
const ATTRIBUTE_OFFSET: usize = ENTITY_OFFSET + ENTITY_LENGTH;
const VALUE_OFFSET: usize = ATTRIBUTE_OFFSET + ATTRIBUTE_LENGTH;

/// Key of the unified history index shared by revisions and claims:
///
/// ```text
/// /edition/origin/entity/attribute/value_hash -> Claim
/// ```
///
/// Edition leads the key so that lexicographic order matches causal depth
/// order. Given a claim's [`Version`] and its `(entity, attribute)`, the
/// claim is located directly; revision lineage claims are stored under
/// `entity = repository_did` so a scan filtered on that entity yields
/// revision history in a total order consistent with causality.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct HistoryKey([u8; HISTORY_KEY_LENGTH]);

impl HistoryKey {
    /// Construct the key at which the given claim, produced by the revision
    /// identified by `version`, is recorded
    pub fn new(version: &Version, claim: &Claim) -> Self {
        Self::from_parts(version, &claim.of, &claim.the, &claim.is.to_reference())
    }

    /// Construct a key from its parts
    pub fn from_parts(
        version: &Version,
        of: &Entity,
        the: &Attribute,
        value_reference: &Blake3Hash,
    ) -> Self {
        let mut bytes = [0u8; HISTORY_KEY_LENGTH];
        bytes[..VERSION_LENGTH].copy_from_slice(&version.key_bytes());
        bytes[ENTITY_OFFSET..ATTRIBUTE_OFFSET].copy_from_slice(of.key_bytes());
        bytes[ATTRIBUTE_OFFSET..VALUE_OFFSET].copy_from_slice(the.key_bytes());
        bytes[VALUE_OFFSET..].copy_from_slice(value_reference);
        Self(bytes)
    }

    /// The inclusive lower and upper bounds of the key range covering every
    /// claim recorded by the revision identified by `version`
    pub fn version_range(version: &Version) -> (Self, Self) {
        let mut min = [0u8; HISTORY_KEY_LENGTH];
        let mut max = [0xFFu8; HISTORY_KEY_LENGTH];
        min[..VERSION_LENGTH].copy_from_slice(&version.key_bytes());
        max[..VERSION_LENGTH].copy_from_slice(&version.key_bytes());
        (Self(min), Self(max))
    }

    /// The inclusive lower and upper bounds of the key range covering every
    /// claim on `(of, the)` recorded by the revision identified by `version`
    pub fn claim_range(version: &Version, of: &Entity, the: &Attribute) -> (Self, Self) {
        let min = Self::from_parts(version, of, the, &[0u8; HASH_SIZE]);
        let max = Self::from_parts(version, of, the, &[0xFFu8; HASH_SIZE]);
        (min, max)
    }

    /// The [`Version`] component of this key
    pub fn version(&self) -> Version {
        Version::from_key_bytes(&self.0[..VERSION_LENGTH])
            .expect("history key version component has the correct width")
    }

    /// The entity key bytes component of this key
    pub fn entity_bytes(&self) -> &[u8] {
        &self.0[ENTITY_OFFSET..ATTRIBUTE_OFFSET]
    }

    /// The attribute key bytes component of this key
    pub fn attribute_bytes(&self) -> &[u8] {
        &self.0[ATTRIBUTE_OFFSET..VALUE_OFFSET]
    }

    /// The value reference component of this key
    pub fn value_reference(&self) -> &[u8] {
        &self.0[VALUE_OFFSET..]
    }

    /// The raw bytes of this key
    pub fn as_bytes(&self) -> &[u8; HISTORY_KEY_LENGTH] {
        &self.0
    }

    /// The [`Origin`] component of this key
    pub fn origin(&self) -> Origin {
        let mut origin = [0u8; ORIGIN_LENGTH];
        origin.copy_from_slice(&self.0[ORIGIN_OFFSET..ENTITY_OFFSET]);
        Origin(origin)
    }
}

impl fmt::Debug for HistoryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HistoryKey({})", self.version())
    }
}
