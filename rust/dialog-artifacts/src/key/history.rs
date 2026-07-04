//! Key layout for the history region of the artifact tree.
//!
//! History records live in the same search tree as the EAV/AEV/VAE index
//! entries, under their own leading tag, keyed by:
//!
//! ```text
//! /tag/edition/origin/entity#/attribute#/value#
//! ```
//!
//! Edition leads (after the tag) so that lexicographic order within the
//! history region matches causal depth order. Unlike the index keys, the
//! entity and attribute components are Blake3 hashes of their key-byte
//! forms rather than the raw 64-byte forms: history lookups are always
//! exact-match on `(entity, attribute)` — never prefix scans — so hashing
//! loses nothing they use, and the compression makes the layout fit the
//! shared fixed key width. The full entity and attribute live in the stored
//! record. This is an interim shape: a variable-length key redesign of the
//! search tree would let the history region carry the raw components.

use dialog_storage::Blake3Hash;

use crate::history::{EDITION_LENGTH, ORIGIN_LENGTH, VERSION_LENGTH, Version};
use crate::{
    Attribute, Entity, HASH_SIZE, Key, KeyBytes, MAXIMUM_KEY, MINIMUM_KEY, TAG_LENGTH,
    make_reference,
};

/// The leading tag byte of history region keys
pub const HISTORY_KEY_TAG: u8 = 3;

const EDITION_OFFSET: usize = TAG_LENGTH;
const ORIGIN_OFFSET: usize = EDITION_OFFSET + EDITION_LENGTH;
const ENTITY_OFFSET: usize = ORIGIN_OFFSET + ORIGIN_LENGTH;
const ATTRIBUTE_OFFSET: usize = ENTITY_OFFSET + HASH_SIZE;
const VALUE_OFFSET: usize = ATTRIBUTE_OFFSET + HASH_SIZE;
const END_OFFSET: usize = VALUE_OFFSET + HASH_SIZE;

/// The hash of an [`Entity`]'s key-byte form, as embedded in history keys
pub fn history_entity_hash(of: &Entity) -> Blake3Hash {
    make_reference(of.key_bytes())
}

/// The hash of an [`Attribute`]'s key-byte form, as embedded in history keys
pub fn history_attribute_hash(the: &Attribute) -> Blake3Hash {
    make_reference(the.key_bytes())
}

/// The key at which the record of a claim on `(of, the)` with the given
/// value reference, produced by the revision identified by `version`, is
/// stored
pub fn history_key(
    version: &Version,
    of: &Entity,
    the: &Attribute,
    value_reference: &Blake3Hash,
) -> Key {
    let mut bytes = MINIMUM_KEY;
    bytes[0] = HISTORY_KEY_TAG;
    bytes[EDITION_OFFSET..ORIGIN_OFFSET + ORIGIN_LENGTH].copy_from_slice(&version.key_bytes());
    bytes[ENTITY_OFFSET..ATTRIBUTE_OFFSET].copy_from_slice(&history_entity_hash(of));
    bytes[ATTRIBUTE_OFFSET..VALUE_OFFSET].copy_from_slice(&history_attribute_hash(the));
    bytes[VALUE_OFFSET..END_OFFSET].copy_from_slice(value_reference);
    Key::from(bytes)
}

/// The inclusive bounds of the key range covering every history record of
/// claims on `(of, the)` produced by the revision identified by `version`
pub fn history_claim_range(version: &Version, of: &Entity, the: &Attribute) -> (Key, Key) {
    let min = history_key(version, of, the, &[u8::MIN; HASH_SIZE]);
    let max = history_key(version, of, the, &[u8::MAX; HASH_SIZE]);
    (min, max)
}

/// The inclusive bounds of the key range covering every history record
/// produced by the revision identified by `version`
pub fn history_version_range(version: &Version) -> (Key, Key) {
    let mut min = MINIMUM_KEY;
    let mut max = MAXIMUM_KEY;
    min[0] = HISTORY_KEY_TAG;
    max[0] = HISTORY_KEY_TAG;
    min[EDITION_OFFSET..ENTITY_OFFSET].copy_from_slice(&version.key_bytes());
    max[EDITION_OFFSET..ENTITY_OFFSET].copy_from_slice(&version.key_bytes());
    (Key::from(min), Key::from(max))
}

/// The inclusive bounds of the key range covering the entire history region
pub fn history_region_range() -> (Key, Key) {
    let mut min = MINIMUM_KEY;
    let mut max = MAXIMUM_KEY;
    min[0] = HISTORY_KEY_TAG;
    max[0] = HISTORY_KEY_TAG;
    (Key::from(min), Key::from(max))
}

/// The [`Version`] component of a history region key
pub fn history_key_version(key: &KeyBytes) -> Result<Version, crate::DialogArtifactsError> {
    Version::from_key_bytes(&key[EDITION_OFFSET..EDITION_OFFSET + VERSION_LENGTH])
}

/// The attribute hash component of a history region key
pub fn history_key_attribute_hash(key: &KeyBytes) -> &[u8] {
    &key[ATTRIBUTE_OFFSET..VALUE_OFFSET]
}

/// The entity hash component of a history region key
pub fn history_key_entity_hash(key: &KeyBytes) -> &[u8] {
    &key[ENTITY_OFFSET..ATTRIBUTE_OFFSET]
}
