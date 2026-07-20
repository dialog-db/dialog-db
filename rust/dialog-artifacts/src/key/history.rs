//! Key layout for the history region of the artifact tree.
//!
//! History records live in the same search tree as the EAV/AEV/VAE index
//! entries, under their own leading tag. The logical key is
//!
//! ```text
//! /tag/origin/edition/entity/attribute/value_type/value_reference
//! ```
//!
//! — 202 bytes, which exceeds the tree's fixed key width. It is fit by the
//! same head-plus-hash rule the entity key form already uses one level down
//! (see [`Uri::key_bytes`](crate::Uri::key_bytes)), applied to the whole
//! key: keep an order-preserving raw head, and make the final
//! [`HASH_SIZE`] bytes a Blake3 hash of the *entire* logical key. The hash
//! covers the truncated tails, so two distinct logical keys always produce
//! distinct stored keys, while the raw head keeps the region range-scannable:
//!
//! ```text
//! offset  width  field
//! 0       1      HISTORY_KEY_TAG
//! 1       32     origin
//! 33      8      edition (big-endian)
//! 41      32     entity head (the raw span of the entity key form)
//! 73      57     attribute head (attributes ≤ 57 bytes appear verbatim)
//! 130     32     Blake3 of the full logical key
//! ```
//!
//! The origin leads (after the tag), so one writer's records form one
//! contiguous span of the region, ordered by edition within it. That
//! per-writer contiguity is what lets a graft merge adopt another
//! replica's log wholesale by subtree hash: two sides' novel records
//! cluster apart instead of interleaving by edition, so only origins
//! both sides wrote need merging. Causally ordered listings come from
//! walking the revision DAG, not from scanning this region. The raw
//! entity/attribute heads order one revision's records by entity, then
//! attribute. The only lookup over this region is [`history_claim_range`], which knows
//! every component; revision metadata lives as an ordinary fact in the
//! data indexes (see [`RevisionRecord`](crate::history::RevisionRecord)),
//! not here.
//!
//! Range scans over `(version, entity, attribute)` are exact when the
//! attribute fits its head and no other entity shares the 32-byte entity
//! head; otherwise they are a tight superset and readers re-check the
//! stored record (the full entity and attribute always live in the record).
//! This is an interim shape: a variable-length key redesign of the search
//! tree would drop the trim-and-hash and carry the raw key through — the
//! head layout here is exactly that future key's prefix.

use dialog_storage::Blake3Hash;

use crate::history::{EDITION_LENGTH, ORIGIN_LENGTH, VERSION_LENGTH, Version};
use crate::key::varkey::{KeyParts, ValuePayload, build_key};
use crate::artifacts::ordkey::encode_bytes;
use crate::{Attribute, Entity, Key, ValueDataType};

/// The leading tag byte of history region keys
pub const HISTORY_KEY_TAG: u8 = 3;

/// The leading tag byte of the coverage region: a compact mirror of the
/// history region holding one entry per *covering* record (a retraction,
/// or a replacement with a non-empty supersedes set), with the same key
/// layout under its own tag and no value bytes in the entry. Its purpose
/// is enumerability: "every deletion or replacement since the sync base"
/// is a scoped tree diff over this region alone, without streaming the
/// (value-bearing) assert records interleaved in the history region.
/// This is what lets a graft merge repair adopted subtrees at a cost
/// proportional to the coverage since base, not the write churn.
pub const COVERAGE_KEY_TAG: u8 = 5;

/// Byte offset of the version prefix within a history/coverage key: it
/// follows the single tag byte.
const VERSION_OFFSET: usize = 1;

/// The key at which the record of a claim on `(of, the)` with the given
/// value type and reference, produced by the revision identified by
/// `version`, is stored
pub fn history_key(
    version: &Version,
    of: &Entity,
    the: &Attribute,
    value_type: ValueDataType,
    value_reference: &Blake3Hash,
) -> Key {
    tagged_key(
        HISTORY_KEY_TAG,
        version,
        of,
        the,
        value_type,
        value_reference,
    )
}

/// The key at which the coverage entry mirroring a covering record is
/// stored: the same layout as [`history_key`] under [`COVERAGE_KEY_TAG`].
pub fn coverage_key(
    version: &Version,
    of: &Entity,
    the: &Attribute,
    value_type: ValueDataType,
    value_reference: &Blake3Hash,
) -> Key {
    tagged_key(
        COVERAGE_KEY_TAG,
        version,
        of,
        the,
        value_type,
        value_reference,
    )
}

/// The version bytes as they appear in a history/coverage key:
/// **origin-major, edition-minor**, which is the reverse of
/// [`Version::key_bytes`].
///
/// This clustering is load-bearing: one writer's records occupy a
/// contiguous span ordered by edition, so a graft merge adopts a peer's log
/// as a range rather than gathering scattered entries. Ordering by edition
/// first would interleave every writer.
fn version_prefix(version: &Version) -> [u8; VERSION_LENGTH] {
    let mut bytes = [0u8; VERSION_LENGTH];
    bytes[..ORIGIN_LENGTH].copy_from_slice(version.origin.key_bytes());
    bytes[ORIGIN_LENGTH..].copy_from_slice(&version.edition.key_bytes());
    bytes
}

/// The parts of a history/coverage key. Every component is lossless: unlike
/// the pre-M3 fixed-width history key (which truncated the entity and
/// attribute and leaned on a trailing whole-key hash to disambiguate), a
/// record reconstructs from its key alone.
fn tagged_parts(
    tag: u8,
    version: &Version,
    of: &Entity,
    the: &Attribute,
    value_type: ValueDataType,
    value: ValuePayload,
) -> KeyParts {
    KeyParts {
        tag,
        entity: of.as_str().as_bytes().to_vec(),
        attribute: the.as_str().as_bytes().to_vec(),
        value_type,
        value,
        version: Some(version_prefix(version)),
    }
}

fn tagged_key(
    tag: u8,
    version: &Version,
    of: &Entity,
    the: &Attribute,
    value_type: ValueDataType,
    value_reference: &Blake3Hash,
) -> Key {
    let parts = tagged_parts(
        tag,
        version,
        of,
        the,
        value_type,
        ValuePayload::Reference(value_reference.as_ref().to_vec()),
    );
    Key::from(build_key(&parts))
}

/// The inclusive bounds of the key range covering every history record of
/// claims on `(of, the)` produced by the revision identified by `version`.
///
/// Every component is lossless now, so the range is exact on
/// `(version, entity, attribute)`: it brackets the value tail alone. Readers
/// no longer need to re-check the entity and attribute of each hit.
pub fn history_claim_range(version: &Version, of: &Entity, the: &Attribute) -> (Key, Key) {
    let mut min = Vec::new();
    min.push(HISTORY_KEY_TAG);
    min.extend_from_slice(&version_prefix(version));
    encode_bytes(of.as_str().as_bytes(), &mut min);
    encode_bytes(the.as_str().as_bytes(), &mut min);
    // The value tail follows; bracket it between the smallest and largest
    // possible tails so the range spans exactly this claim's records.
    let mut max = min.clone();
    min.push(u8::MIN);
    max.push(u8::MAX);
    (Key::from(min), Key::from(max))
}

/// The inclusive bounds of the key range covering the entire history region
pub fn history_region_range() -> (Key, Key) {
    (
        Key::from(vec![HISTORY_KEY_TAG]),
        Key::from(vec![HISTORY_KEY_TAG, u8::MAX]),
    )
}

/// The [`Version`] component of a history region key
pub fn history_key_version(key: &Key) -> Result<Version, crate::DialogArtifactsError> {
    use crate::history::{Edition, Origin};
    let bytes: &[u8] = key.as_ref();
    let at = VERSION_OFFSET;
    if bytes.len() < at + VERSION_LENGTH {
        return Err(crate::DialogArtifactsError::InvalidKey(
            "history key is too short to carry a version".to_string(),
        ));
    }
    // Origin-major, edition-minor: the reverse of `Version::key_bytes`.
    let mut origin = [0u8; ORIGIN_LENGTH];
    origin.copy_from_slice(&bytes[at..at + ORIGIN_LENGTH]);
    let mut edition = [0u8; EDITION_LENGTH];
    edition.copy_from_slice(&bytes[at + ORIGIN_LENGTH..at + VERSION_LENGTH]);
    Ok(Version::new(
        Origin::from(origin),
        Edition::from_key_bytes(edition),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn version(edition: u64, origin: u8) -> Version {
        use crate::history::{Edition, Origin};
        Version::new(Origin::from([origin; 32]), Edition::new(edition))
    }

    #[test]
    fn it_lays_out_the_full_key_width() {
        assert_eq!(ATTRIBUTE_OFFSET, ENTITY_OFFSET + ENTITY_RAW_HEAD);
        assert_eq!(HASH_OFFSET, ATTRIBUTE_OFFSET + HISTORY_ATTRIBUTE_HEAD);
        assert_eq!(KEY_LENGTH, HASH_OFFSET + HASH_SIZE);
        assert_eq!(HISTORY_ATTRIBUTE_HEAD, 57);
    }

    #[test]
    fn it_recovers_the_version_and_clusters_by_origin() -> anyhow::Result<()> {
        let of = Entity::from_str("test:entity")?;
        let the = Attribute::from_str("test/attribute")?;
        let value = crate::Value::String("value".into());

        // One writer's records order by edition within its span.
        let early = version(1, 7);
        let late = version(2, 7);
        let early_key = history_key(&early, &of, &the, value.data_type(), &value.to_reference());
        let late_key = history_key(&late, &of, &the, value.data_type(), &value.to_reference());
        assert_eq!(
            history_key_version(&Key::from(early_key.clone()))?,
            early
        );
        assert!(
            early_key < late_key,
            "one origin's keys order by edition within its span"
        );

        // Different writers cluster apart regardless of edition: a lower
        // origin's later edition still sorts before a higher origin's
        // earlier one. This per-writer contiguity is what a graft merge
        // adopts logs by.
        let low_origin_late = version(9, 5);
        let clustered = history_key(
            &low_origin_late,
            &of,
            &the,
            value.data_type(),
            &value.to_reference(),
        );
        assert!(
            clustered < early_key,
            "origins cluster before editions order"
        );
        Ok(())
    }

    #[test]
    fn it_disambiguates_truncated_tails_by_whole_key_hash() -> anyhow::Result<()> {
        let version = version(1, 1);
        let value = crate::Value::String("value".into());

        // Two entities sharing a 32-byte head
        let shared = "test:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let left = Entity::from_str(&format!("{shared}left"))?;
        let right = Entity::from_str(&format!("{shared}right"))?;
        let the = Attribute::from_str("test/attribute")?;
        let left_key = history_key(
            &version,
            &left,
            &the,
            value.data_type(),
            &value.to_reference(),
        );
        let right_key = history_key(
            &version,
            &right,
            &the,
            value.data_type(),
            &value.to_reference(),
        );
        assert_ne!(left_key, right_key);

        // Two attributes sharing the raw head
        let head = "test/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(head.len(), HISTORY_ATTRIBUTE_HEAD);
        let of = Entity::from_str("test:entity")?;
        let first = Attribute::from_str(&format!("{head}x"))?;
        let second = Attribute::from_str(&format!("{head}y"))?;
        let first_key = history_key(
            &version,
            &of,
            &first,
            value.data_type(),
            &value.to_reference(),
        );
        let second_key = history_key(
            &version,
            &of,
            &second,
            value.data_type(),
            &value.to_reference(),
        );
        assert_ne!(first_key, second_key);

        // Same value bytes under a different value type
        let string = crate::Value::String("a".into());
        let bytes = crate::Value::Bytes(vec![b'a']);
        assert_eq!(string.to_reference(), bytes.to_reference());
        let string_key = history_key(
            &version,
            &of,
            &the,
            string.data_type(),
            &string.to_reference(),
        );
        let bytes_key = history_key(
            &version,
            &of,
            &the,
            bytes.data_type(),
            &bytes.to_reference(),
        );
        assert_ne!(string_key, bytes_key);

        // Truncation-colliding keys still land inside their claim range,
        // where readers re-check against the stored record
        let (min, max) = history_claim_range(&version, &left, &the);
        let inside = |key: &Key| *key >= min && *key <= max;
        assert!(inside(&left_key) && inside(&right_key));
        Ok(())
    }
}
