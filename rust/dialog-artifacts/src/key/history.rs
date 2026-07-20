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

use crate::artifacts::encode_bytes;
use crate::history::{EDITION_LENGTH, ORIGIN_LENGTH, VERSION_LENGTH, Version};
use crate::key::varkey::{KeyParts, ValuePayload, build_key};
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

/// Filler width for the value-tail upper bound of a claim range. A value
/// payload never reaches this many trailing `0xFF` bytes, so a bound built
/// this way sorts above every record of the claim.
const VALUE_TAIL_BOUND: usize = 64;

/// Byte offset of the version prefix within a history/coverage key: it
/// follows the single tag byte.
const VERSION_OFFSET: usize = 1;

/// The key at which the record of a claim on `(of, the)` with the given
/// value type and reference, produced by the revision identified by
/// `version`, is stored
pub fn history_key(version: &Version, of: &Entity, the: &Attribute, value: &crate::Value) -> Key {
    tagged_key(HISTORY_KEY_TAG, version, of, the, value)
}

/// The key at which the coverage entry mirroring a covering record is
/// stored: the same layout as [`history_key`] under [`COVERAGE_KEY_TAG`].
pub fn coverage_key(version: &Version, of: &Entity, the: &Attribute, value: &crate::Value) -> Key {
    // Coverage stays value-free: it matches claims by VERSION, never by
    // content, so the key carries a 32-byte reference rather than the value.
    // That is what keeps "every deletion or replacement since the sync base"
    // a cheap scoped diff instead of one that streams values.
    let parts = tagged_parts(
        COVERAGE_KEY_TAG,
        version,
        of,
        the,
        value.data_type(),
        ValuePayload::Reference(value.to_reference().as_ref().to_vec()),
    );
    Key::from(build_key(&parts))
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
    value: &crate::Value,
) -> Key {
    // The value rides the key inline exactly as it does in the fact
    // orderings, through the same inline-vs-spill decision, so a record
    // reconstructs its claim from its key. Storing a bare reference here
    // would make the value unrecoverable: unlike a spilled fact (whose bytes
    // live in the archive under that reference), nothing else carries it.
    let payload = crate::key::value_payload(value, crate::key::inline_threshold());
    let parts = tagged_parts(tag, version, of, the, value.data_type(), payload);
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
    // The value tail follows: a type byte then the payload. Bracket it from
    // the smallest type byte to above every value of the largest, so the
    // range spans exactly this claim's records whatever their values.
    let mut max = min.clone();
    min.push(u8::MIN);
    max.extend(std::iter::repeat_n(u8::MAX, 1 + VALUE_TAIL_BOUND));
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

    /// The key is lossless: entity and attribute round-trip in full, however
    /// long. The fixed-width key this replaces truncated both to a raw head
    /// and leaned on a trailing whole-key hash to tell collisions apart, so a
    /// reader had to re-check every hit against the stored record.
    #[test]
    fn it_round_trips_long_entities_and_attributes() -> anyhow::Result<()> {
        let of = Entity::from_str(&format!("test:{}", "e".repeat(120)))?;
        // At the attribute cap (64 bytes), which the fixed-width key
        // truncated to its 57-byte raw head.
        let the = Attribute::from_str(&format!("{}/{}", "n".repeat(31), "p".repeat(32)))?;
        let value = crate::Value::String("value".into());
        let key = history_key(&version(1, 7), &of, &the, &value);

        let parts = crate::key::varkey::parse_key(key.as_ref())
            .ok_or_else(|| anyhow::anyhow!("history key did not parse"))?;
        assert_eq!(std::str::from_utf8(&parts.entity)?, of.as_str());
        assert_eq!(std::str::from_utf8(&parts.attribute)?, the.as_str());
        Ok(())
    }

    /// Two claims that share every truncatable prefix but differ in their
    /// tails land at different keys — no collision, and no disambiguating
    /// hash needed.
    #[test]
    fn it_separates_claims_sharing_a_long_prefix() -> anyhow::Result<()> {
        let shared = "test:".to_string() + &"e".repeat(120);
        let left = Entity::from_str(&(shared.clone() + "a"))?;
        let right = Entity::from_str(&(shared + "b"))?;
        let the = Attribute::from_str("test/attribute")?;
        let value = crate::Value::String("value".into());
        let at = version(1, 7);

        let left_key = history_key(&at, &left, &the, &value);
        let right_key = history_key(&at, &right, &the, &value);
        assert_ne!(left_key, right_key);
        Ok(())
    }

    #[test]
    fn it_recovers_the_version_and_clusters_by_origin() -> anyhow::Result<()> {
        let of = Entity::from_str("test:entity")?;
        let the = Attribute::from_str("test/attribute")?;
        let value = crate::Value::String("value".into());

        // One writer's records order by edition within its span.
        let early = version(1, 7);
        let late = version(2, 7);
        let early_key = history_key(&early, &of, &the, &value);
        let late_key = history_key(&late, &of, &the, &value);
        assert_eq!(history_key_version(&Key::from(early_key.clone()))?, early);
        assert!(
            early_key < late_key,
            "one origin's keys order by edition within its span"
        );

        // Different writers cluster apart regardless of edition: a lower
        // origin's later edition still sorts before a higher origin's
        // earlier one. This per-writer contiguity is what a graft merge
        // adopts logs by.
        let low_origin_late = version(9, 5);
        let clustered = history_key(&low_origin_late, &of, &the, &value);
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
        let left_key = history_key(&version, &left, &the, &value);
        let right_key = history_key(&version, &right, &the, &value);
        assert_ne!(left_key, right_key);

        // Two attributes sharing the raw head
        // Long enough that the pre-M3 fixed-width key would have truncated
        // both of these to the same raw head.
        let head = "test/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let of = Entity::from_str("test:entity")?;
        let first = Attribute::from_str(&format!("{head}x"))?;
        let second = Attribute::from_str(&format!("{head}y"))?;
        let first_key = history_key(&version, &of, &first, &value);
        let second_key = history_key(&version, &of, &second, &value);
        assert_ne!(first_key, second_key);

        // Same value bytes under a different value type
        let string = crate::Value::String("a".into());
        let bytes = crate::Value::Bytes(vec![b'a']);
        assert_eq!(string.to_reference(), bytes.to_reference());
        let string_key = history_key(&version, &of, &the, &string);
        let bytes_key = history_key(&version, &of, &the, &bytes);
        assert_ne!(string_key, bytes_key);

        // Each claim's range now contains exactly its own records. Under the
        // pre-M3 truncated key both entities shared a raw head, so both keys
        // fell inside either range and readers had to re-check every hit
        // against the stored record; the lossless key makes the range exact.
        let (min, max) = history_claim_range(&version, &left, &the);
        let inside = |key: &Key| *key >= min && *key <= max;
        assert!(inside(&left_key), "a claim range contains its own record");
        assert!(
            !inside(&right_key),
            "and excludes another entity's, with no re-check needed"
        );
        Ok(())
    }
}
