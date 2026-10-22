//! Key layout for the context region of the artifact tree.
//!
//! A tree carries the causal context of the head that names it: one entry
//! per origin the head's ancestry has ever included, holding that origin's
//! watermark. The head itself carries only the most recently active of
//! those entries and a digest of the rest (see
//! [`Revision`](dialog_capability::identity::Revision)), so the region is
//! the one place the whole map is written down, and it is covered by the
//! head signature through the tree root.
//!
//! ```text
//! offset  width  field
//! 0       1      CONTEXT_KEY_TAG
//! 1       32     origin
//! ```
//!
//! The key is fixed-width and holds nothing but the origin, so an origin
//! has exactly one entry and a raised watermark overwrites it in place.
//! The entry's value is a [`Datum`] whose only field is an opaque 16-byte
//! record, the watermark's edition and revision count, both big-endian.
//! The datum carries no claim version, so a data screen that ever met one
//! would pass it through untouched.

use dialog_capability::history::Watermark;

use crate::history::{Edition, ORIGIN_LENGTH, Origin};
use crate::{Datum, DialogArtifactsError, Key, State};

/// The leading tag byte of the context region.
pub const CONTEXT_KEY_TAG: u8 = 6;

/// The byte width of a context region key: the tag and the origin.
pub const CONTEXT_KEY_LENGTH: usize = 1 + ORIGIN_LENGTH;

/// The byte width of a context region entry's record: edition and count.
const RECORD_LENGTH: usize = 16;

/// The key at which `origin`'s watermark is stored.
pub fn context_key(origin: &Origin) -> Key {
    let mut bytes = Vec::with_capacity(CONTEXT_KEY_LENGTH);
    bytes.push(CONTEXT_KEY_TAG);
    bytes.extend_from_slice(origin.key_bytes());
    Key::from(bytes)
}

/// The region entry recording `watermark` for `origin`.
pub fn context_entry(origin: &Origin, watermark: &Watermark) -> (Key, State<Datum>) {
    let mut record = Vec::with_capacity(RECORD_LENGTH);
    record.extend_from_slice(&watermark.edition.key_bytes());
    record.extend_from_slice(&watermark.count.to_be_bytes());
    (
        context_key(origin),
        State::Added(Datum {
            cause: None,
            blob: Some(record),
            version: None,
            collapsed: Vec::new(),
            supersedes: Vec::new(),
            retraction: false,
        }),
    )
}

/// Read one region entry back as an origin and its watermark.
///
/// The entry is refused unless it has the region's exact shape, and its
/// watermark is refused unless it passes [`Watermark::check`]: the region
/// is read from trees other replicas minted, and a hostile entry above
/// the bounds must not reach a context.
pub fn read_context_entry(
    key: &Key,
    value: &State<Datum>,
) -> Result<(Origin, Watermark), DialogArtifactsError> {
    let bytes = key.as_ref();
    let malformed =
        |what: &str| DialogArtifactsError::InvalidKey(format!("context region entry: {what}"));
    if bytes.len() != CONTEXT_KEY_LENGTH || bytes[0] != CONTEXT_KEY_TAG {
        return Err(malformed("the key is not a tag and an origin"));
    }
    let mut origin = [0u8; ORIGIN_LENGTH];
    origin.copy_from_slice(&bytes[1..]);
    let State::Added(datum) = value else {
        return Err(malformed("the entry is a tombstone"));
    };
    let record = datum
        .blob
        .as_deref()
        .filter(|record| record.len() == RECORD_LENGTH)
        .ok_or_else(|| malformed("the record is not an edition and a count"))?;
    let mut edition = [0u8; 8];
    edition.copy_from_slice(&record[..8]);
    let mut count = [0u8; 8];
    count.copy_from_slice(&record[8..]);
    let watermark = Watermark {
        edition: Edition::from_key_bytes(edition),
        count: u64::from_be_bytes(count),
    };
    watermark.check()?;
    Ok((Origin::from(origin), watermark))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::history::EDITION_CEILING;

    #[test]
    fn it_round_trips_a_watermark_through_the_region() {
        let origin = Origin::from([3u8; 32]);
        let watermark = Watermark {
            edition: Edition::new(41),
            count: 7,
        };
        let (key, value) = context_entry(&origin, &watermark);
        assert_eq!(key.as_ref().len(), CONTEXT_KEY_LENGTH);
        assert_eq!(key.as_ref()[0], CONTEXT_KEY_TAG);
        let (read_origin, read_watermark) =
            read_context_entry(&key, &value).expect("the entry reads back");
        assert_eq!(read_origin, origin);
        assert_eq!(read_watermark, watermark);
    }

    /// A region entry a hostile tree carries above the watermark bounds is
    /// refused at read, so it never reaches a context.
    #[test]
    fn it_refuses_a_region_entry_above_the_bounds() {
        let origin = Origin::from([3u8; 32]);
        for hostile in [
            Watermark {
                edition: Edition::new(EDITION_CEILING),
                count: 1,
            },
            Watermark {
                edition: Edition::new(4),
                count: 6,
            },
        ] {
            let (key, value) = context_entry(&origin, &hostile);
            assert!(
                matches!(
                    read_context_entry(&key, &value),
                    Err(DialogArtifactsError::InvalidKey(_))
                ),
                "{hostile:?} must be refused"
            );
        }
    }

    #[test]
    fn it_refuses_entries_of_the_wrong_shape() {
        let origin = Origin::from([3u8; 32]);
        let watermark = Watermark {
            edition: Edition::new(4),
            count: 1,
        };
        let (key, value) = context_entry(&origin, &watermark);

        let mut long = key.as_ref().to_vec();
        long.push(0);
        assert!(matches!(
            read_context_entry(&Key::from(long), &value),
            Err(DialogArtifactsError::InvalidKey(_))
        ));
        assert!(matches!(
            read_context_entry(&key, &State::Removed),
            Err(DialogArtifactsError::InvalidKey(_))
        ));
        let State::Added(mut datum) = value else {
            unreachable!("context entries are added")
        };
        datum.blob = Some(vec![0; 8]);
        assert!(matches!(
            read_context_entry(&key, &State::Added(datum)),
            Err(DialogArtifactsError::InvalidKey(_))
        ));
    }
}
