//! [CARv1](https://ipld.io/specs/transport/car/carv1/) for a snapshot's
//! [`Item`]s.
//!
//! [`SnapshotExport`](super::SnapshotExport) yields items as a stream and
//! [`SnapshotImport`](super::SnapshotImport) consumes one, which is the
//! right shape for moving content between two live stores and the wrong
//! shape for a file: a blob arrives as a reader, so there is nothing to
//! write down and nothing to read back.
//!
//! This is that missing half, in the format the ecosystem already has for
//! it. A snapshot can be written to disk, served over HTTP, inspected
//! with any CAR tool, or checked in as a test fixture built from a real
//! database rather than a synthetic stand-in.
//!
//! # Layout
//!
//! ```text
//! [ varint | DAG-CBOR header ] [ varint | CID | block ] [ varint | CID | block ] …
//! ```
//!
//! The header is `{version: 1, roots: [...]}`. Each section's varint
//! counts the bytes after it -- CID and data together -- and the CID is
//! in raw byte form.
//!
//! # What a snapshot adds
//!
//! A snapshot carries blocks AND blobs, and CAR has one kind of section.
//! Both are content-addressed by the same blake3 digest, so both travel
//! as sections and the CID's codec says which is which: `0x55` (raw) for
//! a blob's bytes, `0x71` (dag-cbor) for a tree block. Nothing outside
//! this module needs to know that, and a generic CAR reader sees a
//! well-formed file either way.

use dialog_common::{Blake3Hash, Buffer};
use dialog_effects::blob::{BlobError, BlobSource};
use futures_util::StreamExt as _;
use ipld_core::cid::{Cid, multihash::Multihash};

use super::{Block, Item, SnapshotError};

/// The media type these bytes carry.
pub const MEDIA_TYPE: &str = "application/vnd.ipld.car";

/// Multihash code for blake3, from the multicodec table.
const BLAKE3_CODE: u64 = 0x1e;

/// Codec for a blob's bytes: `raw`. A blob IS its bytes, which is
/// exactly what raw means, so a generic CAR reader handles these
/// correctly with no special knowledge.
const CODEC_BLOB: u64 = 0x55;

/// Codec for a tree node.
///
/// `raw` (0x55) is the registered, permanent code for raw binary, and it
/// is right for a blob: a blob IS its bytes. A tree node is not -- it is
/// an rkyv archive with structure a reader can walk -- and DAG-CBOR
/// (0x71) would be an outright lie that makes a CAR tool fail trying to
/// parse it.
///
/// So nodes take `0x54`, which the multicodec table leaves unassigned --
/// adjacent to `raw` and unlikely to collide with anything a reader of
/// these files would also be holding. The codec is what tells `decode`
/// whether a section is a node or a blob, so two distinct codes are what
/// make that distinction survive the round trip.
///
/// Unassigned is not the same as reserved: should `0x54` ever be
/// registered for something else, this constant is the one place to
/// change, and only files written before then would need rewriting.
const CODEC_NODE: u64 = 0x54;

/// A [`BlobSource`] over bytes already in memory.
struct BytesBlob(Option<Vec<u8>>);

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobSource for BytesBlob {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        Ok(self.0.take())
    }
}

/// Why a CAR's bytes could not be read as a snapshot.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// A section claimed more bytes than the input holds.
    #[error("car truncated at byte {at}")]
    Truncated {
        /// Where the section ran out.
        at: usize,
    },
    /// A varint ran past its ten-byte maximum.
    #[error("car has a malformed varint at byte {at}")]
    Varint {
        /// Where the varint began.
        at: usize,
    },
    /// The header was not readable as `{version, roots}`.
    #[error("car header is not a CARv1 header: {0}")]
    Header(String),
    /// A section's CID could not be parsed.
    #[error("car section at byte {at} has an unreadable CID: {reason}")]
    Cid {
        /// Where the section began.
        at: usize,
        /// Why the CID would not parse.
        reason: String,
    },
    /// A CID named a hash this format does not carry.
    #[error("car section at byte {at} is not blake3 (multihash code {code:#x})")]
    Digest {
        /// Where the section began.
        at: usize,
        /// The multihash code the CID stated.
        code: u64,
    },
}

/// The CARv1 header: `{version: 1, roots: [...]}`.
#[derive(serde::Serialize, serde::Deserialize)]
struct CarHeader {
    version: u64,
    roots: Vec<Cid>,
}

/// Serialize a snapshot's items as a CARv1, reading every blob to the end.
///
/// Blobs are read out here because a [`BlobReader`](super::BlobReader)
/// cannot be replayed: the bytes must be captured while the reader is
/// live or they are gone.
///
/// `roots` names what the file is a snapshot OF -- normally the
/// revision's tree root, so a reader can find the entry point without
/// guessing.
pub async fn encode<Items>(items: Items, roots: Vec<Blake3Hash>) -> Result<Vec<u8>, SnapshotError>
where
    Items: futures_util::Stream<Item = Result<Item, SnapshotError>>,
{
    let header = CarHeader {
        version: 1,
        roots: roots.iter().map(|hash| cid(hash, CODEC_NODE)).collect(),
    };
    let header = serde_ipld_dagcbor::to_vec(&header)
        .map_err(|error| {
            SnapshotError::Storage(dialog_storage::DialogStorageError::Storage(
                error.to_string(),
            ))
        })?;

    let mut out = Vec::new();
    put_varint(&mut out, header.len() as u64);
    out.extend_from_slice(&header);

    futures_util::pin_mut!(items);
    while let Some(item) = items.next().await {
        match item? {
            Item::Block(block) => {
                section(&mut out, &cid(&block.digest, CODEC_NODE), block.content.as_ref());
            }
            Item::Blob {
                digest, mut chunks, ..
            } => {
                let mut body = Vec::new();
                while let Some(chunk) = chunks.next().await? {
                    body.extend_from_slice(&chunk);
                }
                section(&mut out, &cid(&digest, CODEC_BLOB), &body);
            }
        }
    }
    Ok(out)
}

/// The CIDv1 addressing `hash` under `codec`.
fn cid(hash: &Blake3Hash, codec: u64) -> Cid {
    #[allow(clippy::expect_used)]
    let multihash =
        Multihash::wrap(BLAKE3_CODE, hash.as_ref()).expect("a blake3 digest fits a multihash");
    Cid::new_v1(codec, multihash)
}

/// Append one CAR section: `varint(len(cid) + len(body)) | cid | body`.
fn section(out: &mut Vec<u8>, cid: &Cid, body: &[u8]) {
    let cid = cid.to_bytes();
    put_varint(out, (cid.len() + body.len()) as u64);
    out.extend_from_slice(&cid);
    out.extend_from_slice(body);
}

/// Append an unsigned LEB128 varint.
fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

/// Read an unsigned LEB128 varint, advancing `at`.
fn take_varint(bytes: &[u8], at: &mut usize) -> Result<u64, CodecError> {
    let start = *at;
    let mut value = 0u64;
    for shift in 0..10 {
        let byte = *bytes.get(*at).ok_or(CodecError::Truncated { at: start })?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << (shift * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(CodecError::Varint { at: start })
}

/// Parse a CARv1 into the items
/// [`Repository::import`](crate::Repository::import) consumes.
///
/// Addresses are derived from content rather than read out of the CID, so
/// the import's verification checks the bytes against something computed
/// here rather than a label that travelled with them. The CID's codec
/// still says whether a section is a block or a blob, and its multihash
/// code is checked as a cheap guard against reading a CAR this format did
/// not write.
pub fn decode(bytes: &[u8]) -> Result<Vec<Result<Item, SnapshotError>>, CodecError> {
    let mut at = 0usize;

    let header_len = take_varint(bytes, &mut at)? as usize;
    let header = bytes
        .get(at..at + header_len)
        .ok_or(CodecError::Truncated { at })?;
    at += header_len;
    let header: CarHeader =
        serde_ipld_dagcbor::from_slice(header).map_err(|e| CodecError::Header(e.to_string()))?;
    if header.version != 1 {
        return Err(CodecError::Header(format!(
            "version {} is not 1",
            header.version
        )));
    }

    let mut items = Vec::new();
    while at < bytes.len() {
        let start = at;
        let section_len = take_varint(bytes, &mut at)? as usize;
        let section = bytes
            .get(at..at + section_len)
            .ok_or(CodecError::Truncated { at: start })?;
        at += section_len;

        let mut reader = section;
        let cid = Cid::read_bytes(&mut reader).map_err(|error| CodecError::Cid {
            at: start,
            reason: error.to_string(),
        })?;
        if cid.hash().code() != BLAKE3_CODE {
            return Err(CodecError::Digest {
                at: start,
                code: cid.hash().code(),
            });
        }
        let body = reader.to_vec();

        let content = Buffer::from(body.as_slice());
        let digest = content.blake3_hash().clone();
        items.push(Ok(if cid.codec() == CODEC_BLOB {
            Item::Blob {
                digest,
                size: body.len() as u64,
                chunks: Box::new(BytesBlob(Some(body))),
            }
        } else {
            Item::Block(Block { digest, content })
        }));
    }

    Ok(items)
}
