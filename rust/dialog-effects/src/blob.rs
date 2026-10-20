//! Blob capability hierarchy.
//!
//! A blob store under the archive: whole, hash-addressable binary objects with
//! streaming and ranged access. It is a distinct capability surface from the
//! block [`Catalog`](crate::archive::Catalog) because the verbs differ —
//! blobs stream, blocks are whole buffers — so it is its own type under
//! [`Archive`](crate::archive::Archive).
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject
//!   └── Archive (/archive)
//!         └── Blob (/archive/blob)
//!               ├── Write                             → BlobWriter  (ingest; finish → hash)
//!               ├── Import { digest, size, chunks }    → BlobWriter
//!               ├── ImportRange { digest, size, range } → BlobWriter  (verified groups)
//!               ├── Read { digest, range }             → BlobReader
//!               ├── Size { digest }                    → Option<u64>
//!               ├── Held { digest }                    → Option<Holding>
//!               └── Usage                               → u64 (bytes kept)
//! ```
//!
//! Bytes never travel inside an effect: the signed capability carries only
//! hash-addressed metadata (`digest`, `range`, `size`, per-part `chunks`), and
//! a blob effect's *output* is a streaming transfer handle that the caller
//! reads from ([`BlobReader`]) or writes into ([`BlobWriter`]).
//!
//! # Partial blobs
//!
//! The hash of a blob is the root of its Bao tree, a tree of BLAKE3 hashes
//! over groups of 16 KiB of the blob. A range of the blob that comes with the
//! hashes on the path from the root to each of its groups can be verified
//! alone, group by group. So a store can hold a blob in part: some of its
//! groups, and the hashes that verified them. [`ImportRange`] keeps each
//! group of such a range that verifies, and [`Held`] says which groups a store
//! holds. A store answers [`Size`] only for a blob that it holds whole, and a
//! [`Read`] of a blob that it holds in part ends where the held part of the
//! range ends.

use crate::archive::Archive;
use crate::method;
use async_trait::async_trait;
use std::marker::PhantomData;

use dialog_common::{Blake3Hash, ConditionalSend};
use serde::{Deserialize, Serialize};

pub use dialog_capability::{
    Attenuate, Attenuation, DialogCapabilityPerformError, Effect, Policy, StorageError, Subject,
    access::AuthorizeError,
};

/// Blob store domain under the archive. Contributes no ability segment of
/// its own: the effects name the whole command (`/use/get/archive/blob`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Blob<V = method::Get>(#[serde(skip)] PhantomData<V>);

impl<V> Blob<V> {
    /// The blob resource under `V`.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<V> Default for Blob<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: crate::Method> Attenuation for Blob<M>
where
    M::Of: dialog_capability::Constraint,
{
    type Of = Archive<M>;

    fn attenuation() -> &'static str {
        "blob"
    }
}

/// A byte range for a ranged read: `length` bytes starting at `offset`, or to
/// end-of-blob when `length` is `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteRange {
    /// Offset of the first byte to read.
    pub offset: u64,
    /// Number of bytes to read, or all remaining when `None`.
    pub length: Option<u64>,
}

/// A streaming source of blob bytes. [`next`](Self::next) yields chunks in
/// order and `None` at end-of-stream, so a reader never buffers the whole blob.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait BlobSource: ConditionalSend {
    /// The next chunk of bytes, or `None` once the blob is exhausted.
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError>;
}

/// A streaming sink for blob bytes. Write chunks with
/// [`write_all`](Self::write_all), then [`finish`](Self::finish) to commit the
/// blob and obtain its content hash.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait BlobSink: ConditionalSend {
    /// Append a chunk to the blob being written.
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError>;
    /// Commit the blob and return its content hash. For an ingest
    /// ([`Write`]) this is the discovered hash; for an [`Import`] it is the
    /// declared digest, after the written bytes are verified against it.
    async fn finish(self: Box<Self>) -> Result<Blake3Hash, BlobError>;
}

/// A streaming reader returned by [`Read`]. `Box<dyn BlobSource>` so any
/// provider (local filesystem, remote) returns one type.
pub type BlobReader = Box<dyn BlobSource>;

/// A streaming writer returned by [`Write`] / [`Import`].
pub type BlobWriter = Box<dyn BlobSink>;

/// Read a blob, optionally a byte range, by hash.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Read {
    /// The blob's content hash.
    #[serde(with = "dialog_common::as_bytes")]
    pub digest: Blake3Hash,
    /// The byte range to read, or the whole blob when `None`.
    pub range: Option<ByteRange>,
}

impl Read {
    /// Read the whole blob `digest`.
    pub fn new(digest: impl Into<Blake3Hash>) -> Self {
        Self {
            digest: digest.into(),
            range: None,
        }
    }

    /// Read a byte range of the blob `digest`.
    pub fn range(digest: impl Into<Blake3Hash>, offset: u64, length: Option<u64>) -> Self {
        Self {
            digest: digest.into(),
            range: Some(ByteRange { offset, length }),
        }
    }
}

impl Policy for Read {
    type Of = Blob<method::Get>;
}

impl Effect for Read {
    type Output = Result<BlobReader, BlobError>;
}

/// The size of a blob by hash, without reading its bytes: `None` when the
/// store holds no blob under `digest`.
///
/// A store answers from what it keeps beside the bytes (a file's length, a
/// buffer's). One that keeps nothing of the kind may answer by reading the
/// blob through, and says so where it implements this.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Size {
    /// The blob's content hash.
    #[serde(with = "dialog_common::as_bytes")]
    pub digest: Blake3Hash,
}

impl Size {
    /// The size of the blob `digest`.
    pub fn new(digest: impl Into<Blake3Hash>) -> Self {
        Self {
            digest: digest.into(),
        }
    }
}

impl Policy for Size {
    type Of = Blob<method::Get>;
}

impl Effect for Size {
    type Output = Result<Option<u64>, BlobError>;
}

/// Which bytes of a blob a store holds: the groups of a blob that it holds in
/// part, or all of a blob that it holds whole.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Holding {
    /// The size of the blob.
    pub size: u64,
    /// The ranges of bytes that the store holds, in order, each from the
    /// start of a group to the end of a group or of the blob, with no two
    /// that touch.
    pub ranges: Vec<(u64, u64)>,
}

impl Holding {
    /// Whether the store holds each byte of the blob.
    pub fn is_complete(&self) -> bool {
        self.ranges.as_slice() == [(0, self.size)] || self.size == 0
    }

    /// Where the held part of the range from `offset` ends: `offset` itself
    /// when the store does not hold the byte at `offset`.
    pub fn held_from(&self, offset: u64) -> u64 {
        self.ranges
            .iter()
            .find(|(start, end)| *start <= offset && offset < *end)
            .map_or(offset, |(_, end)| *end)
    }
}

/// Which bytes of the blob `digest` the store holds, without reading them:
/// `None` when it holds none of them.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Held {
    /// The blob's content hash.
    #[serde(with = "dialog_common::as_bytes")]
    pub digest: Blake3Hash,
}

impl Held {
    /// The held bytes of the blob `digest`.
    pub fn new(digest: impl Into<Blake3Hash>) -> Self {
        Self {
            digest: digest.into(),
        }
    }
}

impl Policy for Held {
    type Of = Blob<method::Get>;
}

impl Effect for Held {
    type Output = Result<Option<Holding>, BlobError>;
}

/// Import a verified range of a blob whose hash and size are **already
/// known**, into the blob store, as a part of the blob.
///
/// The sink takes the Bao encoding of the groups of `range`, in the order of
/// the encoding: each group, after the hashes of the tree that verify it
/// against `digest`, with groups of 16 KiB, as `bao-tree` encodes a range.
/// The store keeps each group that verifies, and the hashes that verified it,
/// as it arrives. A group that does not verify fails the write, and the store
/// keeps nothing of it; the groups before it stay.
/// [`finish`](BlobSink::finish) records what the store keeps, and gives the
/// digest. Once the store holds each group of the blob, it holds the blob
/// whole.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct ImportRange {
    /// The blob's content hash: the root of its Bao tree.
    #[serde(with = "dialog_common::as_bytes")]
    pub digest: Blake3Hash,
    /// The size of the blob, which shapes its tree.
    pub size: u64,
    /// The range of the encoding, from the start of a group to the end of a
    /// group or of the blob.
    pub range: ByteRange,
}

impl ImportRange {
    /// Import the groups of `range` of the blob `digest` of `size` bytes.
    pub fn new(digest: impl Into<Blake3Hash>, size: u64, range: ByteRange) -> Self {
        Self {
            digest: digest.into(),
            size,
            range,
        }
    }
}

impl Policy for ImportRange {
    type Of = Blob<method::Put>;
}

impl Effect for ImportRange {
    type Output = Result<BlobWriter, BlobError>;
}

/// The bytes that the blob store of the subject keeps now: each whole blob
/// and its outboard, and of each partial blob the groups that it holds and
/// the hashes that verified them. A write that has not ended keeps nothing
/// yet. A store counts what it realized, so a blob that a store knows only
/// by its hash costs nothing, and a quota can be a share of the disk.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Usage;

impl Usage {
    /// The bytes that the blob store keeps.
    pub fn new() -> Self {
        Self
    }
}

impl Default for Usage {
    fn default() -> Self {
        Self::new()
    }
}

impl Policy for Usage {
    type Of = Blob<method::Get>;
}

impl Effect for Usage {
    type Output = Result<u64, BlobError>;
}

/// Ingest a blob whose hash is **discovered** during the write. Carries no
/// content-bound arguments — the hash is not known until the bytes are
/// streamed in and hashed; [`BlobSink::finish`] returns it.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Write;

impl Write {
    /// Create a new ingest effect.
    pub fn new() -> Self {
        Self
    }
}

impl Default for Write {
    fn default() -> Self {
        Self::new()
    }
}

impl Policy for Write {
    type Of = Blob<method::Put>;
}

impl Effect for Write {
    type Output = Result<BlobWriter, BlobError>;
}

/// Import a blob whose hash is **already known**: a content-bound write used by
/// replication. `chunks` are the per-part hashes (the unit of upload) and
/// `size` the total length; a local provider ignores them, while a remote
/// provider uses them to plan and verify the transfer.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Import {
    /// The blob's content hash (its address) and the destination key.
    #[serde(with = "dialog_common::as_bytes")]
    pub digest: Blake3Hash,
    /// Total size of the blob in bytes.
    pub size: u64,
    /// Per-part hashes (SHA-256), in order. Empty for a single-part import.
    pub chunks: Vec<[u8; 32]>,
}

impl Import {
    /// Import the whole blob `digest` of `size` bytes as a single part.
    pub fn new(digest: impl Into<Blake3Hash>, size: u64) -> Self {
        Self {
            digest: digest.into(),
            size,
            chunks: Vec::new(),
        }
    }
}

impl Policy for Import {
    type Of = Blob<method::Put>;
}

impl Effect for Import {
    type Output = Result<BlobWriter, BlobError>;
}

pub mod prelude;

mod error;
pub use error::*;

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::prelude::*;
    use dialog_capability::{Subject, did};

    #[dialog_common::test]
    fn it_builds_blob_read_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .reader()
            .archive()
            .blob()
            .read([0u8; 32]);
        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/use/get/archive/blob");
    }

    #[dialog_common::test]
    fn it_builds_blob_size_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .reader()
            .archive()
            .blob()
            .size([0u8; 32]);
        assert_eq!(claim.ability(), "/use/get/archive/blob");
    }

    #[dialog_common::test]
    fn it_builds_blob_write_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .writer()
            .archive()
            .blob()
            .write();
        assert_eq!(claim.ability(), "/use/put/archive/blob");
    }

    #[dialog_common::test]
    fn it_builds_blob_import_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .writer()
            .archive()
            .blob()
            .import([0u8; 32], 4096);
        assert_eq!(claim.ability(), "/use/put/archive/blob");
    }
}
