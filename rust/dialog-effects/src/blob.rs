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
//!               ├── Write                          → BlobWriter  (ingest; finish → hash)
//!               ├── Import { digest, size, chunks } → BlobWriter
//!               └── Read { digest, range }          → BlobReader
//! ```
//!
//! Bytes never travel inside an effect: the signed capability carries only
//! hash-addressed metadata (`digest`, `range`, `size`, per-part `chunks`), and
//! a blob effect's *output* is a streaming transfer handle that the caller
//! reads from ([`BlobReader`]) or writes into ([`BlobWriter`]).

use async_trait::async_trait;
use std::error::Error;

use dialog_common::{Blake3Hash, ConditionalSend};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::archive::Archive;
pub use dialog_capability::{
    Attenuate, Attenuation, DialogCapabilityAuthorizationError, DialogCapabilityPerformError,
    Effect, Policy, StorageError, Subject, access::AuthorizeError,
};

/// Blob store domain under the archive. Adds the `/blob` ability segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Blob;

impl Attenuation for Blob {
    type Of = Archive;
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

impl Effect for Read {
    type Of = Blob;
    type Output = Result<BlobReader, BlobError>;
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

impl Effect for Write {
    type Of = Blob;
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

impl Effect for Import {
    type Of = Blob;
    type Output = Result<BlobWriter, BlobError>;
}

pub mod prelude;

/// Errors that can occur during blob operations.
#[derive(Debug, Error)]
pub enum BlobError {
    /// The blob was not found.
    #[error("Blob not found: {0}")]
    NotFound(String),

    /// The written content did not hash to the declared digest.
    #[error("Blob digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch {
        /// The declared digest.
        expected: String,
        /// The digest computed from the written bytes.
        actual: String,
    },

    /// Authorization failed.
    #[error("Unauthorized error: {0}")]
    AuthorizationError(String),

    /// The operation failed during execution.
    #[error("Execution error: {0}")]
    ExecutionError(String),

    /// The storage backend failed.
    #[error("Storage error: {0}")]
    Storage(String),

    /// An I/O error occurred.
    #[error("IO error: {0}")]
    Io(String),
}

impl From<StorageError> for BlobError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<DialogCapabilityAuthorizationError> for BlobError {
    fn from(value: DialogCapabilityAuthorizationError) -> Self {
        BlobError::AuthorizationError(value.to_string())
    }
}

impl From<AuthorizeError> for BlobError {
    fn from(value: AuthorizeError) -> Self {
        BlobError::AuthorizationError(value.to_string())
    }
}

impl<E: Error> From<DialogCapabilityPerformError<E>> for BlobError {
    fn from(value: DialogCapabilityPerformError<E>) -> Self {
        match value {
            DialogCapabilityPerformError::Authorization(error) => {
                BlobError::AuthorizationError(error.to_string())
            }
            DialogCapabilityPerformError::Execution(error) => {
                BlobError::ExecutionError(error.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::prelude::*;
    use crate::archive::Archive;
    use dialog_capability::{Subject, did};

    #[test]
    fn it_builds_blob_read_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Archive)
            .blob()
            .read([0u8; 32]);
        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/archive/blob/read");
    }

    #[test]
    fn it_builds_blob_write_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Archive)
            .blob()
            .write();
        assert_eq!(claim.ability(), "/archive/blob/write");
    }

    #[test]
    fn it_builds_blob_import_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Archive)
            .blob()
            .import([0u8; 32], 4096);
        assert_eq!(claim.ability(), "/archive/blob/import");
    }
}
