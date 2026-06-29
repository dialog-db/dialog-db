//! Extension traits for fluent blob capability chains.
//!
//! ```
//! use dialog_effects::blob::prelude::*;
//! ```

use dialog_capability::Capability;
use dialog_common::Blake3Hash;

use crate::archive::Archive;

use super::{Blob, Import, Read, Write};

/// Scope an archive capability to its blob store.
pub trait ArchiveBlobExt {
    /// The resulting blob chain type.
    type Blob;
    /// Scope to the blob store.
    fn blob(self) -> Self::Blob;
}

impl ArchiveBlobExt for Capability<Archive> {
    type Blob = Capability<Blob>;
    fn blob(self) -> Capability<Blob> {
        self.attenuate(Blob)
    }
}

/// Invoke effects on the blob store.
pub trait BlobExt {
    /// The resulting read chain type.
    type Read;
    /// The resulting write (ingest) chain type.
    type Write;
    /// The resulting import chain type.
    type Import;

    /// Read a blob by hash.
    fn read(self, digest: impl Into<Blake3Hash>) -> Self::Read;
    /// Ingest a blob whose hash is discovered during the write.
    fn write(self) -> Self::Write;
    /// Import a blob whose hash is already known.
    fn import(self, digest: impl Into<Blake3Hash>, size: u64) -> Self::Import;
}

impl BlobExt for Capability<Blob> {
    type Read = Capability<Read>;
    type Write = Capability<Write>;
    type Import = Capability<Import>;

    fn read(self, digest: impl Into<Blake3Hash>) -> Capability<Read> {
        self.invoke(Read::new(digest))
    }

    fn write(self) -> Capability<Write> {
        self.invoke(Write::new())
    }

    fn import(self, digest: impl Into<Blake3Hash>, size: u64) -> Capability<Import> {
        self.invoke(Import::new(digest, size))
    }
}
