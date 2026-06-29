//! Extension traits for fluent blob capability chains.
//!
//! ```
//! use dialog_effects::blob::prelude::*;
//! ```

use dialog_capability::{Capability, Policy};
use dialog_common::Blake3Hash;

use crate::archive::Archive;

use super::{Blob, ByteRange, Import, Read, Write};

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

/// Field accessors on `Capability<Read>`.
pub trait BlobReadExt {
    /// The blob digest from the capability chain.
    fn digest(&self) -> &Blake3Hash;
    /// The byte range from the capability chain.
    fn range(&self) -> Option<ByteRange>;
}

impl BlobReadExt for Capability<Read> {
    fn digest(&self) -> &Blake3Hash {
        &Read::of(self).digest
    }

    fn range(&self) -> Option<ByteRange> {
        Read::of(self).range
    }
}

/// Field accessors on `Capability<Import>`.
pub trait BlobImportExt {
    /// The blob digest (and destination key) from the capability chain.
    fn digest(&self) -> &Blake3Hash;
    /// The declared total size from the capability chain.
    fn size(&self) -> u64;
    /// The per-part hashes from the capability chain.
    fn chunks(&self) -> &[[u8; 32]];
}

impl BlobImportExt for Capability<Import> {
    fn digest(&self) -> &Blake3Hash {
        &Import::of(self).digest
    }

    fn size(&self) -> u64 {
        Import::of(self).size
    }

    fn chunks(&self) -> &[[u8; 32]] {
        &Import::of(self).chunks
    }
}
