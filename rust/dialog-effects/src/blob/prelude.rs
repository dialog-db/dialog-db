//! Extension traits for fluent blob capability chains.
//!
//! ```
//! use dialog_effects::blob::prelude::*;
//! ```
//!
//! A chain is written in the order the path reads:
//!
//! ```text
//! subject.reader().archive().blob().read(digest)
//!                              = /use/get/archive/blob
//! ```

use dialog_capability::{Capability, Constraint, Policy};
use dialog_common::Blake3Hash;

use crate::archive::Archive;
use crate::{Method, method};

use super::{Blob, ByteRange, Import, Read, Write};

/// Scope the archive to its blob store.
pub trait ArchiveBlobExt {
    /// The resulting blob chain type.
    type Blob;
    /// Scope to the blob store.
    fn blob(self) -> Self::Blob;
}

impl<M: Method> ArchiveBlobExt for Capability<Archive<M>>
where
    M::Of: Constraint,
{
    type Blob = Capability<Blob<M>>;
    fn blob(self) -> Self::Blob {
        self.attenuate(Blob::new())
    }
}

/// Read a blob.
pub trait ReadBlobExt {
    /// Read a blob by hash.
    fn read(self, digest: impl Into<Blake3Hash>) -> Capability<Read>;
}

impl ReadBlobExt for Capability<Blob<method::Get>> {
    fn read(self, digest: impl Into<Blake3Hash>) -> Capability<Read> {
        self.invoke(Read::new(digest))
    }
}

/// Write a blob.
pub trait WriteBlobExt {
    /// Ingest a blob whose hash is discovered during the write.
    fn write(self) -> Capability<Write>;
    /// Import a blob whose hash is already known.
    fn import(self, digest: impl Into<Blake3Hash>, size: u64) -> Capability<Import>;
}

impl WriteBlobExt for Capability<Blob<method::Put>> {
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
