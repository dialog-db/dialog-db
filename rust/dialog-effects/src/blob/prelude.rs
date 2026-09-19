//! Extension traits for fluent blob capability chains.
//!
//! ```
//! use dialog_effects::blob::prelude::*;
//! ```

use dialog_capability::{Capability, Policy, Subject};
use dialog_common::Blake3Hash;

use crate::AttenuateVerb;
use crate::archive::Archive;
use crate::archive::prelude::ArchiveScope;

use super::{Blob, ByteRange, Import, Read, Write};

/// Scope an archive capability to its blob store.
pub trait ArchiveBlobExt {
    /// The resulting blob chain type.
    type Blob;
    /// Scope to the blob store.
    fn blob(self) -> Self::Blob;
}

impl ArchiveBlobExt for ArchiveScope {
    type Blob = BlobScope;
    fn blob(self) -> BlobScope {
        BlobScope {
            subject: self.into_subject(),
        }
    }
}

/// A blob chain that has not chosen its verb yet.
///
/// See the note in [`memory::prelude`](crate::memory::prelude) for why
/// the builder defers.
#[derive(Debug, Clone)]
pub struct BlobScope {
    subject: Subject,
}

impl BlobScope {
    /// Invoke a pre-built effect on this scope.
    ///
    /// The builder methods cover the common cases; this is for a caller
    /// that already holds the effect value, such as one reconstructing a
    /// read with an explicit byte range.
    pub fn invoke<Fx>(self, effect: Fx) -> Capability<Fx>
    where
        Fx: dialog_capability::Effect,
        Fx::Of: dialog_capability::Constraint,
        Self: InvokeOn<Fx>,
    {
        InvokeOn::invoke_on(self, effect)
    }

    /// Build the chain under `V`.
    fn under<V>(self) -> Capability<Blob<V>>
    where
        V: crate::Verb,
        V::Of: dialog_capability::Constraint,
        Subject: AttenuateVerb<V>,
    {
        AttenuateVerb::verb(self.subject)
            .attenuate(Archive::<V>::new())
            .attenuate(Blob::<V>::new())
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

impl BlobExt for BlobScope {
    type Read = Capability<Read>;
    type Write = Capability<Write>;
    type Import = Capability<Import>;

    fn read(self, digest: impl Into<Blake3Hash>) -> Capability<Read> {
        self.under::<crate::Get>().invoke(Read::new(digest))
    }

    fn write(self) -> Capability<Write> {
        self.under::<crate::Put>().invoke(Write::new())
    }

    fn import(self, digest: impl Into<Blake3Hash>, size: u64) -> Capability<Import> {
        self.under::<crate::Put>().invoke(Import::new(digest, size))
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

/// How a scope builds the chain for one effect.
///
/// One impl per effect, so the verb a scope attenuates under is fixed
/// by the effect being invoked rather than by the caller.
pub trait InvokeOn<Fx: dialog_capability::Effect>
where
    Fx::Of: dialog_capability::Constraint,
{
    /// Build this scope's chain and invoke `effect` on it.
    fn invoke_on(self, effect: Fx) -> Capability<Fx>;
}

impl InvokeOn<Read> for BlobScope {
    fn invoke_on(self, effect: Read) -> Capability<Read> {
        self.under::<crate::Get>().invoke(effect)
    }
}

impl InvokeOn<Write> for BlobScope {
    fn invoke_on(self, effect: Write) -> Capability<Write> {
        self.under::<crate::Put>().invoke(effect)
    }
}

impl InvokeOn<Import> for BlobScope {
    fn invoke_on(self, effect: Import) -> Capability<Import> {
        self.under::<crate::Put>().invoke(effect)
    }
}
