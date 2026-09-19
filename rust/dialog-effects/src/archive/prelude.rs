//! Extension traits for fluent archive capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::archive::prelude::*;
//! ```

use dialog_capability::{Capability, Did, Policy, Subject};
use dialog_common::{Blake3Hash, Buffer};

use super::{Archive, Block, Catalog, Get, Import, Put};
use crate::AttenuateVerb;

/// Extension trait to start an archive capability chain.
pub trait ArchiveSubjectExt {
    /// The resulting archive chain type.
    type Archive;
    /// Begin an archive capability chain.
    fn archive(self) -> Self::Archive;
}

impl ArchiveSubjectExt for Subject {
    type Archive = ArchiveScope;
    fn archive(self) -> ArchiveScope {
        ArchiveScope { subject: self }
    }
}

impl ArchiveSubjectExt for Did {
    type Archive = ArchiveScope;
    fn archive(self) -> ArchiveScope {
        ArchiveScope {
            subject: Subject::from(self),
        }
    }
}

/// An archive chain that has not chosen its verb yet.
///
/// See the note in [`memory::prelude`](crate::memory::prelude) for why
/// the builder defers.
#[derive(Debug, Clone)]
pub struct ArchiveScope {
    subject: Subject,
}

impl ArchiveScope {
    /// The subject this scope was started from, for a sibling namespace
    /// (the blob store) that continues from the same root.
    pub(crate) fn into_subject(self) -> Subject {
        self.subject
    }
}

/// Extension methods for scoping archive to a named catalog.
pub trait ArchiveExt {
    /// The resulting catalog chain type.
    type Catalog;
    /// Scope to a named catalog.
    fn catalog(self, name: impl Into<String>) -> Self::Catalog;
}

impl ArchiveExt for ArchiveScope {
    type Catalog = CatalogScope;
    fn catalog(self, name: impl Into<String>) -> CatalogScope {
        CatalogScope {
            subject: self.subject,
            catalog: name.into(),
        }
    }
}

/// A catalog chain that has not chosen its verb yet.
#[derive(Debug, Clone)]
pub struct CatalogScope {
    subject: Subject,
    catalog: String,
}

impl CatalogScope {
    /// Build the block chain under `V`.
    fn under<V>(self) -> Capability<Block<V>>
    where
        V: crate::Verb,
        V::Of: dialog_capability::Constraint,
        Subject: AttenuateVerb<V>,
    {
        AttenuateVerb::verb(self.subject)
            .attenuate(Archive::<V>::new())
            .attenuate(Catalog::<V>::new(self.catalog))
            .attenuate(Block::<V>::new())
    }
}

/// Extension methods for invoking effects on a catalog.
pub trait CatalogExt {
    /// The resulting get chain type.
    type Get;
    /// The resulting put chain type.
    type Put;
    /// The resulting import chain type.
    type Import;
    /// Get content by digest.
    fn get(self, digest: impl Into<Blake3Hash>) -> Self::Get;
    /// Put a single content-addressed block.
    fn put(self, block: impl Into<Buffer>) -> Self::Put;
    /// Import a batch of content-addressed blocks.
    fn import(self, blocks: impl IntoIterator<Item = impl Into<Buffer>>) -> Self::Import;
}

impl CatalogExt for CatalogScope {
    type Get = Capability<Get>;
    type Put = Capability<Put>;
    type Import = Capability<Import>;

    fn get(self, digest: impl Into<Blake3Hash>) -> Capability<Get> {
        self.under::<crate::Get>().invoke(Get::new(digest))
    }

    fn put(self, block: impl Into<Buffer>) -> Capability<Put> {
        self.under::<crate::Put>().invoke(Put::new(block))
    }

    fn import(self, blocks: impl IntoIterator<Item = impl Into<Buffer>>) -> Capability<Import> {
        self.under::<crate::Put>().invoke(Import::new(blocks))
    }
}

/// Field accessors on `Capability<Import>`.
pub trait ImportExt {
    /// Get the catalog name from the capability chain.
    fn catalog(&self) -> &str;
    /// Get the blocks from the capability chain.
    fn blocks(&self) -> &[Buffer];
}

impl ImportExt for Capability<Import> {
    fn catalog(&self) -> &str {
        &Catalog::<crate::Put>::of(self).catalog
    }

    fn blocks(&self) -> &[Buffer] {
        &Import::of(self).blocks
    }
}

/// Field accessors on `Capability<Get>`.
pub trait GetExt {
    /// Get the catalog name from the capability chain.
    fn catalog(&self) -> &str;
    /// Get the digest from the capability chain.
    fn digest(&self) -> &Blake3Hash;
}

impl GetExt for Capability<Get> {
    fn catalog(&self) -> &str {
        &Catalog::<crate::Get>::of(self).catalog
    }

    fn digest(&self) -> &Blake3Hash {
        &Get::of(self).digest
    }
}

/// Field accessors on `Capability<Put>`.
pub trait PutExt {
    /// Get the catalog name from the capability chain.
    fn catalog(&self) -> &str;
    /// Get the digest from the capability chain (derived from the block).
    fn digest(&self) -> &Blake3Hash;
    /// Get the content from the capability chain.
    fn content(&self) -> &[u8];
}

impl PutExt for Capability<Put> {
    fn catalog(&self) -> &str {
        &Catalog::<crate::Put>::of(self).catalog
    }

    fn digest(&self) -> &Blake3Hash {
        Put::of(self).block.blake3_hash()
    }

    fn content(&self) -> &[u8] {
        Put::of(self).block.as_ref()
    }
}
