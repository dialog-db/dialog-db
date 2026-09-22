//! Extension traits for fluent archive capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::archive::prelude::*;
//! ```
//!
//! A chain is written in the order the path reads: the method, then the
//! namespace, then the resource, then the effect.
//!
//! ```text
//! subject.get().archive().block().get(digest)
//!                              = /use/get/archive/block
//! ```
//!
//! The catalog is a parameter, not a path segment, so a chain that
//! names none reaches [`DEFAULT_CATALOG`]. Name one to reach another:
//!
//! ```text
//! subject.get().archive().catalog("blobs").get(digest)
//! ```

use dialog_capability::{Capability, Constraint, Policy};
use dialog_common::{Blake3Hash, Buffer};

use super::{Archive, Block, Catalog, Get, Import, Put};
use crate::blob::{Blob, Import as BlobImport, Read as BlobRead, Write as BlobWrite};
use crate::{Method, MethodExt as _, method};

/// Scope a method to the archive namespace.
pub trait ArchiveExt {
    /// The resulting archive chain type.
    type Archive;
    /// Scope to the archive.
    fn archive(self) -> Self::Archive;
}

impl<M: Method> ArchiveExt for Capability<M>
where
    M::Of: Constraint,
{
    type Archive = Capability<Archive<M>>;
    fn archive(self) -> Self::Archive {
        self.attenuate(Archive::new())
    }
}

/// A subject reaches its archive without naming a method, because a
/// handle held across reads and writes has no one method to name. The
/// operation performed on it picks the method.
impl ArchiveExt for dialog_capability::Subject {
    type Archive = ArchiveScope;
    fn archive(self) -> Self::Archive {
        ArchiveScope::new(self)
    }
}

impl ArchiveExt for dialog_capability::Did {
    type Archive = ArchiveScope;
    fn archive(self) -> Self::Archive {
        ArchiveScope::new(dialog_capability::Subject::from(self))
    }
}

/// Scope the archive to a named catalog.
pub trait CatalogExt {
    /// The resulting catalog chain type.
    type Catalog;
    /// Scope to a named catalog.
    fn catalog(self, name: impl Into<String>) -> Self::Catalog;
}

impl<M: Method> CatalogExt for Capability<Archive<M>>
where
    M::Of: Constraint,
{
    type Catalog = Capability<Catalog<M>>;
    fn catalog(self, name: impl Into<String>) -> Self::Catalog {
        self.attenuate(Catalog::new(name))
    }
}

/// The catalog a chain reaches when it names none.
///
/// A repository keeps its blocks in one catalog and everything else in
/// named ones, so naming it at every call site is noise. The name rides
/// in the parameters, never in the path, so defaulting it changes no
/// command.
pub const DEFAULT_CATALOG: &str = "index";

/// Scope a catalog to its blocks.
pub trait BlockExt {
    /// The resulting block chain type.
    type Block;
    /// Scope to the catalog's blocks.
    fn block(self) -> Self::Block;
}

impl<M: Method> BlockExt for Capability<Catalog<M>>
where
    M::Of: Constraint,
{
    type Block = Capability<Block<M>>;
    fn block(self) -> Self::Block {
        self.attenuate(Block::new())
    }
}

/// Reach the blocks of the default catalog, without naming it.
///
/// `archive().block()` is `archive().catalog("index").block()`.
impl<M: Method> BlockExt for Capability<Archive<M>>
where
    M::Of: Constraint,
{
    type Block = Capability<Block<M>>;
    fn block(self) -> Self::Block {
        self.catalog(DEFAULT_CATALOG).block()
    }
}

/// Read a block.
pub trait GetBlockExt {
    /// Get content by digest.
    fn get(self, digest: impl Into<Blake3Hash>) -> Capability<Get>;
}

impl GetBlockExt for Capability<Catalog<method::Get>> {
    fn get(self, digest: impl Into<Blake3Hash>) -> Capability<Get> {
        self.block().invoke(Get::new(digest))
    }
}

impl GetBlockExt for Capability<Archive<method::Get>> {
    fn get(self, digest: impl Into<Blake3Hash>) -> Capability<Get> {
        self.catalog(DEFAULT_CATALOG).get(digest)
    }
}

/// Write blocks.
pub trait PutBlockExt {
    /// Put a single content-addressed block.
    fn put(self, block: impl Into<Buffer>) -> Capability<Put>;
    /// Import a batch of content-addressed blocks.
    fn import(self, blocks: impl IntoIterator<Item = impl Into<Buffer>>) -> Capability<Import>;
}

impl PutBlockExt for Capability<Catalog<method::Put>> {
    fn put(self, block: impl Into<Buffer>) -> Capability<Put> {
        self.block().invoke(Put::new(block))
    }

    fn import(self, blocks: impl IntoIterator<Item = impl Into<Buffer>>) -> Capability<Import> {
        self.block().invoke(Import::new(blocks))
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
        &Catalog::of(self).catalog
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
        &Catalog::of(self).catalog
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
        &Catalog::of(self).catalog
    }

    fn digest(&self) -> &Blake3Hash {
        Put::of(self).block.blake3_hash()
    }

    fn content(&self) -> &[u8] {
        Put::of(self).block.as_ref()
    }
}

/// A catalog named but not yet reached by any method.
///
/// See [`CellScope`](crate::memory::prelude::CellScope) for why a
/// stored handle keeps names rather than a chain.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogScope {
    subject: dialog_capability::Subject,
    catalog: String,
}

impl CatalogScope {
    /// Name a catalog without choosing a method.
    pub fn new(subject: dialog_capability::Subject, catalog: impl Into<String>) -> Self {
        Self {
            subject,
            catalog: catalog.into(),
        }
    }

    /// The subject this catalog belongs to.
    pub fn subject(&self) -> &dialog_capability::Did {
        self.subject.did()
    }

    /// The catalog's name.
    pub fn catalog_name(&self) -> &str {
        &self.catalog
    }

    /// The chain for reading from it.
    pub fn read(&self) -> Capability<Catalog<method::Get>> {
        self.subject
            .clone()
            .get()
            .archive()
            .catalog(self.catalog.clone())
    }

    /// The chain for writing to it.
    pub fn write(&self) -> Capability<Catalog<method::Put>> {
        self.subject
            .clone()
            .put()
            .archive()
            .catalog(self.catalog.clone())
    }

    /// Read a block by digest.
    pub fn get(&self, digest: impl Into<Blake3Hash>) -> Capability<Get> {
        self.read().get(digest)
    }

    /// Write one content-addressed block.
    pub fn put(&self, block: impl Into<Buffer>) -> Capability<Put> {
        self.write().put(block)
    }

    /// Write a batch of content-addressed blocks.
    pub fn import(
        &self,
        blocks: impl IntoIterator<Item = impl Into<Buffer>>,
    ) -> Capability<Import> {
        self.write().import(blocks)
    }
}

/// An archive named but not yet reached by any method.
#[derive(Debug, Clone, PartialEq)]
pub struct ArchiveScope {
    subject: dialog_capability::Subject,
}

impl ArchiveScope {
    /// Name an archive without choosing a method.
    pub fn new(subject: dialog_capability::Subject) -> Self {
        Self { subject }
    }

    /// The subject this archive belongs to.
    pub fn subject(&self) -> &dialog_capability::Did {
        self.subject.did()
    }

    /// Name a catalog within it.
    pub fn catalog(&self, name: impl Into<String>) -> CatalogScope {
        CatalogScope::new(self.subject.clone(), name)
    }

    /// The catalog a repository keeps its blocks in.
    ///
    /// The same catalog [`DEFAULT_CATALOG`] names, reached without
    /// spelling it.
    pub fn index(&self) -> CatalogScope {
        self.catalog(DEFAULT_CATALOG)
    }

    /// The chain for reading blobs.
    fn read_blob(&self) -> Capability<Blob<method::Get>> {
        use crate::blob::prelude::ArchiveBlobExt as _;
        self.subject.clone().get().archive().blob()
    }

    /// The chain for writing blobs.
    fn write_blob(&self) -> Capability<Blob<method::Put>> {
        use crate::blob::prelude::ArchiveBlobExt as _;
        self.subject.clone().put().archive().blob()
    }

    /// The archive's blobs, named without choosing a method.
    pub fn blob(&self) -> BlobScope {
        BlobScope {
            subject: self.subject.clone(),
        }
    }
}

/// The blobs of an archive, named but not yet reached by any method.
#[derive(Debug, Clone, PartialEq)]
pub struct BlobScope {
    subject: dialog_capability::Subject,
}

impl BlobScope {
    /// Read a blob by digest.
    pub fn read(&self, digest: impl Into<Blake3Hash>) -> Capability<BlobRead> {
        use crate::blob::prelude::ReadBlobExt as _;
        ArchiveScope::new(self.subject.clone())
            .read_blob()
            .read(digest)
    }

    /// Open a blob for writing.
    pub fn write(&self) -> Capability<BlobWrite> {
        use crate::blob::prelude::WriteBlobExt as _;
        ArchiveScope::new(self.subject.clone()).write_blob().write()
    }

    /// Invoke a read effect built by the caller, for a read that names
    /// more than a digest -- a byte range, say.
    pub fn invoke<E>(&self, effect: E) -> Capability<E>
    where
        E: dialog_capability::Effect<Of = Blob<method::Get>>,
    {
        ArchiveScope::new(self.subject.clone())
            .read_blob()
            .invoke(effect)
    }

    /// Record a blob already present, by digest and length.
    pub fn import(&self, digest: impl Into<Blake3Hash>, length: u64) -> Capability<BlobImport> {
        use crate::blob::prelude::WriteBlobExt as _;
        ArchiveScope::new(self.subject.clone())
            .write_blob()
            .import(digest, length)
    }
}
