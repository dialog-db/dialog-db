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
//! subject.get().archive().catalog("index").get(digest)
//!                                        = /use/get/archive/block
//! ```

use dialog_capability::{Capability, Constraint, Policy};
use dialog_common::{Blake3Hash, Buffer};

use super::{Archive, Block, Catalog, Get, Import, Put};
use crate::{Method, method};

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
