//! A replica's archive read as a tree's environment, outside a repository.
//!
//! The census examples read replicas they did not write: a directory of
//! blocks (a file per block, named by hash) and a directory of spilled
//! values beside it. [`Replica`] answers a tree's node reads
//! ([`archive::Load`]) from its blocks and its spilled-value reads
//! ([`blob::Load`]) from its blobs, whatever catalog or subject the read
//! names. It holds nothing, so wrap it in
//! [`Holding`](dialog_search_tree::helpers::Holding) for a node cache.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use base58::ToBase58 as _;
use dialog_capability::{Capability, Provider, Resume};
use dialog_common::{Blake3Hash, Buffer, Held, Holds};
use dialog_effects::archive::{self, ArchiveError, Get};
use dialog_effects::blob;

/// Where a [`Replica`]'s blocks come from.
#[derive(Clone)]
enum Blocks {
    /// Every block in memory, by hash.
    Memory(Arc<HashMap<Blake3Hash, Buffer>>),
    /// A file per block, named by its base58 hash, read on each load.
    Directory(PathBuf),
}

/// A replica's blocks and spilled values, answering a tree's loads.
#[derive(Clone)]
pub struct Replica {
    blocks: Blocks,
    blobs: Option<PathBuf>,
}

impl Replica {
    /// `blocks` in memory, with spilled values read from `blobs` if given.
    pub fn in_memory(blocks: impl IntoIterator<Item = Buffer>, blobs: Option<PathBuf>) -> Self {
        let blocks = blocks
            .into_iter()
            .map(|block| (block.blake3_hash().clone(), block))
            .collect();
        Self {
            blocks: Blocks::Memory(Arc::new(blocks)),
            blobs,
        }
    }

    /// The blocks of `directory`, a file per block, read on each load, with
    /// spilled values read from `blobs` if given.
    pub fn on_disk(directory: impl Into<PathBuf>, blobs: Option<PathBuf>) -> Self {
        Self {
            blocks: Blocks::Directory(directory.into()),
            blobs,
        }
    }

    /// The block `hash` names, if the replica holds it.
    pub fn block(&self, hash: &Blake3Hash) -> Option<Buffer> {
        match &self.blocks {
            Blocks::Memory(blocks) => blocks.get(hash).cloned(),
            Blocks::Directory(directory) => {
                std::fs::read(directory.join(hash.as_bytes().to_base58()))
                    .ok()
                    .map(Buffer::from)
            }
        }
    }
}

/// Holds nothing: reads through a bare replica have no node cache.
impl Holds for Replica {
    fn held(&self, _key: &str) -> Option<Held> {
        None
    }

    fn hold(&self, _key: String, _handle: Held) {}
}

#[dialog_capability::provider]
impl Provider<archive::Load> for Replica {
    async fn execute(&self, load: archive::Load) -> Result<Option<Buffer>, ArchiveError> {
        Ok(self.block(&load.digest))
    }
}

#[dialog_capability::provider]
impl Provider<Get> for Replica {
    async fn execute(&self, get: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        use dialog_effects::archive::prelude::GetExt as _;
        Ok(self
            .block(get.digest())
            .map(|block| block.as_ref().to_vec()))
    }
}

/// A replica keeps blocks, so a tree's node reads go through the
/// translation layer over them; with no cache, a preload has nowhere to
/// land.
impl<N: dialog_search_tree::nodes::Node> Provider<dialog_search_tree::nodes::Read<N>> for Replica {
    fn execute(
        &self,
        read: dialog_search_tree::nodes::Read<N>,
    ) -> Resume<'_, Result<Option<N>, dialog_search_tree::DialogSearchTreeError>> {
        read.through_blocks(self, None)
    }
}

impl<N> Provider<dialog_search_tree::nodes::Preload<N>> for Replica {
    fn execute(&self, _: dialog_search_tree::nodes::Preload<N>) -> Resume<'_, ()> {
        Resume::now(())
    }
}

/// A spilled value is read from the blob directory, else from the blocks,
/// where a value spilled before values moved to the blob store is kept.
#[dialog_capability::provider]
impl Provider<blob::Load> for Replica {
    async fn execute(&self, load: blob::Load) -> Result<Option<Buffer>, ArchiveError> {
        if let Some(blobs) = &self.blobs
            && let Ok(bytes) = std::fs::read(blobs.join(load.digest.as_bytes().to_base58()))
        {
            return Ok(Some(Buffer::from(bytes)));
        }
        Ok(self.block(&load.digest))
    }
}
