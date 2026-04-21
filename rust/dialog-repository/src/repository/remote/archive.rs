//! Remote archive operations -- upload blocks to remote storage.

use dialog_artifacts::{Datum, Key, State};
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::{ArchiveExt, ArchiveSubjectExt, CatalogExt};
use dialog_effects::archive::{ArchiveError, Catalog, Get, Put};
use dialog_prolly_tree::{DialogProllyTreeError, Node};
use dialog_storage::Blake3Hash;
use futures_util::{Stream, StreamExt, TryStreamExt};

use super::{RemoteRepository, RemoteSite};
use crate::repository::error::UploadError;

/// Remote archive scoped to a remote repository.
pub struct RemoteArchive<'a> {
    repository: &'a RemoteRepository,
}

impl<'a> RemoteArchive<'a> {
    /// The index catalog for tree node storage.
    pub fn index(&self) -> RemoteArchiveIndex<'a> {
        let address = self.repository.address();
        let catalog = address.subject.clone().archive().catalog("index");

        RemoteArchiveIndex {
            repository: self.repository,
            catalog,
        }
    }
}

/// Remote archive index for tree node uploads.
pub struct RemoteArchiveIndex<'a> {
    repository: &'a RemoteRepository,
    catalog: Capability<Catalog>,
}

impl RemoteArchiveIndex<'_> {
    /// Read a block from the remote archive by hash.
    pub fn get(&self, hash: Blake3Hash) -> RemoteGet<'_> {
        RemoteGet { index: self, hash }
    }

    /// Write a block to the remote archive.
    pub fn put(&self, hash: Blake3Hash, bytes: Vec<u8>) -> RemotePut<'_> {
        RemotePut {
            index: self,
            hash,
            bytes,
        }
    }
}

/// Command to read a block from the remote archive.
pub struct RemoteGet<'a> {
    index: &'a RemoteArchiveIndex<'a>,
    hash: Blake3Hash,
}

impl RemoteGet<'_> {
    /// Execute the get operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Vec<u8>>, ArchiveError>
    where
        Env: Provider<Fork<RemoteSite, Get>> + ConditionalSync,
    {
        let address = self.index.repository.address();
        self.index
            .catalog
            .clone()
            .get(self.hash)
            .fork(address.site())
            .perform(env)
            .await
    }
}

/// Command to write a block to the remote archive.
pub struct RemotePut<'a> {
    index: &'a RemoteArchiveIndex<'a>,
    hash: Blake3Hash,
    bytes: Vec<u8>,
}

impl RemotePut<'_> {
    /// Execute the put operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), ArchiveError>
    where
        Env: Provider<Fork<RemoteSite, Put>> + ConditionalSync,
    {
        let address = self.index.repository.address();
        self.index
            .catalog
            .clone()
            .put(self.hash, self.bytes)
            .fork(address.site())
            .perform(env)
            .await
    }
}

impl RemoteArchiveIndex<'_> {
    /// Upload a stream of novel nodes to the remote.
    ///
    /// `local_catalog` is used to read raw bytes from local storage.
    pub fn upload<'a, S>(&'a self, nodes: S, local_catalog: Capability<Catalog>) -> Upload<'a, S>
    where
        S: Stream<Item = Result<Node<Key, State<Datum>, Blake3Hash>, DialogProllyTreeError>>,
    {
        Upload {
            index: self,
            nodes,
            local_catalog,
        }
    }
}

/// Command to upload novel nodes to a remote archive.
pub struct Upload<'a, S> {
    index: &'a RemoteArchiveIndex<'a>,
    nodes: S,
    local_catalog: Capability<Catalog>,
}

const UPLOAD_CONCURRENCY: usize = 16;

impl<S> Upload<'_, S>
where
    S: Stream<Item = Result<Node<Key, State<Datum>, Blake3Hash>, DialogProllyTreeError>>,
{
    /// Execute the upload, reading blocks locally and writing to remote
    /// with up to 16 concurrent uploads.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), UploadError>
    where
        Env: Provider<Get> + Provider<Fork<RemoteSite, Put>> + ConditionalSync,
    {
        let index = self.index;
        let local_catalog = &self.local_catalog;

        self.nodes
            .map(|node| async move {
                let node = node?;
                let hash = *node.hash();
                let bytes: Option<Vec<u8>> = local_catalog
                    .clone()
                    .get(hash)
                    .perform(env)
                    .await
                    .map_err(UploadError::LocalRead)?;
                if let Some(bytes) = bytes {
                    index
                        .put(hash, bytes)
                        .perform(env)
                        .await
                        .map_err(UploadError::RemoteWrite)?;
                }
                Ok(())
            })
            .buffer_unordered(UPLOAD_CONCURRENCY)
            .try_collect::<()>()
            .await
    }
}

impl RemoteRepository {
    /// Get the remote archive for this repository.
    pub fn archive(&self) -> RemoteArchive<'_> {
        RemoteArchive { repository: self }
    }
}
