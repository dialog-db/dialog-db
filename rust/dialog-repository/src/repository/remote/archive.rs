//! Remote archive operations -- upload blocks to remote storage.

use crate::{RemoteRepository, RemoteSite, UploadError};
use dialog_artifacts::{Datum, Key, State};
use dialog_capability::{Fork, Provider, Subject};
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::prelude::{ArchiveScope, CatalogScope};
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_search_tree::{DialogSearchTreeError, PersistentNode};
use dialog_storage::Blake3Hash;
use futures_util::{Stream, StreamExt, TryStreamExt};

/// Remote archive scoped to a remote repository.
pub struct RemoteArchive<'a> {
    repository: &'a RemoteRepository,
}

impl<'a> RemoteArchive<'a> {
    /// The index catalog for tree node storage.
    pub fn index(&self) -> RemoteArchiveIndex<'a> {
        let address = self.repository.address();
        let catalog = ArchiveScope::new(Subject::from(address.subject.clone())).index();

        RemoteArchiveIndex {
            repository: self.repository,
            catalog,
        }
    }
}

/// Remote archive index for tree node uploads.
pub struct RemoteArchiveIndex<'a> {
    repository: &'a RemoteRepository,
    catalog: CatalogScope,
}

impl RemoteArchiveIndex<'_> {
    /// Read a block from the remote archive by hash.
    pub fn get(&self, hash: Blake3Hash) -> RemoteGet<'_> {
        RemoteGet { index: self, hash }
    }

    /// Write a block to the remote archive.
    pub fn put(&self, block: Buffer) -> RemotePut<'_> {
        RemotePut { index: self, block }
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
        let catalog = &self.index.catalog;
        let hash = self.hash;
        self.index
            .repository
            .reach(|address| async move {
                catalog
                    .clone()
                    .get(hash)
                    .fork(address.site())
                    .perform(env)
                    .await
            })
            .await
    }
}

/// Command to write a block to the remote archive.
pub struct RemotePut<'a> {
    index: &'a RemoteArchiveIndex<'a>,
    block: Buffer,
}

impl RemotePut<'_> {
    /// Execute the put operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), ArchiveError>
    where
        Env: Provider<Fork<RemoteSite, Put>> + ConditionalSync,
    {
        let catalog = &self.index.catalog;
        let block = &self.block;
        self.index
            .repository
            .reach(|address| async move {
                catalog
                    .clone()
                    .put(block.clone())
                    .fork(address.site())
                    .perform(env)
                    .await
            })
            .await
    }
}

impl RemoteArchiveIndex<'_> {
    /// Upload a stream of novel nodes to the remote.
    ///
    /// `local_catalog` is used to read raw bytes from local storage.
    pub fn upload<'a, S>(&'a self, nodes: S) -> Upload<'a, S>
    where
        S: Stream<Item = Result<PersistentNode<Key, State<Datum>>, DialogSearchTreeError>>,
    {
        Upload { index: self, nodes }
    }
}

/// Command to upload novel nodes to a remote archive.
pub struct Upload<'a, S> {
    index: &'a RemoteArchiveIndex<'a>,
    nodes: S,
}

const UPLOAD_CONCURRENCY: usize = 16;

impl<S> Upload<'_, S>
where
    S: Stream<Item = Result<PersistentNode<Key, State<Datum>>, DialogSearchTreeError>>,
{
    /// Execute the upload, writing the nodes' own buffers to the remote
    /// with up to 16 concurrent uploads.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), UploadError>
    where
        Env: Provider<Get> + Provider<Fork<RemoteSite, Put>> + ConditionalSync,
    {
        let index = self.index;

        self.nodes
            .map(|node| async move {
                let node = node?;
                index
                    .put(node.buffer().clone())
                    .perform(env)
                    .await
                    .map_err(UploadError::RemoteWrite)?;
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
