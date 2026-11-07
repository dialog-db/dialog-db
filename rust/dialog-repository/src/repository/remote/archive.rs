//! Remote archive operations -- upload blocks to remote storage.

use crate::{ConnectedReplica, RemoteSite, UploadError};
use dialog_artifacts::{Datum, Key, State};
use dialog_capability::{Fork, Provider, Subject};
use dialog_common::r#async::spawn;
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::prelude::{ArchiveScope, CatalogScope};
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_search_tree::{DialogSearchTreeError, PersistentNode};
use dialog_storage::Blake3Hash;
use futures_util::{Stream, StreamExt, TryStreamExt};

/// Remote archive scoped to a remote repository.
pub struct RemoteArchive<'a> {
    repository: &'a ConnectedReplica,
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
    repository: &'a ConnectedReplica,
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
        let remote = self.index.repository.connection(env);
        self.index
            .catalog
            .clone()
            .get(self.hash)
            .perform(&remote)
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
        let remote = self.index.repository.connection(env);
        self.index
            .catalog
            .clone()
            .put(self.block)
            .perform(&remote)
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
    ///
    /// A node's buffer may have to be built (a store that keeps nodes as
    /// rows builds the block from them), which is work for a processor, not
    /// a wait. Each is built as a task of its own, so where there are
    /// threads several are built at once, and a block goes to the remote
    /// as soon as it is built rather than holding up the puts in flight
    /// while it is.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), UploadError>
    where
        Env: Provider<Get> + Provider<Fork<RemoteSite, Put>> + ConditionalSync,
    {
        let index = self.index;

        self.nodes
            .map(|node| async move {
                let node = node?;
                spawn(async move { node.buffer().cloned() })
                    .await
                    .map_err(|_| {
                        DialogSearchTreeError::Node("A block's bytes were not built".into())
                    })?
                    .map_err(UploadError::from)
            })
            .buffer_unordered(UPLOAD_CONCURRENCY)
            .map(|buffer| async move {
                index
                    .put(buffer?)
                    .perform(env)
                    .await
                    .map_err(UploadError::RemoteWrite)
            })
            .buffer_unordered(UPLOAD_CONCURRENCY)
            .try_collect::<()>()
            .await
    }
}

impl ConnectedReplica {
    /// Get the remote archive for this repository.
    pub fn archive(&self) -> RemoteArchive<'_> {
        RemoteArchive { repository: self }
    }
}
