//! Remote archive operations -- upload blocks to remote storage.

use dialog_capability::fork::Fork;
use dialog_capability::site::{Site, SiteAddress};
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::archive::prelude::{ArchiveExt, ArchiveSubjectExt, CatalogExt};
use dialog_prolly_tree::Node;
use dialog_remote_s3::S3;
use dialog_storage::Blake3Hash;
use futures_util::{Stream, StreamExt, TryStreamExt};

use super::repository::RemoteRepository;
use crate::repository::error::RepositoryError;
use crate::{DialogArtifactsError, SiteAddress as SiteAddressEnum};
use crate::{Key, State};
use dialog_artifacts::Datum;

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
    catalog: Capability<archive_fx::Catalog>,
}

impl RemoteArchiveIndex<'_> {
    /// Upload a stream of novel nodes to the remote.
    ///
    /// `local_catalog` is used to read raw bytes from local storage.
    pub fn upload<'a, S>(
        &'a self,
        nodes: S,
        local_catalog: Capability<archive_fx::Catalog>,
    ) -> Upload<'a, S>
    where
        S: Stream<Item = Result<Node<Key, State<Datum>, Blake3Hash>, DialogArtifactsError>>,
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
    local_catalog: Capability<archive_fx::Catalog>,
}

const UPLOAD_CONCURRENCY: usize = 16;

impl<S> Upload<'_, S>
where
    S: Stream<Item = Result<Node<Key, State<Datum>, Blake3Hash>, DialogArtifactsError>>,
{
    /// Execute the upload.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<archive_fx::Get>
            + Provider<Fork<S3, archive_fx::Put>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, archive_fx::Put>>
            + ConditionalSync,
    {
        let address = self.index.repository.address();
        let remote_catalog = &self.index.catalog;
        let local_catalog = &self.local_catalog;

        match address.address {
            SiteAddressEnum::S3(ref addr) => {
                upload_to(self.nodes, local_catalog, remote_catalog, addr, env).await
            }
            #[cfg(feature = "ucan")]
            SiteAddressEnum::Ucan(ref addr) => {
                upload_to(self.nodes, local_catalog, remote_catalog, addr, env).await
            }
        }
    }
}

/// Upload novel nodes to a remote catalog, reading raw bytes locally
/// and writing via fork.
async fn upload_to<S, A, Env>(
    nodes: S,
    local_catalog: &Capability<archive_fx::Catalog>,
    remote_catalog: &Capability<archive_fx::Catalog>,
    address: &A,
    env: &Env,
) -> Result<(), RepositoryError>
where
    S: Stream<Item = Result<Node<Key, State<Datum>, Blake3Hash>, DialogArtifactsError>>,
    A: SiteAddress,
    A::Site: Site,
    Env: Provider<archive_fx::Get> + Provider<Fork<A::Site, archive_fx::Put>> + ConditionalSync,
{
    nodes
        .map(|node_result| async move {
            let node = node_result.map_err(|e| RepositoryError::PushFailed {
                cause: format!("Failed to compute novelty: {}", e),
            })?;

            let hash = *node.hash();

            let bytes: Option<Vec<u8>> = local_catalog
                .clone()
                .get(hash)
                .perform(env)
                .await
                .map_err(|e| RepositoryError::PushFailed {
                    cause: format!("Failed to read local block: {}", e),
                })?;

            if let Some(bytes) = bytes {
                remote_catalog
                    .clone()
                    .put(hash, bytes)
                    .fork(address)
                    .perform(env)
                    .await
                    .map_err(|e| RepositoryError::PushFailed {
                        cause: format!("Remote upload failed: {}", e),
                    })?;
            }

            Ok(())
        })
        .buffer_unordered(UPLOAD_CONCURRENCY)
        .try_collect::<()>()
        .await
}

impl RemoteRepository {
    /// Get the remote archive for this repository.
    pub fn archive(&self) -> RemoteArchive<'_> {
        RemoteArchive { repository: self }
    }
}
