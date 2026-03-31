use dialog_capability::fork::Fork;
use dialog_capability::site::{Site, SiteAddress};
use dialog_capability::{Did, Provider, authority, storage};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_storage::{Blake3Hash, CborEncoder, Encoder};

use crate::DialogArtifactsError;
use crate::repository::archive::Archive;
use crate::repository::branch::BranchName;
use crate::repository::error::RepositoryError;
use crate::repository::memory::Memory;
use crate::repository::node_reference::NodeReference;
use crate::repository::revision::Revision;

use crate::{RemoteName, UpstreamState};

mod selector;
#[allow(unused_imports)]
pub use selector::RemoteBranchSelector;

/// A cursor pointing to a specific branch at a remote repository.
///
/// Generic over the address type `A`, which determines the site type
/// used for remote operations (S3 or UcanSite).
#[derive(Debug, Clone)]
pub struct RemoteBranch<A: SiteAddress> {
    remote: RemoteName,
    address: A,
    subject: Did,
    branch: BranchName,
}

impl<A: SiteAddress> RemoteBranch<A> {
    /// Create a new remote branch cursor.
    pub fn new(remote: RemoteName, address: A, subject: Did, branch: BranchName) -> Self {
        Self {
            remote,
            address,
            subject,
            branch,
        }
    }

    /// The remote name (e.g., "origin").
    pub fn remote(&self) -> &RemoteName {
        &self.remote
    }

    /// The address for this remote.
    pub fn address(&self) -> &A {
        &self.address
    }

    /// The subject DID of the remote repository.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// The branch name.
    pub fn branch(&self) -> &BranchName {
        &self.branch
    }

    /// Build the memory cell capability chain for this remote branch.
    fn cell_capability(&self) -> dialog_capability::Capability<memory_fx::Cell> {
        Memory::new(dialog_capability::Subject::from(self.subject.clone()))
            .trace(self.branch.as_str())
            .cell_capability("revision")
    }

    /// Archive capability for the remote repository.
    fn archive(&self) -> Archive {
        Archive::new(dialog_capability::Subject::from(self.subject.clone()))
    }
}

impl<A> RemoteBranch<A>
where
    A: SiteAddress,
    A::Site: Site,
{
    /// Fetch the current revision from the remote branch.
    pub async fn resolve<Env>(&self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<Fork<A::Site, memory_fx::Resolve>>
            + Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync,
    {
        let result: Option<memory_fx::Publication> = self
            .cell_capability()
            .invoke(memory_fx::Resolve)
            .fork(&self.address)
            .perform(env)
            .await
            .map_err(|e| RepositoryError::StorageError(format!("Remote resolve failed: {}", e)))?
            .map_err(|e| RepositoryError::StorageError(format!("Remote resolve failed: {}", e)))?;

        match result {
            None => Ok(None),
            Some(publication) => {
                let revision: Revision =
                    CborEncoder
                        .decode(&publication.content)
                        .await
                        .map_err(|e| {
                            RepositoryError::StorageError(format!(
                                "Failed to decode remote revision: {}",
                                e
                            ))
                        })?;
                Ok(Some(revision))
            }
        }
    }

    /// Publish a revision to the remote branch.
    pub async fn publish<Env>(&self, revision: Revision, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<Fork<A::Site, memory_fx::Resolve>>
            + Provider<Fork<A::Site, memory_fx::Publish>>
            + Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync,
    {
        let cell_cap = self.cell_capability();

        // Resolve to get current edition
        let resolve_result: Option<memory_fx::Publication> = cell_cap
            .clone()
            .invoke(memory_fx::Resolve)
            .fork(&self.address)
            .perform(env)
            .await
            .map_err(|e| RepositoryError::StorageError(format!("Remote resolve failed: {}", e)))?
            .map_err(|e| RepositoryError::StorageError(format!("Remote resolve failed: {}", e)))?;

        let edition = resolve_result.map(|pub_data| pub_data.edition);

        let content = serde_ipld_dagcbor::to_vec(&revision).map_err(|e| {
            RepositoryError::StorageError(format!("Failed to encode revision: {}", e))
        })?;

        cell_cap
            .invoke(memory_fx::Publish::new(content, edition))
            .fork(&self.address)
            .perform(env)
            .await
            .map_err(|e| RepositoryError::StorageError(format!("Remote publish failed: {}", e)))?
            .map_err(|e| RepositoryError::StorageError(format!("Remote publish failed: {}", e)))?;

        Ok(())
    }

    /// Upload a content-addressed block to the remote archive.
    pub async fn upload_block<Env>(
        &self,
        hash: Blake3Hash,
        bytes: Vec<u8>,
        env: &Env,
    ) -> Result<(), DialogArtifactsError>
    where
        Env: Provider<Fork<A::Site, archive_fx::Put>>
            + Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync,
    {
        self.archive()
            .index()
            .invoke(archive_fx::Put::new(hash, bytes))
            .fork(&self.address)
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("Remote upload failed: {}", e)))?
            .map_err(|e| DialogArtifactsError::Storage(format!("Remote upload failed: {}", e)))?;
        Ok(())
    }

    /// Read a content-addressed block from the remote archive.
    pub async fn download_block<Env>(
        &self,
        hash: Blake3Hash,
        env: &Env,
    ) -> Result<Option<Vec<u8>>, DialogArtifactsError>
    where
        Env: Provider<Fork<A::Site, archive_fx::Get>>
            + Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync,
    {
        self.archive()
            .index()
            .invoke(archive_fx::Get::new(hash))
            .fork(&self.address)
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("Remote download failed: {}", e)))?
            .map_err(|e| DialogArtifactsError::Storage(format!("Remote download failed: {}", e)))
    }
}

impl<A: SiteAddress> From<RemoteBranch<A>> for UpstreamState {
    fn from(remote: RemoteBranch<A>) -> Self {
        UpstreamState::Remote {
            name: remote.remote,
            branch: remote.branch,
            tree: NodeReference::default(),
        }
    }
}
