use dialog_capability::{Did, Provider, credential::Authorize};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_s3_credentials::s3::site::{S3Access, S3Invocation, S3Site};
use dialog_storage::{Blake3Hash, CborEncoder, Encoder};

use crate::DialogArtifactsError;
use crate::repository::archive::Archive;
use crate::repository::branch::BranchName;
use crate::repository::error::RepositoryError;
use crate::repository::memory::Memory;
use crate::repository::node_reference::NodeReference;
use crate::repository::revision::Revision;

use super::SiteName;
use super::UpstreamState;

/// A cursor pointing to a specific branch at a remote repository.
///
/// Created by [`RemoteRepository::branch`](super::repository::RemoteRepository::branch).
/// Provides remote operations: resolve, publish, and upload.
#[derive(Debug, Clone)]
pub struct RemoteBranch {
    remote: SiteName,
    site: S3Site,
    subject: Did,
    branch: BranchName,
}

impl RemoteBranch {
    /// Create a new remote branch cursor.
    pub fn new(remote: SiteName, site: S3Site, subject: Did, branch: BranchName) -> Self {
        Self {
            remote,
            site,
            subject,
            branch,
        }
    }

    /// The remote name (e.g., "origin").
    pub fn remote(&self) -> &SiteName {
        &self.remote
    }

    /// The site configuration for this remote.
    pub fn site(&self) -> &S3Site {
        &self.site
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

    /// Fetch the current revision from the remote branch.
    ///
    /// Returns `None` if the remote branch has no state (not yet created).
    pub async fn resolve<Env>(&self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<Authorize<memory_fx::Resolve, S3Access>>
            + Provider<S3Invocation<memory_fx::Resolve>>
            + ConditionalSync,
    {
        let capability = self
            .cell_capability()
            .invoke(memory_fx::Resolve)
            .at(&self.site);

        let invocation = capability.acquire(env).await.map_err(|e| {
            RepositoryError::StorageError(format!("Remote authorize failed: {}", e))
        })?;

        let result: Option<_> =
            <Env as Provider<S3Invocation<memory_fx::Resolve>>>::execute(env, invocation)
                .await
                .map_err(|e| {
                    RepositoryError::StorageError(format!("Remote resolve failed: {}", e))
                })?;

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
    ///
    /// This resolves the remote branch state first to get the current edition,
    /// then publishes the updated state with the new revision.
    pub async fn publish<Env>(&self, revision: Revision, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<Authorize<memory_fx::Resolve, S3Access>>
            + Provider<S3Invocation<memory_fx::Resolve>>
            + Provider<Authorize<memory_fx::Publish, S3Access>>
            + Provider<S3Invocation<memory_fx::Publish>>
            + ConditionalSync,
    {
        let cell_cap = self.cell_capability();

        // Resolve to get current edition
        let resolve_invocation = cell_cap
            .clone()
            .invoke(memory_fx::Resolve)
            .at(&self.site)
            .acquire(env)
            .await
            .map_err(|e| {
                RepositoryError::StorageError(format!("Remote authorize failed: {}", e))
            })?;

        let resolve_result =
            <Env as Provider<S3Invocation<memory_fx::Resolve>>>::execute(env, resolve_invocation)
                .await
                .map_err(|e| {
                    RepositoryError::StorageError(format!("Remote resolve failed: {}", e))
                })?;

        let edition = match resolve_result {
            None => None,
            Some(pub_data) => Some(pub_data.edition),
        };

        let content = serde_ipld_dagcbor::to_vec(&revision).map_err(|e| {
            RepositoryError::StorageError(format!("Failed to encode revision: {}", e))
        })?;

        let publish_invocation = cell_cap
            .invoke(memory_fx::Publish::new(content, edition))
            .at(&self.site)
            .acquire(env)
            .await
            .map_err(|e| {
                RepositoryError::StorageError(format!("Remote authorize failed: {}", e))
            })?;

        <Env as Provider<S3Invocation<memory_fx::Publish>>>::execute(env, publish_invocation)
            .await
            .map_err(|e| RepositoryError::StorageError(format!("Remote publish failed: {}", e)))?;

        Ok(())
    }

    /// Upload a content-addressed block to the remote archive.
    ///
    /// Transfers a single block (identified by its blake3 hash and raw bytes)
    /// to the remote site's archive.
    pub async fn upload_block<Env>(
        &self,
        hash: Blake3Hash,
        bytes: Vec<u8>,
        env: &Env,
    ) -> Result<(), DialogArtifactsError>
    where
        Env: Provider<Authorize<archive_fx::Put, S3Access>>
            + Provider<S3Invocation<archive_fx::Put>>
            + ConditionalSync,
    {
        let catalog = self.archive().index();
        let invocation = catalog
            .invoke(archive_fx::Put::new(hash, bytes))
            .at(&self.site)
            .acquire(env)
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Remote authorize failed: {}", e))
            })?;

        <Env as Provider<S3Invocation<archive_fx::Put>>>::execute(env, invocation)
            .await
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
        Env: Provider<Authorize<archive_fx::Get, S3Access>>
            + Provider<S3Invocation<archive_fx::Get>>
            + ConditionalSync,
    {
        let catalog = self.archive().index();
        let invocation = catalog
            .invoke(archive_fx::Get::new(hash))
            .at(&self.site)
            .acquire(env)
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Remote authorize failed: {}", e))
            })?;

        let result = <Env as Provider<S3Invocation<archive_fx::Get>>>::execute(env, invocation)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("Remote download failed: {}", e)))?;
        Ok(result)
    }
}

impl From<RemoteBranch> for UpstreamState {
    fn from(remote: RemoteBranch) -> Self {
        UpstreamState::Remote {
            name: remote.remote,
            branch: remote.branch,
            subject: remote.subject,
            tree: NodeReference::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use dialog_s3_credentials::Address as S3Address;
    use dialog_s3_credentials::s3::S3Site;

    use super::*;

    fn test_subject() -> Did {
        "did:test:remote-branch".parse().unwrap()
    }

    fn test_site() -> S3Site {
        let s3_addr = S3Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "bucket");
        S3Site::new(s3_addr).unwrap()
    }

    #[test]
    fn it_creates_remote_branch_cursor() {
        let remote = RemoteBranch::new("origin".into(), test_site(), test_subject(), "main".into());

        assert_eq!(remote.subject(), &test_subject());
        assert_eq!(remote.branch(), &BranchName::from("main"));
    }

    #[test]
    fn it_converts_remote_branch_to_upstream_state() {
        let remote = RemoteBranch::new("origin".into(), test_site(), test_subject(), "main".into());

        let upstream: UpstreamState = remote.into();
        match upstream {
            UpstreamState::Remote {
                name,
                branch,
                subject,
                ..
            } => {
                assert_eq!(name, "origin");
                assert_eq!(branch, BranchName::from("main"));
                assert_eq!(subject, test_subject());
            }
            _ => panic!("Expected Remote upstream"),
        }
    }
}
