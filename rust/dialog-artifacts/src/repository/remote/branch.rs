use dialog_capability::{Capability, Did, Provider, Subject};
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;
use crate::environment::Address;
use dialog_storage::{Blake3Hash, CborEncoder, Encoder};

use crate::repository::Site;
use crate::repository::branch::BranchId;
use crate::repository::branch::archive::Archive;
use crate::repository::branch::state::BranchState;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;
use crate::DialogArtifactsError;

use super::UpstreamState;

/// A cursor pointing to a specific branch at a remote repository.
///
/// Created by [`RemoteRepository::branch`](super::repository::RemoteRepository::branch).
/// Provides remote operations: resolve, publish, and upload.
#[derive(Debug, Clone)]
pub struct RemoteBranch {
    /// The remote name (e.g., "origin") used to look up configuration.
    pub(crate) remote: String,
    /// The remote site address (human-readable).
    pub(crate) site: Site,
    /// The credentials used for remote operations.
    pub(crate) address: Address,
    /// The subject DID of the remote repository.
    pub(crate) subject: Did,
    /// The branch identifier.
    pub(crate) branch: BranchId,
}

impl RemoteBranch {
    /// The remote name (e.g., "origin").
    pub fn remote(&self) -> &str {
        &self.remote
    }

    /// The remote site address.
    pub fn site(&self) -> &Site {
        &self.site
    }

    /// The address for this remote.
    pub fn address(&self) -> &Address {
        &self.address
    }

    /// The subject DID of the remote repository.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// The branch identifier.
    pub fn branch(&self) -> &BranchId {
        &self.branch
    }

    /// Build the memory cell capability chain for this remote branch.
    fn cell_capability(&self) -> Capability<memory_fx::Cell> {
        Subject::from(self.subject.clone())
            .attenuate(memory_fx::Memory)
            .attenuate(memory_fx::Space::new("local"))
            .attenuate(memory_fx::Cell::new(self.branch.to_string()))
    }

    /// Archive capability for the remote repository.
    fn archive(&self) -> Archive {
        Archive::new(Subject::from(self.subject.clone()))
    }

    /// Fetch the current revision from the remote branch.
    ///
    /// Returns `None` if the remote branch has no state (not yet created).
    pub async fn resolve<Env>(&self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<RemoteInvocation<memory_fx::Resolve, Address>>,
    {
        let capability = self.cell_capability().invoke(memory_fx::Resolve);

        let result = RemoteInvocation::new(capability, self.address.clone())
            .perform(env)
            .await
            .map_err(|e| {
                RepositoryError::StorageError(format!("Remote resolve failed: {}", e))
            })?;

        match result {
            None => Ok(None),
            Some(publication) => {
                let state: BranchState =
                    CborEncoder.decode(&publication.content).await.map_err(|e| {
                        RepositoryError::StorageError(format!(
                            "Failed to decode remote branch state: {}",
                            e
                        ))
                    })?;
                Ok(Some(state.revision))
            }
        }
    }

    /// Publish a revision to the remote branch.
    ///
    /// This resolves the remote branch state first to get the current edition,
    /// then publishes the updated state with the new revision.
    pub async fn publish<Env>(
        &self,
        revision: Revision,
        env: &Env,
    ) -> Result<(), RepositoryError>
    where
        Env: Provider<RemoteInvocation<memory_fx::Resolve, Address>>
            + Provider<RemoteInvocation<memory_fx::Publish, Address>>,
    {
        let cell_cap = self.cell_capability();

        // Resolve to get current edition
        let resolve_result = RemoteInvocation::new(
            cell_cap.clone().invoke(memory_fx::Resolve),
            self.address.clone(),
        )
        .perform(env)
        .await
        .map_err(|e| {
            RepositoryError::StorageError(format!("Remote resolve failed: {}", e))
        })?;

        let (current_state, edition) = match resolve_result {
            None => (None, None),
            Some(pub_data) => {
                let state: BranchState =
                    CborEncoder.decode(&pub_data.content).await.map_err(|e| {
                        RepositoryError::StorageError(format!(
                            "Failed to decode remote branch state: {}",
                            e
                        ))
                    })?;
                (Some(state), Some(pub_data.edition))
            }
        };

        // Build the new state
        let new_state = match current_state {
            Some(mut state) => {
                state.revision = revision;
                state
            }
            None => BranchState::new(
                self.branch.clone(),
                revision,
                None,
            ),
        };

        let content = serde_ipld_dagcbor::to_vec(&new_state).map_err(|e| {
            RepositoryError::StorageError(format!("Failed to encode branch state: {}", e))
        })?;

        RemoteInvocation::new(
            cell_cap.invoke(memory_fx::Publish::new(content, edition)),
            self.address.clone(),
        )
        .perform(env)
        .await
        .map_err(|e| {
            RepositoryError::StorageError(format!("Remote publish failed: {}", e))
        })?;

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
        Env: Provider<RemoteInvocation<archive_fx::Put, Address>>,
    {
        let catalog = self.archive().index();
        let put_cap = catalog.invoke(archive_fx::Put::new(hash, bytes));
        RemoteInvocation::new(put_cap, self.address.clone())
            .perform(env)
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Remote upload failed: {}", e))
            })?;
        Ok(())
    }

    /// Read a content-addressed block from the remote archive.
    pub async fn download_block<Env>(
        &self,
        hash: Blake3Hash,
        env: &Env,
    ) -> Result<Option<Vec<u8>>, DialogArtifactsError>
    where
        Env: Provider<RemoteInvocation<archive_fx::Get, Address>>,
    {
        let catalog = self.archive().index();
        let get_cap = catalog.invoke(archive_fx::Get::new(hash));
        let result = RemoteInvocation::new(get_cap, self.address.clone())
            .perform(env)
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Remote download failed: {}", e))
            })?;
        Ok(result)
    }
}

impl From<RemoteBranch> for UpstreamState {
    fn from(remote: RemoteBranch) -> Self {
        UpstreamState::Remote {
            site: remote.remote,
            branch: remote.branch,
            subject: remote.subject,
        }
    }
}

#[cfg(test)]
mod tests {
    use dialog_s3_credentials::s3::Credentials as S3Credentials;
    use dialog_s3_credentials::Address as S3Address;

    use super::*;

    fn test_subject() -> Did {
        "did:test:remote-branch".parse().unwrap()
    }

    fn test_address() -> Address {
        let s3_addr = S3Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "bucket",
        );
        Address::S3(S3Credentials::public(s3_addr).unwrap())
    }

    #[test]
    fn it_creates_remote_branch_cursor() {
        let remote = RemoteBranch {
            remote: "origin".to_string(),
            site: "s3://bucket".to_string(),
            address: test_address(),
            subject: test_subject(),
            branch: "main".into(),
        };

        assert_eq!(remote.site(), "s3://bucket");
        assert_eq!(remote.subject(), &test_subject());
        assert_eq!(remote.branch(), &BranchId::from("main"));
    }

    #[test]
    fn it_converts_remote_branch_to_upstream_state() {
        let remote = RemoteBranch {
            remote: "origin".to_string(),
            site: "s3://bucket".to_string(),
            address: test_address(),
            subject: test_subject(),
            branch: "main".into(),
        };

        let upstream: UpstreamState = remote.into();
        match upstream {
            UpstreamState::Remote {
                site,
                branch,
                subject,
            } => {
                assert_eq!(site, "origin");
                assert_eq!(branch, BranchId::from("main"));
                assert_eq!(subject, test_subject());
            }
            _ => panic!("Expected Remote upstream"),
        }
    }
}
