//! Command to create a new remote repository.

use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;

use super::reference::RemoteReference;
use super::repository::RemoteRepository;
use crate::RemoteAddress;
use crate::repository::error::RepositoryError;

/// Command to create a new remote repository, persisting its configuration.
pub struct CreateRemote {
    address: RemoteAddress,
    reference: RemoteReference,
}

impl CreateRemote {
    /// Create from a remote reference and address.
    pub fn new(reference: RemoteReference, address: RemoteAddress) -> Self {
        Self { reference, address }
    }

    /// Override the subject DID for the remote repository.
    ///
    /// By default, the subject is the creating repository's own DID.
    pub fn subject(mut self, subject: impl Into<Did>) -> Self {
        self.address.subject = subject.into();
        self
    }

    /// Execute the create operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteRepository, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        let cell = self.reference.address();
        cell.resolve(env).await?;
        if cell.get().is_some() {
            return Err(RepositoryError::RemoteAlreadyExists {
                remote: self.reference.name(),
            });
        }

        cell.publish(self.address.clone(), env).await?;

        Ok(RemoteRepository::new(
            cell.retain(self.address),
            self.reference,
        ))
    }
}

#[cfg(test)]
mod tests {
    use dialog_credentials::Ed25519Signer;
    use dialog_remote_s3::Address;
    use dialog_storage::provider::Volatile;

    use crate::SiteAddress;
    use crate::repository::Repository;
    use crate::repository::error::RepositoryError;

    fn test_site_address() -> SiteAddress {
        SiteAddress::S3(
            Address::builder("https://s3.us-east-1.amazonaws.com")
                .region("us-east-1")
                .bucket("my-bucket")
                .build()
                .unwrap(),
        )
    }

    async fn test_signer() -> Ed25519Signer {
        Ed25519Signer::import(&[44; 32]).await.unwrap()
    }

    #[dialog_common::test]
    async fn it_creates_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::from(test_signer().await);

        let remote = repo
            .remote("origin")
            .create(test_site_address())
            .perform(&env)
            .await?;

        assert_eq!(remote.site().name(), "origin");
        assert_eq!(remote.address().site(), &test_site_address());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_adding_duplicate_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::from(test_signer().await);

        repo.remote("origin")
            .create(test_site_address())
            .perform(&env)
            .await?;

        let result = repo
            .remote("origin")
            .create(test_site_address())
            .perform(&env)
            .await;

        assert!(matches!(
            result,
            Err(RepositoryError::RemoteAlreadyExists { .. })
        ));

        Ok(())
    }
}
