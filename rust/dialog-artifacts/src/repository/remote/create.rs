//! Command to create a new remote repository.

use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::repository::RemoteRepository;
use crate::RemoteAddress;
use crate::repository::error::RepositoryError;
use crate::repository::memory::Site;

/// Command to create a new remote repository, persisting its configuration.
pub struct CreateRemote {
    address: RemoteAddress,
    site: Site,
}

impl CreateRemote {
    pub(crate) fn new(site: Site, address: RemoteAddress) -> Self {
        Self { site, address }
    }

    /// Execute the create operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteRepository, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        let cell = self.site.address();
        cell.resolve(env).await?;
        if cell.get().is_some() {
            return Err(RepositoryError::RemoteAlreadyExists {
                remote: cell.name().into(),
            });
        }

        cell.publish(self.address.clone(), env).await?;

        Ok(RemoteRepository::new(
            cell.retain(self.address),
            self.site.capability(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use dialog_credentials::Ed25519Signer;
    use dialog_remote_s3::Address;
    use dialog_storage::provider::Volatile;

    use crate::repository::Repository;
    use crate::repository::error::RepositoryError;
    use crate::{RemoteAddress, SiteAddress};

    fn test_address() -> RemoteAddress {
        let s3_addr = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let did: dialog_varsig::Did = "did:key:z6MkTest".parse().expect("valid DID");
        RemoteAddress::new(SiteAddress::S3(s3_addr), did)
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
            .create(test_address())
            .perform(&env)
            .await?;

        assert_eq!(remote.name(), "origin");
        assert_eq!(remote.address(), test_address());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_adding_duplicate_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::from(test_signer().await);

        repo.remote("origin")
            .create(test_address())
            .perform(&env)
            .await?;

        let result = repo
            .remote("origin")
            .create(test_address())
            .perform(&env)
            .await;

        assert!(matches!(
            result,
            Err(RepositoryError::RemoteAlreadyExists { .. })
        ));

        Ok(())
    }
}
