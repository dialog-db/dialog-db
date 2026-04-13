//! Command to load an existing remote repository.

use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::repository::RemoteRepository;
use crate::repository::error::RepositoryError;
use crate::repository::memory::Site;

/// Command to load an existing remote repository.
pub struct LoadRemote(Site);

impl LoadRemote {
    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteRepository, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        let cell = self.0.address();
        cell.resolve(env).await?;
        match cell.get() {
            Some(address) => Ok(RemoteRepository::new(
                cell.retain(address),
                self.0.capability(),
            )),
            None => Err(RepositoryError::RemoteNotFound {
                remote: self.0.name().into(),
            }),
        }
    }
}

impl From<Site> for LoadRemote {
    fn from(site: Site) -> Self {
        Self(site)
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
    async fn it_loads_existing_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::from(test_signer().await);

        repo.remote("origin")
            .create(test_site_address())
            .perform(&env)
            .await?;

        let loaded = repo.remote("origin").load().perform(&env).await?;
        assert_eq!(loaded.name(), "origin");
        assert_eq!(loaded.address().site(), &test_site_address());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_loading_missing_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::from(test_signer().await);

        let result = repo.remote("nonexistent").load().perform(&env).await;
        assert!(matches!(
            result,
            Err(RepositoryError::RemoteNotFound { .. })
        ));

        Ok(())
    }
}
