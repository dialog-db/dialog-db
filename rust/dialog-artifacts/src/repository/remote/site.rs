use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;

use super::repository::RemoteRepository;
use super::state::SiteName;
use crate::RemoteAddress;
use crate::repository::cell::Cell;
use crate::repository::error::RepositoryError;

/// A loaded remote site configuration.
///
/// Represents a named remote (like git's "origin") that has been loaded from
/// or persisted to memory.
#[derive(Debug, Clone)]
pub struct RemoteSite {
    name: SiteName,
    address: RemoteAddress,
}

impl RemoteSite {
    /// The name of this remote.
    pub fn name(&self) -> &SiteName {
        &self.name
    }

    /// The serializable address configuration for this remote.
    pub fn address(&self) -> &RemoteAddress {
        &self.address
    }

    /// Get a cursor into a specific repository at this remote site.
    pub fn repository(&self, subject: Did) -> RemoteRepository {
        RemoteRepository::new(self.name.clone(), self.address.clone(), subject)
    }
}

/// Command to add a new remote site, persisting its configuration.
pub struct CreateSite {
    address: RemoteAddress,
    cell: Cell<RemoteAddress>,
}

impl CreateSite {
    pub(crate) fn new(cell: Cell<RemoteAddress>, address: RemoteAddress) -> Self {
        Self { cell, address }
    }

    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteSite, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        self.cell.resolve(env).await?;
        if self.cell.get().is_some() {
            return Err(RepositoryError::RemoteAlreadyExists {
                remote: self.cell.name().into(),
            });
        }

        self.cell.publish(self.address.clone(), env).await?;

        Ok(RemoteSite {
            name: self.cell.name().into(),
            address: self.address,
        })
    }
}

/// Command to load an existing remote site configuration.
pub struct LoadSite(Cell<RemoteAddress>);

impl LoadSite {
    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteSite, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        self.0.resolve(env).await?;
        match self.0.get() {
            Some(address) => Ok(RemoteSite {
                name: self.0.name().into(),
                address,
            }),
            None => Err(RepositoryError::RemoteNotFound {
                remote: self.0.name().into(),
            }),
        }
    }
}

impl From<Cell<RemoteAddress>> for LoadSite {
    fn from(cell: Cell<RemoteAddress>) -> Self {
        Self(cell)
    }
}

#[cfg(test)]
mod tests {
    use dialog_credentials::Ed25519Signer;
    use dialog_remote_s3::Address;
    use dialog_storage::provider::Volatile;

    use crate::RemoteAddress;

    use crate::repository::Repository;
    use crate::repository::error::RepositoryError;

    fn test_address() -> RemoteAddress {
        let s3_addr = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        RemoteAddress::S3(s3_addr)
    }

    async fn test_signer() -> Ed25519Signer {
        Ed25519Signer::import(&[44; 32]).await.unwrap()
    }

    #[dialog_common::test]
    async fn it_adds_and_loads_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::from(test_signer().await);

        let site = repo
            .site("origin")
            .create(test_address())
            .perform(&env)
            .await?;

        assert_eq!(site.name(), "origin");
        assert_eq!(site.address(), &test_address());

        let loaded = repo.site("origin").load().perform(&env).await?;
        assert_eq!(loaded.name(), "origin");
        assert_eq!(loaded.address(), &test_address());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_loading_missing_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::from(test_signer().await);

        let result = repo.site("nonexistent").load().perform(&env).await;
        assert!(matches!(
            result,
            Err(RepositoryError::RemoteNotFound { .. })
        ));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_adding_duplicate_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::from(test_signer().await);

        repo.site("origin")
            .create(test_address())
            .perform(&env)
            .await?;

        let result = repo
            .site("origin")
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
