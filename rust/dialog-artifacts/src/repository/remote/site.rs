use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;
use dialog_remote_s3::Address;

use super::state::SiteName;
use crate::RemoteAddress;
use crate::environment::to_s3_address;
use crate::repository::cell::Cell;
use crate::repository::error::RepositoryError;
use crate::repository::memory::Space;

use super::repository::RemoteRepository;

/// A loaded remote site configuration.
///
/// Represents a named remote (like git's "origin") that has been loaded from
/// or persisted to memory. Stores both the serializable address (for persistence)
/// and the derived S3 address (for execution).
///
/// Call [`.repository(subject)`](RemoteSite::repository) to get a cursor into
/// a specific repository at this site.
#[derive(Debug, Clone)]
pub struct RemoteSite {
    name: SiteName,
    address: RemoteAddress,
    s3_address: Address,
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

    /// The S3 address for remote operations.
    pub fn s3_address(&self) -> &Address {
        &self.s3_address
    }

    /// Get a cursor into a specific repository at this remote site.
    pub fn repository(&self, subject: Did) -> RemoteRepository {
        RemoteRepository::new(self.name.clone(), self.s3_address.clone(), subject)
    }
}

/// Command to add a new remote site, persisting its configuration.
pub struct Open {
    name: SiteName,
    address: RemoteAddress,
    sites: Space,
}

impl Open {
    pub(crate) fn new(name: impl Into<SiteName>, address: RemoteAddress, sites: Space) -> Self {
        Self {
            name: name.into(),
            address,
            sites,
        }
    }

    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteSite, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        let cell: Cell<RemoteAddress> = self.sites.cell(self.name.as_str().to_string());

        cell.resolve(env).await?;
        if cell.get().is_some() {
            return Err(RepositoryError::RemoteAlreadyExists { remote: self.name });
        }

        cell.publish(self.address.clone(), env).await?;

        let s3_address = to_s3_address(&self.address)
            .map_err(|e| RepositoryError::StorageError(format!("Invalid remote address: {}", e)))?;

        Ok(RemoteSite {
            name: self.name,
            address: self.address,
            s3_address,
        })
    }
}

/// Command to load an existing remote site configuration.
pub struct Load {
    name: SiteName,
    sites: Space,
}

impl Load {
    pub(crate) fn new(name: impl Into<SiteName>, sites: Space) -> Self {
        Self {
            name: name.into(),
            sites,
        }
    }

    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteSite, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        let cell: Cell<RemoteAddress> = self.sites.cell(self.name.as_str().to_string());

        cell.resolve(env).await?;
        match cell.get() {
            Some(address) => {
                let s3_address = to_s3_address(&address).map_err(|e| {
                    RepositoryError::StorageError(format!("Invalid remote address: {}", e))
                })?;
                Ok(RemoteSite {
                    name: self.name,
                    address,
                    s3_address,
                })
            }
            None => Err(RepositoryError::RemoteNotFound { remote: self.name }),
        }
    }
}

#[cfg(test)]
mod tests {
    use dialog_remote_s3::Address;
    use dialog_storage::provider::Volatile;

    use crate::RemoteAddress;
    use crate::repository::Repository;
    use crate::repository::credentials::Credentials;
    use crate::repository::error::RepositoryError;

    fn test_address() -> RemoteAddress {
        let s3_addr = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        RemoteAddress::S3(s3_addr, None)
    }

    async fn test_repo() -> Repository<()> {
        let issuer = Credentials::from_passphrase("test", ()).await.unwrap();
        let subject = "did:test:remote-site".parse().unwrap();
        Repository::new(issuer, subject)
    }

    #[dialog_common::test]
    async fn it_adds_and_loads_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = test_repo().await;

        let site = repo
            .add_remote("origin", test_address())
            .perform(&env)
            .await?;

        assert_eq!(site.name(), "origin");
        assert_eq!(site.address(), &test_address());

        let loaded = repo.load_remote("origin").perform(&env).await?;
        assert_eq!(loaded.name(), "origin");
        assert_eq!(loaded.address(), &test_address());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_loading_missing_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = test_repo().await;

        let result = repo.load_remote("nonexistent").perform(&env).await;
        assert!(matches!(
            result,
            Err(RepositoryError::RemoteNotFound { .. })
        ));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_adding_duplicate_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = test_repo().await;

        repo.add_remote("origin", test_address())
            .perform(&env)
            .await?;

        let result = repo
            .add_remote("origin", test_address())
            .perform(&env)
            .await;

        assert!(matches!(
            result,
            Err(RepositoryError::RemoteAlreadyExists { .. })
        ));

        Ok(())
    }
}
