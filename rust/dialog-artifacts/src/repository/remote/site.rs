use dialog_capability::{Did, Provider, Subject};
use dialog_effects::memory as memory_fx;

use super::state::SiteName;
use crate::RemoteAddress;
use crate::repository::cell::Cell;
use crate::repository::error::RepositoryError;

use super::repository::RemoteRepository;

/// A loaded remote site configuration.
///
/// Represents a named remote (like git's "origin") that has been loaded from
/// or persisted to memory. Use [`add`](RemoteSite::add) to create a new remote
/// or [`load`](RemoteSite::load) to load an existing one.
///
/// Call [`.repository(subject)`](RemoteSite::repository) to get a cursor into
/// a specific repository at this site.
#[derive(Debug, Clone)]
pub struct RemoteSite {
    name: SiteName,
    address: RemoteAddress,
}

impl RemoteSite {
    /// The memory cell where remote configuration is persisted.
    fn cell(name: &SiteName, subject: &Did) -> Cell<RemoteAddress> {
        Cell::new(
            Subject::from(subject.clone()),
            "remotes",
            name.as_str().to_string(),
        )
    }

    /// Add a new remote site configuration.
    ///
    /// Persists the remote config to a memory cell. Returns an error if a
    /// remote with the same name already exists.
    pub async fn add<Env>(
        name: impl Into<SiteName>,
        address: RemoteAddress,
        subject: &Did,
        env: &Env,
    ) -> Result<RemoteSite, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        let name: SiteName = name.into();
        let cell = Self::cell(&name, subject);

        // Resolve to check if it already exists
        cell.resolve(env).await?;
        if cell.get().is_some() {
            return Err(RepositoryError::RemoteAlreadyExists {
                remote: name.clone(),
            });
        }

        cell.publish(address.clone(), env).await?;

        Ok(RemoteSite { name, address })
    }

    /// Load an existing remote site configuration.
    ///
    /// Reads the remote config from a memory cell. Returns an error if the
    /// remote does not exist.
    pub async fn load<Env>(
        name: impl Into<SiteName>,
        subject: &Did,
        env: &Env,
    ) -> Result<RemoteSite, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        let name: SiteName = name.into();
        let cell = Self::cell(&name, subject);

        cell.resolve(env).await?;
        match cell.get() {
            Some(address) => Ok(RemoteSite { name, address }),
            None => Err(RepositoryError::RemoteNotFound {
                remote: name.clone(),
            }),
        }
    }

    /// The name of this remote.
    pub fn name(&self) -> &SiteName {
        &self.name
    }

    /// The address for this remote.
    pub fn address(&self) -> &RemoteAddress {
        &self.address
    }

    /// Get a cursor into a specific repository at this remote site.
    pub fn repository(&self, subject: Did) -> RemoteRepository {
        RemoteRepository::new(self.name.clone(), self.address.clone(), subject)
    }
}

#[cfg(test)]
mod tests {
    use dialog_s3_credentials::Address as S3Address;
    use dialog_s3_credentials::s3::Credentials as S3Credentials;
    use dialog_storage::provider::Volatile;

    use super::*;

    fn test_subject() -> Did {
        "did:test:remote-site".parse().unwrap()
    }

    fn test_address() -> RemoteAddress {
        let s3_addr = S3Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        RemoteAddress::S3(S3Credentials::public(s3_addr).unwrap())
    }

    #[dialog_common::test]
    async fn it_adds_and_loads_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let subject = test_subject();

        let site = RemoteSite::add("origin", test_address(), &subject, &env).await?;

        assert_eq!(site.name(), "origin");
        assert_eq!(site.address(), &test_address());

        // Load the same remote
        let loaded = RemoteSite::load("origin", &subject, &env).await?;
        assert_eq!(loaded.name(), "origin");
        assert_eq!(loaded.address(), &test_address());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_loading_missing_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let subject = test_subject();

        let result = RemoteSite::load("nonexistent", &subject, &env).await;
        assert!(matches!(
            result,
            Err(RepositoryError::RemoteNotFound { .. })
        ));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_adding_duplicate_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let subject = test_subject();

        RemoteSite::add("origin", test_address(), &subject, &env).await?;

        let result = RemoteSite::add("origin", test_address(), &subject, &env).await;

        assert!(matches!(
            result,
            Err(RepositoryError::RemoteAlreadyExists { .. })
        ));

        Ok(())
    }
}
