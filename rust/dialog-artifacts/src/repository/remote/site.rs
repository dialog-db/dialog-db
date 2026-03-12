use crate::environment::Address;
use dialog_capability::{Did, Provider, Subject};
use dialog_effects::memory as memory_fx;

use super::state::RemoteState;
use crate::repository::Site;
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
    /// The name of this remote (e.g., "origin").
    pub(super) name: String,
    /// The remote site address.
    pub(super) site: Site,
    /// The issuer DID that authenticates operations.
    pub(super) issuer: Did,
    /// The address for authenticating remote operations.
    pub(super) address: Address,
}

impl RemoteSite {
    /// The memory cell where remote configuration is persisted.
    fn cell(name: &str, subject: &Did) -> Cell<RemoteState> {
        Cell::new(Subject::from(subject.clone()), "remotes", name.to_string())
    }

    /// Add a new remote site configuration.
    ///
    /// Persists the remote config to a memory cell. Returns an error if a
    /// remote with the same name already exists.
    pub async fn add<Env>(
        name: impl Into<String>,
        site: Site,
        issuer: Did,
        address: Address,
        subject: &Did,
        env: &Env,
    ) -> Result<RemoteSite, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        let name = name.into();
        let cell = Self::cell(&name, subject);

        // Resolve to check if it already exists
        cell.resolve(env).await?;
        if cell.get().is_some() {
            return Err(RepositoryError::RemoteAlreadyExists {
                remote: name.clone(),
            });
        }

        let state = RemoteState {
            site: site.clone(),
            issuer: issuer.clone(),
            address: address.clone(),
        };
        cell.publish(state, env).await?;

        Ok(RemoteSite {
            name,
            site,
            issuer,
            address,
        })
    }

    /// Load an existing remote site configuration.
    ///
    /// Reads the remote config from a memory cell. Returns an error if the
    /// remote does not exist.
    pub async fn load<Env>(
        name: impl Into<String>,
        subject: &Did,
        env: &Env,
    ) -> Result<RemoteSite, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        let name = name.into();
        let cell = Self::cell(&name, subject);

        cell.resolve(env).await?;
        match cell.get() {
            Some(state) => Ok(RemoteSite {
                name,
                site: state.site,
                issuer: state.issuer,
                address: state.address,
            }),
            None => Err(RepositoryError::RemoteNotFound {
                remote: name.clone(),
            }),
        }
    }

    /// The name of this remote.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The site address.
    pub fn site(&self) -> &Site {
        &self.site
    }

    /// The issuer DID.
    pub fn issuer(&self) -> &Did {
        &self.issuer
    }

    /// The address for this remote.
    pub fn address(&self) -> &Address {
        &self.address
    }

    /// Get a cursor into a specific repository at this remote site.
    pub fn repository(&self, subject: Did) -> RemoteRepository {
        RemoteRepository {
            remote: self.name.clone(),
            site: self.site.clone(),
            address: self.address.clone(),
            subject,
        }
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

    fn test_address() -> Address {
        let s3_addr = S3Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        Address::S3(S3Credentials::public(s3_addr).unwrap())
    }

    #[dialog_common::test]
    async fn it_adds_and_loads_remote() -> anyhow::Result<()> {
        let env = Volatile::new();
        let subject = test_subject();

        let site = RemoteSite::add(
            "origin",
            "s3://my-bucket".to_string(),
            "did:key:zAlice".parse()?,
            test_address(),
            &subject,
            &env,
        )
        .await?;

        assert_eq!(site.name(), "origin");
        assert_eq!(site.site(), "s3://my-bucket");
        assert_eq!(site.issuer(), &"did:key:zAlice".parse::<Did>()?);
        assert_eq!(site.address(), &test_address());

        // Load the same remote
        let loaded = RemoteSite::load("origin", &subject, &env).await?;
        assert_eq!(loaded.name(), "origin");
        assert_eq!(loaded.site(), "s3://my-bucket");
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

        RemoteSite::add(
            "origin",
            "s3://bucket-1".to_string(),
            "did:key:zAlice".parse()?,
            test_address(),
            &subject,
            &env,
        )
        .await?;

        let result = RemoteSite::add(
            "origin",
            "s3://bucket-2".to_string(),
            "did:key:zBob".parse()?,
            test_address(),
            &subject,
            &env,
        )
        .await;

        assert!(matches!(
            result,
            Err(RepositoryError::RemoteAlreadyExists { .. })
        ));

        Ok(())
    }
}
