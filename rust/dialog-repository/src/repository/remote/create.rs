//! Command to create a new remote repository.

use crate::{CreateRemoteError, RemoteAddress, RemoteReference, RemoteRepository};
use dialog_capability::{Did, Provider};
use dialog_effects::memory::{Publish, Resolve};

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
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteRepository, CreateRemoteError>
    where
        Env: Provider<Resolve> + Provider<Publish>,
    {
        let cell = self.reference.address();
        cell.resolve().perform(env).await?;
        if cell.content().is_some() {
            return Err(CreateRemoteError::AlreadyExists {
                name: self.reference.name().to_string(),
            });
        }

        cell.publish(self.address.clone()).perform(env).await?;

        Ok(RemoteRepository::new(
            cell.retain(self.address),
            self.reference,
        ))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_credentials::Ed25519Signer;
    use dialog_remote_s3::Address;
    use dialog_storage::provider::Volatile;

    use crate::{CreateRemoteError, Repository, SiteAddress};

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
    async fn it_creates_remote() -> Result<()> {
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
    async fn it_errors_adding_duplicate_remote() -> Result<()> {
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
            Err(CreateRemoteError::AlreadyExists { .. })
        ));

        Ok(())
    }
}
