//! Command to load an existing remote repository.

use dialog_capability::Provider;
use dialog_effects::memory::Resolve;

use super::{RemoteReference, RemoteRepository};
use crate::LoadRemoteError;

/// Command to load an existing remote repository.
pub struct LoadRemote(RemoteReference);

impl LoadRemote {
    /// Create from a remote reference.
    pub fn new(reference: RemoteReference) -> Self {
        Self(reference)
    }

    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteRepository, LoadRemoteError>
    where
        Env: Provider<Resolve>,
    {
        let cell = self.0.address();
        cell.resolve().perform(env).await?;
        match cell.content() {
            Some(address) => Ok(RemoteRepository::new(cell.retain(address), self.0)),
            None => Err(LoadRemoteError::NotFound {
                name: self.0.name().to_string(),
            }),
        }
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

    use crate::LoadRemoteError;
    use crate::repository::Repository;
    use crate::repository::remote::SiteAddress;

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
    async fn it_loads_existing_remote() -> Result<()> {
        let env = Volatile::new();
        let repo = Repository::from(test_signer().await);

        repo.remote("origin")
            .create(test_site_address())
            .perform(&env)
            .await?;

        let loaded = repo.remote("origin").load().perform(&env).await?;
        assert_eq!(loaded.site().name(), "origin");
        assert_eq!(loaded.address().site(), &test_site_address());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_loading_missing_remote() -> Result<()> {
        let env = Volatile::new();
        let repo = Repository::from(test_signer().await);

        let result = repo.remote("nonexistent").load().perform(&env).await;
        assert!(matches!(result, Err(LoadRemoteError::NotFound { .. })));

        Ok(())
    }
}
