use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::{SignerCredential, SignerCredentialExport};
use dialog_effects::credential::CredentialError;
use dialog_storage::provider::FileStore;

use crate::credentials::open::{Open, ProfileSigner};

#[async_trait]
impl Provider<Open> for FileStore {
    async fn execute(&self, input: Open) -> Result<ProfileSigner, CredentialError> {
        let location = self
            .resolve(&input.name)
            .and_then(|loc| loc.resolve("credentials"))
            .and_then(|loc| loc.resolve("self"))
            .map_err(|e| CredentialError::NotFound(e.to_string()))?;

        match location.read().await {
            Ok(data) => {
                let export: SignerCredentialExport = data.try_into().map_err(|_| {
                    CredentialError::NotFound("profile credential has invalid format".into())
                })?;
                let credential = SignerCredential::import(export)
                    .await
                    .map_err(|e| CredentialError::NotFound(e.to_string()))?;
                Ok(ProfileSigner::new(credential.0))
            }
            Err(_) => {
                let signer = Ed25519Signer::generate()
                    .await
                    .map_err(|e| CredentialError::NotFound(e.to_string()))?;

                let credential = SignerCredential::from(signer);
                let export = credential
                    .export()
                    .await
                    .map_err(|e| CredentialError::NotFound(e.to_string()))?;

                location
                    .write(&export.0)
                    .await
                    .map_err(|e| CredentialError::NotFound(e.to_string()))?;

                Ok(ProfileSigner::new(credential.0))
            }
        }
    }
}
