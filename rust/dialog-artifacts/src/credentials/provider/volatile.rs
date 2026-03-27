use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::SignerCredential;
use dialog_effects::credential::{self, CredentialError, Identity};
use dialog_storage::provider::Volatile;

use crate::credentials::open::{Open, ProfileSigner};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Open> for Volatile {
    async fn execute(&self, input: Open) -> Result<ProfileSigner, CredentialError> {
        let subject =
            dialog_capability::did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");

        // Try to load existing
        let loaded = dialog_capability::Subject::from(subject.clone())
            .attenuate(credential::Credential)
            .attenuate(credential::Name::new(&input.name))
            .invoke(credential::Load)
            .perform(self)
            .await
            .map_err(|e| CredentialError::NotFound(e.to_string()))?;

        if let Some(Identity::Signer(signer)) = loaded {
            return Ok(ProfileSigner::new(signer.0));
        }

        // Generate and store
        let signer = Ed25519Signer::generate()
            .await
            .map_err(|e| CredentialError::NotFound(e.to_string()))?;

        let credential = Identity::Signer(SignerCredential(signer.clone()));

        dialog_capability::Subject::from(subject)
            .attenuate(credential::Credential)
            .attenuate(credential::Name::new(&input.name))
            .invoke(credential::Save::new(credential))
            .perform(self)
            .await
            .map_err(|e| CredentialError::NotFound(e.to_string()))?;

        Ok(ProfileSigner::new(signer))
    }
}
