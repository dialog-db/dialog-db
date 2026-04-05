//! Credential capability providers for FileStore.

use super::FileStore;
use async_trait::async_trait;
use dialog_capability::{Capability, Policy, Provider};
use dialog_credentials::credential::{Credential, CredentialExport};
use dialog_effects::credential::{self, CredentialError};

#[async_trait]
impl Provider<credential::Load> for FileStore {
    async fn execute(
        &self,
        input: Capability<credential::Load>,
    ) -> Result<Credential, CredentialError> {
        let address = &credential::Address::of(&input).address;
        let location = self
            .resolve(address)
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        let data = location
            .read()
            .await
            .map_err(|e| CredentialError::NotFound(e.to_string()))?;
        let export = CredentialExport::try_from(data)
            .map_err(|e| CredentialError::Corrupted(e.to_string()))?;
        Credential::import(export)
            .await
            .map_err(|e| CredentialError::Corrupted(e.to_string()))
    }
}

#[async_trait]
impl Provider<credential::Save> for FileStore {
    async fn execute(&self, input: Capability<credential::Save>) -> Result<(), CredentialError> {
        let address = &credential::Address::of(&input).address;
        let credential = &credential::Save::of(&input).credential;
        let location = self
            .resolve(address)
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        let export = credential
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        location
            .write(export.as_bytes())
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))
    }
}
