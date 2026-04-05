//! Credential capability providers for Volatile.

use super::Volatile;
use async_trait::async_trait;
use dialog_capability::{Capability, Policy, Provider};
use dialog_credentials::credential::Credential;
use dialog_effects::credential::{self, CredentialError};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<credential::Load> for Volatile {
    async fn execute(
        &self,
        input: Capability<credential::Load>,
    ) -> Result<Credential, CredentialError> {
        let subject = input.subject().clone();
        let address = credential::Address::of(&input).address.clone();
        let key = if self.mount.is_empty() {
            address
        } else {
            format!("{}/{}", self.mount, address)
        };

        let export = {
            let sessions = self.sessions.read();
            sessions
                .get(&subject)
                .and_then(|session| session.credentials.get(&key))
                .cloned()
        };

        let Some(export) = export else {
            return Err(CredentialError::NotFound(key));
        };

        Credential::import(export)
            .await
            .map_err(|e| CredentialError::Corrupted(e.to_string()))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<credential::Save> for Volatile {
    async fn execute(&self, input: Capability<credential::Save>) -> Result<(), CredentialError> {
        let subject = input.subject().clone();
        let address = credential::Address::of(&input).address.clone();
        let credential = &credential::Save::of(&input).credential;
        let key = if self.mount.is_empty() {
            address
        } else {
            format!("{}/{}", self.mount, address)
        };

        let export = credential
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;

        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject).or_default();
        session.credentials.insert(key, export);
        Ok(())
    }
}
