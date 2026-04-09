//! Credential Load/Save for volatile (in-memory) storage.

use dialog_capability::{Capability, Policy, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::credential::{self, CredentialError};
use dialog_varsig::Principal;

use super::Volatile;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Load> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Load>,
    ) -> Result<Credential, CredentialError> {
        let name = &credential::Name::of(&input).name;
        let key = self.scoped_key(name);

        // Clone the export and drop the lock before awaiting import.
        let export = {
            let sessions = self.sessions.read();
            sessions
                .values()
                .find_map(|session| session.credentials.get(&key).cloned())
        };

        match export {
            Some(export) => Credential::import(export)
                .await
                .map_err(|e| CredentialError::Corrupted(e.to_string())),
            None => Err(CredentialError::NotFound(key)),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Save> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<credential::Save>) -> Result<(), CredentialError> {
        let name = &credential::Name::of(&input).name;
        let credential = &credential::Save::of(&input).credential;
        let key = self.scoped_key(name);

        let export = credential
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;

        let did = credential.did();
        let mut sessions = self.sessions.write();
        let session = sessions.entry(did).or_default();
        session.credentials.insert(key, export);
        Ok(())
    }
}
