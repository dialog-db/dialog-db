//! Credential Load/Save for volatile (in-memory) storage.

use dialog_capability::{Capability, Policy, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::credential::{self, CredentialError, Secret};
use dialog_varsig::Principal;

use super::Volatile;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Load<Credential>> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Load<Credential>>,
    ) -> Result<Credential, CredentialError> {
        let address = &credential::Key::of(&input).address;
        let key = self.scoped_key(&format!("key/{address}"));

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
impl Provider<credential::Save<Credential>> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Save<Credential>>,
    ) -> Result<(), CredentialError> {
        let address = &credential::Key::of(&input).address;
        let credential = &credential::Save::<Credential>::of(&input).credential;
        let key = self.scoped_key(&format!("key/{address}"));

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

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Load<Secret>> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Load<Secret>>,
    ) -> Result<Secret, CredentialError> {
        let address = &credential::Site::of(&input).address;
        let key = self.scoped_key(&format!("site/{address}"));

        let sessions = self.sessions.read();
        sessions
            .values()
            .find_map(|session| session.secrets.get(&key).cloned())
            .map(Secret)
            .ok_or(CredentialError::NotFound(key))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Save<Secret>> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Save<Secret>>,
    ) -> Result<(), CredentialError> {
        let address = &credential::Site::of(&input).address;
        let secret = &credential::Save::<Secret>::of(&input).credential;
        let key = self.scoped_key(&format!("site/{address}"));

        let subject = input.subject().clone();
        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject).or_default();
        session.secrets.insert(key, secret.0.clone());
        Ok(())
    }
}
