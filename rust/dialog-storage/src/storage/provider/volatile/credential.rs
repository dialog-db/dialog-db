//! Credential Load/Save for volatile (in-memory) storage.

use dialog_capability::{Capability, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::credential::prelude::{
    LoadCredentialExt, LoadSecretExt, RetractSecretExt, SaveCredentialExt, SaveSecretExt,
};
use dialog_effects::credential::{CredentialError, Load, Retract, Save, Secret};
use dialog_varsig::Principal;

use super::Volatile;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Load<Credential>> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<Load<Credential>>,
    ) -> Result<Credential, CredentialError> {
        let key = self.scoped_key(&format!("key/{}", input.address()));

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
impl Provider<Save<Credential>> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Save<Credential>>) -> Result<(), CredentialError> {
        let key = self.scoped_key(&format!("key/{}", input.address()));
        let credential = input.credential();

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
impl Provider<Load<Secret>> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Load<Secret>>) -> Result<Secret, CredentialError> {
        let key = self.scoped_key(&format!("site/{}", input.address()));

        let sessions = self.sessions.read();
        sessions
            .values()
            .find_map(|session| session.secrets.get(&key).cloned())
            .map(Secret::from)
            .ok_or(CredentialError::NotFound(key))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Save<Secret>> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Save<Secret>>) -> Result<(), CredentialError> {
        let key = self.scoped_key(&format!("site/{}", input.address()));
        let secret = input.secret().as_bytes().to_vec();

        let subject = input.subject().clone();
        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject).or_default();
        session.secrets.insert(key, secret);
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Retract<Secret>> for Volatile
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Retract<Secret>>) -> Result<(), CredentialError> {
        let key = self.scoped_key(&format!("site/{}", input.address()));

        // Save keys the session by subject but Load searches every
        // session, so retract must clear the key wherever Load would
        // have found it.
        let mut sessions = self.sessions.write();
        for session in sessions.values_mut() {
            session.secrets.remove(&key);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::unique_did;
    use dialog_effects::prelude::*;

    #[dialog_common::test]
    async fn it_retracts_a_stored_site_secret() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let did = unique_did().await;

        did.clone()
            .credential()
            .site("example.com")
            .save(Secret::from(vec![1u8, 2, 3]))
            .perform(&provider)
            .await?;

        did.clone()
            .credential()
            .site("example.com")
            .retract()
            .perform(&provider)
            .await?;

        let result = did
            .credential()
            .site("example.com")
            .load()
            .perform(&provider)
            .await;

        assert!(matches!(result, Err(CredentialError::NotFound(_))));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_a_missing_site_secret_without_error() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let did = unique_did().await;

        did.credential()
            .site("never-saved.example")
            .retract()
            .perform(&provider)
            .await?;

        Ok(())
    }

    #[dialog_common::test]
    async fn it_leaves_other_site_secrets_intact() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let did = unique_did().await;

        did.clone()
            .credential()
            .site("first.example")
            .save(Secret::from(vec![1u8]))
            .perform(&provider)
            .await?;
        did.clone()
            .credential()
            .site("second.example")
            .save(Secret::from(vec![2u8]))
            .perform(&provider)
            .await?;

        did.clone()
            .credential()
            .site("first.example")
            .retract()
            .perform(&provider)
            .await?;

        let survivor = did
            .credential()
            .site("second.example")
            .load()
            .perform(&provider)
            .await?;

        assert_eq!(survivor.as_bytes(), &[2u8]);
        Ok(())
    }
}
