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

        // Save keys the session by subject; look only in that session so
        // one subject's secrets are invisible to another.
        let sessions = self.sessions.read();
        sessions
            .get(input.subject())
            .and_then(|session| session.secrets.get(&key).cloned())
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

        // Clear only the subject's own session, mirroring Save and Load:
        // retracting must never touch another subject's secrets.
        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.get_mut(input.subject()) {
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
    async fn it_isolates_site_secrets_by_subject() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let alice = unique_did().await;
        let bob = unique_did().await;

        alice
            .clone()
            .credential()
            .site("example.com")
            .save(Secret::from(vec![1u8]))
            .perform(&provider)
            .await?;
        bob.clone()
            .credential()
            .site("example.com")
            .save(Secret::from(vec![2u8]))
            .perform(&provider)
            .await?;

        // One subject's secret at an address is invisible to another subject.
        let theirs = alice
            .clone()
            .credential()
            .site("example.com")
            .load()
            .perform(&provider)
            .await?;
        assert_eq!(theirs.as_bytes(), &[1u8]);

        // Retracting under one subject leaves the other subject's secret.
        alice
            .credential()
            .site("example.com")
            .retract()
            .perform(&provider)
            .await?;

        let survivor = bob
            .credential()
            .site("example.com")
            .load()
            .perform(&provider)
            .await?;
        assert_eq!(survivor.as_bytes(), &[2u8]);
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
