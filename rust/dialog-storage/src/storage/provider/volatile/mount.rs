//! Mount and Location providers for Volatile.

use super::Volatile;
use async_trait::async_trait;
use dialog_capability::storage::{self, Mountable, StorageError};
use dialog_capability::{Capability, Did, Policy, Provider};
use dialog_credentials::credential::Credential;

impl Mountable for Volatile {
    type Store = Volatile;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Mount<Volatile>> for Volatile {
    async fn execute(
        &self,
        input: Capability<storage::Mount<Volatile>>,
    ) -> Result<Volatile, StorageError> {
        let path = &storage::Location::of(&input).path();
        let mount = if self.mount.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.mount, path)
        };
        Ok(Volatile {
            mount,
            sessions: self.sessions.clone(),
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Load<Credential>> for Volatile {
    async fn execute(
        &self,
        input: Capability<storage::Load<Credential>>,
    ) -> Result<Credential, StorageError> {
        let subject: Did = input.subject().into();
        let path = storage::Location::of(&input).path().to_owned();
        let key = if self.mount.is_empty() {
            path
        } else {
            format!("{}/{}", self.mount, path)
        };

        let export = {
            let sessions = self.sessions.read();
            sessions
                .get(&subject)
                .and_then(|session| session.credentials.get(&key))
                .cloned()
        };

        let Some(export) = export else {
            return Err(StorageError::Storage(format!("not found: {}", key)));
        };

        Credential::import(export)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Save<Credential>> for Volatile {
    async fn execute(
        &self,
        input: Capability<storage::Save<Credential>>,
    ) -> Result<(), StorageError> {
        let subject: Did = input.subject().into();
        let path = storage::Location::of(&input).path().to_owned();
        let credential = &storage::Save::<Credential>::of(&input).content;
        let key = if self.mount.is_empty() {
            path
        } else {
            format!("{}/{}", self.mount, path)
        };

        let export = credential
            .export()
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject).or_default();
        session.credentials.insert(key, export);
        Ok(())
    }
}
