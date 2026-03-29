//! Mount and Location providers for Volatile.

use super::{Address, Volatile};
use async_trait::async_trait;
use dialog_capability::storage::{self, Location, Mountable, StorageError};
use dialog_capability::{Capability, Did, Policy, Provider};
use dialog_credentials::credential::Credential;

impl Mountable for Volatile {
    type Store = Volatile;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Mount<Volatile, Address>> for Volatile {
    async fn execute(
        &self,
        input: Capability<storage::Mount<Volatile, Address>>,
    ) -> Result<Volatile, StorageError> {
        let prefix = Location::of(&input).address().prefix();
        let mount = if self.mount.is_empty() {
            prefix.to_string()
        } else {
            format!("{}/{}", self.mount, prefix)
        };
        Ok(Volatile {
            mount,
            sessions: self.sessions.clone(),
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Load<Credential, Address>> for Volatile {
    async fn execute(
        &self,
        input: Capability<storage::Load<Credential, Address>>,
    ) -> Result<Credential, StorageError> {
        let subject: Did = input.subject().into();
        let prefix = Location::of(&input).address().prefix().to_owned();
        let key = if self.mount.is_empty() {
            prefix
        } else {
            format!("{}/{}", self.mount, prefix)
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
impl Provider<storage::Save<Credential, Address>> for Volatile {
    async fn execute(
        &self,
        input: Capability<storage::Save<Credential, Address>>,
    ) -> Result<(), StorageError> {
        let subject: Did = input.subject().into();
        let prefix = Location::of(&input).address().prefix().to_owned();
        let credential = &storage::Save::<Credential, Address>::of(&input).content;
        let key = if self.mount.is_empty() {
            prefix
        } else {
            format!("{}/{}", self.mount, prefix)
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Load<Vec<u8>, Address>> for Volatile {
    async fn execute(
        &self,
        input: Capability<storage::Load<Vec<u8>, Address>>,
    ) -> Result<Vec<u8>, StorageError> {
        let subject: Did = input.subject().into();
        let prefix = Location::of(&input).address().prefix().to_owned();
        let key = if self.mount.is_empty() {
            prefix
        } else {
            format!("{}/{}", self.mount, prefix)
        };

        let sessions = self.sessions.read();
        let data = sessions
            .get(&subject)
            .and_then(|session| session.credentials.get(&key))
            .cloned();

        match data {
            Some(export) => Ok(export.as_bytes().to_vec()),
            None => Err(StorageError::Storage(format!("not found: {}", key))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Save<Vec<u8>, Address>> for Volatile {
    async fn execute(
        &self,
        input: Capability<storage::Save<Vec<u8>, Address>>,
    ) -> Result<(), StorageError> {
        let _subject: Did = input.subject().into();
        let _prefix = Location::of(&input).address().prefix().to_owned();
        let _bytes = &storage::Save::<Vec<u8>, Address>::of(&input).content;
        // TODO: implement raw byte storage for volatile
        Err(StorageError::Storage(
            "Vec<u8> save not yet implemented for Volatile".into(),
        ))
    }
}
