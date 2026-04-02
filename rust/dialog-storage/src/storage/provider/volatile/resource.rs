//! Load and Save providers for Volatile.

use super::{Address, Volatile};
use async_trait::async_trait;
use dialog_capability::storage::{self, Location, StorageError};
use dialog_capability::{Capability, Policy, Provider};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Load<Vec<u8>, Address>> for Volatile {
    async fn execute(
        &self,
        input: Capability<storage::Load<Vec<u8>, Address>>,
    ) -> Result<Vec<u8>, StorageError> {
        let subject = input.subject().clone();
        let prefix = Location::of(&input).address().prefix().to_owned();
        let key = if self.mount.is_empty() {
            prefix
        } else {
            format!("{}/{}", self.mount, prefix)
        };

        let sessions = self.sessions.read();
        sessions
            .get(&subject)
            .and_then(|session| session.mounted.get(&key))
            .cloned()
            .ok_or_else(|| StorageError::Storage(format!("not found: {}", key)))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Save<Vec<u8>, Address>> for Volatile {
    async fn execute(
        &self,
        input: Capability<storage::Save<Vec<u8>, Address>>,
    ) -> Result<(), StorageError> {
        let subject = input.subject().clone();
        let prefix = Location::of(&input).address().prefix().to_owned();
        let bytes = storage::Save::<Vec<u8>, Address>::of(&input)
            .content
            .clone();
        let key = if self.mount.is_empty() {
            prefix
        } else {
            format!("{}/{}", self.mount, prefix)
        };

        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject).or_default();
        session.mounted.insert(key, bytes);
        Ok(())
    }
}
