//! Storage capability provider for filesystem.
//!
//! Implements key-value storage effects by storing data in files under
//! `{root}/{subject}/storage/{store}/{url_encoded_key}`.

use std::path::PathBuf;

use super::FileSystem;
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::storage::{
    Delete, DeleteCapability, Get, GetCapability, List, ListCapability, ListResult, Set,
    SetCapability, StorageError,
};

fn to_err(e: impl std::fmt::Display) -> StorageError {
    StorageError::Storage(e.to_string())
}

fn key_to_segment(key: &[u8]) -> String {
    url::form_urlencoded::byte_serialize(key).collect()
}

fn segment_to_key(segment: &str) -> Vec<u8> {
    url::form_urlencoded::parse(segment.as_bytes())
        .next()
        .map(|(k, _)| k.into_owned().into_bytes())
        .unwrap_or_default()
}

#[async_trait]
impl Provider<Get> for FileSystem {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, StorageError> {
        let subject = effect.subject();
        let store = effect.store();
        let key = effect.key();

        let location = self.storage(subject, store).map_err(to_err)?;
        let file = location.resolve(&key_to_segment(key)).map_err(to_err)?;

        match file.read().await {
            Ok(data) => Ok(Some(data)),
            Err(_) => Ok(None),
        }
    }
}

#[async_trait]
impl Provider<Set> for FileSystem {
    async fn execute(&self, effect: Capability<Set>) -> Result<(), StorageError> {
        let subject = effect.subject();
        let store = effect.store();
        let key = effect.key();
        let value = effect.value();

        let location = self.storage(subject, store).map_err(to_err)?;
        let file = location.resolve(&key_to_segment(key)).map_err(to_err)?;

        file.write(value).await.map_err(to_err)
    }
}

#[async_trait]
impl Provider<Delete> for FileSystem {
    async fn execute(&self, effect: Capability<Delete>) -> Result<(), StorageError> {
        let subject = effect.subject();
        let store = effect.store();
        let key = effect.key();

        let location = self.storage(subject, store).map_err(to_err)?;
        let file = location.resolve(&key_to_segment(key)).map_err(to_err)?;

        file.remove().await.map_err(to_err)
    }
}

#[async_trait]
impl Provider<List> for FileSystem {
    async fn execute(&self, effect: Capability<List>) -> Result<ListResult, StorageError> {
        let subject = effect.subject();
        let store = effect.store();

        let location = self.storage(subject, store).map_err(to_err)?;
        let path: Result<PathBuf, _> = location.clone().try_into();

        let mut keys = Vec::new();
        if let Ok(path) = path
            && path.exists()
        {
            let mut entries = tokio::fs::read_dir(&path).await.map_err(StorageError::Io)?;
            while let Some(entry) = entries.next_entry().await.map_err(StorageError::Io)? {
                if let Some(name) = entry.file_name().to_str() {
                    let key_bytes = segment_to_key(name);
                    if let Ok(key_str) = String::from_utf8(key_bytes) {
                        keys.push(key_str);
                    }
                }
            }
        }

        Ok(ListResult {
            keys,
            is_truncated: false,
            next_continuation_token: None,
        })
    }
}
