//! Mount, Load, and Save providers for FileStore and FileSystem.

use super::{FileStore, FileSystem};
use async_trait::async_trait;
use dialog_capability::storage::{Load, Location, Mount, Mountable, Save, StorageError};
use dialog_capability::{Capability, Policy, Provider};
use dialog_credentials::credential::{Credential, CredentialExport};

impl Mountable for FileSystem {
    type Store = FileStore;
}

#[async_trait]
impl Provider<Mount<FileStore>> for FileSystem {
    async fn execute(
        &self,
        input: Capability<Mount<FileStore>>,
    ) -> Result<FileStore, StorageError> {
        FileSystem::mount(Location::of(&input)).map_err(to_err)
    }
}

#[async_trait]
impl Provider<Load<Vec<u8>>> for FileStore {
    async fn execute(&self, input: Capability<Load<Vec<u8>>>) -> Result<Vec<u8>, StorageError> {
        let path = &Location::of(&input).path();
        let location = self
            .resolve(path)
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        location
            .read()
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))
    }
}

#[async_trait]
impl Provider<Save<Vec<u8>>> for FileStore {
    async fn execute(&self, input: Capability<Save<Vec<u8>>>) -> Result<(), StorageError> {
        let path = &Location::of(&input).path();
        let bytes = &Save::<Vec<u8>>::of(&input).content;
        let location = self
            .resolve(path)
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        location
            .write(bytes)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))
    }
}

#[async_trait]
impl Provider<Load<Credential>> for FileStore {
    async fn execute(
        &self,
        input: Capability<Load<Credential>>,
    ) -> Result<Credential, StorageError> {
        let path = &Location::of(&input).path();
        let location = self
            .resolve(path)
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        let data = location
            .read()
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        let export =
            CredentialExport::try_from(data).map_err(|e| StorageError::Storage(e.to_string()))?;
        Credential::import(export)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))
    }
}

#[async_trait]
impl Provider<Save<Credential>> for FileStore {
    async fn execute(&self, input: Capability<Save<Credential>>) -> Result<(), StorageError> {
        let path = &Location::of(&input).path();
        let credential = &Save::<Credential>::of(&input).content;
        let location = self
            .resolve(path)
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        let export = credential
            .export()
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        location
            .write(export.as_bytes())
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))
    }
}

fn to_err(e: impl std::fmt::Display) -> StorageError {
    StorageError::Storage(e.to_string())
}

#[async_trait]
impl Provider<Load<Credential>> for FileSystem {
    async fn execute(
        &self,
        input: Capability<Load<Credential>>,
    ) -> Result<Credential, StorageError> {
        let path = FileSystem::resolve(Location::of(&input)).map_err(to_err)?;
        let data = tokio::fs::read(&path).await.map_err(StorageError::Io)?;
        let export = CredentialExport::try_from(data).map_err(to_err)?;
        Credential::import(export).await.map_err(to_err)
    }
}

#[async_trait]
impl Provider<Save<Credential>> for FileSystem {
    async fn execute(&self, input: Capability<Save<Credential>>) -> Result<(), StorageError> {
        let path = FileSystem::resolve(Location::of(&input)).map_err(to_err)?;
        let credential = &Save::<Credential>::of(&input).content;
        let export = credential.export().await.map_err(to_err)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(StorageError::Io)?;
        }
        tokio::fs::write(&path, export.as_bytes())
            .await
            .map_err(StorageError::Io)
    }
}
