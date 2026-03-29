//! Mount, Load, and Save providers for FileStore and FileSystem.

use super::{Address, FileStore, FileSystem};
use async_trait::async_trait;
use dialog_capability::storage::{Load, Location, Mount, Mountable, Save, StorageError};
use dialog_capability::{Capability, Policy, Provider};
use dialog_credentials::credential::{Credential, CredentialExport};

impl Mountable for FileSystem {
    type Store = FileStore;
}

#[async_trait]
impl Provider<Mount<FileStore, Address>> for FileSystem {
    async fn execute(
        &self,
        input: Capability<Mount<FileStore, Address>>,
    ) -> Result<FileStore, StorageError> {
        let address = Location::of(&input).address();
        FileSystem::mount(address).map_err(to_err)
    }
}

#[async_trait]
impl Provider<Load<Vec<u8>, Address>> for FileStore {
    async fn execute(
        &self,
        input: Capability<Load<Vec<u8>, Address>>,
    ) -> Result<Vec<u8>, StorageError> {
        let address = Location::of(&input).address();
        let location = self
            .resolve(address.path())
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        location
            .read()
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))
    }
}

#[async_trait]
impl Provider<Save<Vec<u8>, Address>> for FileStore {
    async fn execute(&self, input: Capability<Save<Vec<u8>, Address>>) -> Result<(), StorageError> {
        let address = Location::of(&input).address();
        let bytes = &Save::<Vec<u8>, Address>::of(&input).content;
        let location = self
            .resolve(address.path())
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        location
            .write(bytes)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))
    }
}

#[async_trait]
impl Provider<Load<Credential, Address>> for FileStore {
    async fn execute(
        &self,
        input: Capability<Load<Credential, Address>>,
    ) -> Result<Credential, StorageError> {
        let address = Location::of(&input).address();
        let location = self
            .resolve(address.path())
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
impl Provider<Save<Credential, Address>> for FileStore {
    async fn execute(
        &self,
        input: Capability<Save<Credential, Address>>,
    ) -> Result<(), StorageError> {
        let address = Location::of(&input).address();
        let credential = &Save::<Credential, Address>::of(&input).content;
        let location = self
            .resolve(address.path())
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

use crate::provider::Address as EnumAddress;

#[async_trait]
impl Provider<Load<Credential, EnumAddress>> for FileSystem {
    async fn execute(
        &self,
        input: Capability<Load<Credential, EnumAddress>>,
    ) -> Result<Credential, StorageError> {
        let address = match Location::of(&input).address() {
            EnumAddress::FileSystem(addr) => addr,
            _ => {
                return Err(StorageError::Storage(
                    "FileSystem cannot handle non-filesystem address".into(),
                ));
            }
        };
        let path = FileSystem::resolve(address).map_err(to_err)?;
        let data = tokio::fs::read(&path).await.map_err(StorageError::Io)?;
        let export = CredentialExport::try_from(data).map_err(to_err)?;
        Credential::import(export).await.map_err(to_err)
    }
}

#[async_trait]
impl Provider<Save<Credential, EnumAddress>> for FileSystem {
    async fn execute(
        &self,
        input: Capability<Save<Credential, EnumAddress>>,
    ) -> Result<(), StorageError> {
        let address = match Location::of(&input).address() {
            EnumAddress::FileSystem(addr) => addr,
            _ => {
                return Err(StorageError::Storage(
                    "FileSystem cannot handle non-filesystem address".into(),
                ));
            }
        };
        let credential = &Save::<Credential, EnumAddress>::of(&input).content;
        let export = credential.export().await.map_err(to_err)?;
        let path = FileSystem::resolve(address).map_err(to_err)?;
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

#[async_trait]
impl Provider<Load<Credential, Address>> for FileSystem {
    async fn execute(
        &self,
        input: Capability<Load<Credential, Address>>,
    ) -> Result<Credential, StorageError> {
        let address = Location::of(&input).address();
        let path = FileSystem::resolve(address).map_err(to_err)?;
        let data = tokio::fs::read(&path).await.map_err(StorageError::Io)?;
        let export = CredentialExport::try_from(data).map_err(to_err)?;
        Credential::import(export).await.map_err(to_err)
    }
}

#[async_trait]
impl Provider<Save<Credential, Address>> for FileSystem {
    async fn execute(
        &self,
        input: Capability<Save<Credential, Address>>,
    ) -> Result<(), StorageError> {
        let address = Location::of(&input).address();
        let credential = &Save::<Credential, Address>::of(&input).content;
        let export = credential.export().await.map_err(to_err)?;
        let path = FileSystem::resolve(address).map_err(to_err)?;
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
