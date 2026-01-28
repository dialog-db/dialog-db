//! Remote storage backend types.
//!
//! This module provides the [`RemoteBackend`] enum which wraps different
//! storage backend implementations. Each variant is gated behind its
//! respective feature flag.

use async_trait::async_trait;
use base58::{FromBase58, ToBase58};
use dialog_common::Blake3Hash;
use dialog_storage::{
    DialogStorageError, MemoryStorageBackend, StorageBackend, TransactionalMemoryBackend,
};

#[cfg(feature = "s3")]
use {super::Operator, crate::ErrorMappingBackend, dialog_storage::s3::Bucket};

/// A storage backend for remote connections.
///
/// This enum wraps the underlying storage backend implementations.
/// Each variant is gated behind its respective feature flag.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum RemoteBackend {
    /// S3-compatible storage backend.
    #[cfg(feature = "s3")]
    S3(ErrorMappingBackend<Bucket<Operator>>),
    /// In-memory storage backend (useful for testing).
    Memory(MemoryStorageBackend<Vec<u8>, Vec<u8>>),
}

impl std::fmt::Debug for RemoteBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(_) => f.debug_tuple("S3").finish(),
            Self::Memory(_) => f.debug_tuple("Memory").finish(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl StorageBackend for RemoteBackend {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(backend) => backend.get(key).await,
            Self::Memory(backend) => backend.get(key).await,
        }
    }

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(backend) => backend.set(key, value).await,
            Self::Memory(backend) => backend.set(key, value).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl TransactionalMemoryBackend for RemoteBackend {
    type Address = Vec<u8>;
    type Value = Vec<u8>;
    type Error = DialogStorageError;
    type Edition = String;

    async fn resolve(
        &mut self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(backend) => backend.resolve(address).await,
            Self::Memory(backend) => backend
                .resolve(address)
                .await
                .map(|opt| opt.map(|(v, hash)| (v, hash.as_bytes().to_base58()))),
        }
    }

    async fn replace(
        &mut self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(backend) => backend.replace(address, edition, content).await,
            Self::Memory(backend) => {
                let hash = edition.and_then(|e| {
                    e.from_base58()
                        .ok()
                        .and_then(|bytes| Blake3Hash::try_from(bytes).ok())
                });
                backend
                    .replace(address, hash.as_ref(), content)
                    .await
                    .map(|opt| opt.map(|h| h.as_bytes().to_base58()))
            }
        }
    }
}
