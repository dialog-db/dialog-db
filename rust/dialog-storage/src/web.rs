//! This module contains a hack to enable type erasure at the web FFI boundary.
//! The hack is designed for a narrow usage of `StorageBackend`, where the key
//! is a [`Blake3Hash`] and the value is a `Vec<u8>`.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{Blake3Hash, DialogStorageError, StorageBackend};

/// An "object safe" / dyn-compatible trait that approximates the
/// [`StorageBackend`] API but for a fixed key and value type ([`Blake3Hash`]
/// and `Vec<u8>`, respectively), and without using any associated types (which
/// would make the trait dyn-incompatible).
///
/// This trait has a blanket implementation for all implementors of
/// [`StorageBackend<Key = Blake3Hash, Value = Vec<u8>>`]. So, it is
/// suitable to be used to erase the types of those implementors
/// e.g., Arc<Mutex<dyn ObjectSafeStorageBackend>>
#[async_trait(?Send)]
pub trait ObjectSafeStorageBackend {
    /// Retrieve a value (if any) stored against the given key
    async fn get(&self, key: &Blake3Hash) -> Result<Option<Vec<u8>>, DialogStorageError>;
    /// Store the given value against the given key
    async fn set(&mut self, key: Blake3Hash, value: Vec<u8>) -> Result<(), DialogStorageError>;
}

#[async_trait(?Send)]
impl<T> ObjectSafeStorageBackend for T
where
    T: StorageBackend<Key = Blake3Hash, Value = Vec<u8>>,
{
    async fn get(&self, key: &Blake3Hash) -> Result<Option<Vec<u8>>, DialogStorageError> {
        T::get(self, key).await.map_err(|error| error.into())
    }

    async fn set(&mut self, key: Blake3Hash, value: Vec<u8>) -> Result<(), DialogStorageError> {
        T::set(self, key, value).await.map_err(|error| error.into())
    }
}

#[async_trait(?Send)]
impl StorageBackend for Arc<Mutex<dyn ObjectSafeStorageBackend>> {
    type Key = Blake3Hash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let mut inner = self.lock().await;
        inner.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let inner = self.lock().await;
        inner.get(key).await
    }
}
