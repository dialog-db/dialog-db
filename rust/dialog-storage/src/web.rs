//! This module contains a hack to enable type erasure at the web FFI boundary.
//! The hack is designed for a narrow usage of `StorageBackend`, where the key
//! is a [`Blake3Hash`] and the value is a `Vec<u8>`.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{Blake3Hash, DialogStorageError, Resource, StorageBackend};

/// An "object safe" / dyn-compatible trait that approximates the
/// [`Resource`] API but for a fixed value type (`Vec<u8>`), and without using
/// any associated types (which would make the trait dyn-incompatible).
#[async_trait(?Send)]
pub trait ObjectSafeResource {
    /// Returns a reference to the content of this resource.
    fn content(&self) -> &Option<Vec<u8>>;
    /// Consumes the resource and returns the content.
    fn into_content(self: Box<Self>) -> Option<Vec<u8>>;
    /// Reloads the content of this resource and returns the last content.
    async fn reload(&mut self) -> Result<Option<Vec<u8>>, DialogStorageError>;
    /// Replaces the content of this resource, returning the old content.
    async fn replace(
        &mut self,
        value: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, DialogStorageError>;
}

#[async_trait(?Send)]
impl<R> ObjectSafeResource for R
where
    R: Resource<Value = Vec<u8>>,
    R::Error: Into<DialogStorageError>,
{
    fn content(&self) -> &Option<Vec<u8>> {
        Resource::content(self)
    }

    fn into_content(self: Box<Self>) -> Option<Vec<u8>> {
        Resource::into_content(*self)
    }

    async fn reload(&mut self) -> Result<Option<Vec<u8>>, DialogStorageError> {
        Resource::reload(self).await.map_err(|e| e.into())
    }

    async fn replace(
        &mut self,
        value: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, DialogStorageError> {
        Resource::replace(self, value).await.map_err(|e| e.into())
    }
}

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
    /// Open a resource for the given key
    async fn open(&self, key: &Blake3Hash) -> Result<Box<dyn ObjectSafeResource>, DialogStorageError>;
}

#[async_trait(?Send)]
impl<T> ObjectSafeStorageBackend for T
where
    T: StorageBackend<Key = Blake3Hash, Value = Vec<u8>>,
    T::Resource: 'static,
{
    async fn get(&self, key: &Blake3Hash) -> Result<Option<Vec<u8>>, DialogStorageError> {
        T::get(self, key).await.map_err(|error| error.into())
    }

    async fn set(&mut self, key: Blake3Hash, value: Vec<u8>) -> Result<(), DialogStorageError> {
        T::set(self, key, value).await.map_err(|error| error.into())
    }

    async fn open(&self, key: &Blake3Hash) -> Result<Box<dyn ObjectSafeResource>, DialogStorageError> {
        let resource = T::open(self, key).await.map_err(|error| error.into())?;
        Ok(Box::new(resource))
    }
}

/// A Resource wrapper for the type-erased ObjectSafeResource
pub struct ObjectSafeResourceWrapper {
    inner: Box<dyn ObjectSafeResource>,
}

#[async_trait(?Send)]
impl Resource for ObjectSafeResourceWrapper {
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    fn content(&self) -> &Option<Self::Value> {
        self.inner.content()
    }

    fn into_content(self) -> Option<Self::Value> {
        self.inner.into_content()
    }

    async fn reload(&mut self) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.reload().await
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.replace(value).await
    }
}

#[async_trait(?Send)]
impl StorageBackend for Arc<Mutex<dyn ObjectSafeStorageBackend>> {
    type Key = Blake3Hash;
    type Value = Vec<u8>;
    type Resource = ObjectSafeResourceWrapper;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let mut inner = self.lock().await;
        inner.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let inner = self.lock().await;
        inner.get(key).await
    }

    async fn open(&self, key: &Self::Key) -> Result<Self::Resource, Self::Error> {
        let inner = self.lock().await;
        let resource = inner.open(key).await?;
        Ok(ObjectSafeResourceWrapper { inner: resource })
    }
}
