use std::sync::Arc;

use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};
use tokio::sync::Mutex;

use crate::DialogStorageError;

mod memory;
pub use memory::*;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod indexeddb;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use indexeddb::*;

mod rest;
pub use rest::*;

#[cfg(test)]
mod r2_tests;

#[cfg(not(target_arch = "wasm32"))]
mod fs;
#[cfg(not(target_arch = "wasm32"))]
pub use fs::*;

/// A [StorageBackend] is a facade over some generalized storage substrate that
/// is capable of storing and/or retrieving values by some key
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait StorageBackend: Clone {
    /// The key type used by this [StorageBackend]
    type Key: ConditionalSync;
    /// The value type able to be stored by this [StorageBackend]
    type Value: ConditionalSend;
    /// The error type produced by this [StorageBackend]
    type Error: Into<DialogStorageError>;

    /// Store the given value against the given key
    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error>;
    /// Retrieve a value (if any) stored against the given key
    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T> StorageBackend for Arc<Mutex<T>>
where
    T: StorageBackend + ConditionalSend,
{
    type Key = T::Key;
    type Value = T::Value;
    type Error = T::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let mut inner = self.lock().await;
        inner.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let inner = self.lock().await;
        inner.get(key).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use tokio::sync::Mutex;

    use crate::{
        MeasuredStorageBackend, MemoryStorageBackend, StorageBackend, StorageCache, StorageOverlay,
        StorageSink, StorageSource, make_target_storage,
    };

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_writes_and_reads_a_value() -> Result<()> {
        let (mut storage_backend, _tempdir) = make_target_storage().await?;

        storage_backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
        let value = storage_backend.get(&vec![1, 2, 3]).await?;

        assert_eq!(value, Some(vec![4, 5, 6]));

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_wrap_backends_in_an_overlay() -> Result<()> {
        let (storage_backend, _tempdir) = make_target_storage().await?;
        let mut storage_backend = Arc::new(Mutex::new(storage_backend));

        storage_backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;

        let overlay_backend = Arc::new(Mutex::new(MemoryStorageBackend::default()));

        let mut storage_overlay =
            StorageOverlay::new(storage_backend.clone(), overlay_backend.clone());

        storage_overlay.set(vec![2, 3, 4], vec![5, 6, 7]).await?;

        assert_eq!(storage_backend.get(&vec![2, 3, 4]).await?, None);
        assert_eq!(
            overlay_backend.get(&vec![2, 3, 4]).await?,
            Some(vec![5, 6, 7])
        );
        assert_eq!(
            storage_overlay.get(&vec![2, 3, 4]).await?,
            Some(vec![5, 6, 7])
        );
        assert_eq!(
            storage_overlay.get(&vec![1, 2, 3]).await?,
            Some(vec![4, 5, 6])
        );

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_wrap_backends_in_a_transparent_cache() -> Result<()> {
        let (storage_backend, _tempdir) = make_target_storage().await?;
        let measured_storage_backend =
            Arc::new(Mutex::new(MeasuredStorageBackend::new(storage_backend)));
        let mut storage_backend = StorageCache::new(measured_storage_backend.clone(), 100)?;

        storage_backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
        storage_backend.set(vec![2, 3, 4], vec![5, 6, 7]).await?;

        for _ in 0..100 {
            let value_one = storage_backend.get(&vec![1, 2, 3]).await?;
            let value_two = storage_backend.get(&vec![2, 3, 4]).await?;

            assert_eq!(value_one, Some(vec![4, 5, 6]));
            assert_eq!(value_two, Some(vec![5, 6, 7]));
        }

        let measured_storage = measured_storage_backend.lock().await;

        assert_eq!(measured_storage.writes(), 2);
        assert_eq!(measured_storage.reads(), 2);

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_perform_bulk_storage_transfers() -> Result<()> {
        let (mut target_storage_backend, _tempdir) = make_target_storage().await?;
        let mut memory_storage_backend = MemoryStorageBackend::default();

        for i in 0..4usize {
            memory_storage_backend
                .set(
                    i.to_le_bytes().to_vec(),
                    format!("Value{i}").as_bytes().to_vec(),
                )
                .await?;
        }

        target_storage_backend
            .write(memory_storage_backend.drain())
            .await?;

        for i in 0..4usize {
            let value = target_storage_backend
                .get(&i.to_le_bytes().to_vec())
                .await?;
            assert_eq!(value, Some(format!("Value{i}").as_bytes().to_vec()));
        }

        Ok(())
    }
}
