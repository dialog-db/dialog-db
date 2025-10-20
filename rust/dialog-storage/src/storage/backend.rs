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
impl<T> StorageBackend for Box<T>
where
    T: StorageBackend + ConditionalSync,
{
    type Key = T::Key;
    type Value = T::Value;
    type Error = T::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        (*self).set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        (*self).get(key).await
    }
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
    use rand::Rng;
    use tokio::sync::Mutex;

    use crate::{
        CompressedStorage, MeasuredStorage, MemoryStorageBackend, StorageBackend, StorageCache,
        StorageOverlay, StorageSink, StorageSource, make_target_storage,
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
    async fn it_can_compress_stored_values() -> Result<()> {
        let (storage_backend, _tempdir) = make_target_storage().await?;
        let compressed_measured_storage =
            Arc::new(Mutex::new(MeasuredStorage::new(storage_backend)));
        let compressed_storage =
            CompressedStorage::<6, _>::new(compressed_measured_storage.clone());
        let mut measured_storage = Arc::new(Mutex::new(MeasuredStorage::new(compressed_storage)));

        let mut chunk_set = Vec::new();

        for _ in 0..32 {
            chunk_set.push(Vec::from(rand::random::<[u8; 32]>()));
        }

        let mut rng = rand::thread_rng();
        for i in 0..1024usize {
            // let chunks = rand::random_range(8..64usize);
            let chunks = rng.gen_range(8..64usize);
            let mut value = Vec::new();

            for _ in 0..chunks {
                // let index = rand::random_range(0..chunk_set.len());
                let index = rng.gen_range(0..chunk_set.len());
                value.append(&mut chunk_set[index].clone())
            }

            measured_storage
                .set(i.to_le_bytes().to_vec(), value)
                .await?;
        }

        for i in 0..1024usize {
            let _ = measured_storage.get(&i.to_le_bytes().to_vec()).await?;
        }

        let outer_measure = measured_storage.lock().await;
        let inner_measure = compressed_measured_storage.lock().await;

        assert!(outer_measure.reads() > 0);
        assert!(inner_measure.reads() > 0);

        assert!(outer_measure.writes() > 0);
        assert!(inner_measure.writes() > 0);

        assert_eq!(outer_measure.reads(), inner_measure.reads());
        assert_eq!(outer_measure.writes(), inner_measure.writes());

        assert!(outer_measure.read_bytes() > inner_measure.read_bytes());
        assert!(outer_measure.write_bytes() > inner_measure.write_bytes());

        println!("\n=== RAW STORAGE ===");
        println!("Reads: {}", outer_measure.reads());
        println!("Bytes read: {}", outer_measure.read_bytes());
        println!("Writes: {}", outer_measure.writes());
        println!("Bytes written: {}", outer_measure.write_bytes());

        println!("\n=== COMPRESSED STORAGE ===");
        println!("Reads: {}", inner_measure.reads());
        println!("Bytes read: {}", inner_measure.read_bytes());
        println!("Writes: {}", inner_measure.writes());
        println!("Bytes written: {}", inner_measure.write_bytes());

        println!(
            "\nCompression rate: {:.2}%\n",
            (1.0 - (inner_measure.write_bytes() as f64 / outer_measure.write_bytes() as f64))
                * 100.
        );

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
        let measured_storage_backend = Arc::new(Mutex::new(MeasuredStorage::new(storage_backend)));
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
        assert_eq!(measured_storage.reads(), 0);

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_updates_cache_key_on_write() -> Result<()> {
        let (storage_backend, _tempdir) = make_target_storage().await?;
        let mut storage_backend = StorageCache::new(storage_backend, 100)?;

        storage_backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;

        assert_eq!(
            Some(vec![4, 5, 6]),
            storage_backend.get(&vec![1, 2, 3]).await?
        );

        storage_backend.set(vec![1, 2, 3], vec![5, 6, 7]).await?;

        assert_eq!(
            Some(vec![5, 6, 7]),
            storage_backend.get(&vec![1, 2, 3]).await?
        );

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
