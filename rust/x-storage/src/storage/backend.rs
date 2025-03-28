use async_trait::async_trait;
use x_common::{ConditionalSend, ConditionalSync};

use crate::XStorageError;

mod memory;
pub use memory::*;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod indexeddb;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use indexeddb::*;

#[cfg(not(target_arch = "wasm32"))]
mod fs;
#[cfg(not(target_arch = "wasm32"))]
pub use fs::*;

/// A [StorageBackend] is a facade over some generalized storage substrate that
/// is capable of storing and/or retrieving values by some key
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait StorageBackend {
    /// The key type used by this [StorageBackend]
    type Key: ConditionalSync;
    /// The value type able to be stored by this [StorageBackend]
    type Value: ConditionalSend;
    /// The error type produced by this [StorageBackend]
    type Error: Into<XStorageError>;

    /// Store the given value against the given key
    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error>;
    /// Retrieve a value (if any) stored against the given key
    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error>;
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::StorageBackend;

    #[cfg(not(target_arch = "wasm32"))]
    use crate::FileSystemStorageBackend;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use crate::IndexedDbStorageBackend;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    type MakeTargetStorageOutput = (IndexedDbStorageBackend<Vec<u8>, Vec<u8>>, ());
    #[cfg(not(target_arch = "wasm32"))]
    type MakeTargetStorageOutput = (
        FileSystemStorageBackend<Vec<u8>, Vec<u8>>,
        tempfile::TempDir,
    );

    async fn make_target_storage() -> Result<MakeTargetStorageOutput> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        return Ok((
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db", "test_store").await?,
            (),
        ));
        #[cfg(not(target_arch = "wasm32"))]
        {
            let root = tempfile::tempdir()?;
            let storage = FileSystemStorageBackend::<Vec<u8>, Vec<u8>>::new(root.path()).await?;
            Ok((storage, root))
        }
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_writes_and_reads_a_value() -> Result<()> {
        let (mut storage_backend, _tempdir) = make_target_storage().await?;

        storage_backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
        let value = storage_backend.get(&vec![1, 2, 3]).await?;

        assert_eq!(value, Some(vec![4, 5, 6]));

        Ok(())
    }
}
