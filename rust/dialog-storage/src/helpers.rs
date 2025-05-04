use anyhow::Result;

#[cfg(not(target_arch = "wasm32"))]
use crate::FileSystemStorageBackend;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::IndexedDbStorageBackend;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use base58::ToBase58;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
type MakeTargetStorageOutput<K> = (IndexedDbStorageBackend<K, Vec<u8>>, ());
#[cfg(not(target_arch = "wasm32"))]
type MakeTargetStorageOutput<K> = (FileSystemStorageBackend<K, Vec<u8>>, tempfile::TempDir);

/// Creates a platform-specific persisted [`StorageBackend`], for use in tests
pub async fn make_target_storage<K>() -> Result<MakeTargetStorageOutput<K>>
where
    K: AsRef<[u8]> + Clone,
{
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    return Ok((
        IndexedDbStorageBackend::<K, Vec<u8>>::new(
            &format!("test_db_{}", rand::random::<[u8; 8]>().to_base58()),
            &format!("test_store_{}", rand::random::<[u8; 8]>().to_base58()),
        )
        .await?,
        (),
    ));
    #[cfg(not(target_arch = "wasm32"))]
    {
        let root = tempfile::tempdir()?;
        let storage = FileSystemStorageBackend::<K, Vec<u8>>::new(root.path()).await?;
        Ok((storage, root))
    }
}
