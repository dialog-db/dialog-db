//! Archive capability provider for filesystem.
//!
//! Layout: `{space_root}/archive/{catalog}/{base58(digest)}`

use super::{FileSystem, FileSystemError, FileSystemHandle};
use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::{Capability, Provider};

const ARCHIVE: &str = "archive";

impl FileSystem {
    /// Returns the handle for this space's archive directory.
    pub fn archive(&self) -> Result<FileSystemHandle, FileSystemError> {
        self.resolve(ARCHIVE)
    }
}
use dialog_common::Blake3Hash;
use dialog_effects::archive::{ArchiveError, Get, GetCapability, Put, PutCapability};

impl From<FileSystemError> for ArchiveError {
    fn from(e: FileSystemError) -> Self {
        ArchiveError::Storage(e.to_string())
    }
}

#[async_trait]
impl Provider<Get> for FileSystem {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        let catalog = effect.catalog();
        let digest = effect.digest();

        let handle = self
            .archive()?
            .resolve(catalog)?
            .resolve(&digest.as_bytes().to_base58())?;

        Ok(handle.read_optional().await?)
    }
}

#[async_trait]
impl Provider<Put> for FileSystem {
    async fn execute(&self, effect: Capability<Put>) -> Result<(), ArchiveError> {
        let catalog = effect.catalog();
        let digest = effect.digest();
        let content = effect.content();

        // Verify content matches the declared digest
        let actual_digest = Blake3Hash::hash(content);
        if &actual_digest != digest {
            return Err(ArchiveError::DigestMismatch {
                expected: digest.as_bytes().to_base58(),
                actual: actual_digest.as_bytes().to_base58(),
            });
        }

        let key = digest.as_bytes().to_base58();
        let destination = self.archive()?.resolve(catalog)?;
        let handle = destination.resolve(&key)?;

        // Content-addressed storage is idempotent - if file exists with same
        // content hash, no need to rewrite
        if handle.exists().await {
            return Ok(());
        }

        // Write atomically via temp file + rename
        let tmp_handle = destination.resolve(&format!("{}.tmp", key))?;
        tmp_handle.write(content).await?;
        tmp_handle.rename(&handle).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::Resource;
    use dialog_capability::Did;
    use dialog_effects::prelude::*;
    use dialog_effects::storage::{Directory, Location as StorageLocation};

    fn unique_name(prefix: &str) -> String {
        use dialog_common::time;
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let ts = time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{ts}-{seq}")
    }

    async fn unique_did() -> Did {
        let signer = dialog_credentials::Ed25519Signer::generate().await.unwrap();
        dialog_varsig::Principal::did(&signer)
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_content() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_returns_none_for_missing_content"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_did().await;
        let digest = Blake3Hash::hash(b"nonexistent");

        let result = subject
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&provider)
            .await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_content() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_stores_and_retrieves_content"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_did().await;
        let content = b"hello world".to_vec();
        let digest = Blake3Hash::hash(&content);

        // Put content
        let put_effect = subject
            .clone()
            .archive()
            .catalog("index")
            .put(digest.clone(), content.clone());

        put_effect.perform(&provider).await?;

        // Get content
        let result = subject
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_digest_mismatch() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_rejects_digest_mismatch"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_did().await;
        let content = b"hello world".to_vec();
        let wrong_digest = Blake3Hash::hash(b"different content");

        let effect = subject
            .archive()
            .catalog("index")
            .put(wrong_digest, content);

        let result = effect.perform(&provider).await;
        assert!(matches!(result, Err(ArchiveError::DigestMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_catalogs() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_handles_different_catalogs"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_did().await;
        let content1 = b"content for catalog 1".to_vec();
        let content2 = b"content for catalog 2".to_vec();
        let digest1 = Blake3Hash::hash(&content1);
        let digest2 = Blake3Hash::hash(&content2);

        // Store in different catalogs
        subject
            .clone()
            .archive()
            .catalog("catalog1")
            .put(digest1.clone(), content1.clone())
            .perform(&provider)
            .await?;

        subject
            .clone()
            .archive()
            .catalog("catalog2")
            .put(digest2.clone(), content2.clone())
            .perform(&provider)
            .await?;

        // Retrieve from catalog1
        let result1 = subject
            .clone()
            .archive()
            .catalog("catalog1")
            .get(digest1)
            .perform(&provider)
            .await?;
        assert_eq!(result1, Some(content1));

        // Retrieve from catalog2
        let result2 = subject
            .clone()
            .archive()
            .catalog("catalog2")
            .get(digest2.clone())
            .perform(&provider)
            .await?;
        assert_eq!(result2, Some(content2));

        // Cross-catalog lookup should return None
        let cross = subject
            .archive()
            .catalog("catalog1")
            .get(digest2)
            .perform(&provider)
            .await?;
        assert!(cross.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_is_idempotent_for_same_content() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_is_idempotent_for_same_content"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_did().await;
        let content = b"idempotent content".to_vec();
        let digest = Blake3Hash::hash(&content);

        // Put twice - should succeed both times
        subject
            .clone()
            .archive()
            .catalog("index")
            .put(digest.clone(), content.clone())
            .perform(&provider)
            .await?;

        subject
            .clone()
            .archive()
            .catalog("index")
            .put(digest.clone(), content.clone())
            .perform(&provider)
            .await?;

        // Should still be retrievable
        let result = subject
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_content() -> anyhow::Result<()> {
        let location =
            StorageLocation::new(Directory::Temp, unique_name("fs-it_handles_empty_content"));
        let provider = FileSystem::open(&location).await?;
        let subject = unique_did().await;
        let content = vec![];
        let digest = Blake3Hash::hash(&content);

        subject
            .clone()
            .archive()
            .catalog("index")
            .put(digest.clone(), content.clone())
            .perform(&provider)
            .await?;

        let result = subject
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content() -> anyhow::Result<()> {
        let location =
            StorageLocation::new(Directory::Temp, unique_name("fs-it_handles_large_content"));
        let provider = FileSystem::open(&location).await?;
        let subject = unique_did().await;
        // 1MB content
        let content: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let digest = Blake3Hash::hash(&content);

        subject
            .clone()
            .archive()
            .catalog("index")
            .put(digest.clone(), content.clone())
            .perform(&provider)
            .await?;

        let result = subject
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }
}
