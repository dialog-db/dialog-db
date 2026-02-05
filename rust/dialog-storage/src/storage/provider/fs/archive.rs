//! Archive capability provider for filesystem.

use super::{FileSystem, FileSystemError};
use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::{Capability, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::archive::{ArchiveError, Get, GetCapability, Put, PutCapability};
use std::io::ErrorKind;
use std::path::PathBuf;
use tokio::fs::{read, rename, write};

impl From<FileSystemError> for ArchiveError {
    fn from(e: FileSystemError) -> Self {
        ArchiveError::Storage(e.to_string())
    }
}

#[async_trait]
impl Provider<Get> for FileSystem {
    async fn execute(&mut self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        let subject = effect.subject().into();
        let catalog = effect.catalog();
        let digest = effect.digest();

        let path: PathBuf = self
            .archive(&subject)?
            .resolve(catalog)?
            .resolve(&digest.as_bytes().to_base58())?
            .try_into()?;

        match read(&path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(ArchiveError::Storage(e.to_string())),
        }
    }
}

#[async_trait]
impl Provider<Put> for FileSystem {
    async fn execute(&mut self, effect: Capability<Put>) -> Result<(), ArchiveError> {
        let subject = effect.subject().into();
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

        let destination = self.archive(&subject)?.resolve(catalog)?;

        // Ensure destination directory exists
        destination.ensure_dir().await?;

        let key = digest.as_bytes().to_base58();
        let path: PathBuf = destination.resolve(&key)?.try_into()?;

        // Content-addressed storage is idempotent - if file exists with same
        // content hash, no need to rewrite
        if path.exists() {
            return Ok(());
        }

        // Write atomically via temp file + rename
        let temp_path: PathBuf = destination.resolve(&format!("{}.tmp", key))?.try_into()?;

        write(&temp_path, content)
            .await
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        rename(&temp_path, &path)
            .await
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::Subject;
    use dialog_effects::archive::{Archive, Catalog};

    fn unique_subject(prefix: &str) -> Subject {
        Subject::from(format!(
            "did:test:{}-{}",
            prefix,
            dialog_common::time::now()
                .duration_since(dialog_common::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_content() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("archive-get-none");
        let digest = Blake3Hash::hash(b"nonexistent");

        let effect = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest));

        let result = effect.perform(&mut provider).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_content() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("archive-put-get");
        let content = b"hello world".to_vec();
        let digest = Blake3Hash::hash(&content);

        // Put content
        let put_effect = subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest.clone(), content.clone()));

        put_effect.perform(&mut provider).await?;

        // Get content
        let get_effect = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest));

        let result = get_effect.perform(&mut provider).await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_digest_mismatch() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("archive-mismatch");
        let content = b"hello world".to_vec();
        let wrong_digest = Blake3Hash::hash(b"different content");

        let effect = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(wrong_digest, content));

        let result = effect.perform(&mut provider).await;
        assert!(matches!(result, Err(ArchiveError::DigestMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_catalogs() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("archive-catalogs");
        let content1 = b"content for catalog 1".to_vec();
        let content2 = b"content for catalog 2".to_vec();
        let digest1 = Blake3Hash::hash(&content1);
        let digest2 = Blake3Hash::hash(&content2);

        // Store in different catalogs
        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog1"))
            .invoke(Put::new(digest1.clone(), content1.clone()))
            .perform(&mut provider)
            .await?;

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog2"))
            .invoke(Put::new(digest2.clone(), content2.clone()))
            .perform(&mut provider)
            .await?;

        // Retrieve from catalog1
        let result1 = subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog1"))
            .invoke(Get::new(digest1))
            .perform(&mut provider)
            .await?;
        assert_eq!(result1, Some(content1));

        // Retrieve from catalog2
        let result2 = subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog2"))
            .invoke(Get::new(digest2.clone()))
            .perform(&mut provider)
            .await?;
        assert_eq!(result2, Some(content2));

        // Cross-catalog lookup should return None
        let cross = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog1"))
            .invoke(Get::new(digest2))
            .perform(&mut provider)
            .await?;
        assert!(cross.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_is_idempotent_for_same_content() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("archive-idempotent");
        let content = b"idempotent content".to_vec();
        let digest = Blake3Hash::hash(&content);

        // Put twice - should succeed both times
        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest.clone(), content.clone()))
            .perform(&mut provider)
            .await?;

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest.clone(), content.clone()))
            .perform(&mut provider)
            .await?;

        // Should still be retrievable
        let result = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&mut provider)
            .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_content() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("archive-empty");
        let content = vec![];
        let digest = Blake3Hash::hash(&content);

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest.clone(), content.clone()))
            .perform(&mut provider)
            .await?;

        let result = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&mut provider)
            .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("archive-large");
        // 1MB content
        let content: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let digest = Blake3Hash::hash(&content);

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest.clone(), content.clone()))
            .perform(&mut provider)
            .await?;

        let result = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&mut provider)
            .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }
}
