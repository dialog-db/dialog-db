//! Archive capability provider for volatile storage.

use super::{ArchiveKey, Volatile, VolatileError};
use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::{Capability, Provider};
use dialog_effects::archive::prelude::{GetExt, ImportExt, PutExt};
use dialog_effects::archive::{ArchiveError, Get, Import, Put};

impl From<VolatileError> for ArchiveError {
    fn from(e: VolatileError) -> Self {
        ArchiveError::Storage(e.to_string())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Get> for Volatile {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        let subject = effect.subject().into();
        let catalog = effect.catalog();
        let digest = effect.digest();

        let key: ArchiveKey = (catalog.to_string(), digest.as_bytes().to_base58());

        let sessions = self.sessions.read();
        Ok(sessions
            .get(&subject)
            .and_then(|session| session.archive.get(&key).cloned()))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Put> for Volatile {
    async fn execute(&self, effect: Capability<Put>) -> Result<(), ArchiveError> {
        let subject = effect.subject().into();
        let catalog = effect.catalog();
        let digest = effect.digest();
        let content = effect.content();

        let key: ArchiveKey = (catalog.to_string(), digest.as_bytes().to_base58());

        // Content-addressed storage is idempotent
        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject).or_default();
        session
            .archive
            .entry(key)
            .or_insert_with(|| content.to_vec());

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Import> for Volatile {
    async fn execute(&self, effect: Capability<Import>) -> Result<(), ArchiveError> {
        let subject = effect.subject().into();
        let catalog = effect.catalog();
        let blocks = effect.blocks();

        if blocks.is_empty() {
            return Ok(());
        }

        // Content addressing derives each key from the bytes (the buffer
        // memoizes its hash). One lock acquisition for the whole batch.
        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject).or_default();
        for buffer in blocks {
            let key: ArchiveKey = (
                catalog.to_string(),
                buffer.blake3_hash().as_bytes().to_base58(),
            );
            // Content-addressed storage is idempotent
            session
                .archive
                .entry(key)
                .or_insert_with(|| buffer.as_ref().to_vec());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::unique_subject;
    use dialog_common::{Blake3Hash, Buffer};
    use dialog_effects::archive::{Archive, Catalog};

    #[dialog_common::test]
    async fn it_returns_none_for_missing_content() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("archive-get-none");
        let digest = Blake3Hash::hash(b"nonexistent");

        let effect = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest));

        let result = effect.perform(&provider).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_content() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("archive-put-get");
        let content = b"hello world".to_vec();
        let digest = Blake3Hash::hash(&content);

        // Put content
        let put_effect = subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(Buffer::from(content.clone())));

        put_effect.perform(&provider).await?;

        // Get content
        let get_effect = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest));

        let result = get_effect.perform(&provider).await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_catalogs() -> anyhow::Result<()> {
        let provider = Volatile::new();
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
            .invoke(Put::new(Buffer::from(content1.clone())))
            .perform(&provider)
            .await?;

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog2"))
            .invoke(Put::new(Buffer::from(content2.clone())))
            .perform(&provider)
            .await?;

        // Retrieve from catalog1
        let result1 = subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog1"))
            .invoke(Get::new(digest1))
            .perform(&provider)
            .await?;
        assert_eq!(result1, Some(content1));

        // Retrieve from catalog2
        let result2 = subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog2"))
            .invoke(Get::new(digest2.clone()))
            .perform(&provider)
            .await?;
        assert_eq!(result2, Some(content2));

        // Cross-catalog lookup should return None
        let cross = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog1"))
            .invoke(Get::new(digest2))
            .perform(&provider)
            .await?;
        assert!(cross.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_is_idempotent_for_same_content() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("archive-idempotent");
        let content = b"idempotent content".to_vec();
        let digest = Blake3Hash::hash(&content);

        // Put twice - should succeed both times
        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(Buffer::from(content.clone())))
            .perform(&provider)
            .await?;

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(Buffer::from(content.clone())))
            .perform(&provider)
            .await?;

        // Should still be retrievable
        let result = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_content() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("archive-empty");
        let content = vec![];
        let digest = Blake3Hash::hash(&content);

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(Buffer::from(content.clone())))
            .perform(&provider)
            .await?;

        let result = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("archive-large");
        // 1MB content
        let content: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let digest = Blake3Hash::hash(&content);

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(Buffer::from(content.clone())))
            .perform(&provider)
            .await?;

        let result = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_imports_blocks_in_bulk() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("archive-import");

        let blocks: Vec<Buffer> = (0..8u8).map(|i| Buffer::from(vec![i; 64])).collect();
        let digests: Vec<_> = blocks
            .iter()
            .map(|buffer| buffer.blake3_hash().clone())
            .collect();

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Import::new(blocks))
            .perform(&provider)
            .await?;

        for (i, digest) in digests.into_iter().enumerate() {
            let content = subject
                .clone()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest))
                .perform(&provider)
                .await?;
            assert_eq!(content, Some(vec![i as u8; 64]));
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_accepts_empty_and_repeated_imports() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("archive-import-idempotent");

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Import::new(Vec::<Buffer>::new()))
            .perform(&provider)
            .await?;

        let block = Buffer::from(vec![7u8; 32]);
        let digest = block.blake3_hash().clone();
        for _ in 0..2 {
            subject
                .clone()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Import::new([block.clone()]))
                .perform(&provider)
                .await?;
        }

        let content = subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&provider)
            .await?;
        assert_eq!(content, Some(vec![7u8; 32]));

        Ok(())
    }
}
