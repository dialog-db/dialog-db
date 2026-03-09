//! Capability provider implementations for the `dialog-effects` archive hierarchy.
//!
//! When the `archive` feature is enabled, [`BlobStorage`] implements
//! [`Provider`] for [`archive::Get`] and [`archive::Put`], bridging the
//! capability-based authorization model with the underlying VFS-backed
//! content-addressed store.
//!
//! Note: [`BlobStorage`] is a single-tenant, flat store — the `subject` and
//! `catalog` fields from the capability chain are intentionally ignored.
//! If multi-tenant or catalog-scoped storage is needed, create separate
//! [`BlobStorage`] instances per subject/catalog.

use dialog_capability::Provider;
use dialog_common::Blake3Hash;
use dialog_effects::archive::{self, ArchiveError, Capability, GetCapability, PutCapability};
use futures_util::StreamExt;

use crate::BlobStorage;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive::Get> for BlobStorage {
    async fn execute(
        &mut self,
        effect: Capability<archive::Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        let digest = effect.digest().clone();

        let reader = self
            .get(digest)
            .await
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        match reader {
            None => Ok(None),
            Some(mut reader) => {
                let mut buf = Vec::new();
                while let Some(chunk) = reader.next().await {
                    let bytes = chunk.map_err(|e| ArchiveError::Io(e.to_string()))?;
                    buf.extend_from_slice(&bytes);
                }
                Ok(Some(buf))
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive::Put> for BlobStorage {
    async fn execute(&mut self, effect: Capability<archive::Put>) -> Result<(), ArchiveError> {
        let digest = effect.digest().clone();
        let content = effect.content().to_vec();

        // Verify content integrity: the declared digest must match the
        // actual hash of the content, matching the convention used by
        // all other archive providers in dialog-storage.
        let actual = Blake3Hash::hash(&content);
        if actual != digest {
            return Err(ArchiveError::DigestMismatch {
                expected: format!("{digest:?}"),
                actual: format!("{actual:?}"),
            });
        }

        let stream = futures_util::stream::iter(vec![content]);
        self.put(stream)
            .await
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::did;
    use dialog_effects::archive::{Archive, Catalog, Get, Put, Subject};

    #[cfg(not(target_arch = "wasm32"))]
    use crate::Vfs;

    #[cfg(not(target_arch = "wasm32"))]
    fn storage() -> (BlobStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let vfs = Vfs::new(dir.path().to_path_buf());
        (BlobStorage::new(vfs), dir)
    }

    #[dialog_common::test]
    #[cfg(not(target_arch = "wasm32"))]
    async fn it_puts_and_gets_via_capability() {
        let (mut store, _dir) = storage();

        let content = b"hello via capability".to_vec();
        let digest = Blake3Hash::hash(&content);

        Subject::from(did!("key:zTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("blobs"))
            .invoke(Put::new(digest.clone(), content.clone()))
            .perform(&mut store)
            .await
            .unwrap();

        let result = Subject::from(did!("key:zTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("blobs"))
            .invoke(Get::new(digest))
            .perform(&mut store)
            .await
            .unwrap();

        assert_eq!(result, Some(content));
    }

    #[dialog_common::test]
    #[cfg(not(target_arch = "wasm32"))]
    async fn it_returns_none_for_missing_blob_via_capability() {
        let (mut store, _dir) = storage();

        let digest = Blake3Hash::from([0u8; 32]);
        let result = Subject::from(did!("key:zTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("blobs"))
            .invoke(Get::new(digest))
            .perform(&mut store)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[dialog_common::test]
    #[cfg(not(target_arch = "wasm32"))]
    async fn it_rejects_digest_mismatch_via_capability() {
        let (mut store, _dir) = storage();

        let content = b"some content".to_vec();
        let wrong_digest = Blake3Hash::from([0u8; 32]);

        let result = Subject::from(did!("key:zTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("blobs"))
            .invoke(Put::new(wrong_digest, content))
            .perform(&mut store)
            .await;

        assert!(matches!(result, Err(ArchiveError::DigestMismatch { .. })));
    }
}
