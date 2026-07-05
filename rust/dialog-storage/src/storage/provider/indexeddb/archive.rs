//! Archive capability provider for IndexedDB.

use super::{IndexedDb, to_uint8array};
use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::{Capability, Provider};
use dialog_effects::archive::prelude::{GetExt, ImportExt, PutExt};
use dialog_effects::archive::{ArchiveError, Get, Import, Put};
use js_sys::Uint8Array;
use wasm_bindgen::{JsCast, JsValue};

fn storage_error(e: impl std::fmt::Display) -> ArchiveError {
    ArchiveError::Storage(e.to_string())
}

const ARCHIVE: &str = "archive";

impl From<super::IndexedDbError> for ArchiveError {
    fn from(e: super::IndexedDbError) -> Self {
        ArchiveError::Storage(e.to_string())
    }
}

#[async_trait(?Send)]
impl Provider<Get> for IndexedDb {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        let catalog = effect.catalog();
        let digest = effect.digest();

        let store_name = format!("{ARCHIVE}/{catalog}");
        let key = JsValue::from_str(&digest.as_bytes().to_base58());

        let store = self.store(&store_name).await?;
        store
            .query(|object_store| async move {
                let value = object_store.get(key).await.map_err(storage_error)?;

                let Some(value) = value else {
                    return Ok(None);
                };

                let bytes = value
                    .dyn_into::<Uint8Array>()
                    .map_err(|_| ArchiveError::Storage("Value is not Uint8Array".to_string()))?
                    .to_vec();

                Ok(Some(bytes))
            })
            .await
    }
}

#[async_trait(?Send)]
impl Provider<Put> for IndexedDb {
    async fn execute(&self, effect: Capability<Put>) -> Result<(), ArchiveError> {
        let catalog = effect.catalog();
        let digest = effect.digest();
        let content = effect.content();

        let store_name = format!("{ARCHIVE}/{catalog}");
        let key = JsValue::from_str(&digest.as_bytes().to_base58());
        let value: JsValue = to_uint8array(content).into();

        let store = self.store(&store_name).await?;
        store
            .transact(|object_store| async move {
                object_store
                    .put(&value, Some(&key))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
            .await
    }
}

#[async_trait(?Send)]
impl Provider<Import> for IndexedDb {
    async fn execute(&self, effect: Capability<Import>) -> Result<(), ArchiveError> {
        let catalog = effect.catalog();
        let blocks = effect.blocks();

        if blocks.is_empty() {
            return Ok(());
        }

        // Content addressing derives each key from the bytes (the buffer
        // memoizes its hash).
        let entries: Vec<(JsValue, JsValue)> = blocks
            .iter()
            .map(|buffer| {
                let key = JsValue::from_str(&buffer.blake3_hash().as_bytes().to_base58());
                let value: JsValue = to_uint8array(buffer.as_ref()).into();
                (key, value)
            })
            .collect();

        let store_name = format!("{ARCHIVE}/{catalog}");
        let store = self.store(&store_name).await?;
        // One read-write transaction for the whole batch.
        store
            .transact(|object_store| async move {
                for (key, value) in entries {
                    object_store
                        .put(&value, Some(&key))
                        .await
                        .map_err(storage_error)?;
                }
                Ok(())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::{unique_name, unique_subject};
    use dialog_common::{Blake3Hash, Buffer};
    use dialog_effects::archive::{Archive, Catalog};

    #[dialog_common::test]
    async fn it_returns_none_for_missing_content() -> anyhow::Result<()> {
        let provider = IndexedDb::connect(unique_name("archive-get-none")).await?;
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
        let provider = IndexedDb::connect(unique_name("archive-put-get")).await?;
        let subject = unique_subject("archive-put-get");
        let content = b"hello world".to_vec();
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
    async fn it_handles_different_catalogs() -> anyhow::Result<()> {
        let provider = IndexedDb::connect(unique_name("archive-catalogs")).await?;
        let subject = unique_subject("archive-catalogs");
        let content1 = b"content for catalog 1".to_vec();
        let content2 = b"content for catalog 2".to_vec();
        let digest1 = Blake3Hash::hash(&content1);
        let digest2 = Blake3Hash::hash(&content2);

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

        let result1 = subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog1"))
            .invoke(Get::new(digest1))
            .perform(&provider)
            .await?;
        assert_eq!(result1, Some(content1));

        let result2 = subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("catalog2"))
            .invoke(Get::new(digest2.clone()))
            .perform(&provider)
            .await?;
        assert_eq!(result2, Some(content2));

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
    async fn it_imports_blocks_in_bulk() -> anyhow::Result<()> {
        let provider = IndexedDb::connect(unique_name("archive-import")).await?;
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

    /// Not an assertion-bearing benchmark (timing in CI is too noisy to
    /// gate on), but a measurement that prints alongside the test run:
    /// per-block puts vs one import for a flush-sized batch.
    #[dialog_common::test]
    async fn it_reports_import_vs_puts_timing() -> anyhow::Result<()> {
        const BLOCKS: usize = 64;
        const BLOCK_SIZE: usize = 4096;

        let provider = IndexedDb::connect(unique_name("archive-import-timing")).await?;
        let subject = unique_subject("archive-import-timing");

        let blocks: Vec<Buffer> = (0..BLOCKS)
            .map(|i| {
                let mut content = vec![0u8; BLOCK_SIZE];
                content[0] = i as u8;
                content[1] = (i >> 8) as u8;
                Buffer::from(content)
            })
            .collect();

        let start = js_sys::Date::now();
        for block in &blocks {
            subject
                .clone()
                .attenuate(Archive)
                .attenuate(Catalog::new("puts"))
                .invoke(Put::new(block.clone()))
                .perform(&provider)
                .await?;
        }
        let puts_ms = js_sys::Date::now() - start;

        let start = js_sys::Date::now();
        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("import"))
            .invoke(Import::new(blocks))
            .perform(&provider)
            .await?;
        let import_ms = js_sys::Date::now() - start;

        wasm_bindgen_test::console_log!(
            "[measurement] {BLOCKS} blocks x {BLOCK_SIZE}B: per-block puts {puts_ms:.1} ms, single import {import_ms:.1} ms"
        );

        Ok(())
    }
}
