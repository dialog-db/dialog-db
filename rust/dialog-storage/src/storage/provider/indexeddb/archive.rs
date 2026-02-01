//! Archive capability provider for IndexedDB.

use super::IndexedDb;
use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::{Capability, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::archive::{ArchiveError, Get, GetCapability, Put, PutCapability};
use js_sys::Uint8Array;
use rexie::TransactionMode;
use wasm_bindgen::{JsCast, JsValue};

/// Convert bytes to a JS Uint8Array.
fn bytes_to_typed_array(bytes: &[u8]) -> JsValue {
    let array = Uint8Array::new_with_length(bytes.len() as u32);
    array.copy_from(bytes);
    JsValue::from(array)
}

#[async_trait(?Send)]
impl Provider<Get> for IndexedDb {
    async fn execute(&mut self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        let subject = effect.subject().into();
        let catalog = effect.catalog();
        let digest = effect.digest();

        let store_path = format!("archive/{}", catalog);
        self.ensure_store(&store_path);

        let db = self
            .session(&subject)
            .await
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        let tx = db
            .transaction(&[&store_path], TransactionMode::ReadOnly)
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        let store = tx
            .store(&store_path)
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        // Use base58-encoded digest as the key
        let key = JsValue::from_str(&digest.as_bytes().to_base58());

        let value = store
            .get(key)
            .await
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        let Some(value) = value else {
            return Ok(None);
        };

        let bytes = value
            .dyn_into::<Uint8Array>()
            .map_err(|_| ArchiveError::Storage("Value is not Uint8Array".to_string()))?
            .to_vec();

        tx.done()
            .await
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        Ok(Some(bytes))
    }
}

#[async_trait(?Send)]
impl Provider<Put> for IndexedDb {
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

        let store_path = format!("archive/{}", catalog);
        self.ensure_store(&store_path);

        let db = self
            .session(&subject)
            .await
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        let tx = db
            .transaction(&[&store_path], TransactionMode::ReadWrite)
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        let store = tx
            .store(&store_path)
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        // Use base58-encoded digest as the key
        let key = JsValue::from_str(&digest.as_bytes().to_base58());
        let value = bytes_to_typed_array(content);

        store
            .put(&value, Some(&key))
            .await
            .map_err(|e| ArchiveError::Storage(e.to_string()))?;

        tx.done()
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
            js_sys::Date::now() as u64
        ))
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_content() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
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
        let mut provider = IndexedDb::new();
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
        let mut provider = IndexedDb::new();
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
        let mut provider = IndexedDb::new();
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
}
