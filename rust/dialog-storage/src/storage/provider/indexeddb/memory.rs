//! Memory capability provider for IndexedDB.
//!
//! Implements transactional cell storage with CAS (Compare-And-Swap) semantics.
//! Uses a single `memory` store with space and cell encoded in the key.

use super::{IndexedDb, to_uint8array};
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::memory::{
    MemoryError, Publication, Publish, PublishCapability, Resolve, ResolveCapability, Retract,
    RetractCapability,
};
use js_sys::Uint8Array;
use wasm_bindgen::{JsCast, JsValue};

/// The single object store used for all memory operations.
const MEMORY_STORE: &str = "memory";

/// Format edition bytes for error messages.
fn format_edition(edition: Option<&[u8]>) -> Option<String> {
    edition.map(base58::ToBase58::to_base58)
}

/// Build a key from space and cell.
fn make_key(space: &str, cell: &str) -> JsValue {
    JsValue::from_str(&format!("{}/{}", space, cell))
}

fn storage_error(e: impl std::fmt::Display) -> MemoryError {
    MemoryError::Storage(e.to_string())
}

impl From<super::IndexedDbError> for MemoryError {
    fn from(e: super::IndexedDbError) -> Self {
        MemoryError::Storage(e.to_string())
    }
}

#[async_trait(?Send)]
impl Provider<Resolve> for IndexedDb {
    async fn execute(
        &mut self,
        effect: Capability<Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        let subject = effect.subject().into();
        let key = make_key(effect.space(), effect.cell());

        let store = self.open(&subject).await?.store(MEMORY_STORE).await?;

        store
            .query(|object_store| async move {
                let value = object_store.get(key).await.map_err(storage_error)?;

                let Some(value) = value else {
                    return Ok(None);
                };

                let bytes = value
                    .dyn_into::<Uint8Array>()
                    .map_err(|_| MemoryError::Storage("Value is not Uint8Array".to_string()))?
                    .to_vec();

                let edition = Blake3Hash::hash(&bytes);

                Ok(Some(Publication {
                    content: bytes,
                    edition: edition.as_bytes().to_vec(),
                }))
            })
            .await
    }
}

#[async_trait(?Send)]
impl Provider<Publish> for IndexedDb {
    async fn execute(&mut self, effect: Capability<Publish>) -> Result<Vec<u8>, MemoryError> {
        let subject = effect.subject().into();
        let key = make_key(effect.space(), effect.cell());
        let content = effect.content().to_vec();
        let expected_edition = effect.when().map(|e| e.to_vec());

        let store = self.open(&subject).await?.store(MEMORY_STORE).await?;

        store
            .transact(|object_store| async move {
                // Read current value to check CAS condition
                let current = object_store.get(key.clone()).await.map_err(storage_error)?;

                let current_edition = if let Some(value) = &current {
                    let bytes = value
                        .clone()
                        .dyn_into::<Uint8Array>()
                        .map_err(|_| MemoryError::Storage("Value is not Uint8Array".to_string()))?
                        .to_vec();
                    Some(Blake3Hash::hash(&bytes))
                } else {
                    None
                };

                // Compute new edition
                let new_edition = Blake3Hash::hash(&content);

                // If current value already matches desired value, succeed without writing
                if current_edition.as_ref().map(|h| h.as_bytes()) == Some(new_edition.as_bytes()) {
                    return Ok(new_edition.as_bytes().to_vec());
                }

                // Check CAS condition
                match (expected_edition.as_deref(), &current_edition) {
                    // Creating new: require cell doesn't exist
                    (None, Some(_)) => {
                        return Err(MemoryError::EditionMismatch {
                            expected: None,
                            actual: format_edition(
                                current_edition.as_ref().map(|h| h.as_bytes().as_slice()),
                            ),
                        });
                    }
                    // Updating existing: require edition matches
                    (Some(expected), Some(current)) => {
                        if expected != current.as_bytes() {
                            return Err(MemoryError::EditionMismatch {
                                expected: format_edition(Some(expected)),
                                actual: format_edition(Some(current.as_bytes())),
                            });
                        }
                    }
                    // Updating non-existent: fail
                    (Some(expected), None) => {
                        return Err(MemoryError::EditionMismatch {
                            expected: format_edition(Some(expected)),
                            actual: None,
                        });
                    }
                    // Creating new when cell doesn't exist: valid
                    (None, None) => {}
                }

                // Write the new value
                let js_value: JsValue = to_uint8array(&content).into();
                object_store
                    .put(&js_value, Some(&key))
                    .await
                    .map_err(storage_error)?;

                Ok(new_edition.as_bytes().to_vec())
            })
            .await
    }
}

#[async_trait(?Send)]
impl Provider<Retract> for IndexedDb {
    async fn execute(&mut self, effect: Capability<Retract>) -> Result<(), MemoryError> {
        let subject = effect.subject().into();
        let key = make_key(effect.space(), effect.cell());
        let expected_edition = effect.when().to_vec();

        let store = self.open(&subject).await?.store(MEMORY_STORE).await?;

        store
            .transact(|object_store| async move {
                // Read current value to check CAS condition
                let current = object_store.get(key.clone()).await.map_err(storage_error)?;

                // If already deleted, succeed
                let Some(value) = current else {
                    return Ok(());
                };

                let bytes = value
                    .dyn_into::<Uint8Array>()
                    .map_err(|_| MemoryError::Storage("Value is not Uint8Array".to_string()))?
                    .to_vec();
                let current_edition = Blake3Hash::hash(&bytes);

                // Check CAS condition
                if expected_edition != current_edition.as_bytes() {
                    return Err(MemoryError::EditionMismatch {
                        expected: format_edition(Some(&expected_edition)),
                        actual: format_edition(Some(current_edition.as_bytes())),
                    });
                }

                // Delete the value
                object_store.delete(key).await.map_err(storage_error)?;

                Ok(())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Did, Subject};
    use dialog_effects::memory::{Cell, Memory, Space};

    fn unique_subject(prefix: &str) -> Subject {
        let did: Did = format!("did:test:{}-{}", prefix, js_sys::Date::now() as u64)
            .parse()
            .unwrap();
        Subject::from(did)
    }

    #[dialog_common::test]
    async fn it_resolves_non_existent_cell() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-resolve-none");

        let effect = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("missing"))
            .invoke(Resolve);

        let result = effect.perform(&mut provider).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_new_content() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-publish-new");
        let content = b"hello world".to_vec();

        // Publish new content (when = None means expect empty)
        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&mut provider)
            .await?;

        assert!(!edition.is_empty());

        // Resolve to verify
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);
        assert_eq!(publication.edition, edition);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_content() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-publish-update");

        // Create initial content
        let edition1 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"initial", None))
            .perform(&mut provider)
            .await?;

        // Update with correct edition
        let edition2 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"updated", Some(edition1.clone())))
            .perform(&mut provider)
            .await?;

        assert_ne!(edition1, edition2);

        // Verify update
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, b"updated".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_on_edition_mismatch() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-mismatch");

        // Create initial content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"initial", None))
            .perform(&mut provider)
            .await?;

        // Try to update with wrong edition
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"updated", Some(wrong_edition)))
            .perform(&mut provider)
            .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_creating_when_exists() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-create-exists");

        // Create initial content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"initial", None))
            .perform(&mut provider)
            .await?;

        // Try to create again (when = None means expect empty)
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"new", None))
            .perform(&mut provider)
            .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_content() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-retract");

        // Create content
        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"to be deleted", None))
            .perform(&mut provider)
            .await?;

        // Retract with correct edition
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Retract::new(edition))
            .perform(&mut provider)
            .await?;

        // Verify deleted
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;

        assert!(resolved.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_retract_on_edition_mismatch() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-retract-mismatch");

        // Create content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"content", None))
            .perform(&mut provider)
            .await?;

        // Try to retract with wrong edition
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Retract::new(wrong_edition))
            .perform(&mut provider)
            .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_spaces() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-spaces");

        // Publish to different spaces
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("space1"))
            .attenuate(Cell::new("cell"))
            .invoke(Publish::new(b"content1", None))
            .perform(&mut provider)
            .await?;

        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("space2"))
            .attenuate(Cell::new("cell"))
            .invoke(Publish::new(b"content2", None))
            .perform(&mut provider)
            .await?;

        // Resolve from space1
        let result1 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("space1"))
            .attenuate(Cell::new("cell"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;
        assert_eq!(result1.unwrap().content, b"content1".to_vec());

        // Resolve from space2
        let result2 = subject
            .attenuate(Memory)
            .attenuate(Space::new("space2"))
            .attenuate(Cell::new("cell"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;
        assert_eq!(result2.unwrap().content, b"content2".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_with_stale_edition_when_value_matches() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-stale-ok");
        let content = b"desired value".to_vec();

        // Create initial content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&mut provider)
            .await?;

        // Try to publish same content with wrong edition - should succeed
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(content.clone(), Some(wrong_edition)))
            .perform(&mut provider)
            .await;

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Blake3Hash::hash(&content).as_bytes().to_vec()
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_produces_deterministic_content_hash() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-deterministic-hash");
        let content = b"same content".to_vec();

        // Create value at cell1
        let edition1 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("cell1"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&mut provider)
            .await?;

        // Create same value at cell2
        let edition2 = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("cell2"))
            .invoke(Publish::new(content, None))
            .perform(&mut provider)
            .await?;

        // Same content should produce same edition (content hash)
        assert_eq!(edition1, edition2);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_retracting_already_retracted() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("memory-retract-already-retracted");

        // Try to retract non-existent cell - should succeed
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("nonexistent"))
            .invoke(Retract::new(wrong_edition))
            .perform(&mut provider)
            .await;

        assert!(result.is_ok());

        Ok(())
    }
}
