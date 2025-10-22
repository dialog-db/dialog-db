//! In-memory revision backend implementation for testing

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use dialog_artifacts::Revision;
use dialog_storage::StorageBackend;
use tokio::sync::RwLock;

use super::{RevisionBackendError, RevisionUpgrade, RevisionUpgradeRecord, Subject};

/// In-memory revision backend provider.
///
/// This creates a shared revision store that can be accessed by multiple consumers.
/// Useful for testing scenarios where multiple replicas need to coordinate through
/// a shared register.
///
/// # Examples
///
/// ```
/// use dialog_remote::backend::{MemoryBackendProvider, Subject, RevisionUpgrade, RevisionStorageBackend};
/// use dialog_remote::StorageBackend;
/// use dialog_artifacts::Revision;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a provider with an initial revision for a subject
/// let subject = Subject::new("did:key:z6Mkk...");
/// let initial = Revision::new(&[1; 32]);
///
/// let provider = MemoryBackendProvider::new();
/// provider.initialize(&subject, initial.clone()).await?;
///
/// // Create multiple consumers that share the same state
/// let consumer1 = provider.connect();
/// let consumer2 = provider.connect();
///
/// // Both consumers see the same revision
/// let rev1 = consumer1.get(&subject).await?.map(|u| u.revision().clone());
/// let rev2 = consumer2.get(&subject).await?.map(|u| u.revision().clone());
/// assert_eq!(rev1, rev2);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MemoryBackendProvider {
    state: Arc<RwLock<HashMap<Subject, Revision>>>,
}

impl MemoryBackendProvider {
    /// Create a new empty memory backend provider
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize a subject with a revision
    pub async fn initialize(
        &self,
        subject: &Subject,
        revision: Revision,
    ) -> Result<(), RevisionBackendError> {
        let mut state = self.state.write().await;
        state.insert(subject.clone(), revision);
        Ok(())
    }

    /// Create a new consumer connected to this provider
    pub fn connect(&self) -> MemoryBackend {
        MemoryBackend {
            state: Arc::clone(&self.state),
        }
    }
}

impl Default for MemoryBackendProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory revision backend consumer.
///
/// This is created by calling `connect()` on a `MemoryBackendProvider`.
/// All consumers share the same underlying state through the provider.
///
/// Implements `StorageBackend<Key = Subject, Value = RevisionUpgrade, Error = RevisionBackendError>`
/// with compare-and-swap semantics in the `set` method.
#[derive(Clone)]
pub struct MemoryBackend {
    state: Arc<RwLock<HashMap<Subject, Revision>>>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl StorageBackend for MemoryBackend {
    type Key = Subject;
    type Value = RevisionUpgradeRecord;
    type Error = RevisionBackendError;

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let state = self.state.read().await;
        Ok(state.get(key).map(|revision| RevisionUpgradeRecord {
            revision: revision.clone(),
            origin: revision.clone(), // For get, origin = current revision
        }))
    }

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let mut state = self.state.write().await;

        // Get current revision
        let current = state.get(&key);

        // Check compare-and-swap: current must match origin
        match current {
            Some(current_rev) => {
                if current_rev != value.origin() {
                    return Err(RevisionBackendError::RevisionMismatch {
                        subject: key,
                        expected: value.origin().clone(),
                        actual: current_rev.clone(),
                    });
                }
            }
            None => {
                // If no current revision, origin must be zero hash (initial state)
                let zero_hash = Revision::new(&[0; 32]);
                if value.origin() != &zero_hash {
                    return Err(RevisionBackendError::NotFound { subject: key });
                }
            }
        }

        // Perform the swap
        state.insert(key, value.revision().clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_arch = "wasm32"))]
    use tokio::test as async_test;
    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as async_test;

    fn make_revision(byte: u8) -> Revision {
        Revision::new(&[byte; 32])
    }

    fn make_subject() -> Subject {
        Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi")
    }

    #[async_test]
    async fn test_provider_connect() {
        let provider = MemoryBackendProvider::new();
        let subject = make_subject();
        let rev = make_revision(1);

        provider.initialize(&subject, rev.clone()).await.unwrap();

        let consumer1 = provider.connect();
        let consumer2 = provider.connect();

        let upgrade1 = consumer1.get(&subject).await.unwrap().unwrap();
        let upgrade2 = consumer2.get(&subject).await.unwrap().unwrap();

        assert_eq!(upgrade1.revision(), &rev);
        assert_eq!(upgrade2.revision(), &rev);
    }

    #[async_test]
    async fn test_set_success() {
        let provider = MemoryBackendProvider::new();
        let subject = make_subject();
        let rev1 = make_revision(1);
        let rev2 = make_revision(2);

        provider.initialize(&subject, rev1.clone()).await.unwrap();

        let mut consumer = provider.connect();

        // Set should succeed when origin matches current
        let upgrade = RevisionUpgradeRecord::new(rev1.clone(), rev2.clone());
        consumer.set(subject.clone(), upgrade).await.unwrap();

        // Verify the upgrade worked
        let current = consumer.get(&subject).await.unwrap().unwrap();
        assert_eq!(current.revision(), &rev2);
    }

    #[async_test]
    async fn test_set_failure_mismatch() {
        let provider = MemoryBackendProvider::new();
        let subject = make_subject();
        let rev1 = make_revision(1);
        let rev2 = make_revision(2);
        let rev3 = make_revision(3);

        provider.initialize(&subject, rev1.clone()).await.unwrap();

        let mut consumer = provider.connect();

        // Set should fail when origin doesn't match current
        let upgrade = RevisionUpgradeRecord::new(rev2, rev3);
        let result = consumer.set(subject.clone(), upgrade).await;

        assert!(matches!(
            result,
            Err(RevisionBackendError::RevisionMismatch { .. })
        ));

        // Verify nothing changed
        let current = consumer.get(&subject).await.unwrap().unwrap();
        assert_eq!(current.revision(), &rev1);
    }

    #[async_test]
    async fn test_concurrent_set_one_wins() {
        let provider = MemoryBackendProvider::new();
        let subject = make_subject();
        let initial = make_revision(0);

        provider
            .initialize(&subject, initial.clone())
            .await
            .unwrap();

        let mut consumer1 = provider.connect();
        let mut consumer2 = provider.connect();

        // Both try to upgrade from initial to their own revision
        let rev1 = make_revision(1);
        let rev2 = make_revision(2);

        let upgrade1 = RevisionUpgradeRecord::new(initial.clone(), rev1.clone());
        let upgrade2 = RevisionUpgradeRecord::new(initial.clone(), rev2.clone());

        let result1 = consumer1.set(subject.clone(), upgrade1).await;
        let result2 = consumer2.set(subject.clone(), upgrade2).await;

        // One should succeed, one should fail
        let success_count = [result1.is_ok(), result2.is_ok()]
            .iter()
            .filter(|&&x| x)
            .count();
        assert_eq!(success_count, 1);

        // The final revision should be one of the two
        let final_consumer = provider.connect();
        let final_upgrade = final_consumer.get(&subject).await.unwrap().unwrap();
        assert!(final_upgrade.revision() == &rev1 || final_upgrade.revision() == &rev2);
    }

    #[async_test]
    async fn test_set_initial_state() {
        let provider = MemoryBackendProvider::new();
        let subject = make_subject();
        let rev1 = make_revision(1);
        let zero = Revision::new(&[0; 32]);

        let mut consumer = provider.connect();

        // Set initial revision (from zero hash)
        let upgrade = RevisionUpgradeRecord::new(zero, rev1.clone());
        consumer.set(subject.clone(), upgrade).await.unwrap();

        // Verify it was set
        let current = consumer.get(&subject).await.unwrap().unwrap();
        assert_eq!(current.revision(), &rev1);
    }

    #[async_test]
    async fn test_set_initial_state_wrong_origin() {
        let provider = MemoryBackendProvider::new();
        let subject = make_subject();
        let rev1 = make_revision(1);
        let rev2 = make_revision(2);

        let mut consumer = provider.connect();

        // Try to set with wrong origin (should be zero hash)
        let upgrade = RevisionUpgradeRecord::new(rev1, rev2);
        let result = consumer.set(subject.clone(), upgrade).await;

        assert!(matches!(result, Err(RevisionBackendError::NotFound { .. })));
    }
}
