use std::sync::Arc;

use parking_lot::RwLock;

use dialog_capability::{Capability, Did, Policy};
use dialog_common::ConditionalSync;
use dialog_effects::memory;
use dialog_effects::memory::prelude::CellExt;
use dialog_effects::memory::{Edition, Version};
use dialog_storage::{CborEncoder, Encoder};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

use super::publish::{Publish, RetainPublish};
use super::resolve::{Resolve, RetainResolve};
use crate::RepositoryError;

/// Cached [`Edition`] behind a shared lock.
pub type SharedState<T> = Arc<RwLock<Option<Edition<T>>>>;

/// Typed cache over shared state. Handles encode/decode and cache updates.
#[derive(Debug)]
pub struct Cache<T, Codec: Clone = CborEncoder> {
    /// The encoder used to serialize values written to the cell.
    pub codec: Codec,
    /// Shared state holding the last-known edition for this cell.
    pub state: SharedState<T>,
}

impl<T, Codec: Clone> Clone for Cache<T, Codec> {
    fn clone(&self) -> Self {
        Self {
            codec: self.codec.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl<T: Clone, Codec: Clone> Cache<T, Codec> {
    /// Read the cached content.
    pub fn content(&self) -> Option<T> {
        self.state.read().as_ref().map(|e| e.content.clone())
    }

    /// Read the full cached edition.
    pub fn edition(&self) -> Option<Edition<T>> {
        self.state.read().clone()
    }

    /// Read just the cached version.
    pub fn version(&self) -> Option<Version> {
        self.state.read().as_ref().map(|e| e.version.clone())
    }

    /// Update the cache with a new edition.
    pub fn update(&self, edition: Edition<T>) {
        *self.state.write() = Some(edition);
    }

    /// Clear the cache.
    pub fn clear(&self) {
        *self.state.write() = None;
    }
}

impl<T, Codec> Cache<T, Codec>
where
    T: DeserializeOwned + ConditionalSync,
    Codec: Encoder + Clone,
{
    /// Decode bytes into a typed value.
    pub async fn decode(&self, bytes: &[u8]) -> Result<T, RepositoryError> {
        Ok(self.codec.decode(bytes).await.map_err(Into::into)?)
    }
}

impl<T, Codec> Cache<T, Codec>
where
    T: DeserializeOwned + Clone + ConditionalSync,
    Codec: Encoder + Clone,
{
    /// Apply a raw edition to the cache, decoding the content in place.
    /// Clears the cache if the edition is empty.
    pub async fn apply(
        &self,
        edition: Option<memory::Edition<Vec<u8>>>,
    ) -> Result<(), RepositoryError> {
        match edition {
            None => self.clear(),
            Some(raw) => {
                self.update(memory::Edition {
                    content: self.decode(&raw.content).await?,
                    version: raw.version,
                });
            }
        }
        Ok(())
    }
}

impl<T, Codec> Cache<T, Codec>
where
    T: Serialize + ConditionalSync + Debug,
    Codec: Encoder<Bytes = Vec<u8>> + Clone,
{
    /// Encode a value into bytes.
    pub async fn encode(&self, value: &T) -> Result<Vec<u8>, RepositoryError> {
        let (_hash, content) = self.codec.encode(value).await.map_err(Into::into)?;
        Ok(content)
    }
}

/// A transactional memory cell that stores a typed value with edition tracking.
///
/// `Cell<T>` wraps a capability chain (`Subject -> Memory -> Space -> Cell`) and
/// manages its own cached value + edition internally. This eliminates the need
/// for callers to thread editions through publish/resolve calls.
///
/// The cached state is stored behind `Arc<RwLock<>>`, so clones share state
/// and writes propagate to all references.
///
/// - [`get`](Cell::get) reads the cache synchronously, returning a cloned `T`
/// - [`resolve`](Cell::resolve) returns a [`Resolve`] command to fetch from env
/// - [`publish`](Cell::publish) returns a [`Publish`] command to write a value
#[derive(Debug, Clone)]
pub struct Cell<T, Codec: Clone = CborEncoder> {
    capability: Capability<memory::Cell>,
    cache: Cache<T, Codec>,
}

impl<T> Cell<T> {
    /// Returns the name of this cell.
    pub fn name(&self) -> &str {
        &memory::Cell::of(&self.capability).cell
    }
}

impl<T> From<Capability<memory::Cell>> for Cell<T> {
    fn from(capability: Capability<memory::Cell>) -> Self {
        Self {
            capability,
            cache: Cache {
                codec: CborEncoder,
                state: SharedState::default(),
            },
        }
    }
}

impl<T, Codec: Clone> Cell<T, Codec>
where
    T: Clone,
{
    /// Read the cached value without hitting env.
    /// Returns `None` if the cell has not been resolved or published yet.
    pub fn content(&self) -> Option<T> {
        self.cache.content()
    }

    /// Read the cached edition (content + version) without hitting env.
    /// Returns `None` if the cell has not been resolved or published yet.
    pub fn edition(&self) -> Option<Edition<T>> {
        self.cache.edition()
    }

    /// Reset the in-memory cache to a known edition, without hitting the
    /// backend. Used to restore cache state across sessions.
    pub fn reset(&self, edition: Edition<T>) {
        self.cache.update(edition);
    }

    /// Returns the subject DID from the capability chain.
    pub fn subject(&self) -> &Did {
        self.capability.subject()
    }
}

impl<T, Codec> Cell<T, Codec>
where
    T: DeserializeOwned + ConditionalSync,
    Codec: Encoder + Clone,
{
    /// Create a command to fetch the cell value from env.
    ///
    /// Call `.perform(&env)` for local, or `.fork(&address).perform(&env)`
    /// for remote.
    pub fn resolve(&self) -> Resolve<T, Codec> {
        Resolve {
            effect: self.capability.clone().resolve(),
            cache: self.cache.clone(),
        }
    }
}

impl<T, Codec> Cell<T, Codec>
where
    T: Serialize + Clone,
    Codec: Clone,
{
    /// Create a command to publish a new value to this cell.
    ///
    /// Call `.perform(&env)` for local, or `.fork(&address).perform(&env)`
    /// for remote.
    pub fn publish(&self, content: T) -> Publish<T, Codec> {
        Publish {
            capability: self.capability.clone(),
            cache: self.cache.clone(),
            content,
        }
    }
}

/// A cell that always has a value.
///
/// Constructed with an initial value via [`Cell::retain`]. On resolve,
/// updates to the latest remote value, but if the remote is empty (deleted),
/// the last known value is retained. [`get()`](Retain::get) always returns `T`.
#[derive(Debug, Clone)]
pub struct Retain<T, Codec: Clone = CborEncoder> {
    cell: Cell<T, Codec>,
    value: Arc<RwLock<T>>,
}

impl<T: Clone> Retain<T> {
    /// Read the current value, syncing from the inner cell first.
    ///
    /// If the cell has a newer value, the sticky cache is updated.
    /// Returns a read guard that derefs to `&T`.
    pub fn get(&self) -> parking_lot::RwLockReadGuard<'_, T> {
        if let Some(value) = self.cell.content() {
            *self.value.write() = value;
        }
        self.value.read()
    }

    /// Returns the name of the underlying cell.
    pub fn name(&self) -> &str {
        self.cell.name()
    }

    /// Returns the subject DID from the capability chain.
    pub fn subject(&self) -> &Did {
        self.cell.subject()
    }
}

impl<T, Codec> Retain<T, Codec>
where
    T: DeserializeOwned + Clone + ConditionalSync,
    Codec: Encoder + Clone,
{
    /// Create a command to resolve from the environment.
    ///
    /// If the remote has a value, the local cache is updated.
    /// If the remote is empty (deleted), the current value is retained.
    pub fn resolve(&self) -> RetainResolve<'_, T, Codec> {
        RetainResolve {
            inner: self.cell.resolve(),
            value: &self.value,
        }
    }
}

impl<T, Codec> Retain<T, Codec>
where
    T: Serialize + Clone,
    Codec: Clone,
{
    /// Create a command to publish a new value.
    pub fn publish(&self, value: T) -> RetainPublish<'_, T, Codec> {
        RetainPublish {
            inner: self.cell.publish(value.clone()),
            sticky: &self.value,
            value,
        }
    }
}

impl<T> Cell<T> {
    /// Equip this cell with an initial value, creating a [`Retain`]
    /// that always has a value and never drops back to empty.
    pub fn retain(self, initial: T) -> Retain<T> {
        Retain {
            cell: self,
            value: Arc::new(RwLock::new(initial)),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_capability::Subject;
    use dialog_effects::memory::prelude::*;
    use dialog_storage::provider::Volatile;
    use dialog_varsig::did;

    fn test_subject() -> Subject {
        Subject::from(did!("key:zCellTests"))
    }

    fn test_cell<T>(name: &str) -> Cell<T> {
        test_subject()
            .memory()
            .space("branch/test")
            .cell(name)
            .into()
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestValue {
        count: u32,
        name: String,
    }

    impl Default for TestValue {
        fn default() -> Self {
            Self {
                count: 0,
                name: "default".into(),
            }
        }
    }

    #[dialog_common::test]
    async fn it_resolves_empty_cell() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let cell: Cell<TestValue> = test_cell("missing");

        cell.resolve().perform(&provider).await?;
        assert!(cell.content().is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_then_resolves() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let cell: Cell<TestValue> = test_cell("test");

        let value = TestValue {
            count: 42,
            name: "hello".into(),
        };

        cell.publish(value.clone()).perform(&provider).await?;
        assert_eq!(cell.content(), Some(value.clone()));

        cell.resolve().perform(&provider).await?;
        assert_eq!(cell.content(), Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_with_automatic_edition() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let cell: Cell<TestValue> = test_cell("update");

        let v1 = TestValue {
            count: 1,
            name: "first".into(),
        };
        cell.publish(v1).perform(&provider).await?;

        let v2 = TestValue {
            count: 2,
            name: "second".into(),
        };
        cell.publish(v2.clone()).perform(&provider).await?;

        cell.resolve().perform(&provider).await?;
        assert_eq!(cell.content(), Some(v2));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_caches_on_resolve() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let cell: Cell<TestValue> = test_cell("cache");

        let value = TestValue {
            count: 7,
            name: "cached".into(),
        };

        let writer: Cell<TestValue> = test_cell("cache");
        writer.publish(value.clone()).perform(&provider).await?;

        assert!(cell.content().is_none());
        cell.resolve().perform(&provider).await?;
        assert_eq!(cell.content(), Some(value.clone()));

        cell.resolve().perform(&provider).await?;
        assert_eq!(cell.content(), Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn clones_share_published_state() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let cell: Cell<TestValue> = test_cell("shared");
        let clone = cell.clone();

        let value = TestValue {
            count: 42,
            name: "shared".into(),
        };

        cell.publish(value.clone()).perform(&provider).await?;
        assert_eq!(clone.content(), Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn publish_on_clone_visible_from_original() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let original: Cell<TestValue> = test_cell("shared-reverse");
        let clone = original.clone();

        let value = TestValue {
            count: 99,
            name: "from clone".into(),
        };

        clone.publish(value.clone()).perform(&provider).await?;
        assert_eq!(original.content(), Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn equipped_publishes_and_reads() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let equipped = test_cell::<TestValue>("equipped-pub").retain(TestValue::default());

        assert_eq!(
            *equipped.get(),
            TestValue::default(),
            "empty before publish"
        );

        let value = TestValue {
            count: 42,
            name: "equipped".into(),
        };
        equipped.publish(value.clone()).perform(&provider).await?;
        assert_eq!(*equipped.get(), value.clone());

        equipped.resolve().perform(&provider).await?;
        assert_eq!(*equipped.get(), value);

        Ok(())
    }

    #[dialog_common::test]
    async fn equipped_updates_on_resolve() -> anyhow::Result<()> {
        let provider = Volatile::new();

        let cell: Cell<TestValue> = test_cell("equipped-update");
        let v1 = TestValue {
            count: 1,
            name: "first".into(),
        };
        cell.publish(v1.clone()).perform(&provider).await?;

        let equipped = test_cell::<TestValue>("equipped-update").retain(TestValue::default());
        equipped.resolve().perform(&provider).await?;
        assert_eq!(*equipped.get(), v1);

        let v2 = TestValue {
            count: 2,
            name: "second".into(),
        };
        cell.publish(v2.clone()).perform(&provider).await?;

        equipped.resolve().perform(&provider).await?;
        assert_eq!(*equipped.get(), v2);

        Ok(())
    }

    #[dialog_common::test]
    async fn equipped_retains_value_when_remote_empty() -> anyhow::Result<()> {
        let provider = Volatile::new();

        let cell: Cell<TestValue> = test_cell("equipped-retain");
        let value = TestValue {
            count: 42,
            name: "retained".into(),
        };
        cell.publish(value.clone()).perform(&provider).await?;

        let equipped = test_cell::<TestValue>("equipped-retain").retain(TestValue::default());
        equipped.resolve().perform(&provider).await?;
        assert_eq!(*equipped.get(), value.clone());

        let empty_equipped = test_cell::<TestValue>("nonexistent").retain(TestValue::default());
        empty_equipped.resolve().perform(&provider).await?;
        assert_eq!(
            *empty_equipped.get(),
            TestValue::default(),
            "equipped on nonexistent cell retains default"
        );

        assert_eq!(
            *equipped.get(),
            value,
            "equipped retains value independently"
        );

        Ok(())
    }

    #[dialog_common::test]
    fn equipped_returns_none_before_any_value() -> anyhow::Result<()> {
        let equipped = test_cell::<TestValue>("equipped-empty").retain(TestValue::default());
        assert_eq!(*equipped.get(), TestValue::default());
        Ok(())
    }

    #[dialog_common::test]
    async fn publish_preserves_edition_for_subsequent_publish() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let cell: Cell<TestValue> = test_cell("edition");

        let v1 = TestValue {
            count: 1,
            name: "first".into(),
        };
        cell.publish(v1).perform(&provider).await?;

        // Second publish should use the edition from the first
        let v2 = TestValue {
            count: 2,
            name: "second".into(),
        };
        cell.publish(v2.clone()).perform(&provider).await?;

        // Resolve from a separate cell to verify the value was written
        let reader: Cell<TestValue> = test_cell("edition");
        reader.resolve().perform(&provider).await?;
        assert_eq!(
            reader.content(),
            Some(v2),
            "second publish should succeed with correct edition"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn edition_mismatch_fails_publish() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let cell_a: Cell<TestValue> = test_cell("conflict");
        let cell_b: Cell<TestValue> = test_cell("conflict");

        // Both resolve to get the same (empty) edition
        cell_a.resolve().perform(&provider).await?;
        cell_b.resolve().perform(&provider).await?;

        // A publishes successfully
        let v1 = TestValue {
            count: 1,
            name: "from A".into(),
        };
        cell_a.publish(v1).perform(&provider).await?;

        // B tries to publish with the stale edition -- should fail
        let v2 = TestValue {
            count: 2,
            name: "from B".into(),
        };
        let result = cell_b.publish(v2).perform(&provider).await;
        assert!(result.is_err(), "publish with stale edition should fail");

        Ok(())
    }
}
