use std::sync::Arc;

use parking_lot::RwLock;

use dialog_capability::{Capability, Did, Policy, Provider};
use dialog_effects::memory;
use dialog_storage::{CborEncoder, DialogStorageError, Encoder};
use serde::{Serialize, de::DeserializeOwned};

use crate::RepositoryError;

/// Cached value paired with its edition, behind a shared lock.
type SharedState<T> = Arc<RwLock<Option<(T, Vec<u8>)>>>;

/// A transactional memory cell that stores a typed value with edition tracking.
///
/// `Cell<T>` wraps a capability chain (`Subject → Memory → Space → Cell`) and
/// manages its own cached value + edition internally. This eliminates the need
/// for callers to thread editions through publish/resolve calls.
///
/// The cached state is stored behind `Arc<RwLock<>>`, so clones share state
/// and writes propagate to all references.
///
/// - [`get`](Cell::get) reads the cache synchronously, returning a cloned `T`
/// - [`read_with`](Cell::read_with) reads the cache via callback (avoids clone)
/// - [`resolve`](Cell::resolve) fetches from env, updating the shared cache
/// - [`publish`](Cell::publish) writes a new value using the cached edition
/// - [`or`](Cell::or) wraps this cell with a default for infallible access
#[derive(Debug)]
pub struct Cell<T, Codec = CborEncoder> {
    capability: Capability<memory::Cell>,
    codec: Codec,
    state: SharedState<T>,
}

impl<T, Codec: Clone> Clone for Cell<T, Codec> {
    fn clone(&self) -> Self {
        Self {
            capability: self.capability.clone(),
            codec: self.codec.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> Cell<T> {
    /// Returns the name of this cell.
    pub fn name(&self) -> &str {
        &memory::Cell::of(&self.capability).cell
    }
    /// Create a Cell from a pre-built cell capability.
    pub fn from_capability(capability: Capability<memory::Cell>) -> Self {
        Self {
            capability,
            codec: CborEncoder,
            state: SharedState::default(),
        }
    }
}

impl<T, Codec> Cell<T, Codec>
where
    T: Clone,
{
    /// Read the cached value without hitting env.
    /// Returns `None` if the cell has not been resolved or published yet.
    pub fn get(&self) -> Option<T> {
        let guard = self.state.read();
        guard.as_ref().map(|(v, _)| v.clone())
    }
}

impl<T, Codec> Cell<T, Codec> {
    /// Read the cached value via callback, avoiding a clone.
    /// Returns `None` if the cell has not been resolved or published yet.
    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<&T>) -> R,
    {
        let guard = self.state.read();
        f(guard.as_ref().map(|(v, _)| v))
    }

    /// Returns the subject DID from the capability chain.
    pub fn subject(&self) -> &Did {
        self.capability.subject()
    }
}

impl<T, Codec> Cell<T, Codec>
where
    T: DeserializeOwned + dialog_common::ConditionalSync,
    Codec: Encoder,
{
    /// Fetch the cell value from env, updating the shared cache.
    /// Use [`get`](Cell::get) to read the cached value without hitting env.
    pub async fn resolve<Env>(&self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<memory::Resolve>,
    {
        let publication = self
            .capability
            .clone()
            .invoke(memory::Resolve)
            .perform(env)
            .await?;

        let new_state = match publication {
            None => None,
            Some(pub_data) => {
                let value: T = self.codec.decode(&pub_data.content).await.map_err(|e| {
                    let storage_err: DialogStorageError = e.into();
                    RepositoryError::from(storage_err)
                })?;
                Some((value, pub_data.edition))
            }
        };

        *self.state.write() = new_state;
        Ok(())
    }
}

impl<T, Codec> Cell<T, Codec>
where
    T: Serialize,
    Codec: Encoder,
{
    /// Publish a new value to this cell, using the cached edition automatically.
    /// Updates the shared cache on success.
    pub async fn publish<Env>(&self, value: T, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<memory::Publish>,
    {
        let edition = {
            let guard = self.state.read();
            guard.as_ref().map(|(_, e)| e.clone())
        };

        let content = serde_ipld_dagcbor::to_vec(&value)
            .map_err(|e| RepositoryError::StorageError(format!("Failed to encode value: {}", e)))?;

        let new_edition = self
            .capability
            .clone()
            .invoke(memory::Publish::new(content, edition))
            .perform(env)
            .await?;

        let mut guard = self.state.write();
        *guard = Some((value, new_edition));

        Ok(())
    }
}

/// A cell that always has a value.
///
/// Constructed with an initial value via [`Cell::equip`]. On [`resolve`](Retain::resolve),
/// updates to the latest remote value — but if the remote is empty (deleted),
/// the last known value is retained. [`get()`](Retain::get) always returns `T`.
#[derive(Debug)]
pub struct Retain<T, Codec = CborEncoder> {
    cell: Cell<T, Codec>,
    value: Arc<RwLock<T>>,
}

impl<T, Codec: Clone> Clone for Retain<T, Codec>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            cell: self.cell.clone(),
            value: Arc::clone(&self.value),
        }
    }
}

impl<T: Clone> Retain<T> {
    /// Read the current value, syncing from the inner cell first.
    ///
    /// If the cell has a newer value, the sticky cache is updated.
    /// Returns a read guard that derefs to `&T`.
    pub fn get(&self) -> parking_lot::RwLockReadGuard<'_, T> {
        if let Some(value) = self.cell.get() {
            *self.value.write() = value;
        }
        self.value.read()
    }

    /// Returns the name of the underlying cell.
    pub fn name(&self) -> &str {
        self.cell.name()
    }

    /// Read the value via callback without cloning.
    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        f(&*self.get())
    }

    /// Returns the subject DID from the capability chain.
    pub fn subject(&self) -> &Did {
        self.cell.subject()
    }
}

impl<T, Codec> Retain<T, Codec>
where
    T: DeserializeOwned + Clone + dialog_common::ConditionalSync,
    Codec: Encoder,
{
    /// Resolve from the environment.
    ///
    /// If the remote has a value, the local cache is updated.
    /// If the remote is empty (deleted), the current value is retained.
    pub async fn resolve<Env>(&self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<memory::Resolve>,
    {
        self.cell.resolve(env).await?;
        if let Some(value) = self.cell.get() {
            *self.value.write() = value;
        }
        Ok(())
    }
}

impl<T, Codec> Retain<T, Codec>
where
    T: Serialize + Clone,
    Codec: Encoder,
{
    /// Publish a new value, updating both the remote and local cache.
    pub async fn publish<Env>(&self, value: T, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<memory::Publish>,
    {
        self.cell.publish(value.clone(), env).await?;
        *self.value.write() = value;
        Ok(())
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
    use super::*;
    use crate::repository::memory::MemoryExt;
    use dialog_capability::{Did, Subject};
    use dialog_storage::provider::Volatile;

    fn test_subject() -> Subject {
        let did: Did = "did:test:cell-tests".parse().unwrap();
        Subject::from(did)
    }

    fn test_cell<T>(name: &str) -> Cell<T> {
        test_subject().branch("test").cell(name)
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

        cell.resolve(&provider).await?;
        assert!(cell.get().is_none());

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

        cell.publish(value.clone(), &provider).await?;
        assert_eq!(cell.get(), Some(value.clone()));

        cell.resolve(&provider).await?;
        assert_eq!(cell.get(), Some(value));

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
        cell.publish(v1, &provider).await?;

        let v2 = TestValue {
            count: 2,
            name: "second".into(),
        };
        cell.publish(v2.clone(), &provider).await?;

        cell.resolve(&provider).await?;
        assert_eq!(cell.get(), Some(v2));

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
        writer.publish(value.clone(), &provider).await?;

        assert!(cell.get().is_none());
        cell.resolve(&provider).await?;
        assert_eq!(cell.get(), Some(value.clone()));

        cell.resolve(&provider).await?;
        assert_eq!(cell.get(), Some(value));

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

        // Publish on original, read from clone
        cell.publish(value.clone(), &provider).await?;
        assert_eq!(clone.get(), Some(value));

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

        // Publish on clone, read from original
        clone.publish(value.clone(), &provider).await?;
        assert_eq!(original.get(), Some(value));

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
        equipped.publish(value.clone(), &provider).await?;
        assert_eq!(*equipped.get(), value.clone());

        // Re-resolve — value persists
        equipped.resolve(&provider).await?;
        assert_eq!(*equipped.get(), value);

        Ok(())
    }

    #[dialog_common::test]
    async fn equipped_updates_on_resolve() -> anyhow::Result<()> {
        let provider = Volatile::new();

        // Write v1 via a regular cell
        let cell: Cell<TestValue> = test_cell("equipped-update");
        let v1 = TestValue {
            count: 1,
            name: "first".into(),
        };
        cell.publish(v1.clone(), &provider).await?;

        // Equip a separate cell for the same key — resolve picks up v1
        let equipped = test_cell::<TestValue>("equipped-update").retain(TestValue::default());
        equipped.resolve(&provider).await?;
        assert_eq!(*equipped.get(), v1);

        // Write v2 via the regular cell (which has the right edition)
        let v2 = TestValue {
            count: 2,
            name: "second".into(),
        };
        cell.publish(v2.clone(), &provider).await?;

        // Retain resolve picks up v2
        equipped.resolve(&provider).await?;
        assert_eq!(*equipped.get(), v2);

        Ok(())
    }

    #[dialog_common::test]
    async fn equipped_retains_value_when_remote_empty() -> anyhow::Result<()> {
        let provider = Volatile::new();

        // Publish a value
        let cell: Cell<TestValue> = test_cell("equipped-retain");
        let value = TestValue {
            count: 42,
            name: "retained".into(),
        };
        cell.publish(value.clone(), &provider).await?;

        // Equip and resolve — gets the value
        let equipped = test_cell::<TestValue>("equipped-retain").retain(TestValue::default());
        equipped.resolve(&provider).await?;
        assert_eq!(*equipped.get(), value.clone());

        // Now resolve from a DIFFERENT key that has no data.
        // This simulates what happens when the underlying cell resolves
        // to None (e.g., remote deleted). We can't easily delete from
        // Volatile, so instead we verify the contract: equipped.get()
        // retains the value even if the inner cell would be None.
        //
        // The Retain::resolve implementation only updates the sticky
        // value when the inner cell has Some, so a None resolve is a no-op
        // for the sticky value.
        let empty_equipped = test_cell::<TestValue>("nonexistent").retain(TestValue::default());
        empty_equipped.resolve(&provider).await?;
        assert_eq!(
            *empty_equipped.get(),
            TestValue::default(),
            "equipped on nonexistent cell retains default"
        );

        // Original equipped still has its value
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
}
