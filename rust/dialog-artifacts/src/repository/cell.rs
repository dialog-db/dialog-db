use std::sync::Arc;

use parking_lot::RwLock;

use dialog_capability::{Capability, Policy, Provider};
use dialog_effects::memory;
use dialog_storage::{CborEncoder, DialogStorageError, Encoder};
use serde::{Serialize, de::DeserializeOwned};

use super::RepositoryError;

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
    pub fn subject(&self) -> &dialog_capability::Did {
        self.capability.subject()
    }

    /// Wrap this cell with a default value, so [`CellOr::get`] always
    /// returns `T` — falling back to `default` when nothing has been
    /// resolved or published.
    pub fn or(self, default: T) -> CellOr<T, Codec> {
        CellOr {
            cell: self,
            default,
        }
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
            .await
            .map_err(|e| RepositoryError::StorageError(format!("Memory resolve failed: {}", e)))?;

        let new_state = match publication {
            None => None,
            Some(pub_data) => {
                let value: T = self.codec.decode(&pub_data.content).await.map_err(|e| {
                    RepositoryError::StorageError(format!(
                        "Failed to decode cell value: {}",
                        Into::<DialogStorageError>::into(e)
                    ))
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
            .await
            .map_err(|e| RepositoryError::StorageError(format!("Memory publish failed: {}", e)))?;

        let mut guard = self.state.write();
        *guard = Some((value, new_edition));

        Ok(())
    }
}

/// A [`Cell`] paired with a default value for infallible access.
///
/// Created by [`Cell::or`]. [`get`](CellOr::get) always returns `T`,
/// falling back to the default when the cell has not been resolved or
/// the remote value is empty.
#[derive(Debug)]
pub struct CellOr<T, Codec = CborEncoder> {
    cell: Cell<T, Codec>,
    default: T,
}

impl<T, Codec: Clone> Clone for CellOr<T, Codec>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            cell: self.cell.clone(),
            default: self.default.clone(),
        }
    }
}

impl<T, Codec> CellOr<T, Codec>
where
    T: Clone,
{
    /// Read the current value, falling back to the default.
    pub fn get(&self) -> T {
        self.cell.get().unwrap_or_else(|| self.default.clone())
    }

    /// Read the current value via callback, falling back to the default.
    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        self.cell.read_with(|opt| f(opt.unwrap_or(&self.default)))
    }
}

impl<T, Codec> CellOr<T, Codec> {
    /// Access the underlying [`Cell`].
    pub fn inner(&self) -> &Cell<T, Codec> {
        &self.cell
    }

    /// Returns the subject DID from the capability chain.
    pub fn subject(&self) -> &dialog_capability::Did {
        self.cell.subject()
    }
}

impl<T, Codec> CellOr<T, Codec>
where
    T: DeserializeOwned + Clone + dialog_common::ConditionalSync,
    Codec: Encoder,
{
    /// Fetch the cell value from env, updating the shared cache.
    /// Use [`get`](CellOr::get) to read the cached value without hitting env.
    pub async fn resolve<Env>(&self, env: &Env) -> Result<T, RepositoryError>
    where
        Env: Provider<memory::Resolve>,
    {
        self.cell.resolve(env).await?;
        Ok(self.get())
    }
}

impl<T, Codec> CellOr<T, Codec>
where
    T: Serialize + DeserializeOwned + Clone + dialog_common::ConditionalSync,
    Codec: Encoder,
{
    /// Resolve the cell, publishing the default value if the cell is empty
    /// in env. After this call the cell is guaranteed to be synced.
    pub async fn get_or_init<Env>(&self, env: &Env) -> Result<T, RepositoryError>
    where
        Env: Provider<memory::Resolve> + Provider<memory::Publish>,
    {
        self.cell.resolve(env).await?;
        if self.cell.read_with(|opt| opt.is_none()) {
            self.cell.publish(self.default.clone(), env).await?;
        }
        Ok(self.get())
    }
}

impl<T, Codec> CellOr<T, Codec>
where
    T: Serialize,
    Codec: Encoder,
{
    /// Publish a new value to this cell.
    pub async fn publish<Env>(&self, value: T, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<memory::Publish>,
    {
        self.cell.publish(value, env).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::memory::Memory;
    use dialog_capability::{Did, Subject};
    use dialog_storage::provider::Volatile;

    fn test_memory() -> Memory {
        let did: Did = "did:test:cell-tests".parse().unwrap();
        Memory::new(Subject::from(did))
    }

    fn test_cell<T>(name: &str) -> Cell<T> {
        test_memory().space("test").cell(name)
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
    fn or_returns_default_before_resolve() -> anyhow::Result<()> {
        let cell = test_cell::<TestValue>("or-default").or(TestValue::default());

        assert_eq!(cell.get().count, 0);
        assert_eq!(cell.get().name, "default");

        Ok(())
    }

    #[dialog_common::test]
    async fn or_resolves_to_persisted_value() -> anyhow::Result<()> {
        let provider = Volatile::new();

        let value = TestValue {
            count: 42,
            name: "hello".into(),
        };

        let writer: Cell<TestValue> = test_cell("or-read");
        writer.publish(value.clone(), &provider).await?;

        let cell = test_cell::<TestValue>("or-read").or(TestValue::default());
        cell.resolve(&provider).await?;

        assert_eq!(cell.get(), value);

        Ok(())
    }

    #[dialog_common::test]
    async fn or_get_or_init_publishes_default_when_empty() -> anyhow::Result<()> {
        let provider = Volatile::new();

        let default = TestValue {
            count: 99,
            name: "initial".into(),
        };
        let cell = test_cell::<TestValue>("or-init").or(default.clone());

        cell.get_or_init(&provider).await?;

        assert_eq!(cell.get(), default);

        // Verify persisted by reading from a separate cell
        let reader: Cell<TestValue> = test_cell("or-init");
        reader.resolve(&provider).await?;
        assert_eq!(reader.get(), Some(default));

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
}
