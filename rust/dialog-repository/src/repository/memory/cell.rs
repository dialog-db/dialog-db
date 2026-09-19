use crate::{Publish, PublishError, Resolve, ResolveError, RetainPublish, RetainResolve};
use dialog_capability::{Capability, Did, Policy};
use dialog_common::ConditionalSync;
use dialog_common::time::{self, Duration, SystemTime};
use dialog_effects::memory::prelude::CellExt;
use dialog_effects::memory::prelude::CellScope;
use dialog_effects::memory::{self, Edition, Version};
use dialog_storage::{CborEncoder, DialogStorageError, Encoder};
use parking_lot::RwLock;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use std::sync::Arc;

/// An edition and when this replica last observed it.
///
/// The stamp is this replica's own reading of its clock at the moment
/// the value was confirmed — a resolve that answered, or a publish that
/// landed — not anything a writer minted. It says how old our knowledge
/// is, which is what a caller weighing "refresh, or act on what I hold"
/// needs to know.
///
/// It is an observation, never an input to correctness: a version is
/// what decides whether a write may land, and a clock that jumps must
/// not be able to change that. The cost of a stamp read wrongly is one
/// refresh paid or skipped.
#[derive(Debug, Clone)]
pub struct Observed<T> {
    /// The edition observed.
    pub edition: Edition<T>,
    /// When this replica confirmed it.
    pub at: SystemTime,
}

impl<T> Observed<T> {
    /// Stamp `edition` as observed now.
    pub fn now(edition: Edition<T>) -> Self {
        Self {
            edition,
            at: time::now(),
        }
    }

    /// How long ago this was observed.
    pub fn age(&self) -> Age {
        match time::now().duration_since(self.at) {
            Ok(elapsed) => Age::Since(elapsed),
            Err(_) => Age::Unknown,
        }
    }
}

/// How old a cached value's observation is.
///
/// Three answers, not two: a caller that treats "never observed" and "a
/// clock that moved" alike still gets the safe reading from
/// [`is_fresher_than`](Age::is_fresher_than), while one that meters or
/// logs can tell them apart. Collapsing them into an absent duration
/// hid a real distinction behind a value that is easy to misread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Age {
    /// Nothing is cached, so nothing has been observed.
    Unobserved,
    /// Observed this long ago.
    Since(Duration),
    /// Observed, but the clock has moved backwards since, so how long
    /// ago cannot be said.
    Unknown,
}

impl Age {
    /// Whether the observation is newer than `limit`.
    ///
    /// False for anything that is not a duration we can vouch for, so a
    /// caller asking "may I act on what I hold?" is told no when the
    /// answer is unknown.
    pub fn is_fresher_than(&self, limit: Duration) -> bool {
        matches!(self, Age::Since(elapsed) if *elapsed < limit)
    }

    /// The elapsed time, when it is known.
    pub fn elapsed(&self) -> Option<Duration> {
        match self {
            Age::Since(elapsed) => Some(*elapsed),
            Age::Unobserved | Age::Unknown => None,
        }
    }
}

/// Cached [`Observed`] edition behind a shared lock.
pub type SharedState<T> = Arc<RwLock<Option<Observed<T>>>>;

/// Typed cache over shared state. Handles encode/decode and cache updates.
#[derive(Debug)]
pub struct Cache<T, Codec: Clone = CborEncoder> {
    /// The encoder used to serialize values written to the cell.
    pub codec: Codec,
    /// Shared state holding the last-known edition for this cell.
    pub state: SharedState<T>,
}

impl<T, Codec: Clone> Cache<T, Codec> {
    /// How long ago this cell's value was confirmed.
    ///
    /// Reads only the stamp, so it asks nothing of `T`: a caller
    /// weighing a refresh should not have to be able to clone the value
    /// to ask how old it is.
    pub fn age(&self) -> Age {
        self.state
            .read()
            .as_ref()
            .map_or(Age::Unobserved, |o| o.age())
    }
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
        self.state
            .read()
            .as_ref()
            .map(|o| o.edition.content.clone())
    }

    /// Read the full cached edition.
    pub fn edition(&self) -> Option<Edition<T>> {
        self.state.read().as_ref().map(|o| o.edition.clone())
    }

    /// Read just the cached version.
    pub fn version(&self) -> Option<Version> {
        self.state
            .read()
            .as_ref()
            .map(|o| o.edition.version.clone())
    }

    /// Update the cache with a new edition, observed now.
    pub fn update(&self, edition: Edition<T>) {
        *self.state.write() = Some(Observed::now(edition));
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
    pub async fn decode(&self, bytes: &[u8]) -> Result<T, ResolveError> {
        self.codec.decode(bytes).await.map_err(|error| {
            let error: DialogStorageError = error.into();
            ResolveError::Decode(error.to_string())
        })
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
    ) -> Result<(), ResolveError> {
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
    pub async fn encode(&self, value: &T) -> Result<Vec<u8>, PublishError> {
        let (_hash, content) = self.codec.encode(value).await.map_err(|error| {
            let error: DialogStorageError = error.into();
            PublishError::Encode(error.to_string())
        })?;
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
    capability: CellScope,
    cache: Cache<T, Codec>,
}

impl<T> Cell<T> {
    /// Returns the name of this cell.
    pub fn name(&self) -> &str {
        self.capability.cell_name()
    }

    /// How long ago this replica confirmed this cell's value.
    ///
    /// For a cell read over the network, this is how stale the local
    /// answer is, which is what a caller weighing another round trip
    /// against acting on what it holds needs to know.
    pub fn age(&self) -> Age {
        self.cache.age()
    }
}

impl<T> From<CellScope> for Cell<T> {
    fn from(capability: CellScope) -> Self {
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

    /// Capture this cell's current version as a [`Checkpoint`] to publish
    /// against later.
    ///
    /// The read-modify-write guard. A caller that reads the cell, computes a
    /// new value over some `await`s, then writes it back, should `checkpoint()`
    /// up front and publish through the checkpoint. The checkpoint remembers
    /// the version current *now*; [`Checkpoint::publish`] CAS's against it, so
    /// if a concurrent write advanced the cell in the meantime the publish
    /// fails with
    /// [`VersionMismatch`](dialog_effects::memory::MemoryError::VersionMismatch)
    /// rather than reading the now-advanced version live and silently
    /// clobbering that write.
    ///
    /// Unlike a detached copy, a checkpoint shares this cell's cache: a
    /// successful publish through it advances this cell (and every clone), so
    /// there is nothing to copy back. See `Branch::pull`.
    pub fn checkpoint(&self) -> Checkpoint<T, Codec> {
        Checkpoint {
            cell: self.clone(),
            expected: self.cache.version(),
        }
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
    /// The CAS precondition is the cache's *current* version at perform time:
    /// "overwrite whatever the cell holds now". For a write computed from a
    /// value read earlier, [`checkpoint`](Self::checkpoint) the cell first and
    /// publish through the checkpoint, so a concurrent write makes the CAS fail
    /// rather than silently win.
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

/// A cell paired with the version it held when the checkpoint was taken.
///
/// Created by [`Cell::checkpoint`]. Publishing through it CAS's against the
/// checkpointed version rather than the cell's live version, so a write
/// computed from a value read at checkpoint time fails (rather than silently
/// clobbers) if a concurrent write advanced the cell in the meantime.
pub struct Checkpoint<T, Codec: Clone = CborEncoder> {
    cell: Cell<T, Codec>,
    expected: Option<Version>,
}

impl<T, Codec> Checkpoint<T, Codec>
where
    T: Serialize + Clone + ConditionalSync + Debug,
    Codec: Encoder<Bytes = Vec<u8>> + Clone,
{
    /// Publish `content`, CAS'd against the checkpointed version.
    ///
    /// Fails with
    /// [`VersionMismatch`](dialog_effects::memory::MemoryError::VersionMismatch)
    /// if the cell advanced past the checkpoint. On success the cell's shared
    /// cache is updated, so the originating [`Cell`] and its clones observe the
    /// new edition — there is nothing to copy back.
    pub async fn publish<Env>(self, content: T, env: &Env) -> Result<(), PublishError>
    where
        Env: dialog_capability::Provider<memory::Publish>,
    {
        let bytes = self.cell.cache.encode(&content).await?;
        let version = self
            .cell
            .capability
            .publish(bytes, self.expected)
            .perform(env)
            .await?;
        self.cell.cache.update(Edition { content, version });
        Ok(())
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
    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_effects::memory::prelude::*;
    use dialog_storage::provider::Volatile;
    use dialog_varsig::did;

    fn test_cell<T>(name: &str) -> Cell<T> {
        Subject::from(did!("key:zCellTests"))
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
    async fn it_resolves_empty_cell() -> Result<()> {
        let provider = Volatile::new();
        let cell: Cell<TestValue> = test_cell("missing");

        cell.resolve().perform(&provider).await?;
        assert!(cell.content().is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_then_resolves() -> Result<()> {
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
    async fn it_updates_with_automatic_edition() -> Result<()> {
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
    async fn it_caches_on_resolve() -> Result<()> {
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
    async fn clones_share_published_state() -> Result<()> {
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
    async fn publish_on_clone_visible_from_original() -> Result<()> {
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
    async fn equipped_publishes_and_reads() -> Result<()> {
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
    async fn equipped_updates_on_resolve() -> Result<()> {
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
    async fn equipped_retains_value_when_remote_empty() -> Result<()> {
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
    fn equipped_returns_none_before_any_value() -> Result<()> {
        let equipped = test_cell::<TestValue>("equipped-empty").retain(TestValue::default());
        assert_eq!(*equipped.get(), TestValue::default());
        Ok(())
    }

    #[dialog_common::test]
    async fn publish_preserves_edition_for_subsequent_publish() -> Result<()> {
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
    async fn edition_mismatch_fails_publish() -> Result<()> {
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

    /// A cached value carries how long ago this replica confirmed it,
    /// so a caller can weigh acting on what it holds against paying for
    /// a refresh. Nothing is cached until something is observed.
    #[dialog_common::test]
    fn it_reports_how_long_ago_a_value_was_observed() -> Result<()> {
        let cell: Cell<String> = test_cell("observed");
        assert_eq!(
            cell.cache.age(),
            Age::Unobserved,
            "a cell that has observed nothing says so"
        );
        assert!(
            !cell.cache.age().is_fresher_than(Duration::from_secs(60)),
            "an unobserved value is never fresh enough to act on"
        );

        // A value is observed at the moment it is cached, and the stamp
        // is compared against the cell's own clock, so the observation
        // is stale by construction once that clock has moved past it.
        let observed = Observed::now(Edition {
            content: "first".to_string(),
            version: Version::from("v1"),
        });
        let elapsed = Duration::from_millis(50);
        *cell.cache.state.write() = Some(Observed {
            at: observed.at - elapsed,
            ..observed
        });
        let first = cell
            .cache
            .age()
            .elapsed()
            .expect("an observed value has an elapsed age");
        assert!(
            first >= elapsed,
            "an observation made {elapsed:?} ago is at least that old, got {first:?}"
        );
        assert!(
            !cell.cache.age().is_fresher_than(Duration::from_millis(10)),
            "and is not fresher than a limit it has already outlived"
        );

        // Re-observing resets the clock: the age is of the observation,
        // not of the value, so a re-confirmed value reads as fresh even
        // when its content and version never changed.
        cell.cache.update(Edition {
            content: "first".to_string(),
            version: Version::from("v1"),
        });
        let second = cell
            .cache
            .age()
            .elapsed()
            .expect("an observed value has an elapsed age");
        assert!(
            second < first,
            "re-observing the same value makes it fresh again, {second:?} vs {first:?}"
        );
        assert!(
            cell.cache.age().is_fresher_than(Duration::from_secs(60)),
            "a value observed just now is fresh"
        );

        // Clearing forgets the observation along with the value.
        cell.cache.clear();
        assert_eq!(
            cell.cache.age(),
            Age::Unobserved,
            "a cleared cell forgets the observation with the value"
        );

        Ok(())
    }
}
