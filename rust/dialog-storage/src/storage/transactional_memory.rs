use super::backend::TransactionalMemoryBackend;
use crate::{DialogStorageError, Encoder};
use dialog_common::{ConditionalSend, ConditionalSync, SharedCell};
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;
use std::sync::Arc;

/// Represents the cached state of a value at a specific address in storage.
///
/// This struct holds both the value and its edition (version identifier),
/// enabling Compare-And-Swap (CAS) operations.
#[derive(Debug, Clone)]
pub struct State<Backend: TransactionalMemoryBackend> {
    /// The cached value, if present
    pub value: Option<Backend::Value>,
    /// The edition/version identifier for CAS operations
    pub edition: Option<Backend::Edition>,
}

impl<Backend: TransactionalMemoryBackend> Default for State<Backend> {
    fn default() -> Self {
        Self {
            value: None,
            edition: None,
        }
    }
}

/// State for typed transactional memory - caches decoded values.
#[derive(Debug, Clone)]
pub struct TypedState<T, Edition> {
    /// The cached decoded value
    pub value: Option<T>,
    /// The edition/version identifier for CAS operations
    pub edition: Option<Edition>,
}

impl<T, Edition> Default for TypedState<T, Edition> {
    fn default() -> Self {
        Self {
            value: None,
            edition: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum UpdatePolicy {
    MaxRetries(usize),
}
impl Default for UpdatePolicy {
    fn default() -> Self {
        UpdatePolicy::MaxRetries(1)
    }
}

/// A cursor-like handle into backend storage with cached value and edition tracking.
///
/// `TransactionalMemory` acts as a positioned pointer to a specific address in a
/// [`TransactionalMemoryBackend`], maintaining a cached copy of the value and its
/// edition (version). This enables efficient Compare-And-Swap (CAS) operations
/// with optimistic concurrency control.
///
/// # Key Features
///
/// - **Cached State**: Holds both value and edition in memory for fast access
/// - **CAS Semantics**: Ensures writes only succeed if the edition hasn't changed
/// - **Lock-Free Reads**: Multiple operations can read concurrently without blocking
/// - **Retry Logic**: Built-in retry mechanism for handling transient conflicts
///
/// # Usage Pattern
///
/// ```ignore
/// use dialog_storage::storage::transactional_memory::TransactionalMemory;
/// use dialog_storage::storage::backend::MemoryStorageBackend;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
/// let address = b"my-key".to_vec();
///
/// // Open a cursor to the address
/// let memory = TransactionalMemory::open(address, &backend).await?;
///
/// // Read the current value
/// let current = memory.read();
///
/// // Replace with a new value (CAS operation)
/// memory.replace(Some(b"new-value".to_vec()), &backend).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Concurrency
///
/// This type is designed for safe concurrent access:
/// - Multiple clones can exist, sharing the same cached state via `Arc<SharedCell<_>>`
/// - Reads acquire a lock briefly and release immediately
/// - Writes use CAS to detect conflicts and fail gracefully
/// - Successful writes update the cache for all clones
///
/// # Edition Tracking
///
/// The edition serves as a version identifier:
/// - Each successful write increments or updates the edition
/// - CAS operations check that the edition matches before writing
/// - Stale editions cause writes to fail, protecting against lost updates
pub struct TransactionalMemory<Backend: TransactionalMemoryBackend> {
    /// The address (key) this cursor points to
    pub address: Backend::Address,
    /// The cached state (value + edition) shared across clones
    pub state: Arc<SharedCell<State<Backend>>>,
}

impl<Backend: TransactionalMemoryBackend> Clone for TransactionalMemory<Backend>
where
    Backend::Address: Clone,
{
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl<Backend: TransactionalMemoryBackend> TransactionalMemory<Backend> {
    /// Creates a new `TransactionalMemory` with the given address and optional initial state.
    ///
    /// This is typically called internally by `open()`. If no state is provided, defaults to
    /// a state with `None` value and edition.
    pub fn new(address: Backend::Address, entry: Option<State<Backend>>) -> Self {
        Self {
            address,
            state: Arc::new(SharedCell::new(entry.unwrap_or_default())),
        }
    }

    /// Read the cached value with a callback.
    /// Lock is held only during the callback execution.
    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Option<Backend::Value>) -> R,
    {
        let entry = self.state.read();
        f(&entry.value)
    }

    /// Read and clone the cached value.
    pub fn read(&self) -> Option<Backend::Value>
    where
        Backend::Value: Clone,
    {
        self.read_with(|value| value.clone())
    }

    /// Opens an asset from storage, loading its current content and edition into cache.
    pub async fn open(address: Backend::Address, from: &Backend) -> Result<Self, Backend::Error> {
        let entry = if let Some((value, edition)) = from.resolve(&address).await? {
            Some(State {
                value: Some(value),
                edition: Some(edition),
            })
        } else {
            None
        };

        Ok(Self::new(address, entry))
    }

    /// Reloads content from storage into cache.
    /// Lock is acquired, updated, and released - no lock held during await.
    pub async fn reload(&self, from: &Backend) -> Result<(), Backend::Error> {
        let entry = if let Some((value, edition)) = from.resolve(&self.address).await? {
            State {
                value: Some(value),
                edition: Some(edition),
            }
        } else {
            State::default()
        };

        // Update cache (lock held briefly, released immediately)
        *self.state.write() = entry;

        Ok(())
    }

    /// Replaces content using CAS semantics.
    ///
    /// This performs three steps with careful lock management:
    /// 1. Read current edition (lock acquired and dropped)
    /// 2. Send CAS request to backend (no lock held during await)
    /// 3. Update cache with new state (lock acquired and dropped)
    pub async fn replace(
        &mut self,
        content: Option<Backend::Value>,
        target: &Backend,
    ) -> Result<(), Backend::Error>
    where
        Backend::Value: Clone,
    {
        // Step 1: Read current edition (lock acquired and immediately dropped)
        let current_edition = {
            let entry = self.state.read();
            entry.edition.clone()
        }; // Lock released here, before await

        // Step 2: Perform CAS operation (no lock held during await)
        let new_edition = target
            .replace(&self.address, current_edition.as_ref(), content.clone())
            .await?;

        // Step 3: Update cache (lock acquired and immediately dropped)
        {
            let mut entry = self.state.write();
            entry.value = content;
            entry.edition = new_edition;
        } // Lock released here

        Ok(())
    }

    /// Replaces content with computed value and retry logic.
    ///
    /// Calls `f` with current cached value to compute new value, then attempts CAS.
    /// On CAS conflict, reloads and retries up to `max_retries` times.
    pub async fn replace_with<F>(
        &mut self,
        f: F,
        max_retries: usize,
        target: &Backend,
    ) -> Result<(), Backend::Error>
    where
        F: Fn(&Option<Backend::Value>) -> Option<Backend::Value> + ConditionalSend,
        Backend::Value: Clone,
    {
        for attempt in 0..max_retries {
            // Read current value and compute new value
            let new_value = {
                let entry = self.state.read();
                f(&entry.value)
            };

            // Try CAS
            match self.replace(new_value, target).await {
                Ok(_) => return Ok(()),
                Err(_) if attempt + 1 < max_retries => {
                    // Reload and retry
                    self.reload(target).await?;
                }
                Err(e) => return Err(e),
            }
        }

        // One last attempt
        let new_value = {
            let entry = self.state.read();
            f(&entry.value)
        };
        self.replace(new_value, target).await
    }

    /// Returns a reference to the key this asset is positioned at
    pub fn address(&self) -> &Backend::Address {
        &self.address
    }
}

/// A typed wrapper around backend storage that caches decoded values.
///
/// `TypedTransactionalMemory` provides transparent serialization/deserialization of values
/// while maintaining Compare-And-Swap (CAS) semantics. It caches the *decoded* value of type T,
/// so `read()` is synchronous and returns the cached value without decoding.
/// Decoding only happens during `open()` and `reload()`, and encoding only happens during `replace()`.
///
/// # Type Parameters
///
/// - `T`: The type of values to store (must implement `Serialize` and `DeserializeOwned`)
/// - `Backend`: The storage backend implementing `TransactionalMemoryBackend`
/// - `Codec`: The encoder/decoder for serialization
///
/// # Key Features
///
/// - **Cached Decoded Values**: Decoding happens only during open() and reload()
/// - **Synchronous Reads**: read() is sync and returns the cached decoded value
/// - **CAS Semantics**: Maintains edition tracking and Compare-And-Swap guarantees
/// - **Type Safety**: Enforces type safety at compile time
/// - **Shared State**: Multiple clones share the same cached decoded state
pub struct TypedTransactionalMemory<T, Backend, Codec>
where
    Backend: TransactionalMemoryBackend,
{
    /// The address (key) this cursor points to
    pub address: Backend::Address,
    /// The cached decoded state (value + edition) shared across clones
    pub state: Arc<SharedCell<TypedState<T, Backend::Edition>>>,
    /// The codec for encoding/decoding
    pub codec: Codec,
}

impl<T, Backend, Codec> std::fmt::Debug for TypedTransactionalMemory<T, Backend, Codec>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: std::fmt::Debug,
    Codec: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedTransactionalMemory")
            .field("address", &self.address)
            .field("state", &"<cached>")
            .field("codec", &self.codec)
            .finish()
    }
}

impl<T, Backend, Codec> Clone for TypedTransactionalMemory<T, Backend, Codec>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Clone,
    Codec: Clone,
{
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            state: Arc::clone(&self.state),
            codec: self.codec.clone(),
        }
    }
}

impl<T, Backend, Codec> TypedTransactionalMemory<T, Backend, Codec>
where
    T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone,
    Backend: TransactionalMemoryBackend<Value = Vec<u8>, Error = DialogStorageError>,
    Backend::Address: Clone,
    Backend::Edition: Clone,
    Codec: Encoder + ConditionalSync + Clone,
    Codec::Bytes: AsRef<[u8]>,
    Codec::Error: std::fmt::Display,
{
    /// Opens a typed transactional memory at the given address.
    /// Loads and decodes the value once during open.
    pub async fn open(
        address: Backend::Address,
        backend: &Backend,
        codec: Codec,
    ) -> Result<Self, DialogStorageError> {
        // Fetch from backend
        let (value, edition) = if let Some((bytes, edition)) = backend.resolve(&address).await? {
            // Decode the bytes
            let decoded: T = codec
                .decode(bytes.as_ref())
                .await
                .map_err(|e| DialogStorageError::DecodeFailed(e.to_string()))?;
            (Some(decoded), Some(edition))
        } else {
            (None, None)
        };

        Ok(Self {
            address,
            state: Arc::new(SharedCell::new(TypedState { value, edition })),
            codec,
        })
    }

    /// Creates a new typed transactional memory with the given state.
    pub fn new(
        address: Backend::Address,
        state: TypedState<T, Backend::Edition>,
        codec: Codec,
    ) -> Self {
        Self {
            address,
            state: Arc::new(SharedCell::new(state)),
            codec,
        }
    }

    /// Read the cached decoded value (synchronous).
    pub fn read(&self) -> Option<T> {
        let entry = self.state.read();
        entry.value.clone()
    }

    /// Read the cached decoded value with a callback.
    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Option<T>) -> R,
    {
        let entry = self.state.read();
        f(&entry.value)
    }

    /// Get the current decoded value (backwards compatibility alias for read).
    pub fn content(&self) -> Option<T> {
        self.read()
    }

    /// Reloads content from storage, decoding it once.
    pub async fn reload(&mut self, backend: &Backend) -> Result<(), DialogStorageError> {
        let (value, edition) =
            if let Some((bytes, edition)) = backend.resolve(&self.address).await? {
                let decoded: T = self
                    .codec
                    .decode(bytes.as_ref())
                    .await
                    .map_err(|e| DialogStorageError::DecodeFailed(e.to_string()))?;
                (Some(decoded), Some(edition))
            } else {
                (None, None)
            };

        *self.state.write() = TypedState { value, edition };
        Ok(())
    }

    /// Replace the value with CAS semantics, encoding it once before storage.
    pub async fn replace(
        &mut self,
        value: Option<T>,
        backend: &Backend,
    ) -> Result<(), DialogStorageError> {
        // Encode the value
        let encoded = if let Some(ref v) = value {
            let (_, bytes) = self
                .codec
                .encode(v)
                .await
                .map_err(|e| DialogStorageError::EncodeFailed(e.to_string()))?;
            Some(bytes.as_ref().to_vec())
        } else {
            None
        };

        // Get current edition
        let current_edition = {
            let entry = self.state.read();
            entry.edition.clone()
        };

        // Perform CAS
        let new_edition = backend
            .replace(&self.address, current_edition.as_ref(), encoded)
            .await?;

        // Update cache with decoded value
        *self.state.write() = TypedState {
            value: value.clone(),
            edition: new_edition,
        };

        Ok(())
    }

    /// Replaces content with computed value and retry logic.
    pub async fn replace_with<F>(
        &mut self,
        f: F,
        max_retries: usize,
        backend: &Backend,
    ) -> Result<(), DialogStorageError>
    where
        F: Fn(&Option<T>) -> Option<T> + ConditionalSend,
    {
        for attempt in 0..max_retries {
            let new_value = self.read_with(|current| f(current));

            match self.replace(new_value, backend).await {
                Ok(_) => return Ok(()),
                Err(_) if attempt + 1 < max_retries => {
                    self.reload(backend).await?;
                }
                Err(e) => return Err(e),
            }
        }

        // One last attempt
        let new_value = self.read_with(|current| f(current));
        self.replace(new_value, backend).await
    }

    /// Returns a reference to the address this memory is positioned at.
    pub fn address(&self) -> &Backend::Address {
        &self.address
    }
}
