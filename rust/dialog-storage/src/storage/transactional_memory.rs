use super::backend::TransactionalMemoryBackend;
use crate::{CborEncoder, DialogStorageError, Encoder};
use dialog_common::{ConditionalSend, ConditionalSync, SharedCell};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

/// State for transactional memory - caches decoded values.
#[derive(Debug, Clone)]
pub struct State<T, Edition> {
    /// The cached decoded value
    pub value: Option<T>,
    /// The edition/version identifier for CAS operations
    pub edition: Option<Edition>,
}

impl<T, Edition> Default for State<T, Edition> {
    fn default() -> Self {
        Self {
            value: None,
            edition: None,
        }
    }
}

/// Policy controlling retry behavior for transactional operations.
///
/// Determines how many times to retry a failed Compare-And-Swap (CAS) operation
/// after reloading fresh state from storage.
#[derive(Debug, Clone)]
pub enum UpdatePolicy {
    /// Retry up to the specified number of times after the initial optimistic attempt.
    ///
    /// With `MaxRetries(n)`, the operation will:
    /// 1. Try once optimistically with the current cached state
    /// 2. On failure: reload from storage and retry up to `n` more times
    ///
    /// Example: `MaxRetries(1)` means 1 optimistic attempt + 1 reload/retry = 2 total attempts
    MaxRetries(usize),
}
impl Default for UpdatePolicy {
    fn default() -> Self {
        UpdatePolicy::MaxRetries(1)
    }
}

/// A typed wrapper around backend storage that caches decoded values.
///
/// `TransactionalMemory` provides transparent serialization/deserialization of values
/// while maintaining Compare-And-Swap (CAS) semantics. It caches the *decoded* value of type T,
/// so `read()` is synchronous and returns the cached value without decoding.
/// Decoding only happens during `open()` and `reload()`, and encoding only happens during `replace()`.
///
/// # Type Parameters
///
/// - `T`: The type of values to store (must implement `Serialize` and `DeserializeOwned`)
/// - `Backend`: The storage backend implementing `TransactionalMemoryBackend`
/// - `Codec`: The encoder/decoder for serialization (defaults to `CborEncoder`)
///
/// # Key Features
///
/// - **Cached Decoded Values**: Decoding happens only during open() and reload()
/// - **Synchronous Reads**: read() is sync and returns the cached decoded value
/// - **CAS Semantics**: Maintains edition tracking and Compare-And-Swap guarantees
/// - **Type Safety**: Enforces type safety at compile time
/// - **Shared State**: Multiple clones share the same cached decoded state
pub struct TransactionalMemory<T, Backend, Codec = CborEncoder>
where
    Backend: TransactionalMemoryBackend,
{
    /// The address (key) this cursor points to
    pub address: Backend::Address,
    /// The cached decoded state (value + edition) shared across clones
    pub state: Arc<SharedCell<State<T, Backend::Edition>>>,
    /// The codec for encoding/decoding
    pub codec: Codec,
    /// Policy for retry behavior during replace_with operations
    pub policy: UpdatePolicy,
}

impl<T, Backend, Codec> std::fmt::Debug for TransactionalMemory<T, Backend, Codec>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: std::fmt::Debug,
    Codec: std::fmt::Debug + Encoder,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionalMemory")
            .field("address", &self.address)
            .field("state", &"<cached>")
            .field("codec", &self.codec)
            .finish()
    }
}

impl<T, Backend, Codec> Clone for TransactionalMemory<T, Backend, Codec>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Clone,
    Codec: Clone + Encoder,
{
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            state: Arc::clone(&self.state),
            codec: self.codec.clone(),
            policy: self.policy.clone(),
        }
    }
}

impl<T, Backend, Codec> TransactionalMemory<T, Backend, Codec>
where
    T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone,
    Backend: TransactionalMemoryBackend<Value = Vec<u8>>,
    Backend::Error: Into<DialogStorageError>,
    Backend::Address: Clone,
    Backend::Edition: Clone,
    Codec: Encoder + ConditionalSync + Clone,
    Codec::Bytes: AsRef<[u8]>,
    Codec::Error: std::fmt::Display,
{
    /// Opens a transactional memory at the given address.
    /// Loads and decodes the value once during open.
    pub async fn open(
        address: Backend::Address,
        backend: &Backend,
        codec: Codec,
    ) -> Result<Self, DialogStorageError> {
        // Fetch from backend
        let (value, edition) = if let Some((bytes, edition)) = backend.resolve(&address).await.map_err(|e| e.into())? {
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
            state: Arc::new(SharedCell::new(State { value, edition })),
            codec,
            policy: UpdatePolicy::default(),
        })
    }

    /// Creates a new transactional memory with the given state.
    pub fn new(address: Backend::Address, state: State<T, Backend::Edition>, codec: Codec) -> Self {
        Self {
            address,
            state: Arc::new(SharedCell::new(state)),
            codec,
            policy: UpdatePolicy::default(),
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
            if let Some((bytes, edition)) = backend.resolve(&self.address).await.map_err(|e| e.into())? {
                let decoded: T = self
                    .codec
                    .decode(bytes.as_ref())
                    .await
                    .map_err(|e| DialogStorageError::DecodeFailed(e.to_string()))?;
                (Some(decoded), Some(edition))
            } else {
                (None, None)
            };

        *self.state.write() = State { value, edition };
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
            .await
            .map_err(|e| e.into())?;

        // Update cache with decoded value
        *self.state.write() = State {
            value: value.clone(),
            edition: new_edition,
        };

        Ok(())
    }

    /// Replaces content with computed value and retry logic.
    ///
    /// Calls `f` with current cached value to compute new value, then attempts CAS.
    /// On CAS conflict, reloads and retries according to the policy.
    ///
    /// MaxRetries(n) means: try once optimistically, then reload and retry up to n more times.
    pub async fn replace_with<F>(
        &mut self,
        f: F,
        backend: &Backend,
    ) -> Result<(), DialogStorageError>
    where
        F: Fn(&Option<T>) -> Option<T> + ConditionalSend,
    {
        let UpdatePolicy::MaxRetries(mut n) = self.policy;

        loop {
            // Try CAS with current cached state
            let input = f(&self.state.read().value);
            match self.replace(input, backend).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if n > 0 {
                        n -= 1;
                        self.reload(backend).await?;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Returns a reference to the address this memory is positioned at.
    pub fn address(&self) -> &Backend::Address {
        &self.address
    }
}
