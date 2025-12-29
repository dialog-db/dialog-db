//! Capability-based effects system.
//!
//! This module provides a command/effect pattern where operations are represented
//! as data (commands) that are executed by an environment with the required capabilities.
//!
//! # Design
//!
//! - [`Command`] - Trait for data types describing operations with known output/error types
//! - [`Capability<C>`] - Trait for environments that can execute a specific command
//! - [`Fx`] - Trait for composable effects
//!
//! # Example
//!
//! ```ignore
//! use dialog_artifacts::capability::*;
//!
//! // Build and perform commands
//! let value = Site::local("main")
//!     .memory()
//!     .get(b"branch/main")
//!     .perform(env.clone())
//!     .await?;
//!
//! // Compose effects - no lifetime issues because env is Clone
//! fn copy_value(from: String, to: String) -> impl Fx<Output = (), Error = CapabilityError> {
//!     Effect(move |env: TestEnv| async move {
//!         let value = Site::local(&from)
//!             .memory()
//!             .get(b"key")
//!             .perform(env.clone())
//!             .await?;
//!
//!         if let Some(v) = value {
//!             Site::local(&to)
//!                 .memory()
//!                 .set(b"key", v)
//!                 .perform(env)
//!                 .await?;
//!         }
//!         Ok(())
//!     })
//! }
//!
//! copy_value("alice".into(), "bob".into()).perform(env).await?;
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};

// =============================================================================
// Core Traits
// =============================================================================

/// A command describes an operation with known output and error types.
///
/// Commands are pure data - they don't execute anything themselves.
/// Execution happens when the command is performed against an environment
/// that has the required [`Capability`].
pub trait Command: Sized {
    /// The successful output type of this command.
    type Output;
    /// The error type for this command.
    type Error;

    /// Perform this command against an environment with the required capability.
    fn perform<E>(self, env: E) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        E: Capability<Self>,
    {
        env.execute(self)
    }
}

/// An environment's capability to execute a specific command.
///
/// Environments implement this trait for each command type they can execute.
/// The environment is taken by value (owned) to avoid lifetime issues with async.
/// Environments should be cheaply cloneable (e.g., wrap state in Arc).
pub trait Capability<C: Command>: Clone {
    /// Execute the command and return its result.
    fn execute(self, cmd: C) -> impl Future<Output = Result<C::Output, C::Error>>;
}

// =============================================================================
// Fx - Composable Effects
// =============================================================================

/// Trait for composable effects.
///
/// Effects are operations that can be performed against an environment.
/// Unlike [`Command`], effects can compose multiple commands together.
pub trait Fx: Sized {
    /// The environment type this effect requires.
    type Env: Clone;
    /// The successful output type.
    type Output;
    /// The error type.
    type Error;

    /// Perform this effect against the environment.
    fn perform(self, env: Self::Env) -> impl Future<Output = Result<Self::Output, Self::Error>>;
}

/// Wrapper for creating effects from closures.
///
/// Use `Effect(|env| async move { ... })` to create an effect from a closure.
///
/// # Example
///
/// ```ignore
/// fn copy_value(from: String, to: String) -> impl Fx<Env = TestEnv, Output = (), Error = CapabilityError> {
///     Effect(move |env: TestEnv| async move {
///         let value = Site::local(&from).memory().get(b"key").perform(env.clone()).await?;
///         if let Some(v) = value {
///             Site::local(&to).memory().set(b"key", v).perform(env).await?;
///         }
///         Ok(())
///     })
/// }
/// ```
pub struct Effect<E, O, Err, F>(pub F, std::marker::PhantomData<fn(E) -> Result<O, Err>>);

impl<E, O, Err, F> Effect<E, O, Err, F> {
    /// Create a new effect from a closure.
    pub fn new(f: F) -> Self {
        Effect(f, std::marker::PhantomData)
    }
}

impl<E, O, Err, F, Fut> Fx for Effect<E, O, Err, F>
where
    E: Clone,
    F: FnOnce(E) -> Fut,
    Fut: Future<Output = Result<O, Err>>,
{
    type Env = E;
    type Output = O;
    type Error = Err;

    fn perform(self, env: E) -> impl Future<Output = Result<O, Err>> {
        (self.0)(env)
    }
}

// =============================================================================
// Address Types
// =============================================================================

/// Local address - identifies local storage by repository path/DID.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Local {
    /// A repository identified by path or DID.
    Repository(String),
}

impl Local {
    /// Create a new repository address.
    pub fn repository(id: impl Into<String>) -> Self {
        Self::Repository(id.into())
    }
}

/// Remote address - identifies remote storage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Remote {
    /// REST API endpoint.
    Rest(RestAddress),
}

impl Remote {
    /// Create a new REST remote address.
    pub fn rest(address: RestAddress) -> Self {
        Self::Rest(address)
    }
}

/// Address for REST-backed storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestAddress {
    /// The REST configuration.
    pub config: dialog_storage::RestStorageConfig,
}

impl RestAddress {
    /// Create a new REST address.
    pub fn new(config: dialog_storage::RestStorageConfig) -> Self {
        Self { config }
    }
}

impl std::hash::Hash for RestAddress {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.config.endpoint.hash(state);
        self.config.bucket.hash(state);
        self.config.key_prefix.hash(state);
    }
}

// =============================================================================
// Site Builder
// =============================================================================

/// Builder for constructing commands targeting a specific site.
///
/// `Site` doesn't hold any storage - it's just a builder that produces
/// command values targeting the specified address.
#[derive(Clone, Debug)]
pub struct Site<A> {
    address: A,
}

impl Site<Local> {
    /// Create a site builder for a local repository.
    pub fn local(id: impl Into<String>) -> Self {
        Self {
            address: Local::repository(id),
        }
    }
}

impl Site<Remote> {
    /// Create a site builder for a remote REST endpoint.
    pub fn remote(address: RestAddress) -> Self {
        Self {
            address: Remote::rest(address),
        }
    }
}

impl<A: Clone> Site<A> {
    /// Get the memory operations builder for this site.
    pub fn memory(&self) -> MemoryOps<A> {
        MemoryOps {
            address: self.address.clone(),
        }
    }

    /// Get the store operations builder for this site.
    pub fn store(&self) -> StoreOps<A> {
        StoreOps {
            address: self.address.clone(),
        }
    }
}

// =============================================================================
// Memory Operations Builder
// =============================================================================

/// Builder for memory (transactional) storage commands.
#[derive(Clone, Debug)]
pub struct MemoryOps<A> {
    address: A,
}

impl<A: Clone> MemoryOps<A> {
    /// Build a get command.
    pub fn get(&self, key: impl Into<Vec<u8>>) -> MemoryGet<A> {
        MemoryGet {
            address: self.address.clone(),
            key: key.into(),
        }
    }

    /// Build a set command.
    pub fn set(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> MemorySet<A> {
        MemorySet {
            address: self.address.clone(),
            key: key.into(),
            value: value.into(),
        }
    }

    /// Build a resolve command (get current value for transactional update).
    pub fn resolve(&self, key: impl Into<Vec<u8>>) -> MemoryResolve<A> {
        MemoryResolve {
            address: self.address.clone(),
            key: key.into(),
        }
    }

    /// Build a replace command (compare-and-swap).
    pub fn replace(
        &self,
        key: impl Into<Vec<u8>>,
        old: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    ) -> MemoryReplace<A> {
        MemoryReplace {
            address: self.address.clone(),
            key: key.into(),
            old,
            new,
        }
    }
}

// =============================================================================
// Store Operations Builder
// =============================================================================

/// Builder for store (content-addressed) storage commands.
#[derive(Clone, Debug)]
pub struct StoreOps<A> {
    address: A,
}

impl<A: Clone> StoreOps<A> {
    /// Build a get command.
    pub fn get(&self, key: impl Into<Vec<u8>>) -> StoreGet<A> {
        StoreGet {
            address: self.address.clone(),
            key: key.into(),
        }
    }

    /// Build a set command.
    pub fn set(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> StoreSet<A> {
        StoreSet {
            address: self.address.clone(),
            key: key.into(),
            value: value.into(),
        }
    }
}

// =============================================================================
// Memory Commands
// =============================================================================

/// Command to get a value from memory storage.
#[derive(Clone, Debug)]
pub struct MemoryGet<A> {
    /// The site address.
    pub address: A,
    /// The key to get.
    pub key: Vec<u8>,
}

impl<A> Command for MemoryGet<A> {
    type Output = Option<Vec<u8>>;
    type Error = CapabilityError;
}

/// Command to set a value in memory storage.
#[derive(Clone, Debug)]
pub struct MemorySet<A> {
    /// The site address.
    pub address: A,
    /// The key to set.
    pub key: Vec<u8>,
    /// The value to set.
    pub value: Vec<u8>,
}

impl<A> Command for MemorySet<A> {
    type Output = ();
    type Error = CapabilityError;
}

/// Command to resolve (get) a value for transactional update.
#[derive(Clone, Debug)]
pub struct MemoryResolve<A> {
    /// The site address.
    pub address: A,
    /// The key to resolve.
    pub key: Vec<u8>,
}

impl<A> Command for MemoryResolve<A> {
    type Output = Option<Vec<u8>>;
    type Error = CapabilityError;
}

/// Command to replace a value atomically (compare-and-swap).
#[derive(Clone, Debug)]
pub struct MemoryReplace<A> {
    /// The site address.
    pub address: A,
    /// The key to replace.
    pub key: Vec<u8>,
    /// Expected old value (None means key should not exist).
    pub old: Option<Vec<u8>>,
    /// New value to set (None means delete).
    pub new: Option<Vec<u8>>,
}

impl<A> Command for MemoryReplace<A> {
    type Output = bool;
    type Error = CapabilityError;
}

// =============================================================================
// Store Commands
// =============================================================================

/// Command to get a value from content-addressed store.
#[derive(Clone, Debug)]
pub struct StoreGet<A> {
    /// The site address.
    pub address: A,
    /// The key (hash) to get.
    pub key: Vec<u8>,
}

impl<A> Command for StoreGet<A> {
    type Output = Option<Vec<u8>>;
    type Error = CapabilityError;
}

/// Command to set a value in content-addressed store.
#[derive(Clone, Debug)]
pub struct StoreSet<A> {
    /// The site address.
    pub address: A,
    /// The key (hash) to set.
    pub key: Vec<u8>,
    /// The value to set.
    pub value: Vec<u8>,
}

impl<A> Command for StoreSet<A> {
    type Output = ();
    type Error = CapabilityError;
}

// =============================================================================
// Errors
// =============================================================================

/// Error type for capability operations.
#[derive(Debug, Clone)]
pub enum CapabilityError {
    /// Storage operation failed.
    Storage(String),
    /// Address not found or not accessible.
    NotFound(String),
    /// Compare-and-swap conflict.
    Conflict(String),
}

impl std::fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CapabilityError::Storage(msg) => write!(f, "Storage error: {}", msg),
            CapabilityError::NotFound(msg) => write!(f, "Not found: {}", msg),
            CapabilityError::Conflict(msg) => write!(f, "Conflict: {}", msg),
        }
    }
}

impl std::error::Error for CapabilityError {}

// =============================================================================
// Env Trait - Convenience alias for common capability combinations
// =============================================================================

/// Trait alias for environments that provide full local and remote site capabilities.
pub trait Env:
    Clone
    + Capability<MemoryGet<Local>>
    + Capability<MemorySet<Local>>
    + Capability<MemoryResolve<Local>>
    + Capability<MemoryReplace<Local>>
    + Capability<StoreGet<Local>>
    + Capability<StoreSet<Local>>
    + Capability<MemoryGet<Remote>>
    + Capability<MemorySet<Remote>>
    + Capability<MemoryResolve<Remote>>
    + Capability<MemoryReplace<Remote>>
    + Capability<StoreGet<Remote>>
    + Capability<StoreSet<Remote>>
{
}

impl<T> Env for T where
    T: Clone
        + Capability<MemoryGet<Local>>
        + Capability<MemorySet<Local>>
        + Capability<MemoryResolve<Local>>
        + Capability<MemoryReplace<Local>>
        + Capability<StoreGet<Local>>
        + Capability<StoreSet<Local>>
        + Capability<MemoryGet<Remote>>
        + Capability<MemorySet<Remote>>
        + Capability<MemoryResolve<Remote>>
        + Capability<MemoryReplace<Remote>>
        + Capability<StoreGet<Remote>>
        + Capability<StoreSet<Remote>>
{
}

// =============================================================================
// Test Environment
// =============================================================================

/// A test environment with in-memory storage.
///
/// This environment is cheaply cloneable (uses Arc internally) and can be
/// used for testing capability-based code.
#[derive(Clone, Debug)]
pub struct TestEnv {
    local_memory: Arc<RwLock<HashMap<(Local, Vec<u8>), Vec<u8>>>>,
    local_store: Arc<RwLock<HashMap<(Local, Vec<u8>), Vec<u8>>>>,
    remote_memory: Arc<RwLock<HashMap<(Remote, Vec<u8>), Vec<u8>>>>,
    remote_store: Arc<RwLock<HashMap<(Remote, Vec<u8>), Vec<u8>>>>,
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl TestEnv {
    /// Create a new empty test environment.
    pub fn new() -> Self {
        Self {
            local_memory: Arc::new(RwLock::new(HashMap::new())),
            local_store: Arc::new(RwLock::new(HashMap::new())),
            remote_memory: Arc::new(RwLock::new(HashMap::new())),
            remote_store: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

// Local Memory capabilities
impl Capability<MemoryGet<Local>> for TestEnv {
    async fn execute(self, cmd: MemoryGet<Local>) -> Result<Option<Vec<u8>>, CapabilityError> {
        let guard = self.local_memory.read().unwrap();
        Ok(guard.get(&(cmd.address, cmd.key)).cloned())
    }
}

impl Capability<MemorySet<Local>> for TestEnv {
    async fn execute(self, cmd: MemorySet<Local>) -> Result<(), CapabilityError> {
        let mut guard = self.local_memory.write().unwrap();
        guard.insert((cmd.address, cmd.key), cmd.value);
        Ok(())
    }
}

impl Capability<MemoryResolve<Local>> for TestEnv {
    async fn execute(self, cmd: MemoryResolve<Local>) -> Result<Option<Vec<u8>>, CapabilityError> {
        let guard = self.local_memory.read().unwrap();
        Ok(guard.get(&(cmd.address, cmd.key)).cloned())
    }
}

impl Capability<MemoryReplace<Local>> for TestEnv {
    async fn execute(self, cmd: MemoryReplace<Local>) -> Result<bool, CapabilityError> {
        let mut guard = self.local_memory.write().unwrap();
        let key = (cmd.address, cmd.key);
        let current = guard.get(&key).cloned();
        if current == cmd.old {
            match cmd.new {
                Some(v) => {
                    guard.insert(key, v);
                }
                None => {
                    guard.remove(&key);
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

// Local Store capabilities
impl Capability<StoreGet<Local>> for TestEnv {
    async fn execute(self, cmd: StoreGet<Local>) -> Result<Option<Vec<u8>>, CapabilityError> {
        let guard = self.local_store.read().unwrap();
        Ok(guard.get(&(cmd.address, cmd.key)).cloned())
    }
}

impl Capability<StoreSet<Local>> for TestEnv {
    async fn execute(self, cmd: StoreSet<Local>) -> Result<(), CapabilityError> {
        let mut guard = self.local_store.write().unwrap();
        guard.insert((cmd.address, cmd.key), cmd.value);
        Ok(())
    }
}

// Remote Memory capabilities
impl Capability<MemoryGet<Remote>> for TestEnv {
    async fn execute(self, cmd: MemoryGet<Remote>) -> Result<Option<Vec<u8>>, CapabilityError> {
        let guard = self.remote_memory.read().unwrap();
        Ok(guard.get(&(cmd.address, cmd.key)).cloned())
    }
}

impl Capability<MemorySet<Remote>> for TestEnv {
    async fn execute(self, cmd: MemorySet<Remote>) -> Result<(), CapabilityError> {
        let mut guard = self.remote_memory.write().unwrap();
        guard.insert((cmd.address, cmd.key), cmd.value);
        Ok(())
    }
}

impl Capability<MemoryResolve<Remote>> for TestEnv {
    async fn execute(self, cmd: MemoryResolve<Remote>) -> Result<Option<Vec<u8>>, CapabilityError> {
        let guard = self.remote_memory.read().unwrap();
        Ok(guard.get(&(cmd.address, cmd.key)).cloned())
    }
}

impl Capability<MemoryReplace<Remote>> for TestEnv {
    async fn execute(self, cmd: MemoryReplace<Remote>) -> Result<bool, CapabilityError> {
        let mut guard = self.remote_memory.write().unwrap();
        let key = (cmd.address, cmd.key);
        let current = guard.get(&key).cloned();
        if current == cmd.old {
            match cmd.new {
                Some(v) => {
                    guard.insert(key, v);
                }
                None => {
                    guard.remove(&key);
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

// Remote Store capabilities
impl Capability<StoreGet<Remote>> for TestEnv {
    async fn execute(self, cmd: StoreGet<Remote>) -> Result<Option<Vec<u8>>, CapabilityError> {
        let guard = self.remote_store.read().unwrap();
        Ok(guard.get(&(cmd.address, cmd.key)).cloned())
    }
}

impl Capability<StoreSet<Remote>> for TestEnv {
    async fn execute(self, cmd: StoreSet<Remote>) -> Result<(), CapabilityError> {
        let mut guard = self.remote_store.write().unwrap();
        guard.insert((cmd.address, cmd.key), cmd.value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_memory_get_set() {
        let env = TestEnv::new();

        // Set a value
        Site::local("alice")
            .memory()
            .set(b"key", b"value")
            .perform(env.clone())
            .await
            .unwrap();

        // Get it back
        let value = Site::local("alice")
            .memory()
            .get(b"key")
            .perform(env.clone())
            .await
            .unwrap();

        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_local_store_get_set() {
        let env = TestEnv::new();

        Site::local("alice")
            .store()
            .set(b"hash123", b"content")
            .perform(env.clone())
            .await
            .unwrap();

        let value = Site::local("alice")
            .store()
            .get(b"hash123")
            .perform(env.clone())
            .await
            .unwrap();

        assert_eq!(value, Some(b"content".to_vec()));
    }

    #[tokio::test]
    async fn test_memory_replace() {
        let env = TestEnv::new();

        // Replace on non-existent key (old = None)
        let success = Site::local("alice")
            .memory()
            .replace(b"key", None, Some(b"value".to_vec()))
            .perform(env.clone())
            .await
            .unwrap();
        assert!(success);

        // Replace with wrong old value should fail
        let success = Site::local("alice")
            .memory()
            .replace(b"key", Some(b"wrong".to_vec()), Some(b"new".to_vec()))
            .perform(env.clone())
            .await
            .unwrap();
        assert!(!success);

        // Replace with correct old value should succeed
        let success = Site::local("alice")
            .memory()
            .replace(b"key", Some(b"value".to_vec()), Some(b"new".to_vec()))
            .perform(env.clone())
            .await
            .unwrap();
        assert!(success);

        let value = Site::local("alice")
            .memory()
            .get(b"key")
            .perform(env.clone())
            .await
            .unwrap();
        assert_eq!(value, Some(b"new".to_vec()));
    }

    #[tokio::test]
    async fn test_different_sites_isolated() {
        let env = TestEnv::new();

        Site::local("alice")
            .memory()
            .set(b"key", b"alice-value")
            .perform(env.clone())
            .await
            .unwrap();

        // Bob doesn't see Alice's data
        let value = Site::local("bob")
            .memory()
            .get(b"key")
            .perform(env.clone())
            .await
            .unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_effect_composition() {
        let env = TestEnv::new();

        // Set initial value
        Site::local("main")
            .memory()
            .set(b"source", b"data")
            .perform(env.clone())
            .await
            .unwrap();

        // Define a composed effect using Effect wrapper
        // Returns impl Fx which hides the complex closure type
        fn copy_value(
            from: String,
            to: String,
        ) -> impl Fx<Env = TestEnv, Output = (), Error = CapabilityError> {
            Effect::new(move |env: TestEnv| async move {
                let value = Site::local(&from)
                    .memory()
                    .get(b"key")
                    .perform(env.clone())
                    .await?;

                if let Some(v) = value {
                    Site::local(&to)
                        .memory()
                        .set(b"key", v)
                        .perform(env)
                        .await?;
                }
                Ok(())
            })
        }

        // First set a key in "source" site
        Site::local("source")
            .memory()
            .set(b"key", b"hello")
            .perform(env.clone())
            .await
            .unwrap();

        // Use the composed effect
        copy_value("source".into(), "dest".into())
            .perform(env.clone())
            .await
            .unwrap();

        // Verify it was copied
        let value = Site::local("dest")
            .memory()
            .get(b"key")
            .perform(env.clone())
            .await
            .unwrap();
        assert_eq!(value, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn test_effect_with_fx_trait() {
        let env = TestEnv::new();

        Site::local("main")
            .memory()
            .set(b"source", b"world")
            .perform(env.clone())
            .await
            .unwrap();

        // Use Fx trait for more flexibility
        fn append_suffix(
            key: Vec<u8>,
            suffix: Vec<u8>,
        ) -> impl Fx<Env = TestEnv, Output = (), Error = CapabilityError> {
            Effect::new(move |env: TestEnv| async move {
                let value = Site::local("main")
                    .memory()
                    .get(key.clone())
                    .perform(env.clone())
                    .await?;

                if let Some(mut v) = value {
                    v.extend_from_slice(&suffix);
                    Site::local("main")
                        .memory()
                        .set(key, v)
                        .perform(env)
                        .await?;
                }
                Ok(())
            })
        }

        append_suffix(b"source".to_vec(), b"!".to_vec())
            .perform(env.clone())
            .await
            .unwrap();

        let value = Site::local("main")
            .memory()
            .get(b"source")
            .perform(env.clone())
            .await
            .unwrap();
        assert_eq!(value, Some(b"world!".to_vec()));
    }
}
