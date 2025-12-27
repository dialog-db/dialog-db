//! Environment and capability-based resource acquisition.
//!
//! This module provides a composable way to acquire resources (storage, memory, etc.)
//! from different providers (filesystem, S3, in-memory, etc.).
//!
//! # Design Overview
//!
//! The system is built around the [`Acquire<P>`] trait which environments implement
//! for each parameter type they support. This enables:
//!
//! - **Type-based dispatch**: The parameter type determines which resource is returned
//! - **Composable environments**: Combine capabilities via trait bounds
//! - **Flexible resources**: Resources can be any type, not just storage backends
//!
//! # Example
//!
//! ```ignore
//! use dialog_artifacts::environment::*;
//!
//! // Define your parameter types
//! struct DbPath(String);
//! struct ApiEndpoint { url: String }
//!
//! // Environments implement Acquire<P> for each parameter type
//! struct MyEnv { db: Database, api: ApiClient }
//!
//! impl Acquire<DbPath> for MyEnv {
//!     type Resource = DbConnection;
//!     type Error = DbError;
//!     async fn acquire(&self, p: &DbPath) -> Result<DbConnection, DbError> { ... }
//! }
//!
//! impl Acquire<ApiEndpoint> for MyEnv {
//!     type Resource = ApiSession;
//!     type Error = ApiError;
//!     async fn acquire(&self, p: &ApiEndpoint) -> Result<ApiSession, ApiError> { ... }
//! }
//!
//! // Use trait bounds to require specific capabilities
//! async fn sync_data<E>(env: &E) -> Result<(), Error>
//! where
//!     E: Acquire<DbPath> + Acquire<ApiEndpoint>,
//! {
//!     let db = env.acquire(&DbPath("./data.db")).await?;
//!     let api = env.acquire(&ApiEndpoint { url: "...".into() }).await?;
//!     // ...
//! }
//! ```
//!
//! # Composing Environments
//!
//! Use trait bounds to express what capabilities you need:
//!
//! ```ignore
//! // A function that needs local and remote storage
//! async fn replicate<E, L, R>(env: &E, local: L, remote: R)
//! where
//!     E: Acquire<L> + Acquire<R>,
//!     <E as Acquire<L>>::Resource: Site,
//!     <E as Acquire<R>>::Resource: Site,
//! {
//!     let local_site = env.acquire(&local).await?;
//!     let remote_site = env.acquire(&remote).await?;
//!     // sync between sites...
//! }
//! ```

use async_trait::async_trait;
use dialog_storage::{DialogStorageError, MemoryStorageBackend, StorageBackend, TransactionalMemoryBackend};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// =============================================================================
// Core Traits
// =============================================================================

/// A capability that can acquire resources given a parameter.
///
/// Capabilities are stateful providers (like a database connection pool or
/// HTTP client) that can produce resources when given parameters.
///
/// The `Resource` type is intentionally unconstrained - it can be anything
/// from a simple storage backend to a complex site with multiple services.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Capability: Send + Sync + 'static {
    /// The parameter type used to acquire resources.
    type Parameter: Clone + Serialize + DeserializeOwned + Send + Sync + 'static;

    /// The resource type produced by this capability.
    /// This is intentionally unconstrained - it can be any type.
    type Resource: Send + Sync + 'static;

    /// Error type for acquisition failures.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Acquire a resource for the given parameter.
    async fn acquire(&self, parameter: &Self::Parameter) -> Result<Self::Resource, Self::Error>;
}

/// Trait for environments that can acquire resources for a given parameter type.
///
/// This is the key trait that enables the `env.acquire(param)` pattern.
/// Environments implement this for each parameter type they support.
///
/// # Example
///
/// ```ignore
/// impl Acquire<Local> for MyEnv {
///     type Resource = LocalSite;
///     type Error = LocalError;
///
///     async fn acquire(&self, param: &Local) -> Result<LocalSite, LocalError> {
///         self.local_capability.acquire(param).await
///     }
/// }
///
/// impl Acquire<Remote> for MyEnv {
///     type Resource = RemoteSite;
///     type Error = RemoteError;
///
///     async fn acquire(&self, param: &Remote) -> Result<RemoteSite, RemoteError> {
///         self.remote_capability.acquire(param).await
///     }
/// }
/// ```
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Acquire<P>: Send + Sync
where
    P: Send + Sync,
{
    /// The resource type returned for this parameter type.
    type Resource: Send + Sync + 'static;

    /// Error type for acquisition failures.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Acquire a resource using the given parameter.
    async fn acquire(&self, parameter: &P) -> Result<Self::Resource, Self::Error>;
}

// =============================================================================
// Site - A common resource pattern
// =============================================================================

/// A site provides access to storage backends.
///
/// This is a common pattern for resources that need both content-addressed
/// storage and transactional memory, but capabilities can return any type.
pub trait Site: Send + Sync + 'static {
    /// The transactional memory backend type.
    type Memory: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + Send
        + Sync;

    /// The content-addressed storage backend type.
    type Store: StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + Send
        + Sync;

    /// Get the transactional memory backend.
    fn memory(&self) -> Self::Memory;

    /// Get the content-addressed storage backend.
    fn store(&self) -> Self::Store;
}

/// A generic site implementation that wraps storage and memory backends.
///
/// This is useful when both storage and memory use the same or compatible
/// backend types.
#[derive(Clone)]
pub struct TheSite<Store, Memory = Store> {
    store: Store,
    memory: Memory,
}

impl<Store, Memory> TheSite<Store, Memory> {
    /// Create a new site with separate store and memory backends.
    pub fn new(store: Store, memory: Memory) -> Self {
        Self { store, memory }
    }
}

impl<Store: Clone> TheSite<Store, Store> {
    /// Create a new site where store and memory share the same backend.
    pub fn shared(backend: Store) -> Self {
        Self {
            store: backend.clone(),
            memory: backend,
        }
    }
}

impl<Store, Memory> Site for TheSite<Store, Memory>
where
    Store: StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + Send
        + Sync
        + 'static,
    Memory: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + Send
        + Sync
        + 'static,
{
    type Memory = Memory;
    type Store = Store;

    fn memory(&self) -> Self::Memory {
        self.memory.clone()
    }

    fn store(&self) -> Self::Store {
        self.store.clone()
    }
}

// =============================================================================
// Memory Capability (for testing)
// =============================================================================

/// In-memory storage capability for testing.
pub mod memory {
    use super::*;
    use serde::{Deserialize, Serialize};

    /// A parameter for in-memory storage, identified by namespace.
    #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct Mem {
        /// The namespace for this memory region.
        pub namespace: String,
    }

    impl Mem {
        /// Create a new memory parameter with the given namespace.
        pub fn new(namespace: impl Into<String>) -> Self {
            Self {
                namespace: namespace.into(),
            }
        }
    }

    /// Shared registry of memory backends, enabling cross-replica communication in tests.
    #[derive(Clone, Default)]
    pub struct MemoryCapability {
        sites: Arc<RwLock<HashMap<String, MemoryStorageBackend<Vec<u8>, Vec<u8>>>>>,
    }

    impl MemoryCapability {
        /// Create a new memory capability with an empty registry.
        pub fn new() -> Self {
            Self::default()
        }

        /// Create a memory capability that shares state with another.
        ///
        /// This is useful for testing scenarios where multiple replicas
        /// need to communicate through shared "remote" storage.
        pub fn shared(&self) -> Self {
            Self {
                sites: self.sites.clone(),
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Capability for MemoryCapability {
        type Parameter = Mem;
        type Resource = MemorySite;
        type Error = std::convert::Infallible;

        async fn acquire(&self, parameter: &Mem) -> Result<MemorySite, Self::Error> {
            let mut sites = self.sites.write().unwrap();
            let backend = sites
                .entry(parameter.namespace.clone())
                .or_insert_with(MemoryStorageBackend::default)
                .clone();

            Ok(MemorySite { backend })
        }
    }

    /// A site backed by in-memory storage.
    #[derive(Clone)]
    pub struct MemorySite {
        backend: MemoryStorageBackend<Vec<u8>, Vec<u8>>,
    }

    impl Site for MemorySite {
        type Memory = MemoryStorageBackend<Vec<u8>, Vec<u8>>;
        type Store = MemoryStorageBackend<Vec<u8>, Vec<u8>>;

        fn memory(&self) -> Self::Memory {
            self.backend.clone()
        }

        fn store(&self) -> Self::Store {
            self.backend.clone()
        }
    }

    /// A test environment that uses memory storage for both local and remote.
    ///
    /// This is useful for testing replica synchronization without real storage.
    #[derive(Clone)]
    pub struct TestEnv {
        /// The local memory capability.
        pub local: MemoryCapability,
        /// The remote memory capability (shared across replicas in tests).
        pub remote: MemoryCapability,
    }

    impl TestEnv {
        /// Create a new test environment with isolated local and shared remote storage.
        pub fn new(shared_remote: &MemoryCapability) -> Self {
            Self {
                local: MemoryCapability::new(),
                remote: shared_remote.shared(),
            }
        }

        /// Create a completely isolated test environment.
        pub fn isolated() -> Self {
            Self {
                local: MemoryCapability::new(),
                remote: MemoryCapability::new(),
            }
        }
    }

    /// Parameter for local storage in tests.
    #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct Local(pub String);

    impl Local {
        /// Create a new local parameter.
        pub fn new(namespace: impl Into<String>) -> Self {
            Self(namespace.into())
        }
    }

    /// Parameter for remote storage in tests.
    #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct Remote(pub String);

    impl Remote {
        /// Create a new remote parameter.
        pub fn new(namespace: impl Into<String>) -> Self {
            Self(namespace.into())
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Acquire<Local> for TestEnv {
        type Resource = MemorySite;
        type Error = std::convert::Infallible;

        async fn acquire(&self, parameter: &Local) -> Result<MemorySite, Self::Error> {
            self.local.acquire(&Mem::new(&parameter.0)).await
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Acquire<Remote> for TestEnv {
        type Resource = MemorySite;
        type Error = std::convert::Infallible;

        async fn acquire(&self, parameter: &Remote) -> Result<MemorySite, Self::Error> {
            self.remote.acquire(&Mem::new(&parameter.0)).await
        }
    }
}

// =============================================================================
// Re-exports
// =============================================================================

pub use memory::{Local, Mem, MemoryCapability, MemorySite, Remote, TestEnv};

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_acquire_local() {
        let env = TestEnv::isolated();

        // Acquire using the parameter type to determine the capability
        let site = env.acquire(&Local::new("test")).await.unwrap();

        // We got a site with memory and store backends
        let _memory = site.memory();
        let _store = site.store();
    }

    #[tokio::test]
    async fn test_acquire_remote() {
        let shared_remote = MemoryCapability::new();
        let env = TestEnv::new(&shared_remote);

        // Acquire remote storage
        let site = env.acquire(&Remote::new("test")).await.unwrap();

        let _memory = site.memory();
        let _store = site.store();
    }

    #[tokio::test]
    async fn test_shared_remote_across_envs() {
        let shared_remote = MemoryCapability::new();

        // Two environments sharing the same remote
        let env1 = TestEnv::new(&shared_remote);
        let env2 = TestEnv::new(&shared_remote);

        // Both acquire the same remote namespace - they share storage
        let site1 = env1.acquire(&Remote::new("shared")).await.unwrap();
        let site2 = env2.acquire(&Remote::new("shared")).await.unwrap();

        // Write through one, read through the other
        use dialog_storage::StorageBackend;

        let mut store1 = site1.store();
        store1
            .set(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();

        let store2 = site2.store();
        let value = store2.get(&b"key".to_vec()).await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_isolated_local_storage() {
        let shared_remote = MemoryCapability::new();

        let env1 = TestEnv::new(&shared_remote);
        let env2 = TestEnv::new(&shared_remote);

        // Local storage is isolated per environment
        let site1 = env1.acquire(&Local::new("data")).await.unwrap();
        let site2 = env2.acquire(&Local::new("data")).await.unwrap();

        use dialog_storage::StorageBackend;

        // Write to env1's local storage
        let mut store1 = site1.store();
        store1
            .set(b"key".to_vec(), b"env1-value".to_vec())
            .await
            .unwrap();

        // env2's local storage is isolated
        let store2 = site2.store();
        let value = store2.get(&b"key".to_vec()).await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_type_inference() {
        let env = TestEnv::isolated();

        // The type of `site` is inferred from the parameter type
        async fn use_local_site<E>(env: &E) -> Result<(), std::convert::Infallible>
        where
            E: Acquire<Local, Resource = MemorySite, Error = std::convert::Infallible>,
        {
            let _site = env.acquire(&Local::new("test")).await?;
            Ok(())
        }

        async fn use_remote_site<E>(env: &E) -> Result<(), std::convert::Infallible>
        where
            E: Acquire<Remote, Resource = MemorySite, Error = std::convert::Infallible>,
        {
            let _site = env.acquire(&Remote::new("test")).await?;
            Ok(())
        }

        use_local_site(&env).await.unwrap();
        use_remote_site(&env).await.unwrap();
    }

    #[tokio::test]
    async fn test_composable_environment_bounds() {
        // This test demonstrates that environments are composable via trait bounds.
        // You can require any combination of Acquire<P> implementations.

        // A function that requires both Local and Remote capabilities
        async fn sync<E, L, R>(
            env: &E,
            local_param: &L,
            remote_param: &R,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
        where
            E: Acquire<L> + Acquire<R>,
            L: Send + Sync,
            R: Send + Sync,
            <E as Acquire<L>>::Resource: Site,
            <E as Acquire<R>>::Resource: Site,
            <E as Acquire<L>>::Error: std::error::Error + Send + Sync + 'static,
            <E as Acquire<R>>::Error: std::error::Error + Send + Sync + 'static,
        {
            let local_site = env.acquire(local_param).await?;
            let remote_site = env.acquire(remote_param).await?;

            // Both are Sites, so we can use them uniformly
            let _local_store = local_site.store();
            let _remote_store = remote_site.store();

            Ok(())
        }

        let env = TestEnv::isolated();
        sync(&env, &Local::new("data"), &Remote::new("origin"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_generic_parameter_types() {
        // Environments can implement Acquire for custom parameter types.
        // This enables domain-specific addressing schemes.

        use serde::{Deserialize, Serialize};

        #[derive(Clone, Serialize, Deserialize)]
        struct BranchRef {
            site: String,
            branch: String,
        }

        // Create an environment that can acquire by BranchRef
        struct BranchEnv {
            capability: MemoryCapability,
        }

        #[async_trait]
        impl Acquire<BranchRef> for BranchEnv {
            type Resource = MemorySite;
            type Error = std::convert::Infallible;

            async fn acquire(&self, param: &BranchRef) -> Result<MemorySite, Self::Error> {
                // Combine site and branch into namespace
                let namespace = format!("{}/{}", param.site, param.branch);
                self.capability.acquire(&Mem::new(namespace)).await
            }
        }

        let env = BranchEnv {
            capability: MemoryCapability::new(),
        };

        let branch_ref = BranchRef {
            site: "alice".into(),
            branch: "main".into(),
        };

        let site = env.acquire(&branch_ref).await.unwrap();
        let _store = site.store();
    }
}

