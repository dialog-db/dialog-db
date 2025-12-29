//! Site abstraction for storage resources.
//!
//! A [`Site`] provides access to storage backends (store and memory).
//! Sites are acquired from addresses via the [`Capability`] trait.
//!
//! # Design
//!
//! - [`Capability<A>`] - Trait for types that can be constructed from address `A`
//! - [`CapabilityProvider<A, R>`] - Caching wrapper that provides `R` from address `A`
//! - [`Provider<A>`] - Trait for types that can provide resources (with caching)
//! - [`Site`] - Provides `store()` and `memory()` access to backends
//! - Tuple `(A, B)` implements `Provider` by delegating to its elements
//!
//! # Example
//!
//! ```ignore
//! use dialog_artifacts::site::*;
//! use dialog_artifacts::site::memory::MemorySite;
//!
//! // Define environment as tuple of capability providers
//! type TestEnv = (
//!     CapabilityProvider<Local, MemorySite>,
//!     CapabilityProvider<Remote, MemorySite>,
//! );
//!
//! let mut env: TestEnv = Default::default();
//!
//! // Acquire sites - type inferred from address
//! let local = env.acquire(&Local::repository("alice"))?;
//! let remote = env.acquire(&Remote::rest(config))?;
//!
//! // Use the site
//! let store = local.store();
//! let memory = local.memory();
//! ```

use dialog_storage::{DialogStorageError, StorageBackend, TransactionalMemoryBackend};
use std::collections::HashMap;
use std::hash::Hash;

pub mod memory;
pub mod rest;

// =============================================================================
// Core Traits
// =============================================================================

/// A site provides access to storage backends.
pub trait Site: Clone + Send + Sync + 'static {
    /// The content-addressed storage backend type.
    type Store: StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + Send
        + Sync;

    /// The transactional memory backend type.
    type Memory: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + Send
        + Sync;

    /// Get the content-addressed storage backend.
    fn store(&self) -> Self::Store;

    /// Get the transactional memory backend.
    fn memory(&self) -> Self::Memory;
}

/// Capability to construct a resource from an address.
///
/// Types implement this trait to describe how to construct themselves from an address.
/// This is a pure construction trait with no caching.
/// Use [`CapabilityProvider`] to wrap a `Capability` and add caching.
pub trait Capability<A>: Sized {
    /// Error type for acquisition failures.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Construct a new instance from the given address.
    fn acquire(address: &A) -> Result<Self, Self::Error>;
}

/// Provider trait for acquiring resources from addresses.
///
/// Types implement this trait to provide resources with potential caching.
/// Unlike [`Capability`], this takes `&mut self` allowing implementations
/// to maintain state (like caches).
pub trait Provider<A> {
    /// The resource type produced.
    type Resource;
    /// Error type for acquisition failures.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Acquire a resource from the given address.
    fn acquire(&mut self, address: &A) -> Result<Self::Resource, Self::Error>;
}

/// Trait alias for environments that can provide both local and remote sites.
pub trait Env:
    Provider<Local, Resource: Site> + Provider<Remote, Resource: Site>
{
}

impl<T> Env for T where
    T: Provider<Local> + Provider<Remote>,
    <T as Provider<Local>>::Resource: Site,
    <T as Provider<Remote>>::Resource: Site,
{
}

// =============================================================================
// CapabilityProvider - Caching Provider Wrapper
// =============================================================================

/// A caching provider that produces resources from addresses.
///
/// `CapabilityProvider<A, R>` wraps a type `R` that implements [`Capability<A>`]
/// and adds caching so that the same address always returns the same (cloned) resource.
#[derive(Debug)]
pub struct CapabilityProvider<A, R> {
    cache: HashMap<A, R>,
}

impl<A, R> Default for CapabilityProvider<A, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A, R> Clone for CapabilityProvider<A, R>
where
    A: Clone,
    R: Clone,
{
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
        }
    }
}

impl<A, R> CapabilityProvider<A, R> {
    /// Create a new empty provider.
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

impl<A, R> Provider<A> for CapabilityProvider<A, R>
where
    A: Hash + Eq + Clone,
    R: Capability<A> + Clone,
{
    type Resource = R;
    type Error = R::Error;

    fn acquire(&mut self, address: &A) -> Result<R, R::Error> {
        if let Some(resource) = self.cache.get(address) {
            return Ok(resource.clone());
        }
        let resource = R::acquire(address)?;
        self.cache.insert(address.clone(), resource.clone());
        Ok(resource)
    }
}

// =============================================================================
// Tuple implementations for environments
// =============================================================================

impl<A, B> Provider<Local> for (A, B)
where
    A: Provider<Local>,
{
    type Resource = A::Resource;
    type Error = A::Error;

    fn acquire(&mut self, address: &Local) -> Result<Self::Resource, Self::Error> {
        self.0.acquire(address)
    }
}

impl<A, B> Provider<Remote> for (A, B)
where
    B: Provider<Remote>,
{
    type Resource = B::Resource;
    type Error = B::Error;

    fn acquire(&mut self, address: &Remote) -> Result<Self::Resource, Self::Error> {
        self.1.acquire(address)
    }
}

// =============================================================================
// TheSite - Generic Site Implementation
// =============================================================================

/// A generic site implementation that wraps storage and memory backends.
#[derive(Clone, Debug)]
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

impl<Store, Memory> Default for TheSite<Store, Memory>
where
    Store: Default,
    Memory: Default,
{
    fn default() -> Self {
        Self {
            store: Store::default(),
            memory: Memory::default(),
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
    type Store = Store;
    type Memory = Memory;

    fn store(&self) -> Self::Store {
        self.store.clone()
    }

    fn memory(&self) -> Self::Memory {
        self.memory.clone()
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
    Rest(rest::RestAddress),
}

impl Remote {
    /// Create a new REST remote address.
    pub fn rest(address: rest::RestAddress) -> Self {
        Self::Rest(address)
    }
}

// =============================================================================
// Type Aliases for Common Configurations
// =============================================================================

/// Test environment using memory storage for both local and remote.
pub type TestEnv = (
    CapabilityProvider<Local, memory::MemorySite>,
    CapabilityProvider<Remote, memory::MemorySite>,
);

/// Production environment using memory for local and REST for remote.
pub type ProdEnv = (
    CapabilityProvider<Local, memory::MemorySite>,
    CapabilityProvider<Remote, rest::RestSite>,
);
