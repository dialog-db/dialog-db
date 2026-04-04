//! Storage — Address-based Load/Save with DID-routed effect dispatch.
//!
//! [`Storage`] handles credential and byte Load/Save by unwrapping the
//! [`Address`] enum and forwarding to the matching [`Store`] variant.
//!
//! [`Stores`] handles runtime effects (archive, memory, storage) by
//! routing on the subject DID.

use dialog_capability::storage::{Location, StorageError};
use dialog_capability::{Capability, Effect, Policy, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_storage::provider::{Address, Store};
use dialog_varsig::Did;
use parking_lot::RwLock;
use std::collections::HashMap;

use dialog_capability::storage::{Load, Save};
use dialog_credentials::credential::Credential;

use std::sync::Arc;

/// DID-routed store table.
///
/// Blanket `Provider<Fx>` impl routes any effect to the [`Store`]
/// registered for the subject DID in the capability chain.
///
/// Cheaply cloneable — shares the underlying table via `Arc`.
#[derive(Clone)]
pub struct Stores(Arc<RwLock<HashMap<Did, Store>>>);

impl Stores {
    /// Create an empty store table.
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    /// Register a DID → Store mapping.
    pub fn mount(&self, did: Did, store: Store) {
        self.0.write().insert(did, store);
    }

    /// Look up the store for a DID.
    pub fn lookup(&self, did: &Did) -> Option<Store> {
        self.0.read().get(did).cloned()
    }

    /// Whether a DID is mounted.
    pub fn contains(&self, did: &Did) -> bool {
        self.0.read().contains_key(did)
    }
}

impl Default for Stores {
    fn default() -> Self {
        Self::new()
    }
}

trait FromUnmounted {
    fn unmounted(did: &Did) -> Self;
}

impl<T, E: From<StorageError>> FromUnmounted for Result<T, E> {
    fn unmounted(did: &Did) -> Self {
        Err(StorageError::Storage(format!("no mount for {did}")).into())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx> Provider<Fx> for Stores
where
    Fx: Effect + ConditionalSend + 'static,
    Fx::Output: FromUnmounted,
    Capability<Fx>: ConditionalSend,
    Store: Provider<Fx> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Fx>) -> Fx::Output {
        let did = input.subject().clone();
        match self.lookup(&did) {
            Some(store) => store.execute(input).await,
            None => Fx::Output::unmounted(&did),
        }
    }
}

use dialog_effects::{archive, memory, storage as fx_storage};


/// Address-dispatched storage with DID-routed effect table.
#[derive(Clone, Provider)]
pub struct Storage {
    #[provide(
        archive::Get,
        archive::Put,
        memory::Resolve,
        memory::Publish,
        memory::Retract,
        fx_storage::Get,
        fx_storage::Set,
        fx_storage::Delete,
        fx_storage::List
    )]
    stores: Stores,
}

use dialog_capability::storage::Storage as CapStorage;

/// Extension trait for resolving sub-paths on location capabilities.
pub trait LocationExt {
    /// Resolve a sub-path under this location capability.
    ///
    /// Returns a new capability with the resolved address.
    /// Errors if the segment would escape the base address.
    fn resolve(&self, segment: &str) -> Result<Capability<Location<Address>>, StorageError>;
}

impl LocationExt for Capability<Location<Address>> {
    fn resolve(&self, segment: &str) -> Result<Capability<Location<Address>>, StorageError> {
        let address = Location::of(self).address();
        let resolved = address.resolve(segment)?;
        Ok(CapStorage::locate(resolved))
    }
}

impl Storage {
    /// Create an empty storage.
    pub fn new() -> Self {
        Self {
            stores: Stores::new(),
        }
    }

    /// Location capability for the platform profile directory with the given name.
    ///
    /// On native: `profile:///name` (resolves to platform data dir).
    /// On web: IndexedDb address.
    pub fn profile(name: &str) -> Capability<Location<Address>> {
        CapStorage::locate(Address::profile(name))
    }

    /// Location capability for a named storage space under the current directory.
    ///
    /// On native: `storage:///name` (resolves to cwd).
    /// On web: IndexedDb address.
    pub fn current(name: &str) -> Capability<Location<Address>> {
        CapStorage::locate(Address::current(name))
    }

    /// Location capability for a named temporary directory.
    pub fn temp(name: &str) -> Capability<Location<Address>> {
        CapStorage::locate(Address::temp(name))
    }

    /// Register a DID → Store mapping.
    pub fn mount(&self, did: Did, store: Store) {
        self.stores.0.write().insert(did, store);
    }

    /// Mount a DID at the given location.
    ///
    /// Creates a Store from the address and registers `did → store`
    /// so that future effects with this DID as subject are routed
    /// to the correct storage backend.
    pub fn mount_at(
        &self,
        did: Did,
        location: &Capability<Location<Address>>,
    ) -> Result<(), StorageError> {
        let address = Location::of(location).address();
        let store = Store::mount(address)?;
        self.mount(did, store);
        Ok(())
    }

    /// The DID-routed store table for runtime effects.
    pub fn stores(&self) -> &Stores {
        &self.stores
    }

    /// Create a Storage backed by a temporary filesystem directory.
    ///
    /// Mounts a FileStore for `did:local:storage` at a unique temp path.
    /// Useful for tests.
    /// Create a Storage backed by a temporary filesystem directory.
    ///
    /// Mounts a FileStore for `did:local:storage` at a unique temp path.
    /// Useful for tests.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn temp_storage() -> Self {
        let address = Address::temp(&unique_id());
        let store = Store::mount(&address).expect("mount temp");
        let storage = Self::new();
        storage.mount(dialog_capability::did!("local:storage"), store);
        storage
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub fn temp_storage() -> Self {
        let address = Address::temp(&unique_id());
        let store = Store::mount(&address).expect("mount temp");
        let storage = Self::new();
        storage.mount(dialog_capability::did!("local:storage"), store);
        storage
    }
}

fn unique_id() -> String {
    use dialog_common::time;
    format!(
        "dialog-{}",
        time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

use dialog_capability::storage::Mount;

/// Mount effect — registers a DID → Store mapping in the store table.
///
/// The subject DID from the capability chain is mounted at the address
/// from the Location. Uses `Mount<(), Address>` (unit Resource) to
/// distinguish from provider-specific mounts that return a Store.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Mount<Address>> for Storage
where
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Mount<Address>>) -> Result<(), StorageError> {
        let did = input.subject().clone();
        let address = Location::of(&input).address();
        let store = Store::mount(address)?;
        self.stores.mount(did, store);
        Ok(())
    }
}

macro_rules! impl_addressed {
    ($content:ty) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl Provider<Load<$content, Address>> for Storage
        where
            Self: ConditionalSend + ConditionalSync,
        {
            async fn execute(
                &self,
                input: Capability<Load<$content, Address>>,
            ) -> Result<$content, StorageError> {
                let did = input.subject();
                let expected = dialog_capability::did!("local:storage");
                if *did != expected {
                    return Err(StorageError::Storage(format!(
                        "addressed Load requires subject did:local:storage, got {did}"
                    )));
                }
                let address = Location::of(&input).address().clone();
                let store = Store::mount(&address)?;

                match (store, address) {
                    #[cfg(not(target_arch = "wasm32"))]
                    (Store::FileSystem(fs), Address::FileSystem(addr)) => {
                        CapStorage::locate(addr)
                            .load::<$content>()
                            .perform(&fs)
                            .await
                    }
                    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                    (Store::IndexedDb(idb), Address::IndexedDb(addr)) => {
                        CapStorage::locate(addr)
                            .load::<$content>()
                            .perform(&idb)
                            .await
                    }
                    (Store::Volatile(v), Address::Volatile(addr)) => {
                        CapStorage::locate(addr)
                            .load::<$content>()
                            .perform(&v)
                            .await
                    }
                    _ => Err(StorageError::Storage("store/address mismatch".into())),
                }
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl Provider<Save<$content, Address>> for Storage
        where
            Self: ConditionalSend + ConditionalSync,
        {
            async fn execute(
                &self,
                input: Capability<Save<$content, Address>>,
            ) -> Result<(), StorageError> {
                let did = input.subject();
                let expected = dialog_capability::did!("local:storage");
                if *did != expected {
                    return Err(StorageError::Storage(format!(
                        "addressed Save requires subject did:local:storage, got {did}"
                    )));
                }
                let address = Location::of(&input).address().clone();
                let content = Save::<$content, Address>::of(&input).content.clone();
                let store = Store::mount(&address)?;

                match (store, address) {
                    #[cfg(not(target_arch = "wasm32"))]
                    (Store::FileSystem(fs), Address::FileSystem(addr)) => {
                        CapStorage::locate(addr).save(content).perform(&fs).await
                    }
                    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                    (Store::IndexedDb(idb), Address::IndexedDb(addr)) => {
                        CapStorage::locate(addr).save(content).perform(&idb).await
                    }
                    (Store::Volatile(v), Address::Volatile(addr)) => {
                        CapStorage::locate(addr).save(content).perform(&v).await
                    }
                    _ => Err(StorageError::Storage("store/address mismatch".into())),
                }
            }
        }
    };
}

impl_addressed!(Vec<u8>);
impl_addressed!(Credential);
