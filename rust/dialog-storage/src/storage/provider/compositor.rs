//! Storage compositor — routes effects by subject DID to mounted stores.
//!
//! [`Store`] is a platform-agnostic enum over concrete storage backends.
//! [`Compositor`] maps DIDs to `Store` instances and routes effects accordingly.

use dialog_capability::storage::StorageError;
use dialog_capability::{Capability, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_varsig::Did;
use parking_lot::RwLock;
use std::collections::HashMap;

use super::Volatile;

#[cfg(not(target_arch = "wasm32"))]
use super::FileStore;

/// A concrete storage backend.
///
/// Platform-gated: native gets `FileSystem` + `Volatile`,
/// web gets `IndexedDb` + `Volatile`.
#[derive(Clone, Debug)]
pub enum Store {
    /// Filesystem-backed store (native only).
    #[cfg(not(target_arch = "wasm32"))]
    FileSystem(FileStore),

    /// IndexedDB-backed store (web only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    IndexedDb(super::IndexedDb),

    /// In-memory volatile store.
    Volatile(Volatile),
}

/// Storage compositor — routes effects to the right store by subject DID.
///
/// Call `.mount()` to register a DID → Store mapping. All Provider impls
/// look up the subject DID and dispatch to the registered store.
pub struct Compositor {
    mounts: RwLock<HashMap<Did, Store>>,
}

impl Compositor {
    /// Create an empty compositor.
    pub fn new() -> Self {
        Self {
            mounts: RwLock::new(HashMap::new()),
        }
    }

    /// Register a DID → Store mapping.
    pub fn mount(&self, did: Did, store: Store) {
        self.mounts.write().insert(did, store);
    }

    /// Look up the store for a DID, cloning it out of the lock.
    fn lookup(&self, did: &Did) -> Result<Store, StorageError> {
        let mounts = self.mounts.read();
        mounts
            .get(did)
            .cloned()
            .ok_or_else(|| StorageError::Storage(format!("no mount for {did}")))
    }
}

impl Default for Compositor {
    fn default() -> Self {
        Self::new()
    }
}

use dialog_effects::{archive, credential, memory, storage};

/// Generate `Provider<Fx>` for `Store` by dispatching to the inner variant.
macro_rules! dispatch {
    ($($effect:ty),+ $(,)?) => {
        $(
            #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
            #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
            impl Provider<$effect> for Store
            where
                Self: ConditionalSend + ConditionalSync,
            {
                async fn execute(
                    &self,
                    input: Capability<$effect>,
                ) -> <$effect as dialog_capability::Effect>::Output {
                    match self {
                        #[cfg(not(target_arch = "wasm32"))]
                        Self::FileSystem(s) => Provider::<$effect>::execute(s, input).await,
                        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                        Self::IndexedDb(s) => Provider::<$effect>::execute(s, input).await,
                        Self::Volatile(s) => Provider::<$effect>::execute(s, input).await,
                    }
                }
            }
        )+
    };
}

dispatch!(
    archive::Get,
    archive::Put,
    memory::Resolve,
    memory::Publish,
    memory::Retract,
    storage::Get,
    storage::Set,
    storage::Delete,
    storage::List,
    credential::Load,
    credential::Save,
);

/// Route Compositor effects by looking up the subject DID and delegating to Store.
macro_rules! route {
    ($($effect:ty),+ $(,)?) => {
        $(
            #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
            #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
            impl Provider<$effect> for Compositor
            where
                Self: ConditionalSend + ConditionalSync,
            {
                async fn execute(
                    &self,
                    input: Capability<$effect>,
                ) -> <$effect as dialog_capability::Effect>::Output {
                    let did = input.subject().clone();
                    match self.lookup(&did) {
                        Ok(store) => Provider::<$effect>::execute(&store, input).await,
                        Err(e) => Err(e.into()),
                    }
                }
            }
        )+
    };
}

route!(
    archive::Get,
    archive::Put,
    memory::Resolve,
    memory::Publish,
    memory::Retract,
    storage::Get,
    storage::Set,
    storage::Delete,
    storage::List,
    credential::Load,
    credential::Save,
);
