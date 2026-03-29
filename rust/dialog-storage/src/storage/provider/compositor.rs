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
#[derive(Clone, Debug, dialog_capability::Provider)]
#[provide(
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
    credential::Save
)]
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
}

impl Default for Compositor {
    fn default() -> Self {
        Self::new()
    }
}

use dialog_effects::{archive, credential, memory, storage};

/// Produce an error when no mount is registered for a DID.
trait FromUnmounted {
    fn unmounted(did: &Did) -> Self;
}

impl<T, E: From<StorageError>> FromUnmounted for Result<T, E> {
    fn unmounted(did: &Did) -> Self {
        Err(StorageError::Storage(format!("no mount for {did}")).into())
    }
}

/// Blanket Provider impl for Compositor — routes by subject DID to the mounted Store.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx> Provider<Fx> for Compositor
where
    Fx: dialog_capability::Effect + ConditionalSend + 'static,
    Fx::Output: FromUnmounted,
    Capability<Fx>: ConditionalSend,
    Store: Provider<Fx> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Fx>) -> Fx::Output {
        let did = input.subject().clone();
        let mounted = {
            let mounts = self.mounts.read();
            mounts.get(&did).cloned()
        };

        match mounted {
            Some(store) => store.execute(input).await,
            None => Fx::Output::unmounted(&did),
        }
    }
}
