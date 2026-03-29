//! Storage — DID-routed effect dispatcher.
//!
//! Routes capability effects to the right [`Store`] by looking up the
//! subject DID. Populated via `.mount()` as profiles and repositories
//! are opened.

use dialog_capability::storage::StorageError;
use dialog_capability::{Capability, Effect, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_storage::provider::Store;
use dialog_varsig::Did;
use parking_lot::RwLock;
use std::collections::HashMap;

/// DID-routed storage dispatcher.
///
/// Maps subject DIDs to [`Store`] instances. When a capability effect
/// arrives, the subject DID is extracted and the corresponding store
/// handles the operation.
pub struct Storage {
    mounts: RwLock<HashMap<Did, Store>>,
}

impl Storage {
    /// Create an empty storage dispatcher.
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

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

/// Produce an error when no mount is registered for a DID.
trait FromUnmounted {
    fn unmounted(did: &Did) -> Self;
}

impl<T, E: From<StorageError>> FromUnmounted for Result<T, E> {
    fn unmounted(did: &Did) -> Self {
        Err(StorageError::Storage(format!("no mount for {did}")).into())
    }
}

/// Routes any capability effect to the [`Store`] registered for the
/// subject DID in the capability chain.
///
/// When an effect arrives (e.g. `archive::Get` with subject `did:key:zRepo`):
/// 1. The subject DID is extracted from the capability.
/// 2. The DID is looked up in the mount table (cloned out of the lock).
/// 3. If found, the effect is forwarded to that [`Store`].
/// 4. If not found, a `StorageError` is returned (converted to the
///    effect's error type via `From<StorageError>`).
///
/// The lock is held only during the lookup — not across the async
/// execution — so concurrent effects on different DIDs don't block.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx> Provider<Fx> for Storage
where
    Fx: Effect + ConditionalSend + 'static,
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
