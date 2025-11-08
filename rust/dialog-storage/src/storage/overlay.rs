use super::StorageBackend;
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use std::hash::Hash;

/// Tagged edition that tracks which backend an edition came from
#[derive(Debug, Clone, PartialEq)]
pub enum OverlayEdition<B, O> {
    /// Edition from the base backend
    Backend(B),
    /// Edition from the overlay backend
    Overlay(O),
}

/// A [`StorageOverlay`] is a conjunction of two [`StorageBackend`]s: a "true"
/// backend, and (you guessed it) an overlay backend. All writes to storage are
/// written to the overlay. All reads first check the overlay, and then fall
/// back to the true backend.
///
/// This arrangement enables us to checkpoint persisted storage and aggregate
/// new writes to a dedicated separate storage.
#[derive(Clone)]
pub struct StorageOverlay<Backend, Overlay>
where
    Backend: StorageBackend,
    Backend::Key: Eq + Clone + Hash,
    Backend::Value: Clone,
    Backend::Error: From<Overlay::Error>,
    Overlay: StorageBackend<Key = Backend::Key, Value = Backend::Value>,
{
    backend: Backend,
    overlay: Overlay,
}

impl<Backend, Overlay> StorageOverlay<Backend, Overlay>
where
    Backend: StorageBackend,
    Backend::Key: Eq + Clone + Hash,
    Backend::Value: Clone,
    Backend::Error: From<Overlay::Error>,
    Overlay: StorageBackend<Key = Backend::Key, Value = Backend::Value>,
{
    /// Instantiate a new [`StorageOverlay`] pairing the provided `backend` and
    /// `overlay`.
    pub fn new(backend: Backend, overlay: Overlay) -> Self {
        Self { backend, overlay }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend, Overlay> StorageBackend for StorageOverlay<Backend, Overlay>
where
    Backend: StorageBackend + ConditionalSync,
    Backend::Key: Eq + Clone + Hash + ConditionalSync,
    Backend::Value: Clone + ConditionalSync,
    Backend::Error: From<Overlay::Error> + ConditionalSync,
    Overlay: StorageBackend<Key = Backend::Key, Value = Backend::Value> + ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Error = Backend::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        Ok(self.overlay.set(key, value).await?)
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        if let overlay_result @ Some(_) = self.overlay.get(key).await? {
            Ok(overlay_result)
        } else {
            self.backend.get(key).await
        }
    }
}
