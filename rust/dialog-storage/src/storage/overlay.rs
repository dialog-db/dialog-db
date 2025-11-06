use super::{Resource, StorageBackend};
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use std::hash::Hash;

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

/// A resource that combines overlay and backend Resources, preferring overlay
/// content but writing to overlay only.
#[derive(Debug, Clone)]
pub struct OverlayResource<Backend, Overlay>
where
    Backend: StorageBackend,
    Overlay: StorageBackend<Key = Backend::Key, Value = Backend::Value>,
{
    content: Option<Backend::Value>,
    overlay_resource: Overlay::Resource,
    backend_resource: Backend::Resource,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend, Overlay> Resource for OverlayResource<Backend, Overlay>
where
    Backend: StorageBackend + ConditionalSync,
    Backend::Value: Clone + ConditionalSync,
    Backend::Error: From<Overlay::Error> + ConditionalSync,
    Overlay: StorageBackend<Key = Backend::Key, Value = Backend::Value> + ConditionalSync,
    Backend::Resource: ConditionalSync,
    Overlay::Resource: ConditionalSync,
{
    type Value = Backend::Value;
    type Error = Backend::Error;

    fn content(&self) -> &Option<Self::Value> {
        &self.content
    }

    fn into_content(self) -> Option<Self::Value> {
        self.content
    }

    async fn reload(&mut self) -> Result<Option<Self::Value>, Self::Error> {
        let prior = self.content.clone();

        // Reload both resources
        self.overlay_resource.reload().await?;
        self.backend_resource.reload().await?;

        // Prefer overlay content, fall back to backend
        self.content = if let Some(overlay_value) = self.overlay_resource.content() {
            Some(overlay_value.clone())
        } else {
            self.backend_resource.content().clone()
        };

        Ok(prior)
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        let prior = self.content.clone();

        // Write to overlay only
        self.overlay_resource.replace(value.clone()).await?;

        // Update our content to match
        self.content = value;

        Ok(prior)
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
    Backend::Resource: ConditionalSync,
    Overlay::Resource: ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Resource = OverlayResource<Backend, Overlay>;
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

    async fn open(&self, key: &Self::Key) -> Result<Self::Resource, Self::Error> {
        // Open both backend and overlay resources
        let backend_resource = self.backend.open(key).await?;
        let overlay_resource = self.overlay.open(key).await?;

        // Determine initial content: prefer overlay, fall back to backend
        let content = if let Some(overlay_value) = overlay_resource.content() {
            Some(overlay_value.clone())
        } else {
            backend_resource.content().clone()
        };

        Ok(OverlayResource {
            content,
            overlay_resource,
            backend_resource,
        })
    }
}
