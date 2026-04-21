//! Publish command for writing a cell value.

use dialog_capability::{Capability, Fork, Provider, SiteAddress};
use dialog_common::ConditionalSync;
use dialog_effects::memory;
use dialog_effects::memory::prelude::CellExt;
use dialog_storage::Encoder;
use parking_lot::RwLock;
use serde::Serialize;
use std::fmt::Debug;

use super::cell::Cache;
use crate::PublishError;

/// Command to publish a cell value.
///
/// Created by [`Cell::publish`](super::Cell::publish). Invoke `.perform(&env)`
/// to run against the local environment, or `.fork(&address)` to build a
/// [`ForkPublish`] that runs against a remote site.
pub struct Publish<T, Codec: Clone> {
    /// Capability chain targeting the cell to publish to.
    pub capability: Capability<memory::Cell>,
    /// Cached edition used for edition tracking and cache updates.
    pub cache: Cache<T, Codec>,
    /// Value to publish.
    pub content: T,
}

impl<T, Codec> Publish<T, Codec>
where
    T: Serialize + Clone + ConditionalSync + Debug,
    Codec: Encoder<Bytes = Vec<u8>> + Clone,
{
    /// Perform the publish against the local environment.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), PublishError>
    where
        Env: Provider<memory::Publish>,
    {
        let content = self.cache.encode(&self.content).await?;
        let when = self.cache.version();
        let version = self.capability.publish(content, when).perform(env).await?;
        self.cache.update(memory::Edition {
            content: self.content,
            version,
        });
        Ok(())
    }

    /// Build a remote-site version of this command.
    ///
    /// The returned [`ForkPublish`] can be executed against an environment
    /// that knows how to reach the remote site identified by `address`.
    pub fn fork<A: SiteAddress>(self, address: &A) -> ForkPublish<T, A, Codec> {
        ForkPublish {
            capability: self.capability,
            cache: self.cache,
            content: self.content,
            address: address.clone(),
        }
    }
}

/// A [`Publish`] retargeted at a remote site. Execute via `.perform(&env)`
/// against an environment configured with the matching site provider.
///
/// The remote's edition comes from the local cache, so call
/// `cell.resolve().fork(&addr).perform(&env)` first to sync the remote's
/// current edition if you do not already hold it.
pub struct ForkPublish<T, A: SiteAddress, Codec: Clone> {
    capability: Capability<memory::Cell>,
    cache: Cache<T, Codec>,
    content: T,
    address: A,
}

impl<T, A, Codec> ForkPublish<T, A, Codec>
where
    T: Serialize + Clone + ConditionalSync + Debug,
    A: SiteAddress,
    Codec: Encoder<Bytes = Vec<u8>> + Clone,
{
    /// Perform the publish against the remote site.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), PublishError>
    where
        Env: Provider<Fork<A::Site, memory::Publish>> + ConditionalSync,
    {
        let content = self.cache.encode(&self.content).await?;
        let when = self.cache.version();
        let version = self
            .capability
            .publish(content, when)
            .fork(&self.address)
            .perform(env)
            .await?;
        self.cache.update(memory::Edition {
            content: self.content,
            version,
        });
        Ok(())
    }
}

/// Command to publish to a retained cell.
pub struct RetainPublish<'a, T, Codec: Clone> {
    /// Inner publish command.
    pub inner: Publish<T, Codec>,
    /// Sticky cache updated on successful publish.
    pub sticky: &'a RwLock<T>,
    /// Value to publish and retain.
    pub value: T,
}

impl<T, Codec> RetainPublish<'_, T, Codec>
where
    T: Serialize + Clone + ConditionalSync + Debug,
    Codec: Encoder<Bytes = Vec<u8>> + Clone,
{
    /// Perform the publish against the local environment. On success the
    /// sticky cache is updated to reflect the new value.
    pub async fn perform(
        self,
        env: &(impl Provider<memory::Publish> + ConditionalSync),
    ) -> Result<(), PublishError> {
        self.inner.perform(env).await?;
        *self.sticky.write() = self.value;
        Ok(())
    }
}
