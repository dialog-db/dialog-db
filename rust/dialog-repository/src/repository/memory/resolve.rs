//! Resolve command for fetching a cell value.

use dialog_capability::{Capability, Fork, Provider, Site, SiteAddress};
use dialog_common::ConditionalSync;
use dialog_effects::memory;
use dialog_storage::Encoder;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;

use super::cell::Cache;
use crate::ResolveError;

/// Command to resolve (fetch) a cell value.
///
/// Created by [`Cell::resolve`](super::Cell::resolve). Invoke `.perform(&env)`
/// to run against the local environment, or `.fork(&address)` to build a
/// [`ForkResolve`] that runs against a remote site.
pub struct Resolve<T, Codec: Clone> {
    /// Capability chain resolving a cell's latest edition.
    pub effect: Capability<memory::Resolve>,
    /// Cache to populate with the resolved edition.
    pub cache: Cache<T, Codec>,
}

impl<T, Codec> Resolve<T, Codec>
where
    T: DeserializeOwned + Clone + ConditionalSync,
    Codec: Encoder + Clone,
{
    /// Perform the resolve against the local environment.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), ResolveError>
    where
        Env: Provider<memory::Resolve>,
    {
        let edition = self.effect.perform(env).await?;
        self.cache.apply(edition).await
    }

    /// Build a remote-site version of this command.
    ///
    /// The returned [`ForkResolve`] can be executed against an environment
    /// that knows how to reach the remote site identified by `address`.
    pub fn fork<A: SiteAddress>(self, address: &A) -> ForkResolve<T, A::Site, Codec> {
        ForkResolve {
            fork: self.effect.fork(address),
            cache: self.cache,
        }
    }
}

/// A [`Resolve`] retargeted at a remote site. Execute via `.perform(&env)`
/// against an environment configured with the matching site provider.
pub struct ForkResolve<T, S: Site, Codec: Clone> {
    fork: Fork<S, memory::Resolve>,
    cache: Cache<T, Codec>,
}

impl<T, S, Codec> ForkResolve<T, S, Codec>
where
    T: DeserializeOwned + Clone + ConditionalSync,
    S: Site,
    Codec: Encoder + Clone,
{
    /// Perform the resolve against the remote site.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), ResolveError>
    where
        Env: Provider<Fork<S, memory::Resolve>> + ConditionalSync,
    {
        let edition = self.fork.perform(env).await?;
        self.cache.apply(edition).await
    }
}

/// Command to resolve a retained cell.
pub struct RetainResolve<'a, T, Codec: Clone> {
    /// Inner resolve command.
    pub inner: Resolve<T, Codec>,
    /// Sticky cache updated when the resolved edition is non-empty.
    pub value: &'a RwLock<T>,
}

impl<T, Codec> RetainResolve<'_, T, Codec>
where
    T: DeserializeOwned + Clone + ConditionalSync,
    Codec: Encoder + Clone,
{
    /// Perform the resolve against the local environment. On success the
    /// sticky cache is updated with the resolved value; an empty remote
    /// leaves the retained value untouched.
    pub async fn perform(
        self,
        env: &(impl Provider<memory::Resolve> + ConditionalSync),
    ) -> Result<(), ResolveError> {
        let cache = self.inner.cache.clone();
        self.inner.perform(env).await?;
        if let Some(value) = cache.content() {
            *self.value.write() = value;
        }
        Ok(())
    }
}
