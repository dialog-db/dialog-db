//! Resolve command for fetching a cell value.

use dialog_capability::fork::Fork;
use dialog_capability::site::{Site, SiteAddress};
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::memory;
use dialog_storage::Encoder;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;

use super::cell::Cache;
use crate::RepositoryError;

/// Command to resolve (fetch) a cell value.
///
/// Created by [`Cell::resolve`](super::Cell::resolve). Execute with
/// `.perform(&env)` for local or `.fork(&address).perform(&env)` for remote.
pub struct Resolve<T, Codec: Clone> {
    pub(super) effect: Capability<memory::Resolve>,
    pub(super) cache: Cache<T, Codec>,
}

impl<T, Codec> Resolve<T, Codec>
where
    T: DeserializeOwned + Clone + ConditionalSync,
    Codec: Encoder + Clone,
{
    /// Execute locally.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<memory::Resolve>,
    {
        let publication = self.effect.perform(env).await?;
        apply(&self.cache, publication).await
    }

    /// Fork to a remote site.
    pub fn fork<A: SiteAddress>(self, address: &A) -> ForkResolve<T, A::Site, Codec> {
        ForkResolve {
            fork: self.effect.fork(address),
            cache: self.cache,
        }
    }
}

/// Command to resolve a cell value from a remote site.
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
    /// Execute against a remote site.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<Fork<S, memory::Resolve>> + ConditionalSync,
    {
        let publication = self.fork.perform(env).await?;
        apply(&self.cache, publication).await
    }
}

/// Decode a publication and update the cache.
async fn apply<T, Codec>(
    cache: &Cache<T, Codec>,
    edition: Option<memory::Edition<Vec<u8>>>,
) -> Result<(), RepositoryError>
where
    T: DeserializeOwned + Clone + ConditionalSync,
    Codec: Encoder + Clone,
{
    match edition {
        None => cache.clear(),
        Some(pub_data) => {
            let value: T = cache.decode(&pub_data.content).await?;
            cache.update(value, pub_data.version.into_bytes());
        }
    }
    Ok(())
}

/// Command to resolve a retained cell.
pub struct RetainResolve<'a, T, Codec: Clone> {
    pub(super) inner: Resolve<T, Codec>,
    pub(super) value: &'a RwLock<T>,
}

impl<T, Codec> RetainResolve<'_, T, Codec>
where
    T: DeserializeOwned + Clone + ConditionalSync,
    Codec: Encoder + Clone,
{
    /// Execute locally.
    pub async fn perform(
        self,
        env: &(impl Provider<memory::Resolve> + ConditionalSync),
    ) -> Result<(), RepositoryError> {
        let cache = self.inner.cache.clone();
        self.inner.perform(env).await?;
        if let Some(value) = cache.get() {
            *self.value.write() = value;
        }
        Ok(())
    }
}
